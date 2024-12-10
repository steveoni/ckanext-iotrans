"""Utils functions for iotrans.py
"""

import os
import re
import sys
import csv
import json
import codecs
from fiona.crs import from_epsg
from fiona.transform import transform_geom
from zipfile import ZipFile
import xml.etree.cElementTree as ET
from fiona import Geometry
from typing import Dict, Any
from typing import Generator
from .to_file import EPSG, geometry_to_json


import ckan.plugins.toolkit as tk


def is_falsey(arg: Any) -> bool:
    return arg in ["false", "False", False]


def transform_epsg(source_epsg, target_epsg, geometry):
    """standardize processing when transforming epsg"""

    # if input is empty, return it as is
    if geometry in [None, "None"]:
        return None

    # if input is a string, make it a json object
    if isinstance(geometry, str):
        geometry = json.loads(geometry.replace("'", '"'))  # replace '' with ""
        assert "coordinates" in geometry.keys(), "No coordinates in geometry!"

    original_geometry_type = geometry["type"]
    if not geometry["type"].startswith("Multi"):
        geometry["type"] = "Multi" + geometry["type"]

    # 0,0 coords need not be transformed - only their brackets changed
    if geometry["coordinates"] in [[0, 0], [[0, 0]]]:
        geometry["coordinates"] = [[0, 0]]
        return geometry

    # null coords need not be transformed - only their brackets changed
    if geometry["coordinates"] in [[None, None], [[None, None]]]:
        geometry["coordinates"] = []
        return geometry

    # force to multigeometry
    coordinates = list(geometry.get("coordinates", None))
    if not original_geometry_type.startswith("Multi"):
        coordinates = list([list(coord) for coord in [coordinates]])
    geometry["coordinates"] = coordinates

    # if the source and target epsg dont match, consider transforming them
    if target_epsg != source_epsg:
        geometry = transform_geom(
            from_epsg(source_epsg),
            from_epsg(target_epsg),
            geometry,
        )

        # conversion can change round brackets to square brackets
        # this converts to round brackets to keep CSVs consistent
        if geometry["type"].startswith("Multi"):
            geometry["coordinates"] = json.loads(
                json.dumps(geometry["coordinates"]).replace("(", "[").replace(")", "]")
            )

    return geometry


def dump_generator(resource_id, fieldnames, context):
    """reads a CKAN datastore_search calls, returns a python generator"""
    # init some vars
    chunk = 20000
    i = 0

    while True:
        # get a chunk of records from datastore resource
        records = tk.get_action("datastore_search")(
            context,
            {
                "resource_id": resource_id,
                "limit": chunk,
                "offset": chunk * i,
            },
        )["records"]

        if len(records):
            for record in records:
                yield record
            i += 1
            continue

        else:
            break


def transform_epsg_generator(
    generator: Generator[Dict, None, None],
    source_epsg: EPSG,
    target_epsg: EPSG,
    geometry_column: str,
    jsonify: bool,
) -> Generator[Dict, None, None]:
    for row in generator:
        geometry = transform_epsg(source_epsg, target_epsg, row.get(geometry_column))
        if jsonify:
            geometry = geometry_to_json(geometry)
        yield {
            "type": "Feature",
            "properties": dict(row),
            "geometry": geometry,
        }


def dump_to_geospatial_generator(
    dump_filepath, fieldnames, target_format, source_epsg, target_epsg, col_map=None
):
    """reads a CKAN CSV dump, creates generator with converted CRS"""
    # TODO DRY: should be consolidated w/ dump_to_geospatial_generator

    # For each row in the dump ...
    with codecs.open(dump_filepath, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f, fieldnames=fieldnames)
        next(reader)
        for row in reader:

            # if the data contains a "geometry" column, we know its spatial
            geometry = row.pop("geometry")

            # if geometry not in ["None", None]:
            # shapefile column names need to be mapped from col_map
            if target_format == "shp":
                working_row = {}
                for key, value in row.items():
                    working_row[col_map[key]] = value
                row = working_row

            # if we need to transform the EPSG, we do it here
            geometry = transform_epsg(source_epsg, target_epsg, geometry)

            output = {
                "type": "Feature",
                "properties": dict(row),
                "geometry": geometry,
            }

            yield (output)


def transform_dump_epsg(dump_filepath, fieldnames, source_epsg, target_epsg):
    """generator yields dump rows with epsg reformatted/converted"""
    # TODO DRY: should be consolidated w/ dump_to_geospatial_generator

    # Open the dump CSV into a dictreader
    with codecs.open(dump_filepath, "r", encoding="utf-8") as f:
        dictreader = csv.DictReader(f, fieldnames=fieldnames)
        next(dictreader)

        # For each fow, convert the CRS
        for row in dictreader:

            geometry = transform_epsg(source_epsg, target_epsg, row["geometry"])
            row["geometry"] = (
                geometry_to_json(geometry) if geometry is not None else None
            )
            yield (row)


def get_filepath(dir_path, resource_name, epsg, file_format):
    """Gets a filepath using input resource name, and desired format/epsg"""

    epsg_suffix = " - " + str(epsg) if epsg else ""
    return os.path.join(
        dir_path, "{0}{1}.{2}".format(resource_name, epsg_suffix, file_format.lower())
    )


def append_to_output(output, target_format, target_epsg, output_filepath):
    """Sorts created file filepath into dict output of to_file()"""

    output[str(target_format) + "-" + str(target_epsg)] = output_filepath

    return output


def write_to_csv(dump_filepath, fieldnames, rows_generator):
    """Streams a dump into a CSV file"""
    csv.field_size_limit(sys.maxsize)

    with codecs.open(dump_filepath, "w", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames)
        writer.writeheader()
        writer.writerows(rows_generator)


def write_to_zipped_shapefile(
    fieldnames, dir_path, resource_metadata, output_filepath, col_map
):
    """Zips shp component files together with optional colname mapping csv"""

    # put a mapping of full names to truncated names into a csv
    file_name = f'{resource_metadata["name"]} fields.csv'
    fields_filepath = os.path.join(dir_path, file_name)
    with codecs.open(fields_filepath, "w", encoding="utf-8") as fields_file:
        writer = csv.DictWriter(fields_file, fieldnames=["field", "name"])
        writer.writeheader()
        for fieldname in [
            fieldname for fieldname in fieldnames if fieldname != "geometry"
        ]:
            writer.writerow({"field": col_map[fieldname], "name": fieldname})

    # put shapefile components into a .zip
    output_filepath = output_filepath.replace(".shp", ".zip")
    with ZipFile(output_filepath, "w") as zipfile:
        shp_components = ["shp", "cpg", "dbf", "prj", "shx"]

        for file in os.listdir(dir_path):
            if file[-3:] in shp_components or file == file_name:
                file_path = os.path.join(dir_path, file)
                zipfile.write(file_path, arcname=file)
                os.remove(file_path)

    return output_filepath


def write_to_json(dump_filepath, output_filepath, datastore_resource, context):
    """Stream into a JSON file by running datastore_search over and over"""
    # TODO why are we re-downloading instead of using the dump_filepath?
    with codecs.open(output_filepath, "w", encoding="utf-8") as jsonfile:
        # write starting bracket
        jsonfile.write("[")

        # grab first chunk of records
        chunk_size = 20000
        iteration = 0
        data_chunk = tk.get_action("datastore_search")(
            context,
            {
                "resource_id": datastore_resource["resource_id"],
                "limit": chunk_size,
            },
        )
        # as long as there is more to grab, grab the next chunk
        while len(data_chunk["records"]):

            for record in data_chunk["records"]:
                jsonfile.write(json.dumps(record))
                jsonfile.write(", ")
            iteration += 1
            data_chunk = tk.get_action("datastore_search")(
                context,
                {
                    "resource_id": datastore_resource["resource_id"],
                    "limit": chunk_size,
                    "offset": chunk_size * iteration,
                },
            )

    with codecs.open(output_filepath, "rb+", encoding="utf-8") as jsonfile:
        # remove last ", "
        jsonfile.seek(-2, 2)
        jsonfile.truncate()

    with codecs.open(output_filepath, "a", encoding="utf-8") as jsonfile:
        # add last closing ]
        jsonfile.write("]")


def write_to_xml(dump_filepath, output_filepath, chunk_size=5000):
    """Stream into an XML file"""
    XML_ENCODING = "utf-8"
    CSV_ENCODING = "utf-8"

    with open(output_filepath, "w", encoding=XML_ENCODING) as xml_file:
        with codecs.open(dump_filepath, "r", encoding=CSV_ENCODING) as csvfile:
            dictreader = csv.DictReader(csvfile)

            root_tag = "DATA"
            xml_file.write(f'<?xml version="1.0" encoding="{XML_ENCODING}"?>\n')
            xml_file.write(f"<{root_tag}>")
            i = 0

            # chunk writes to disk based on chunk_size so that:
            # 1. we don't do it all in one batch and end up w/ a MemoryError
            # 2. we don't perform disk io for every single record which is inefficient
            chunk = []

            for csv_row in dictreader:
                xml_row = ET.Element("ROW", count=str(i))
                for key, value in csv_row.items():
                    keyname = re.sub(r"[^a-zA-Z0-9-_]", "", key)
                    ET.SubElement(xml_row, keyname).text = value
                chunk.append(ET.tostring(xml_row, encoding="unicode"))

                i += 1

                if len(chunk) >= chunk_size:
                    xml_file.writelines(chunk)
                    chunk = []

            # Flush any rows in the remaining chunk
            if chunk:
                xml_file.writelines(chunk)

            xml_file.write(f"</{root_tag}>")


def iotrans_auth_function(context, data_dict=None):
    """CKAN auth function - requires authorized uses for certain actions"""
    if context.get("auth_user_obj", False):
        return {"success": True}
    elif not context.get("auth_user_obj", None):
        return {
            "success": False,
            "msg": "This endpoint is for authorized accounts only",
        }
