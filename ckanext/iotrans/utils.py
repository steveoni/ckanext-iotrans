'''Utils functions for iotrans.py
'''

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
from typing import Dict
from concurrent.futures import ProcessPoolExecutor

import ckan.plugins.toolkit as tk

def _geometry_to_json(geom: Geometry) -> str:
    """_geometry_to_json

    :param geom: the fiona geometry
    :type geom: Geometry
    :return: geojson compliant JSON string
    :rtype: str
    """
    geom_dict = dict(geom)

    # GeoJSON spec does not indicate a case for `null` to be valid json (only mentions
    # it to be a list of geometries or DNE in the json at all.)
    # So if it is explicitly None, remove it before jsonifying
    if 'geometries' in geom_dict and geom_dict.get("geometries") is None:
        del geom_dict["geometries"]

    return json.dumps(geom_dict)

def transform_epsg(source_epsg, target_epsg, geometry):
    '''standardize processing when transforming epsg'''

    # if input is empty, return it as is
    if geometry in [None, "None"]:        
        return None

    # if input is a string, make it a json object
    if isinstance(geometry, str):
        geometry = json.loads(geometry.replace("'", '"')) # replace '' with ""
        assert "coordinates" in geometry.keys(), "No coordinates in geometry!"   

    original_geometry_type = geometry["type"]
    if not geometry["type"].startswith("Multi"):
        geometry["type"] = "Multi" + geometry["type"]

    # 0,0 coords need not be transformed - only their brackets changed
    if geometry["coordinates"] in [[0,0], [[0,0]]]:
        geometry["coordinates"] = [[0,0]]                
        return geometry

    # null coords need not be transformed - only their brackets changed
    if geometry["coordinates"] in [[None,None], [[None,None]]]:
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
                json.dumps(
                    geometry["coordinates"]).replace("(","[").replace(")","]")
                )

    return geometry

    
def dump_generator(resource_id, fieldnames, context):
    '''reads a CKAN datastore_search calls, returns a python generator'''
    # init some vars
    chunk = 20000
    i = 0

    while True:
        # get a chunk of records from datastore resource
        records = tk.get_action("datastore_search")(
            context, {
                "resource_id": resource_id,
                "limit": chunk,
                "offset": chunk * i,
                }
        )["records"]

        if len(records):
            for record in records:
                yield record
            i += 1
            continue

        else:
            break


def dump_to_geospatial_generator(
    dump_filepath, fieldnames, target_format, source_epsg, target_epsg, col_map=None
):
    """Streamlines CRS conversion and row processing for geospatial data."""

    with codecs.open(dump_filepath, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f, fieldnames=fieldnames)
        next(reader)  # Skip header

        for row in reader:
            geometry = row.pop("geometry")

            # Process only valid geometries
            if geometry and geometry != "None":
                # Transform geometry CRS if needed
                geometry = transform_epsg(source_epsg, target_epsg, geometry)

                # Adjust shapefile-specific column mapping
                if target_format == "shp" and col_map:
                    row = {col_map.get(k, k): v for k, v in row.items()}

                # Yield a single feature to avoid keeping multiple records in memory
                yield {
                    "type": "Feature",
                    "properties": row,
                    "geometry": geometry,
                }



def transform_dump_epsg(dump_filepath, fieldnames, source_epsg, target_epsg):
    '''generator yields dump rows with epsg reformatted/converted'''

    # Open the dump CSV into a dictreader
    with codecs.open(dump_filepath, "r", encoding="utf-8") as f:
        dictreader = csv.DictReader(f, fieldnames=fieldnames)
        # skip header
        next(dictreader)

        with ProcessPoolExecutor() as executor:
            tasks = ((row, source_epsg, target_epsg) for row in dictreader)
            # Process each row in parallel using multiple processes
            for row in executor.map(transform_row_epsg_wrapper, tasks):
                yield row


def transform_row_epsg_wrapper(row_source_epsg_target_epsg):
    row, source_epsg, target_epsg = row_source_epsg_target_epsg
    return transform_row_epsg(row, source_epsg, target_epsg)

def transform_row_epsg(row, source_epsg, target_epsg):
    '''Helper function to transform EPSG for each row'''

    geometry = transform_epsg(source_epsg, target_epsg, row["geometry"])
    row["geometry"] = _geometry_to_json(geometry)
    return row

def create_filepath(dir_path, resource_name, epsg, file_format):
    '''Creates a filepath using input resource name, and desired format/epsg'''

    epsg_suffix = " - " + str(epsg) if epsg else ""
    return os.path.join(
        dir_path,
        "{0}{1}.{2}".format(resource_name, epsg_suffix, file_format.lower())
    )


def append_to_output(output, target_format, target_epsg, output_filepath):
    '''Sorts created file filepath into dict output of to_file()'''

    output[
        str(target_format) + "-" + str(target_epsg)
    ] = output_filepath

    return output


def write_to_csv(dump_filepath, fieldnames, rows_generator):
    '''Streams a dump into a CSV file'''
    csv.field_size_limit(sys.maxsize)
    
    with codecs.open(dump_filepath, "w", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames)
        writer.writeheader()
        writer.writerows(rows_generator)


def write_to_zipped_shapefile(fieldnames, dir_path,
                              resource_metadata, output_filepath, col_map):
    '''Zips shp component files together with optional colname mapping csv'''

    # put a mapping of full names to truncated names into a csv
    fields_filepath = dir_path + "/" + resource_metadata["name"]+" fields.csv"
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
            if (
                file[-3:] in shp_components
                or file == resource_metadata["name"] + " fields.csv"
            ):
                zipfile.write(dir_path + "/" + file, arcname=file)
                os.remove(dir_path + "/" + file)

    return output_filepath


def write_to_json(dump_filepath, output_filepath, datastore_resource, context):
    '''Stream into a JSON file by running datastore_search over and over'''
    with codecs.open(output_filepath, "w", encoding="utf-8") as jsonfile:
        # write starting bracket
        jsonfile.write("[")

        # grab first chunk of records
        chunk_size = 20000
        iteration = 0
        data_chunk = tk.get_action("datastore_search")(
            context, {
                "resource_id": datastore_resource["resource_id"],
                "limit": chunk_size,
                }
            )
        # as long as there is more to grab, grab the next chunk
        while len(data_chunk["records"]):

            for record in data_chunk["records"]:
                jsonfile.write(json.dumps(record))
                jsonfile.write(", ")
            iteration += 1
            data_chunk = tk.get_action("datastore_search")(
                context, {
                    "resource_id": datastore_resource["resource_id"],
                    "limit": chunk_size,
                    "offset": chunk_size*iteration,
                    }
                )

    with codecs.open(output_filepath, "rb+", encoding="utf-8") as jsonfile:
        # remove last ", "
        jsonfile.seek(-2, 2)
        jsonfile.truncate()

    with codecs.open(output_filepath, "a", encoding="utf-8") as jsonfile:
        # add last closing ]
        jsonfile.write("]")


def write_to_xml(dump_filepath, output_filepath):
    '''Stream into an XML file'''

    with codecs.open(dump_filepath, "r", encoding="utf-8") as csvfile:
        dictreader = csv.DictReader(csvfile)
        root = ET.Element("DATA")
        i = 0
        for csvrow in dictreader:
            xmlrow = ET.SubElement(root, "ROW", count = str(i))
            i+=1
            for key, value in csvrow.items():
                keyname = re.sub(r"[^a-zA-Z0-9-_]","",key)
                ET.SubElement(xmlrow, keyname).text = value
        tree = ET.ElementTree(root)
        tree.write(output_filepath, encoding='utf-8', xml_declaration=True)            


def iotrans_auth_function(context, data_dict=None):
    '''CKAN auth function - requires authorized uses for certain actions'''
    if context.get("auth_user_obj", False):
        return {'success': True}
    elif not context.get("auth_user_obj", None):
        return {'success': False,
                'msg': 'This endpoint is for authorized accounts only'}
