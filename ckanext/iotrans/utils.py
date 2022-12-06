'''Utils functions for iotrans.py
'''

import os
import io
import csv
import json
from fiona.crs import from_epsg
from fiona.transform import transform_geom
import requests
from zipfile import ZipFile


def dump_generator(dump_url, fieldnames):
    '''reads a CKAN dumped CSV, returns a python generator'''

    r = requests.get(dump_url, stream=True)
    csv.field_size_limit(180000)

    # we iterate over the source CSV line by line
    lines = r.iter_lines(decode_unicode=True)

    # skip the first line of the csv since its a header
    next(lines)

    for lineno, line in enumerate(lines, 2):

        # Create in-memory file. We save the row of the incoming CSV file here
        f = io.StringIO()

        # Write one line to the in-memory file.
        f.write(line)

        # Seek sends the file handle to the top of the file.
        f.seek(0)

        # We initiate a CSV reader to read and parse each line of the CSV file
        reader = csv.reader(f)
        row = next(reader)

        # if the line is broken mid-record, concat next line to working line
        while len(row) < len(fieldnames):
            line += next(lines)
            f = io.StringIO()

            # Write one line to the in-memory file.
            f.write(line)

            # Seek sends the file handle to the top of the file.
            f.seek(0)

            # We initiate a CSV reader to read and parse each line
            reader = csv.reader(f)
            row = next(reader)

        yield (row)


def dump_to_geospatial_generator(
    dump_filepath, fieldnames, target_format, source_epsg, target_epsg
):
    '''reads a CKAN CSV dump, creates generator with converted CRS'''

    # Shapefiles can only have colnames of max 10 characters
    if target_format == "shp":
        working_fieldnames = [fieldname[:10] for fieldname in fieldnames]
    else:
        working_fieldnames = fieldnames

    # For each row in the dump ...
    with open(dump_filepath, "r") as f:
        reader = csv.DictReader(f, fieldnames=working_fieldnames)
        next(reader)
        for row in reader:

            # if the data contains a "geometry" column, we know its spatial
            if "geometry" in row.keys():
                geometry = row.pop("geometry")

                # if we need to transform the EPSG, we do it here
                if target_epsg != source_epsg:
                    geometry = transform_geom(
                        from_epsg(source_epsg),
                        from_epsg(target_epsg),
                        json.loads(geometry),
                    )
                    geometry["coordinates"] = list(geometry["coordinates"])

                    output = {
                        "type": "Feature",
                        "properties": dict(row),
                        "geometry": geometry,
                    }

                else:
                    output = {
                        "type": "Feature",
                        "properties": dict(row),
                        "geometry": json.loads(geometry),
                    }

                yield (output)
        f.close()


def transform_dump_epsg(dump_filepath, fieldnames, source_epsg, target_epsg):
    '''generator yields dump rows in a different epsg than the source'''

    # Open the dump CSV into a dictreader
    with open(dump_filepath, "r") as f:
        dictreader = csv.DictReader(f, fieldnames=fieldnames)
        # skip header
        next(dictreader)

        # For each fow, convert the CRS
        for row in dictreader:
            row["geometry"] = transform_geom(
                from_epsg(source_epsg),
                from_epsg(target_epsg),
                json.loads(row["geometry"]),
            )
            # transform the coordinates into a list
            coordinates = list(row["geometry"]["coordinates"])
            row["geometry"]["coordinates"] = coordinates

            output = row.values()
            yield (output)

        f.close()


def create_filepath(dir_path, resource_name, epsg, format):
    '''Creates a filepath using input resource name, and desired format/epsg'''

    epsg_suffix = " - " + str(epsg) if epsg else ""
    return os.path.join(
        dir_path,
        "{0}{1}.{2}".format(resource_name, epsg_suffix, format.lower())
    )


def append_to_output(output, target_format, target_epsg, output_filepath):
    '''Sorts created file filepath into dict output of to_file()'''

    output[
        str(target_format) + "-" + str(target_epsg)
    ] = output_filepath

    return output


def write_to_csv(dump_filepath, fieldnames, rows_generator):
    '''Streams a dump into a CSV file'''
    with open(dump_filepath, "w") as f:
        writer = csv.writer(f)
        writer.writerow(fieldnames)
        writer.writerows(rows_generator)
        f.close()


def write_to_zipped_shapefile(fieldnames, dir_path,
                              resource_metadata, output_filepath):
    '''Zips shp component files together with optional colname mapping csv'''

    # put a mapping of full names to truncated names into a csv
    fields_filepath = dir_path + "/" + resource_metadata["name"]+" fields.csv"
    with open(fields_filepath, "w") as fields_file:
        writer = csv.DictWriter(fields_file, fieldnames=["field", "name"])
        writer.writeheader()
        for fieldname in [
            fieldname for fieldname in fieldnames if fieldname != "geometry"
        ]:
            writer.writerow({"field": fieldname[:10], "name": fieldname})

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


def write_to_json(dump_filepath, output_filepath, datastore_resource):
    '''Stream into a JSON file'''

    # First, we need to ensure we map data types correctly
    # We do this below, otherwise all data will be string

    # make a map from ckan data types to python data types
    datatype_conversion = {
        "text": str,
        "date": str,
        "timestamp": str,
        "float": float,
        "int": int,
    }

    # map column name to python data types
    fields_metadata = {
        field["id"]: datatype_conversion[
            "".join(
                [char for char in field["type"] if not char.isdigit()])]
            for field in datastore_resource["fields"]
            if field["id"] != "geometry"
    }

    # Loop through each col in each row and transform data types
    with open(dump_filepath, "r") as csvfile:
        dictreader = csv.DictReader(csvfile)
        with open(output_filepath, "w") as jsonfile:
            # write lines, delineated by ", "
            jsonfile.write("[")
            for row in dictreader:
                # ensure output data types arent always strings
                working_row = {}
                for field in row.keys():
                    converter = fields_metadata[field]
                    # make sure nulls are null and not empty strings
                    if row[field]:
                        working_row[field] = converter(row[field])
                    else:
                        working_row[field] = None

                jsonfile.write(json.dumps(working_row))
                jsonfile.write(", ")
                
        with open(output_filepath, "rb+") as jsonfile:
            # remove last ", "
            jsonfile.seek(-2, 2)
            jsonfile.truncate()
        with open(output_filepath, "a") as jsonfile:
            # add last closing ]
            jsonfile.write("]")


def write_to_xml(dump_filepath, output_filepath):
    '''Stream into an XML file'''

    with open(dump_filepath, "r") as csvfile:
        dictreader = csv.DictReader(csvfile)
        with open(output_filepath, "a") as xmlfile:
            xmlfile.write('<?xml version="1.0" encoding="utf-8"?>')
            xmlfile.write("<DATA>")
            i = 0
            for row in dictreader:
                xml_row = '<ROW count="{}">'.format(str(i))
                for key, value in row.items():
                    xml_row += "<{key}>{value}</{key}>".format(
                        key=key, value=value)
                    xmlfile.write(xml_row)
                    xmlfile.write("</ROW>")
                i += 1
            xmlfile.write("</DATA>")
