# utils.py - utils for the main functions stored in iotrans.py

import ckan.plugins.toolkit as tk
import tempfile
import os
import io
import csv
import json
import fiona
from fiona.crs import from_epsg
from fiona.transform import transform_geom
import httpx
from . import utils
from zipfile import ZipFile


def dump_generator(dump_url, fieldnames):

    with httpx.Client() as client:
        with client.stream("GET", dump_url, follow_redirects=True, timeout=60) as r:
            csv.field_size_limit(180000)

            f = io.StringIO()

            # we iterate over the source CSV line by line
            lines = r.iter_lines()

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
                        
                    # We initiate a CSV reader to read and parse each line of the CSV file
                    reader = csv.reader(f)
                    row = next(reader)

                yield(row)

                # set file handle needs to the top
                #f.seek(0)

                # Clean up the buffer
                #f.flush()


def dump_to_geospatial_generator(dump_filepath, fieldnames, target_format, source_epsg = 4326, target_epsg=4326):

    if target_format == "shp":
        working_fieldnames = [fieldname[:10] for fieldname in fieldnames]
    else:
        working_fieldnames = fieldnames

    with open(dump_filepath, "r") as f: 
        reader = csv.DictReader(f, fieldnames=working_fieldnames)
        next(reader)
        for row in reader:

            

            # if the data contains a "geometry" column, we know its spatial
            if "geometry" in row.keys():
                geometry = row.pop("geometry")

                # if we need to transform the EPSG, we do it here 
                if target_epsg != source_epsg:
                    geometry = transform_geom( from_epsg(source_epsg), from_epsg(target_epsg), json.loads( geometry ) )
                    geometry["coordinates"] = list(geometry["coordinates"])

                    output = { "type": "Feature", "properties": dict(row), "geometry":  geometry }  
                
                else:
                    output = { "type": "Feature", "properties": dict(row), "geometry": json.loads( geometry ) }  

                yield(output)
        f.close()


def transform_dump_epsg( dump_filepath, fieldnames, source_epsg, target_epsg ):
    # generator yields dump rows in a different epsg than the source
    with open(dump_filepath, "r") as f:
        dictreader = csv.DictReader( f, fieldnames=fieldnames )
        # skip header
        next(dictreader)

        for row in dictreader:
            row["geometry"] = transform_geom( from_epsg(source_epsg), from_epsg(target_epsg), json.loads(row["geometry"]) )
            row["geometry"]["coordinates"] = list(row["geometry"]["coordinates"])

            output = row.values()
            yield(output)

        f.close()

def create_filepath(dir_path, resource_name, epsg, format):
    epsg_suffix = " - " + str(epsg) if epsg else ""
    return os.path.join(dir_path, "{0}{1}.{2}".format( resource_name, epsg_suffix, format.lower()))

def append_to_output(output, target_format, target_epsg, output_filepath):
    output[ str(target_format)+"-"+str(target_epsg) ] = output_filepath # io.BytesIO( open(output_filepath, "rb").read() )
    return output

def write_to_csv(dump_filepath, fieldnames, rows_generator):
    with open(dump_filepath, "w") as f:
        writer = csv.writer(f)
        writer.writerow( fieldnames )
        writer.writerows( rows_generator )
        f.close()

def write_to_zipped_shapefile(fieldnames, dir_path, resource_metadata, output_filepath):
    # put a mapping of full names to truncated names into a csv
    fields_filepath = dir_path + "/" + resource_metadata["name"] + " fields.csv"
    with open( fields_filepath, "w" ) as fields_file:
        writer = csv.DictWriter(fields_file, fieldnames = ["field", "name"])
        writer.writeheader()
        for fieldname in [fieldname for fieldname in fieldnames if fieldname != "geometry"]:
            writer.writerow({ "field": fieldname[:10], "name": fieldname})

    # put shapefile components into a .zip
    output_filepath = output_filepath.replace(".shp", ".zip")
    with ZipFile(output_filepath, 'w') as zipfile:
        shp_components = ["shp", "cpg", "dbf", "prj", "shx"]

        for file in os.listdir(dir_path):
            if file[-3:] in shp_components or file == resource_metadata["name"] + " fields.csv":
                zipfile.write( dir_path + "/" + file, arcname=file )
                os.remove( dir_path + "/" + file )

    return output_filepath

def write_to_json(dump_filepath, output_filepath):
    with open(dump_filepath, "r") as csvfile:
        dictreader = csv.DictReader(csvfile)
        with open(output_filepath, "a") as jsonfile:
            jsonfile.write("[")
            for row in dictreader:
                jsonfile.write( json.dumps(row) )
            jsonfile.write("]")

def write_to_xml(dump_filepath, output_filepath):
    with open(dump_filepath, "r") as csvfile:
        dictreader = csv.DictReader(csvfile)
        with open(output_filepath, "a") as xmlfile:
            xmlfile.write('<?xml version="1.0" encoding="utf-8"?>')
            xmlfile.write('<DATA>')
            i = 0
            for row in dictreader:
                xml_row = '<ROW count="{}">'.format( str(i) )
                for key, value in row.items():
                    xml_row += "<{key}>{value}</{key}>".format( key=key, value=value )
                    xmlfile.write( xml_row )
                    xmlfile.write("</ROW>")
                i += 1
            xmlfile.write('</DATA>')