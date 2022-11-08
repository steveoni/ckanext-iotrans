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


def dump_generator(dump_url):

    with httpx.Client() as client:
        with client.stream("GET", dump_url, follow_redirects=True, timeout=60) as r:
            csv.field_size_limit(180000)

            # Create in-memory file. We save the row of the incoming CSV file here
            f = io.StringIO()

            # we iterate over the source CSV line by line
            lines = r.iter_lines()

            # skip the first line of the csv since its a header
            next(lines)

            for lineno, line in enumerate(lines, 2):
                # Write one line to the in-memory file.                    
                f.write(line)

                # Seek sends the file handle to the top of the file.
                f.seek(0)

                # We initiate a CSV reader to read and parse each line of the CSV file
                reader = csv.reader(f)
                row = next(reader)

                yield(row)

                # set file handle needs to the top
                f.seek(0)

                # Clean up the buffer
                f.flush()


def dump_to_geospatial_generator(dump_filepath, fieldnames, target_format, source_epsg = 4326, target_epsg=4326):

    with open(dump_filepath, "r") as f: 
        reader = csv.DictReader(f, fieldnames=fieldnames)
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