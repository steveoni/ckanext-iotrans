# functions that support the to_file() function
# to_file() takes as inputs:
#   resource_id (str): resource_id for a datastore resource in CKAN
#   source_epsg (int): epsg of the existing dataset's coordinate system
#   target_formats (list): list of file formats desired for the output
#   target_epsgs (list): list of epsg integers desired for the output

# to_file() returns as outputs:
#   a list of filepaths, where the outputs are stored on disk 

# TODO:
# prune() as a CKAN action
# write_to_filestore to add each filepath as a filestore object to the input resource_id's resource
# make this an authenticated api call


# assumes geometry column in dataset contains geometry
# also assumes geometry objects within a dataset are all the same geometry type (all Point, all Line, or all Polygon)

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


@tk.side_effect_free
def to_file(context, data_dict):

    # make sure an authorized user is making this call
    assert context["auth_user_obj"], "This endpoint can be used by authorized accounts only"

    # create a temp directory to store the file we create on disk
    dir_path = tempfile.mkdtemp()

    # TODO - function to delete temp directory once we dont need it

    # all the outputs of this action will be stored here
    output = {}

    # Make sure a resource id is provided
    if not data_dict.get("resource_id", None):
        raise tk.ValidationError( {"constraints": [ "Input CKAN Resource ID required!" ]} )

    if not isinstance( data_dict.get("target_formats", None), list):
        raise tk.ValidationError( {"constraints": [ "Input target_formats required and must be a list of strings" ]} )

    #if not isinstance( data_dict.get("target_epsgs", None), list):
    #    raise tk.ValidationError( {"constraints": [ "Input target_epsgs required and must be a list of epsg codes as integers" ]} )

    # Make sure the resource id provided is for a datastore 
    resource_metadata = tk.get_action("resource_show")(context, {"id": data_dict["resource_id"]})
    if resource_metadata.get("datastore_active", None) in ["false", "False", False] :
        raise tk.ValidationError( {"constraints": [ data_dict["resource_id"] + " is not a datastore resource!" ]} )
    
    datastore_resource = tk.get_action("datastore_search")(context, {"resource_id": data_dict["resource_id"]})
    

    # get fieldnames for the resource
    fieldnames = [ field["id"] for field in datastore_resource["fields"] ]
    dump_url = "http://0.0.0.0:8080/datastore/dump/" + data_dict["resource_id"]
    
    # create filepath for temp working file - this CSV will be used for all outputs going forward
    dump_filepath = create_filepath(dir_path, resource_metadata["name"], data_dict.get("source_epsg", None), "csv")
    write_to_csv(dump_filepath, fieldnames, dump_generator(dump_url))

    # Now that we have our dump on the disk, let's figure out what to do with it
    # Let's first determine whether geometry is involved

    # geometric transformations
    if "geometry" in fieldnames:
        if not isinstance(data_dict.get("target_epsgs", None), list):
            data_dict["target_epsgs"] = list(data_dict["target_epsgs"])
            data_dict["target_formats"] = list(data_dict["target_formats"])

        # for each target EPSG...
        for target_epsg in data_dict["target_epsgs"]:
            # for each target format...
            for target_format in data_dict["target_formats"]:

                # init fiona driver list, which helps us determine if fiona needs to get involved
                drivers = {"shp":"ESRI Shapefile", "geojson":"GeoJSON", "gpkg": "GPKG"}

                # if the format+epsg combo match the dump, add dump to the output
                if target_format.lower() == "csv" and target_epsg == data_dict["source_epsg"]:
                    output = append_to_output(output, target_format, target_epsg, dump_filepath)

                # if format matches the dump but epsg doesnt, convert the dump and add it to output
                elif target_format.lower() == "csv" and target_epsg != data_dict["source_epsg"]:
                    output_filepath = create_filepath(dir_path, resource_metadata["name"], target_epsg, "csv")
                    write_to_csv(output_filepath, fieldnames,  transform_dump_epsg(dump_filepath, fieldnames, data_dict["source_epsg"], target_epsg) )
                    output = append_to_output(output, target_format, target_epsg, output_filepath)

                # if format doesnt match the dump, get fiona drivers involved
                elif target_format.lower() in drivers.keys():
                
                    # first, we need to build a schema
                    ckan_to_fiona_typemap = {"text": "str", "date":"date", "timestamp":"str", "float":"float", "int":"int"}
                    # get Point, Line, or Polygon from the first row of our data. !!! This code assumes all geometries in the dataset are the same type
                    geometry_type = json.loads(datastore_resource["records"][0]["geometry"])["type"]
                    # get all the field data types (other than geometry) and map them to fiona data types
                    fields_metadata = { field["id"]: ckan_to_fiona_typemap[''.join( [char for char in field["type"] if not char.isdigit()] )]  for field in datastore_resource["fields"] if field["id"] != "geometry"  }
                    schema = { 'geometry': geometry_type, 'properties': fields_metadata }
                    output_filepath = create_filepath(dir_path, resource_metadata["name"], target_epsg, target_format)
                    

                    with fiona.open(output_filepath, 'w', schema=schema, driver=drivers[target_format], crs=from_epsg(target_epsg)) as outlayer:
                        outlayer.writerecords( dump_to_geospatial_generator(dump_filepath, fieldnames, target_format, data_dict["source_epsg"], target_epsg) )
                        outlayer.close()
                    
                    output = append_to_output(output, target_format, target_epsg, output_filepath)


    # non geometric transformations
    elif "geometry" not in fieldnames:
        # for each target format...
        for target_format in data_dict["target_formats"]:
            output_filepath = create_filepath(dir_path, resource_metadata["name"], None, target_format) 
        
            # CSV
            if target_format.lower() == "csv":
                output = append_to_output(output, target_format, None, dump_filepath)

            # JSON
            elif target_format.lower() == "json":
                with open(dump_filepath, "r") as csvfile:
                    dictreader = csv.DictReader(csvfile)
                    with open(output_filepath, "a") as jsonfile:
                        jsonfile.write("[")
                        for row in dictreader:
                            jsonfile.write( json.dumps(row) )
                        jsonfile.write("]")
                output = append_to_output(output, target_format, None, output_filepath)

            # XML
            elif target_format.lower() == "xml":
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

                output = append_to_output(output, target_format, None, output_filepath)


                    
    return output


def dump_generator(dump_url):
    with httpx.Client() as client:
        with client.stream("GET", dump_url, follow_redirects=True, timeout=60) as r:
            print("connection made")

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

@tk.side_effect_free
def prune(context, data_dict):
    '''
    Taken from https://github.com/open-data-toronto/iotrans/blob/master/iotrans/utils.py
    Deletes a file or a directory
    
    Parameters:
    path    (str): Path to be removed
    '''

    # make sure an authorized user is making this call
    assert context["auth_user_obj"], "This endpoint can be used by authorized accounts only"

    if not data_dict.get("path", None):
        raise tk.ValidationError( {"constraints": [ "Input path of dir to delete required!" ]} )

    path = data_dict["path"]

    if not data_dict.get("path", None).startswith("/tmp/"):
        raise tk.ValidationError( {"constraints": [ "This action is only meant for deleting folders in the /tmp/ directory" ]} )



    if os.path.isdir(path):
        # Empty the contents of the folder before removing the directory
        for f in os.listdir(path):
            os.remove(os.path.join(path, f))

        os.rmdir(path)
    else:
        os.remove(path)

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

def write_to_fiona(target_format, schema):
    pass