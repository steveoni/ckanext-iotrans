# streams data from datastore dump into csv
# from the csv, other files in other coordinate systems can be made
# function could be a generator?? It might not be a ckan extension then ...

# assumes geometry column in dataset contains geometry
# also assumes geometry objects within a dataset are all the same geometry type (all Point, all Line, or all Polygon)

import ckan.plugins.toolkit as tk


# if they want a csv of the same epsg as source_epsg, then just return the starting file!

# if they want a conversion to a spatial file format, then we need to make sure we have legit spatial data
#   and make sure the starting data has a geometry, or similar, attribute
# if they dont care about anything geospatial, then we need to get other libraries involved


@tk.side_effect_free
def to_file(context, data_dict):

    # init important variables
    dir_path = os.path.dirname(os.path.realpath(__file__))

    output = []

    # Make sure a resource id is provided
    assert resource_id, "Input CKAN Resource ID required!"
    # Make sure the resource id provided is for a datastore resource
    datastore_resource = tk.get_action("datastore_search")(context, {"resource_id": data_dict["resource_id"]})
    assert datastore_resource["success"], data_dict["resource_id"] + " is not a datastore resource!"

    # get fieldnames for the resource
    fieldnames = [ field["id"] for field in datastore_resource["fields"] ]
    dump_url = "http://0.0.0.0:8080/datastore/dump/" + data_dict["resource_id"]
    
    # create filepath for temp working file - this CSV will be used for all outputs going forward
    dump_filepath = dir_path + "/" + datastore_resource["resource_id"] + "-" + data_dict["source_epsg"] + ".csv"
    with open(dump_filepath, "w") as f:
        writer = csv.writer(f)
        writer.writerow( fieldnames )
        writer.writerows(dump_generator(dump_url))
        f.close()

    # Now that we have our dump on the disk, let's figure out what to do with it
    # Let's first determine whether geometry is involved

    # geometric transformations
    if "geometry" in fieldnames:
        if not isinstance(data_dict["target_epsgs"], list):
            data_dict["target_epsgs"] = list(data_dict["target_epsgs"])
            data_dict["target_formats"] = list(data_dict["target_formats"])

        # for each target EPSG...
        for target_epsg in data_dict["target_epsgs"]:
            # for each target format
            for target_format in data_dict["target_formats"]:
                # if the format+epsg combo match the dump, add dump to the output
                if target_format.lower() == "csv" and target_epsg == data_dict["source_epsg"]:
                    # dump is added as an io.BytesIO object
                    output[ target_format+"-"+target_epsg ] = io.BytesIO( open(dump_filepath, "rb").read() )

                # if format matches the dump but epsg doesnt, convert the dump and add it to output
                if target_format.lower() == "csv" and target_epsg != data_dict["source_epsg"]:
                    output_filepath = dir_path + "/" + datastore_resource["resource_id"] + "-" + data_dict["source_epsg"] + ".csv"
                    
                    with open(output_filepath, "w") as f:
                        writer = csv.writer(f)        
                        writer.writerows(transform_dump_epsg(dump_url, resource_id))
                        f.close()

                    output[ target_format+"-"+target_epsg ] = io.BytesIO( open(dump_filepath, "rb").read() )

                # if format doesnt match the dump, get fiona involved

    # non geometric transformations
    elif "geometry" not in fieldnames:
        pass
                    


        # this maps ckan data type names to fiona data type names
        ckan_to_fiona_typemap = {
            "text": "str",
            "date":"date",
            "timestamp":"str",
            "float":"float",
            "int":"int",
        }

        # this creates a schema for our output spatial files
        geometry_type = json.loads(datastore_resource["records"][0]["geometry"])["type"]
        fields_metadata = { field["id"]: ckan_to_fiona_typemap[''.join( [char for char in field["type"] if not char.isdigit()] )]  for field in datastore_resource["result"]["fields"] if field["id"] != "geometry"  }
        schema = { 'geometry': geometry_type, 'properties': fields_metadata }


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


    def dump_to_geospatial_generator(target_format, target_epsg=4326):

        with open(dump_filepath, "r") as f: 
            reader = csv.DictReader(f, fieldnames=fieldnames)
            row = next(reader)


            # if the data contains a "geometry" column, we know its spatial
            if "geometry" in row.keys():
                geometry = row.pop("geometry")
                
                # if we need to transform the EPSG, we do it here 
                if target_epsg != source_epsg:
                    geometry = transform_geom( from_epsg(4326), from_epsg(2952), json.loads( geometry ) )
                    geometry["coordinates"] = list(geometry["coordinates"])

                output = { "type": "Feature", "properties": dict(row), "geometry": json.loads(geometry) }  

                yield(output)

    def transform_dump_epsg( dump_filepath, fieldnames, source_epsg, target_epsg ):
        # generator yields dump rows in a different epsg than the source
        with open(dump_filepath, "r") as file:
            dictreader = csv.DictReader( file, fieldnames=fieldnames )
            for row in dictreader:
            
                row["geometry"] = transform_geom( from_epsg(source_epsg), from_epsg(target_epsg), json.loads(row["geometry"]) )
                row["geometry"]["coordinates"] = list(row["geometry"]["coordinates"])

                output = row.values()
                yield(output)

    

    