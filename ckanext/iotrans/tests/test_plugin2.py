'''Tests for ckanext-iotrans to run in context of a CKAN instance'''


import json
import pytest
import filecmp
import os
import fiona
import zipfile

import ckan.plugins as p
import ckan.tests.factories as factories
import ckan.tests.helpers as helpers

# Define fixed variables
correct_dir_path = os.path.dirname(os.path.realpath(__file__)) + "/correct_files/"


@pytest.mark.usefixtures("with_request_context")
class TestIOTrans(object):

    @pytest.mark.ckan_config("ckan.plugins", "datastore iotrans")
    @pytest.mark.usefixtures("clean_db", "with_plugins")
    def test_to_file_on_nonspatial_data(self):
        '''Checks if to_file creates correct non-spatial files'''

        # create datastore resource
        resource = factories.Resource()
        data = {
            "resource_id": resource["id"],
            "force": True,
            "records": [{"the year": 2014}, {"the year": 2013}],
        }
        result = helpers.call_action("datastore_create", **data)
        
        # run to_file on datastore_resource
        target_formats = ["csv", "xml", "json"]
        data = {
            "resource_id": resource["id"],
            "target_formats": target_formats,
        }
        result = helpers.call_action("to_file", **data)

        # check if outputs are correct
        for format in target_formats:
            test_path = result[format+ "-None"]

            # compare new file to correct file
            correct_filepath = correct_dir_path + "correct_nonspatial." + format
            assert filecmp.cmp(test_path, correct_filepath)

    
    @pytest.mark.ckan_config("ckan.plugins", "datastore iotrans")
    @pytest.mark.usefixtures("clean_db", "with_plugins")
    def test_to_file_on_large_nonspatial_data(self):
        '''Checks if to_file creates correct non-spatial files'''

        # create datastore resource
        resource = factories.Resource()
        data = {
            "resource_id": resource["id"],
            "force": True,
            "records": [{"the attr": val} for val in range(0,21000)],
        }
        result = helpers.call_action("datastore_create", **data)
        
        # run to_file on datastore_resource
        target_formats = ["csv", "xml", "json"]
        data = {
            "resource_id": resource["id"],
            "target_formats": target_formats,
        }
        result = helpers.call_action("to_file", **data)

        # check if outputs are correct
        for format in target_formats:
            test_path = result[format+ "-None"]

            # compare new file to correct file
            correct_filepath = correct_dir_path + "correct_large_nonspatial." + format
            assert filecmp.cmp(test_path, correct_filepath)


    @pytest.mark.ckan_config("ckan.plugins", "datastore iotrans")
    @pytest.mark.usefixtures("clean_db", "with_plugins")
    def test_to_file_on_spatial_data_human_readable_formats(self):
        '''Checks if to_file creates correct non-spatial files'''

        # create datastore resource
        resource = factories.Resource()
        data = {
            "resource_id": resource["id"],
            "force": True,
            "records": [
                {"the year": 2014, "geometry": json.dumps({
                    "type": "Point", 
                    "coordinates": [-79.556501959627, 43.632603612174]
                })},
                {"the year": 2013, "geometry": json.dumps({
                    "type": "Point", 
                    "coordinates": [-79.252341959627, 43.332603432174]
                })}
            ],
        }
        result = helpers.call_action("datastore_create", **data)
        
        # run to_file on datastore_resource
        target_formats = ["csv", "geojson"]
        target_epsgs = [4326, 2952]
        data = {
            "resource_id": resource["id"],
            "source_epsg": 4326,
            "target_epsgs": target_epsgs,
            "target_formats": target_formats,
        }
        result = helpers.call_action("to_file", **data)
        # check if outputs are correct
        for format in target_formats:
            for epsg in target_epsgs:
                test_path = result[format + "-" + str(epsg)]                

                # compare new file to correct file
                correct_filepath = (correct_dir_path + "correct_spatial"
                    " - {}.{}").format(epsg, format)

                assert filecmp.cmp(test_path, correct_filepath)
            
    @pytest.mark.ckan_config("ckan.plugins", "datastore iotrans")
    @pytest.mark.usefixtures("clean_db", "with_plugins")
    def test_to_file_on_large_spatial_data_human_readable_formats(self):
        '''Checks if to_file creates correct non-spatial files'''

        # create datastore resource
        resource = factories.Resource()
        data = {
            "resource_id": resource["id"],
            "force": True,
            "records": [
                {"the year": val, "geometry": json.dumps({
                    "type": "Point", 
                    "coordinates": [
                        -79.556501959 + (val*0.00000001),
                        43.632603612 - (val*0.00000001)
                    ]
                })}
            for val in range(0, 21000)],
        }
        result = helpers.call_action("datastore_create", **data)
        
        # run to_file on datastore_resource
        target_formats = ["csv", "geojson"]
        target_epsgs = [4326, 2952]
        data = {
            "resource_id": resource["id"],
            "source_epsg": 4326,
            "target_epsgs": target_epsgs,
            "target_formats": target_formats,
        }
        result = helpers.call_action("to_file", **data)        
        # check if outputs are correct
        for format in target_formats:
            for epsg in target_epsgs:
                test_path = result[format + "-" + str(epsg)]

                # compare new file to correct file
                correct_filepath = (correct_dir_path + "correct_large_spatial"
                    " - {}.{}").format(epsg, format)

                assert filecmp.cmp(test_path, correct_filepath)
            

    @pytest.mark.ckan_config("ckan.plugins", "datastore iotrans")
    @pytest.mark.usefixtures("clean_db", "with_plugins")
    def test_to_file_on_shapefile(self):
        '''Checks if to_file creates correct shp'''

        # create datastore resource
        resource = factories.Resource(name="test_spatial")
        data = {
            "resource_id": resource["id"],
            "force": True,
            "records": [
                {"the year value column name": 2014, "geometry": json.dumps({
                    "type": "Point", 
                    "coordinates": [-79.556501959627, 43.632603612174]
                })},
                {"the year value column name": 2013, "geometry": json.dumps({
                    "type": "Point", 
                    "coordinates": [-79.252341959627, 43.332603432174]
                })}
            ],
        }
        result = helpers.call_action("datastore_create", **data)
        
        # run to_file on datastore_resource
        target_epsgs = [4326, 2952]
        data = {
            "resource_id": resource["id"],
            "source_epsg": 4326,
            "target_epsgs": target_epsgs,
            "target_formats": ["shp"],
        }
        result = helpers.call_action("to_file", **data)
        print(result)

        # check if outputs are correct
        # dbf files cant be compared this way, so we compare records below
        shp_components = ["shp", "cpg", "prj", "shx"] 
        
        for epsg in target_epsgs:
            test_path = result["shp-" + str(epsg)]
            with zipfile.ZipFile(test_path, "r") as thiszip:
                # extract zip contents to their current /tmp dir
                test_folder = "/".join(test_path.split("/")[:-1])
                thiszip.extractall(test_folder)

                for format in shp_components:
                    # compare new file to correct file
                    correct_filepath = (correct_dir_path + "correct_spatial"
                        " - {}.{}").format(epsg, format)

                    test_path = test_folder + "/test_spatial - {}.{}".format(
                        str(epsg), 
                        format,
                    )

                    assert filecmp.cmp(test_path, correct_filepath)

                # make sure txt mapping file is correct
                test_txt = test_folder + "/test_spatial fields.csv"
                correct_txt = correct_dir_path + "correct_spatial fields.csv"
                assert filecmp.cmp(test_txt, correct_txt)

                # check shapefile records one by one
                with fiona.open(test_path, "r") as test_shp:
                    with fiona.open(correct_filepath) as correct_shp:
                        assert test_shp.schema == correct_shp.schema

                        while True:
                            try:
                                assert next(test_shp) == next(correct_shp)
                            except StopIteration:
                                break 


    @pytest.mark.ckan_config("ckan.plugins", "datastore iotrans")
    @pytest.mark.usefixtures("clean_db", "with_plugins")
    def test_to_file_on_gpkg(self):
        '''Checks if to_file creates correct gpkg file'''

        # create datastore resource
        resource = factories.Resource(name="test_spatial")
        data = {
            "resource_id": resource["id"],
            "force": True,
            "records": [
                {"the year": 2014, "geometry": json.dumps({
                    "type": "Point", 
                    "coordinates": [-79.556501959627, 43.632603612174]
                })},
                {"the year": 2013, "geometry": json.dumps({
                    "type": "Point", 
                    "coordinates": [-79.252341959627, 43.332603432174]
                })}
            ],
        }
        result = helpers.call_action("datastore_create", **data)
        
        # run to_file on datastore_resource
        target_epsgs = [4326, 2952]
        data = {
            "resource_id": resource["id"],
            "source_epsg": 4326,
            "target_epsgs": target_epsgs,
            "target_formats": ["gpkg"],
        }
        result = helpers.call_action("to_file", **data)
        print(result)

        # check if outputs are correct        
        for epsg in target_epsgs:
            test_path = result["gpkg-" + str(epsg)]

            correct_filepath = (correct_dir_path + "correct_spatial"
                " - {}.{}").format(epsg, "gpkg")

            # check records one by one
            print(test_path)
            print(correct_filepath)
            with fiona.open(test_path, "r") as test_gpkg:
                with fiona.open(correct_filepath) as correct_gpkg:
                    print(test_gpkg.schema, correct_gpkg.schema)
                    assert test_gpkg.schema == correct_gpkg.schema

                    while True:
                        try:
                            assert next(test_gpkg) == next(correct_gpkg)
                        except StopIteration:
                            break 


    @pytest.mark.ckan_config("ckan.plugins", "datastore iotrans")
    @pytest.mark.usefixtures("clean_db", "with_plugins")
    def test_to_file_on_spatial_multigeometries(self):
        '''Checks if to_file creates correct non-spatial files'''

        # create datastore resource
        resource = factories.Resource()
        data = {
            "resource_id": resource["id"],
            "force": True,
            "records": [
                {"the year": 2014, "geometry": json.dumps({
                    "type": "LineString", 
                    "coordinates": [[-79.556501919627, 43.632603612711],[-79.526501959627, 43.632603612199]]
                })},
                {"the year": 2013, "geometry": json.dumps({
                    "type": "MultiLineString", 
                    "coordinates": [[[-79.556501959627, 43.632643612174],[-79.556501951227, 43.632611612174]], [[-79.556501569627, 43.632603645174],[-79.632603612174, 43.632603612174]]]
                })}
            ],
        }
        result = helpers.call_action("datastore_create", **data)
        
        # run to_file on datastore_resource
        target_formats = ["csv", "geojson"]
        target_epsgs = [4326, 2952]
        data = {
            "resource_id": resource["id"],
            "source_epsg": 4326,
            "target_epsgs": target_epsgs,
            "target_formats": target_formats,
        }
        result = helpers.call_action("to_file", **data)
        print(result)
        # check if outputs are correct
        for format in target_formats:
            for epsg in target_epsgs:
                test_path = result[format + "-" + str(epsg)]                

                # compare new file to correct file
                correct_filepath = (correct_dir_path + "correct_spatial"
                    " - {}.{}").format(epsg, format)

                assert filecmp.cmp(test_path, correct_filepath)
# TODO
# Multi geometries are managed
#   point line polygon and their multi versions
# SHP attributes are not lost
# SHP .txt mapping is correct
# test when data has linebreaks inside a text field
# large files of over 20000 records for spatial
