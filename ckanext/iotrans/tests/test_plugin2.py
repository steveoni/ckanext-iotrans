'''Tests for ckanext-iotrans to run in context of a CKAN instance'''


import json
import pytest
import filecmp
import os

import ckan.plugins as p
import ckan.tests.factories as factories
import ckan.tests.helpers as helpers

# Define fixed variables
test_dir_path = os.path.dirname(os.path.realpath(__file__)) + "/correct_files/"


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
            correct_filepath = test_dir_path + "correct_nonspatial." + format
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
            correct_filepath = test_dir_path + "correct_large_nonspatial." + format
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
        print(result)
        # check if outputs are correct
        for format in target_formats:
            for epsg in target_epsgs:
                test_path = result[format + "-" + str(epsg)]
                print(test_path)

                # compare new file to correct file
                correct_filepath = (test_dir_path + "correct_spatial"
                    " - {}.{}").format(epsg, format)

                assert filecmp.cmp(test_path, correct_filepath)
            

# TODO
# make a fixture or something that creates a spatial datastore dataset
# make a fixture or something that creates a non-spatial datastore dataset
# test gpkg, shp creation
# Multi geometries are managed
#   point line polygon and their multi versions
# SHP attributes are not lost
# SHP .txt mapping is correct
# test when data has linebreaks inside a text field
# large files of over 20000 records for spatial and non spatial
