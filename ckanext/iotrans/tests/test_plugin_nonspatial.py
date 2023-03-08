'''Tests for ckanext-iotrans to run nonspatial functions 
in context of a CKAN instance'''


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
        '''Checks if to_file creates correct non-spatial files
        if the contents contain more than 20 000 records'''

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
    def test_to_file_on_nonspatial_data_w_linebreaks(self):
        '''Checks if to_file creates correct non-spatial files
        if they have linebreaks in them'''

        # create datastore resource
        resource = factories.Resource()
        data = {
            "resource_id": resource["id"],
            "force": True,
            "records": [{"the text": "some text with a line \r\t\n break"}, {"the text": """this is a text with some line breaks. Here's one now!\nAnd another one!"
                                                                                            2 whole line breaks wow"""}],
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
            correct_filepath = correct_dir_path + "correct_nonspatial_linebreaks." + format
            assert filecmp.cmp(test_path, correct_filepath)

