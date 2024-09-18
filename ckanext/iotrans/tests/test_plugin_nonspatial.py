'''Tests for ckanext-iotrans to run nonspatial functions 
in context of a CKAN instance'''
import pytest
import os

import ckan.tests.helpers as helpers
from .utils import csv_rows_eq, json_small, xml_eq, CORRECT_DIR_PATH


target_formats = [
    "csv",
    "json",
    "xml",
]

@pytest.mark.usefixtures("with_request_context")
class TestIOTransNonSpatial(object):

    @pytest.mark.ckan_config("ckan.plugins", "datastore iotrans")
    @pytest.mark.usefixtures("with_plugins")
    @pytest.mark.parametrize("file_format,compare_fn", [csv_rows_eq, json_small, xml_eq])
    def test_to_file_on_nonspatial_data(self, file_format, compare_fn , resource):
        '''Checks if to_file creates correct non-spatial files'''

        data = {
            "resource_id": resource["id"],
            "force": True,
            "records": [{"the year": 2014}, {"the year": 2013}],
        }
        result = helpers.call_action("datastore_create", **data)
        
        # run to_file on datastore_resource
        data = {
            "resource_id": resource["id"],
            "target_formats": target_formats,
        }
        result = helpers.call_action("to_file", **data)

        # check if outputs are correct
        test_path = result[f"{file_format}-None"]

        # compare new file to correct file
        correct_filepath = os.path.join(
            CORRECT_DIR_PATH,
            f"correct_nonspatial.{file_format}"
        )
        
        assert compare_fn(test_path, correct_filepath)

    
    @pytest.mark.ckan_config("ckan.plugins", "datastore iotrans")
    @pytest.mark.usefixtures("with_plugins")
    @pytest.mark.parametrize("file_format,compare_fn", [csv_rows_eq, json_small, xml_eq])
    def test_to_file_on_large_nonspatial_data(self, file_format, compare_fn, resource):
        '''Checks if to_file creates correct non-spatial files
        if the contents contain more than 20 000 records'''

        data = {
            "resource_id": resource["id"],
            "force": True,
            "records": [{"the attr": val} for val in range(0,21000)],
        }
        result = helpers.call_action("datastore_create", **data)
        
        # run to_file on datastore_resource
        data = {
            "resource_id": resource["id"],
            "target_formats": target_formats,
        }
        result = helpers.call_action("to_file", **data)

        # check if outputs are correct
        test_path = result[f"{file_format}-None"]

        # compare new file to correct file
        correct_filepath = os.path.join(
            CORRECT_DIR_PATH,
            f"correct_large_nonspatial.{file_format}"
        )
        assert compare_fn(test_path, correct_filepath)


    @pytest.mark.ckan_config("ckan.plugins", "datastore iotrans")
    @pytest.mark.usefixtures("with_plugins")
    @pytest.mark.parametrize("file_format,compare_fn", [csv_rows_eq, json_small, xml_eq])
    def test_to_file_on_nonspatial_data_w_linebreaks(self, file_format, compare_fn, resource):
        '''Checks if to_file creates correct non-spatial files
        if they have linebreaks in them'''

        data = {
            "resource_id": resource["id"],
            "force": True,
            "records": [{"the text": "some text with a line \r\t\n break"}, {"the text": """this is a text with some line breaks. Here's one now!\nAnd another one!"
                                                                                            2 whole line breaks wow"""}],
        }
        result = helpers.call_action("datastore_create", **data)
        
        # run to_file on datastore_resource
        data = {
            "resource_id": resource["id"],
            "target_formats": target_formats,
        }
        result = helpers.call_action("to_file", **data)

        # check if outputs are correct
        test_path = result[f"{file_format}-None"]

        # compare new file to correct file
        correct_filepath = os.path.join(
            CORRECT_DIR_PATH,
            f"correct_nonspatial_linebreaks.{file_format}",
        )
        assert compare_fn(test_path, correct_filepath)

