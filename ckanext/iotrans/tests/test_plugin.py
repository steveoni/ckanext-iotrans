"""
Tests for plugin.py.

Tests are written using the pytest library (https://docs.pytest.org), and you
should read the testing guidelines in the CKAN docs:
https://docs.ckan.org/en/2.9/contributing/testing.html

To write tests for your extension you should install the pytest-ckan package:

    pip install pytest-ckan

This will allow you to use CKAN specific fixtures on your tests.

For instance, if your test involves database access you can use `clean_db` to
reset the database:

    import pytest

    from ckan.tests import factories

    @pytest.mark.usefixtures("clean_db")
    def test_some_action():

        dataset = factories.Dataset()

        # ...

For functional tests that involve requests to the application, you can use the
`app` fixture:

    from ckan.plugins import toolkit

    def test_some_endpoint(app):

        url = toolkit.url_for('myblueprint.some_endpoint')

        response = app.get(url)

        assert response.status_code == 200


To temporary patch the CKAN configuration for the duration of a test, use:

    import pytest

    @pytest.mark.ckan_config("ckanext.myext.some_key", "some_value")
    def test_some_action():
        pass
"""
import ckanext.iotrans.utils as utils
import ckanext.iotrans.iotrans as iotrans
import filecmp
import json
import os
import pytest
import ckan.tests.helpers as helpers
import ckan.tests.factories as factories
import ckan.model as model
import six


test_dir_path = os.path.dirname(os.path.realpath(__file__))
test_tmp_path = "/tmp/iotrans_test_folder/"

correct_filepath_with_epsg = test_tmp_path + "resource_name - 4326.csv"
correct_filepath_without_epsg = test_tmp_path + "resource_name.csv"

test_filepath_with_epsg = utils.create_filepath(test_tmp_path + "",
                                                "resource_name", 4326, "csv")
test_filepath_no_epsg = utils.create_filepath(test_tmp_path + "",
                                              "resource_name", None, "csv")

correct_spatial_dump_csv_filepath = test_dir_path + "/correct_geo_dump.csv"
correct_spatial_csv_dump_fieldnames = [
    "service_system_manager",
    "agency",
    "loc_id",
    "program_name",
    "serviceName",
    "buildingName",
    "address",
    "full_address",
    "major_intersection",
    "ward",
    "ward_name",
    "located_in_school",
    "school_name",
    "geometry",
    "centre_type",
]
correct_dump_csv_filepath = test_dir_path + "/correct_dump.csv"
correct_dump_json_filepath = test_dir_path + "/correct_dump.json"
correct_dump_xml_filepath = test_dir_path + "/correct_dump.xml"

test_dump_json_filepath = test_dir_path + "/test_dump.json"
test_dump_xml_filepath = test_dir_path + "/test_dump.xml"
test_dump_csv_filepath = test_dir_path + "/test_dump.csv"

with open(test_dir_path + "/correct_datastore_resource.json") as jsonfile:
    correct_datastore_resource = json.load(jsonfile)
    utils.write_to_json(correct_dump_csv_filepath,
                        test_dump_json_filepath,
                        correct_datastore_resource)

utils.write_to_xml(correct_dump_csv_filepath, test_dump_xml_filepath)

correct_geospatial_generator = utils.dump_to_geospatial_generator(
    correct_spatial_dump_csv_filepath,
    correct_spatial_csv_dump_fieldnames,
    "geojson",
    4326,
    2952
)


def test_create_filepath_with_epsg():
    """test case for utils.create_filepath with an input epsg"""
    assert correct_filepath_with_epsg == test_filepath_with_epsg


def test_create_filepath_without_epsg():
    """test case for utils.create_filepath without an input epsg"""
    assert correct_filepath_without_epsg == test_filepath_no_epsg

def test_write_to_json():
    """test case for utils.write_to_json"""
    assert filecmp.cmp(test_dump_json_filepath, correct_dump_json_filepath)


def test_write_to_xml():
    """test case for utils.write_to_xml"""
    assert filecmp.cmp(test_dump_json_filepath, correct_dump_json_filepath)


def test_dump_to_geospatial_generator():
    """checks if generator made by utils.dump_to_geospatial_generator
    contains dicts with valid, non-empty data"""
    for item in correct_geospatial_generator:
        assert isinstance(item["properties"], dict)
        assert len(item["properties"])
        assert isinstance(item["geometry"], dict)
        assert len(item["geometry"])
