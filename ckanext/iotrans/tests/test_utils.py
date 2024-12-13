"""
Test module for various iotrans functions
"""

import csv
import filecmp
import json
import os

import pytest

import ckanext.iotrans.utils as utils

from .utils import CORRECT_DIR_PATH, TEST_TMP_PATH

# Define fixtures


@pytest.fixture
def test_dump_json_filepath():
    # create filepath string
    correct_dump_csv_filepath = os.path.join(CORRECT_DIR_PATH, "correct_dump.csv")
    filepath = os.path.join(CORRECT_DIR_PATH, "test_dump.json")

    # delete existing test file if it exists
    if os.path.exists(filepath):
        os.remove(filepath)

    # create test file
    with open(
        os.path.join(CORRECT_DIR_PATH, "correct_datastore_resource.json")
    ) as jsonfile:
        correct_datastore_resource = json.load(jsonfile)
        utils.write_to_json(
            correct_dump_csv_filepath, filepath, correct_datastore_resource
        )

    # return location of test file
    return filepath


@pytest.fixture
def test_dump_xml_filepath():
    # create filepath string
    correct_dump_csv_filepath = os.path.join(CORRECT_DIR_PATH, "correct_dump.csv")
    filepath = os.path.join(CORRECT_DIR_PATH, "test_dump.xml")

    # delete existing test file if it exists
    if os.path.exists(filepath):
        os.remove(filepath)

    # create test file
    utils.write_to_xml(correct_dump_csv_filepath, filepath)

    # return test file location
    return filepath


@pytest.fixture
def correct_geospatial_generator():
    correct_spatial_dump_csv_filepath = os.path.join(
        CORRECT_DIR_PATH, "correct_geo_dump.csv"
    )
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

    return utils.dump_to_geospatial_generator(
        correct_spatial_dump_csv_filepath,
        correct_spatial_csv_dump_fieldnames,
        "geojson",
        4326,
        2952,
    )


def test_create_filepath_with_epsg():
    """test case for utils.create_filepath with an input epsg"""
    correct_filepath_with_epsg = os.path.join(TEST_TMP_PATH, "resource_name - 4326.csv")
    test_filepath_with_epsg = utils.get_filepath(
        TEST_TMP_PATH, "resource_name", 4326, "csv"
    )

    assert correct_filepath_with_epsg == test_filepath_with_epsg


def test_create_filepath_without_epsg():
    """test case for utils.create_filepath without an input epsg"""
    correct_filepath_without_epsg = os.path.join(TEST_TMP_PATH, "resource_name.csv")
    test_filepath_no_epsg = utils.get_filepath(
        TEST_TMP_PATH, "resource_name", None, "csv"
    )

    assert correct_filepath_without_epsg == test_filepath_no_epsg


def test_write_to_xml(test_dump_xml_filepath):
    """test case for utils.write_to_xml"""
    correct_dump_xml_filepath = os.path.join(CORRECT_DIR_PATH, "correct_dump.xml")
    assert filecmp.cmp(test_dump_xml_filepath, correct_dump_xml_filepath)


def test_dump_to_geospatial_generator(correct_geospatial_generator):
    """checks if generator made by utils.dump_to_geospatial_generator
    contains dicts with valid, non-empty data"""
    for item in correct_geospatial_generator:
        assert isinstance(item["properties"], dict)
        assert len(item["properties"])
        assert all(x in item["geometry"] for x in ("coordinates", "type"))
        assert len(item["geometry"])


_valid_geometry = json.dumps({"type": "Point", "coordinates": [-122.4194, 37.7749]})


@pytest.mark.parametrize(
    "csv_data",
    (
        # "geometry": "None" tests a case of transform_epsg resulting in a `None`
        # geometry
        [{"_id": 1, "the year": "2024", "geometry": "None"}],
        # valid geojson in 'geometry' cell
        [{"_id": 1, "the year": "2024", "geometry": _valid_geometry}],
        # Multiple rows. Some with a valid geometry, some without
        [
            {"_id": 1, "the year": "2024", "geometry": _valid_geometry},
            {"_id": 2, "the year": "1970", "geometry": "None"},
            {"_id": 3, "the year": "2025", "geometry": _valid_geometry},
        ],
    ),
)
def test_transform_dump_epsg(tmp_path_factory, csv_data):
    dir_path = tmp_path_factory.mktemp("test-tmp")
    dump_suffix = "csv-dump"
    dump_filepath = utils.get_filepath(dir_path, "test-resource", 4326, dump_suffix)

    fieldnames = csv_data[0].keys()

    with open(dump_filepath, "w") as file:
        writer = csv.DictWriter(
            file,
            fieldnames=fieldnames,
        )
        writer.writeheader()
        writer.writerows(csv_data)
    source_epsg = 4326
    target_epsg = 4326
    result = [
        x
        for x in utils.transform_dump_epsg(
            dump_filepath=dump_filepath,
            fieldnames=fieldnames,
            source_epsg=source_epsg,
            target_epsg=target_epsg,
        )
    ]
    assert len(result) == len(csv_data)
