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
