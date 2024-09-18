'''Tests for ckanext-iotrans to run spatial functions
in context of a CKAN instance'''


import json
import pytest
import fiona
import zipfile
import os
import ckan.tests.helpers as helpers
from .utils import (
    CORRECT_DIR_PATH, csv_rows_eq, geojson_small, fiona_collections_eq,
    geographic_files_eq
)
import filecmp

target_formats = ["csv", "geojson"]

@pytest.mark.usefixtures("with_request_context")
class TestIOTransSpatial(object):

    @pytest.mark.ckan_config("ckan.plugins", "datastore iotrans")
    @pytest.mark.usefixtures("with_plugins")
    @pytest.mark.parametrize("file_format", target_formats)
    def test_to_file_on_spatial_data_human_readable_formats(self, file_format, resource):
        '''Checks if to_file creates correct spatial files'''

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
        
        target_epsgs = [
            4326,
            2952
        ]
        data = {
            "resource_id": resource["id"],
            "source_epsg": 4326,
            "target_epsgs": target_epsgs,
            "target_formats": [file_format],
        }
        result = helpers.call_action("to_file", **data)        
        # check if outputs are correct
        for epsg in target_epsgs:
            test_path = result[f"{file_format}-{epsg}"]

            # compare new file to correct file
            correct_filepath = os.path.join(
                CORRECT_DIR_PATH,
                f"correct_spatial - {epsg}.{file_format}"
            )
            assert filecmp.cmp(test_path, correct_filepath)
            
    @pytest.mark.ckan_config("ckan.plugins", "datastore iotrans")
    @pytest.mark.usefixtures("with_plugins")
    def test_to_file_on_shapefile(self, resource):
        '''Checks if to_file creates a shapefile correctly
        This checks:
        - the shapefile's .zip files contents (excluding .dbf)
        - that the records and attribute names are correct
        - that the mapping .txt included in the .zip is correct
        '''

        resource_name = resource["name"]
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

        # check if outputs are correct
        # dbf files cant be compared this way, so we compare records below

        # "format": comparison_function
        shp_components = {
            # files that can be loaded directly: compare values as file-based comparson
            # is error-prone
            "shp": geographic_files_eq,
            "shx": geographic_files_eq,
            # Not able to load these in w/ fiona so resort to file comparison
            "cpg": filecmp.cmp,
            "prj": filecmp.cmp,
        }
        
        for epsg in target_epsgs:
            zip_path = result[f"shp-{epsg}"]
            with zipfile.ZipFile(zip_path, "r") as thiszip:
                # extract zip contents to their current /tmp dir
                test_folder = os.path.dirname(zip_path)
                thiszip.extractall(test_folder)

                for file_format, compare_fn in shp_components.items():
                    # compare new file to correct file
                    file_ending = f" - {epsg}.{file_format}"
                    correct_filepath = os.path.join(CORRECT_DIR_PATH, f"correct_spatial{file_ending}")
                    test_path = os.path.join(test_folder, f"{resource_name}{file_ending}")
                    assert compare_fn(test_path, correct_filepath)

                # make sure txt mapping file is correct
                test_txt = os.path.join(test_folder, f"{resource_name} fields.csv")
                correct_txt = os.path.join(CORRECT_DIR_PATH, "correct_spatial fields.csv")
                assert filecmp.cmp(test_txt, correct_txt)


    @pytest.mark.ckan_config("ckan.plugins", "datastore iotrans")
    @pytest.mark.usefixtures("with_plugins")
    def test_to_file_on_gpkg(self, resource):
        '''Checks if to_file creates correct gpkg file'''

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

        # check if outputs are correct        
        for epsg in target_epsgs:
            test_path = result[f"gpkg-{epsg}"]

            correct_filepath = os.path.join(CORRECT_DIR_PATH, f"correct_spatial - {epsg}.gpkg")

            with fiona.open(test_path, "r") as test_gpkg:
                with fiona.open(correct_filepath) as correct_gpkg:                    
                    assert fiona_collections_eq(test_gpkg, correct_gpkg, 0.98)


    @pytest.mark.ckan_config("ckan.plugins", "datastore iotrans")
    @pytest.mark.usefixtures("with_plugins")
    def test_to_file_on_spatial_multigeometries(self, resource):
        '''Checks if to_file creates correct spatial files
        if their geometries include multi-geometries'''

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
        # check if outputs are correct
        for file_format in target_formats:
            for epsg in target_epsgs:
                test_path = result[f"{file_format}-{epsg}"]

                # compare new file to correct file
                correct_filepath = os.path.join(
                    CORRECT_DIR_PATH,
                    f"correct_spatial_multigeometry - {epsg}.{file_format}"
                )

                assert filecmp.cmp(test_path, correct_filepath)
    

    @pytest.mark.ckan_config("ckan.plugins", "datastore iotrans")
    @pytest.mark.usefixtures("with_plugins")
    def test_to_file_on_human_readable_spatial_data_w_linebreaks(self, resource):
        '''Checks if to_file creates correct CSV and GEOJSON spatial files
        if the files contain linebreaks'''

        data = {
            "resource_id": resource["id"],
            "force": True,
            "records": [
                {
                    "the text": "some text with a line \r\t\n break", 
                    "geometry": json.dumps({
                        "type": "Point", 
                        "coordinates": [-79.156501959987, 43.232603612123]
                    }),
                },
                {
                    "the text": """this is a text with some line breaks. Here's one now!\nAnd another one!"
                                                                                            2 whole line breaks wow""",
                    "geometry": json.dumps({
                        "type": "Point", 
                        "coordinates": [-79.956501959345, 43.932603612987]
                    }),
                }
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
        for file_format in target_formats:
            for epsg in target_epsgs:
                test_path = result[f"{file_format}-{epsg}"]

                # compare new file to correct file
                correct_filepath = os.path.join(
                    CORRECT_DIR_PATH,
                    f"correct_spatial_linebreaks - {epsg}.{file_format}",
                )

                assert filecmp.cmp(test_path, correct_filepath)


    @pytest.mark.ckan_config("ckan.plugins", "datastore iotrans")
    @pytest.mark.usefixtures("with_plugins")
    def test_to_file_on_spatial_data_empty_coordinates(self, resource):
        '''Checks if to_file behaves correct if input has empty geometry
        Specifically, it checks if geometry is None, or has empty coords'''

        data = {
            "resource_id": resource["id"],
            "force": True,
            "records": [
                {"the year": 2014, "geometry": json.dumps({
                    "type": "Point", 
                    "coordinates": [0,0]
                })},
                {"the year": 2012, "geometry": json.dumps({
                    "type": "Point", 
                    "coordinates": [None,None]
                })}
            ]
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
        for file_format in target_formats:
            for epsg in target_epsgs:
                test_path = result[f"{file_format}-{epsg}"]

                # compare new file to correct file
                correct_filepath = os.path.join(
                    CORRECT_DIR_PATH,
                    f"correct_empty_spatial - {epsg}.{file_format}"
                )

                assert filecmp.cmp(test_path, correct_filepath)
