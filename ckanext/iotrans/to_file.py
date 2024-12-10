"""the to_file() and prune() functions
These function are the top level logic for this extension's CKAN actions
"""

import ckan.plugins.toolkit as tk
from ckan.common import config
import csv

import tempfile
import shutil
import os
import json
import fiona
import logging
from fiona.crs import from_epsg
from ..utils import generic
from datetime import datetime
from memory_profiler import memory_usage, profile

from pydantic import BaseModel, ValidationError

from typing import Dict, Any, Literal, List, Tuple, Optional
from typing import TypedDict

EPSG_TYPES = Literal[4326, 2952]

SPATIAL_TARGET_FORMAT = Literal["shp", "geojson", "gpkg", "csv"]
NON_SPATIAL_TARGET_FORMAT = Literal["csv", "json", "xml"]

FIONA_DRIVERS = {
    "shp": "ESRI Shapefile",
    "geojson": "GeoJSON",
    "gpkg": "GPKG",
}


class ToFileParamsSpatial(BaseModel):
    resource_id: str
    # TODO is this really a list of possible source targets? how do we know which is which?
    source_epsg: EPSG_TYPES
    target_epsgs: List[EPSG_TYPES]
    target_formats: List[SPATIAL_TARGET_FORMAT]


class ToFileParamsNonSpatial(BaseModel):
    resource_id: str
    target_formats: List[NON_SPATIAL_TARGET_FORMAT]


_CKAN_TO_FIONA_TYPE_MAP = {
    "text": "str",
    "date": "str",
    "timestamp": "str",
    "float": "float",
    "int": "int",
    "numeric": "float",
    "time": "str",
}


def _fiona_type_to_python_type(fiona_type: str) -> str:
    type_digits_removed = "".join([char for char in fiona_type if not char.isdigit()])
    return _CKAN_TO_FIONA_TYPE_MAP[type_digits_removed]


def _is_fasley(arg: Any) -> bool:
    return arg in ["false", "False", False]


def _to_file_spatial_shp(
    output_filepath: str,
    schema,
    target_format: SPATIAL_TARGET_FORMAT,
    target_epsg: EPSG_TYPES,
):
    with fiona.open(
        output_filepath,
        "w",
        schema=schema,
        driver=FIONA_DRIVERS[target_format],
        crs=from_epsg(target_epsg),
    ) as outlayer:
        outlayer.writerecords(
            generic.dump_to_geospatial_generator(
                dump_filepath,
                field_ids,
                target_format,
                data.source_epsg,
                target_epsg,
            )
        )


def get_resource_generator(context, resource_id: str, chunk_size: int):
    # TODO: warn/error if chunk_size greater than ckan.datastore.search.rows_max?
    i = 0
    while True:
        # get a chunk of records from datastore resource
        records = tk.get_action("datastore_search")(
            context,
            {
                "resource_id": resource_id,
                "limit": chunk_size,
                "offset": chunk_size * i,
            },
        )["records"]

        if not len(records):
            break

        for record in records:
            yield record
        i += 1


def to_csv_dump(resource_id, generator) -> str:
    tmp = tempfile.mkdtemp(dir=config.get("ckan.storage_path"))
    filepath = os.path.join(tmp, resource_id)
    with open(filepath, "w") as file:
        file.writelines(generator)
    return filepath


from typing import Callable


from fiona.transform import transform_geom


class ToFileSpatialHandler:

    def __init__(
        self,
        params: ToFileParamsSpatial,
        resource: Dict,
        datastore_resource: Dict,
    ):
        self.params = params
        self.resource = resource
        self.datastore_resource = datastore_resource

    @staticmethod
    def _python_type_to_fiona_type(python_type: str) -> str:
        _CKAN_TO_FIONA_TYPE_MAP = {
            "text": "str",
            "date": "str",
            "timestamp": "str",
            "float": "float",
            "int": "int",
            "numeric": "float",
            "time": "str",
        }
        no_chars = "".join([char for char in python_type if not char.isdigit()])
        return _CKAN_TO_FIONA_TYPE_MAP[no_chars]

    def _truncate_long_field_names(self, schema) -> Tuple[Dict, Optional[Dict]]:
        field_ids = [field["id"] for field in self.datastore_resource["fields"]]

        if not any([len(field_id) for field_id in field_ids]):
            return schema, None

        col_map = {field_id: field_id for field_id in field_ids}
        schema["properties"]

        non_geometry_fields = [
            field
            for field in self.datastore_resource["fields"]
            if field["id"] != "geometry"
        ]
        file_count = 1
        for field in non_geometry_fields:
            name = f'{field["id"][:7]}{file_count}'
            col_map[field["id"]] = name
            # TODO: why are we stripping fiona types for python types? which is more suited
            # to the geospatial context (I'd expect probably the geospatial types?)
            schema["properties"][name] = _fiona_type_to_python_type(field["type"])
            file_count += 1
        return schema, col_map

    def _get_fiona_schema(
        self, datastore_resource, target_format: SPATIAL_TARGET_FORMAT
    ) -> Tuple[Dict, Dict]:

        # Get Point, Line, or Polygon from the first row of data
        geom_type_map = {
            "Point": "MultiPoint",
            "LineString": "MultiLineString",
            "Polygon": "MultiPolygon",
            "MultiPoint": "MultiPoint",
            "MultiLineString": "MultiLineString",
            "MultiPolygon": "MultiPolygon",
        }
        # and convert to multi (ex point to multipoint) and single quotes to double quotes
        geometry_type = geom_type_map[
            json.loads(datastore_resource["records"][0]["geometry"])["type"]
        ]

        # Get all the field data types (other than geometry)
        # Map them to fiona data types
        fields_metadata = {
            field["id"]: _CKAN_TO_FIONA_TYPE_MAP[
                "".join([char for char in field["type"] if not char.isdigit()])
            ]
            for field in datastore_resource["fields"]
            if field["id"] != "geometry"
        }
        schema = {"geometry": geometry_type, "properties": fields_metadata}
        col_map = None

        ################
        if target_format == "shp":
            # By default, shp colnames are renamed FIELD_#
            # ... if their name is more than 10 characters long
            # We dont like that, so we truncate all fieldnames
            # ... w concat'd increasing integer so no duplicates
            # ... but only if there are colnames >= 10 chars
            # We make a csv mapping truncated to full colnames

            schema, col_map = self._truncate_long_field_names(
                schema, datastore_resource
            )

        return schema, col_map

    def _get_schema(self) -> Dict[str, Any]:
        geom_type_map = {
            "Point": "MultiPoint",
            "LineString": "MultiLineString",
            "Polygon": "MultiPolygon",
            "MultiPoint": "MultiPoint",
            "MultiLineString": "MultiLineString",
            "MultiPolygon": "MultiPolygon",
        }
        geometry = geom_type_map[
            json.loads(self.datastore_resource["recourds"][0])["type"]
        ]
        non_geometry_fields = [
            field
            for field in self.datastore_resource["fields"]
            if field["id"] != "geometry"
        ]
        properties = {
            field["id"]: self._python_type_to_fiona_type(field["type"])
            for field in non_geometry_fields
        }
        return {
            "geometry": geometry,
            "properties": properties,
        }

    def _get_transform_generator(csv_reader, source_epsg, target_epsg):
        """Handles transforming coordinate system"""
        for row in csv_reader:
            geom = json.loads(row["geometry"])
            converted_geom = transform_geom(
                from_epsg(source_epsg), from_epsg(target_epsg), geom
            )
            row["geometry"] = converted_geom
            """
            TODO/NOTE/ISSUE:
            - for csv-type output: we want to yield the row
            - for geospatial type objects:
                - we want to convert to a {type: Feature, properties:row, geometry: geometry} dict
            - for shp files  output we may need to truncate names using col-map

            Soln: pass these concerns to downstream generator
            """
            yield row

    @staticmethod
    def _row_to_geospatial_obj(row: Dict) -> Dict:
        geometry = row["geometry"]
        properties = {}
        for key, value in row.items():
            mapped_key = col_map.get(key, key)
            properties[mapped_key] = value
        return {
            "type": "Feature",
            "properties": properties,
            "geometry": geometry,
        }

    @staticmethod
    def _get_output_format_generator(generator, target_format: SPATIAL_TARGET_FORMAT):
        """
        Responsible for any convertions needed to a different `target_format`
        """
        converters: Dict[SPATIAL_TARGET_FORMAT, Callable] = {
            # "csv": lambda x: x,
            "geojson": lambda x: x,
            "gpkg": lambda x: x,
            "shp": lambda x: x,
        }
        converter = converters.get(target_format)
        for row in generator:
            if converter is not None:
                yield converter(row)
            yield row

    def _create_cached_file(
        self,
        csv_reader: csv.DictReader,
        output_filepath: str,
        source_epsg: EPSG_TYPES,
        target_epsg: EPSG_TYPES,
        target_format: SPATIAL_TARGET_FORMAT,
    ) -> str:
        schema = self._get_schema()
        # transform coordinate system (if needed)
        crs_transformed = self._get_transform_generator(
            csv_reader, source_epsg, target_epsg
        )
        # transform object that ultimately gets written to file
        formatted_for_file = self._get_output_format_generator(
            crs_transformed, target_format
        )

        # What about csv?

        with fiona.open(
            output_filepath,
            "w",
            schema=schema,
            driver=FIONA_DRIVERS[target_format],
            crs=from_epsg(target_epsg),
        ) as outlayer:
            outlayer.writerecords(row_generator)
        return output_filepath

    def to_file(self, csv_file_path: str) -> Dict[str, str]:
        # csv_file_path = to_csv_dump(self.params.resource_id, csv_generator)
        output = {}
        for target_epsg in self.params.target_espgs:
            for target_format in self.params.target_formats:
                filepath = self._create_cached_file(
                    csv.DictReader(csv_file_path),
                    self.params.source_epsg,
                    target_epsg,
                    target_format,
                )
                output[f"{target_epsg}-{target_format}"] = filepath
        return output


def _to_file_spatial(
    output_dir: str,
    dump_filepath: str,
    resource_metadata: Dict,
    datastore_resource: Dict,
    data: ToFileParamsSpatial,
) -> Dict[SPATIAL_TARGET_FORMAT, str]:
    logging.info("[ckanext-iotrans] Geometric iotrans transformation started")

    field_ids = [field["id"] for field in datastore_resource["fields"]]

    FIONA_DRIVERS = {
        "shp": "ESRI Shapefile",
        "geojson": "GeoJSON",
        "gpkg": "GPKG",
    }
    for target_epsg in data.target_epsgs:
        for target_format in data.target_formats:
            logging.info(
                "[ckanext-iotrans] starting {}-{}".format(
                    target_format, str(target_epsg)
                )
            )

            # TODO .shp -> .zip... this feels wrong, perhaps refactor
            file_extension = ".zip" if target_format == ".shp" else target_format
            output_filepath = generic.get_filepath(
                output_dir, resource_metadata["name"], target_epsg, file_extension
            )

            csv_reader = csv.DictReader(dump_filepath, fieldnames=field_ids)
            next(csv_reader)  # header

            # TODO: we `transform_dump_epsg` even if taret_epsg is equivalent to
            # source epsg to ensure consistent formatting
            # Is this really necessary?
            generator = generic.transform_epsg_generator(
                csv_reader, source_epsg=data.source_epsg, target_epsg=target_epsg
            )

            schema, col_map = _get_fiona_schema(datastore_resource, target_format)

            generator = generic.dump_to_geospatial_generator(
                csv_reader,
                # field_ids,
                target_format,
                data.source_epsg,
                target_epsg,
                col_map=col_map,
            )

            if target_format == "csv":
                name, output_filepath = generic.write_to_csv(
                    output_filepath, field_ids, generator
                )
            elif target_format == "shp":
                name, output_filepath = generic.write_to_zipped_shapefile(
                    field_ids,
                    output_dir,
                    resource_metadata,
                    output_filepath,
                    col_map,
                )

            # if...
            else:

                ## AFAICT the only difference between shp and non-shp:
                # - `schema` (vs. `working_schema`)
                # - what is happening with col_map

                """
                branches: csv, shp, other
                Options here:
                - each format gets its own function
                - template pattern
                -
                """

                schema, col_map = _get_fiona_schema(datastore_resource, target_format)

                generator = generic.dump_to_geospatial_generator(
                    dump_filepath,
                    field_ids,
                    target_format,
                    data.source_epsg,
                    target_epsg,
                    col_map=col_map,
                )

                with fiona.open(
                    output_filepath,
                    "w",
                    schema=schema,
                    driver=FIONA_DRIVERS[target_format],
                    crs=from_epsg(target_epsg),
                ) as outlayer:
                    outlayer.writerecords(generator)

                if target_format.lower() == "shp":
                    # Shapefiles are special

                    # By default, shapefiles are made of many files
                    # We zip those files in a single zip

                    # By default, shp colnames are renamed FIELD_#
                    # ... if their name is more than 10 characters long

                    # We dont like that, so we truncate all fieldnames
                    # ... w concat'd increasing integer so no duplicates
                    # ... but only if there are colnames >= 10 chars
                    # We make a csv mapping truncated to full colnames

                    output_filepath = generic.write_to_zipped_shapefile(
                        field_ids,
                        output_dir,
                        resource_metadata,
                        output_filepath,
                        col_map,
                    )

                output = generic.append_to_output(
                    output, target_format, target_epsg, output_filepath
                )
