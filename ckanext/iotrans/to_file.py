from abc import ABC
from typing import Callable, Generator
from typing import Dict, Any, Literal, List
from zipfile import ZipFile
import csv
import json
import os
import re
import sys
import xml.etree.cElementTree as ET

from fiona.crs import from_epsg
from pydantic import BaseModel
import fiona
import fiona.transform


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


from typing import TypedDict


class DatastoreResourceField:
    id: str
    type: str


GeometryType = Literal[
    "LineString",
    "MultiLineString",
    "MultiPoint",
    "MultiPolygon",
    "Point",
    "Polygon",
]


class DatastoreResourceMetadata(TypedDict):
    fields: List[DatastoreResourceField]
    name: str
    geometry_type: GeometryType


def is_fasley(arg: Any) -> bool:
    return arg in ["false", "False", False]


class ToFileHandler(ABC):

    def name(self) -> str:
        """name

        :raises NotImplementedError: abstract: this class does not implement
        :return: a string identifying this output (e.g. for the purpose of creating a
          mapping from str to outputpath in request responses)
        :rtype: str
        """
        raise NotImplementedError()

    def to_file(self, row_generator: Generator[Dict, None, None]) -> str:
        raise NotImplementedError()


class NonSpatialHandler(ToFileHandler):

    def __init__(
        self,
        target_format: NON_SPATIAL_TARGET_FORMAT,
        output_filepath: str,
        datastore_metadata: DatastoreResourceMetadata,
    ):
        self.target_format: NON_SPATIAL_TARGET_FORMAT = target_format
        self.output_filepath = output_filepath
        self.datastore_metadata = datastore_metadata

    def name(self) -> str:
        return self.target_format

    def to_file(self, row_generator: Generator[Dict, None, None]):
        handlers: Dict[NON_SPATIAL_TARGET_FORMAT, Callable] = {
            "csv": self.to_csv,
            "json": self.to_json,
            "xml": self.to_xml,
        }
        handler = handlers.get(self.target_format)
        if handler is None:
            raise ValueError(f"{self.target_format} is not an accepted target format")
        return handler(
            row_generator, self.output_filepath, self.datastore_metadata.fields
        )

    def _to_csv(
        self,
        row_generator: Generator[Dict, None, None],
    ) -> None:
        with open(self.output_filepath, "w") as csv_file:
            writer = csv.DictWriter(
                csv_file,
                fieldnames=[field["id"] for field in self.datastore_metadata.fields],
            )
            writer.writeheader()
            writer.writerows(row_generator)
        return self.output_filepath

    @staticmethod
    def _row_to_json_generator(
        csv: Generator[Dict, None, None]
    ) -> Generator[str, None, None]:
        n_rows = len(csv)
        i = 0
        for row in csv:
            json_str = json.dumps(row)
            i += 1
            if i < n_rows:
                json_str = f"{json_str},"
            yield json_str

    def _to_json(
        self,
        row_generator: Generator[Dict, None, None],
    ) -> None:
        with open(self.output_filepath, "w") as json_file:
            # write starting bracket
            json_file.write("[")
            json_file.writelines(self._row_to_json_generator(row_generator))
            json_file.write("]")
        return self.output_filepath

    def _to_xml(
        self,
        row_generator: Generator[Dict, None, None],
    ) -> None:
        XML_ENCODING = "utf-8"
        root_tag = "DATA"
        chunk_size = 5000

        with open(self.output_filepath, "w", encoding=XML_ENCODING) as xml_file:

            xml_file.write(f'<?xml version="1.0" encoding="{XML_ENCODING}"?>\n')
            xml_file.write(f"<{root_tag}>")
            i = 0

            # chunk writes to disk based on chunk_size so that:
            # 1. we don't do it all in one batch and end up w/ a MemoryError
            # 2. we don't perform disk io for every single record which is inefficient
            chunk = []

            for csv_row in row_generator:
                xml_row = ET.Element("ROW", count=str(i))
                for key, value in csv_row.items():
                    keyname = re.sub(r"[^a-zA-Z0-9-_]", "", key)
                    ET.SubElement(xml_row, keyname).text = value
                chunk.append(ET.tostring(xml_row, encoding="unicode"))

                i += 1

                if len(chunk) >= chunk_size:
                    xml_file.writelines(chunk)
                    chunk = []

            # Flush any rows in the remaining chunk
            if chunk:
                xml_file.writelines(chunk)

            xml_file.write(f"</{root_tag}>")
        return self.output_filepath


class SpatialHandler(ToFileHandler):

    def __init__(
        self,
        source_epsg: EPSG_TYPES,
        target_epsg: EPSG_TYPES,
        output_filepath: str,
        datastore_metadata: DatastoreResourceMetadata,
    ):
        self.source_epsg = source_epsg
        self.target_epsg = target_epsg
        self.output_filepath = output_filepath
        self.datastore_metadata = datastore_metadata

    def name(self) -> str:
        return f"{self.target_format} - {self.target_epsg}"

    @property
    def field_ids(self):
        return [field["id"] for field in self.datastore_metadata["fields"]]

    def to_file(self, input_row_generator: Generator[Dict, None, None]):
        transformed_geom = self.transform_geom(input_row_generator)
        formatted_row = self.format_row(transformed_geom)
        self.save_to_file(formatted_row)
        return self.output_filepath

    def transform_geom(
        self, row_generator: Generator[Dict, None, None]
    ) -> Generator[Dict, None, None]:
        for row in row_generator:
            geom = json.loads(row["geometry"])
            converted_geom = fiona.transform.transform_geom(
                from_epsg(self.source_epsg), from_epsg(self.target_epsg), geom
            )
            row["geometry"] = converted_geom
            yield row

    def format_row(
        self, row_generator: Generator[Dict, None, None]
    ) -> Generator[Dict, None, None]:
        # Default implementation is identity function (not transformation/modification)
        for row in row_generator:
            yield row

    def save_to_file(self, row_generator: Generator[Dict, None, None]):
        raise NotImplementedError("to_file not implemented")


class SpatialToCsv(SpatialHandler):

    def save_to_file(self, row_generator):
        csv.field_size_limit(sys.maxsize)
        with open(self.output_filepath, "w", encoding="utf-8") as csv_file:
            writer = csv.DictWriter(csv_file, fieldnames=self.field_ids)
            writer.writeheader()
            writer.writerows(row_generator)


class SpatialToSpatial(SpatialHandler):

    #############################
    # Private                   #
    #############################

    @staticmethod
    def _python_type_to_fiona_type(python_type: str) -> str:
        ckan_to_fiona_type_map = {
            "text": "str",
            "date": "str",
            "timestamp": "str",
            "float": "float",
            "int": "int",
            "numeric": "float",
            "time": "str",
        }
        no_chars = "".join([char for char in python_type if not char.isdigit()])
        return ckan_to_fiona_type_map[no_chars]

    def _get_col_map(self) -> Dict[str, str]:
        # by default no col-map (children can override)
        return {}

    def _get_schema(self) -> Dict[str, str]:
        geom_type_map = {
            "Point": "MultiPoint",
            "LineString": "MultiLineString",
            "Polygon": "MultiPolygon",
            "MultiPoint": "MultiPoint",
            "MultiLineString": "MultiLineString",
            "MultiPolygon": "MultiPolygon",
        }
        col_map = self._get_col_map()
        non_geom_fields = [
            field
            for field in self.datastore_metadata["fields"]
            if field["id"] != "geometry"
        ]

        properties = {}
        for field in non_geom_fields:
            key = col_map.get(field["id"], field["id"])
            properties[key] = self._python_type_to_fiona_type(field["type"])

        return {
            "geometry": geom_type_map[self.datastore_metadata.geometry_type],
            "properties": properties,
        }

    #############################
    # Hooks                     #
    #############################

    def format_row(self, row_generator):
        col_map = self._get_col_map()
        for row in row_generator:
            geometry = row["geometry"]
            properties = {}
            for key, value in row.items():
                mapped_key = col_map.get(key, key)
                properties[mapped_key] = value
            yield {
                "type": "Feature",
                "properties": properties,
                "geometry": geometry,
            }

    def save_to_file(self, row_generator):
        schema = self._get_schema()
        with fiona.open(
            self.output_filepath,
            "w",
            schema=schema,
            driver=FIONA_DRIVERS[self.target_format],
            crs=from_epsg(self.target_epsg),
        ) as outlayer:
            outlayer.writerecords(row_generator)


class SpatialToShp(SpatialToSpatial):

    def __init__(
        self,
        source_epsg: EPSG_TYPES,
        target_epsg: EPSG_TYPES,
        output_filepath: str,
        datastore_metadata: DatastoreResourceMetadata,
    ):
        assert target_epsg == "shp"
        super().__init__(source_epsg, target_epsg, output_filepath, datastore_metadata)

    #############################
    # Private                   #
    #############################

    def _get_col_map(self) -> Dict[str, str]:
        long_non_geom_fields = [
            field
            for field in self.field_id
            if (field != "geometry" and len(field) > 10)
        ]
        return {
            field: f"{field[:7]}{i+1}" for i, field in enumerate(long_non_geom_fields)
        }

    def _write_fields_file(self, path):
        col_map = self._get_col_map()
        with open(path, "w", encodoing="utf-8") as fields_file:
            writer = csv.DictWriter(fields_file, fieldnames=("field", "name"))
            writer.writeheader()
            writer.writerows(
                {"field": col_map.get(field_id, field_id), "name": field_id}
                for field_id in self.field_ids
            )

    def _zip_files(self, output_filepath: str, files: List[str]) -> None:

        out_path = f"{os.path.splitext(output_filepath)[0]}.zip"
        with ZipFile(out_path, "w") as zipfile:
            for file in files:
                zipfile.write(file)

        for file in files:
            os.remove(file)

    #############################
    # Hooks                     #
    #############################

    def save_to_file(self, row_generator):
        schema = self._get_schema()

        shp_folder = os.path.join(
            os.path.basename(os.path.join(self.output_filepath)),
            "tmp-shp",
        )
        os.makedirs(shp_folder)

        with fiona.open(
            shp_folder,
            "w",
            schema=schema,
            driver=FIONA_DRIVERS[self.target_format],
            crs=from_epsg(self.target_epsg),
        ) as outlayer:
            outlayer.writerecords(row_generator)

        shp_files = [
            file
            for file in os.listdir(shp_folder)
            if os.path.splitext(file)[1] in ["shp", "cpg", "dbf", "prj", "shx"]
        ]
        data_dict_path = os.path.join(
            shp_folder, f"{self.datastore_metadata.name}-data-dictionary.csv"
        )
        self._write_fields_file(data_dict_path)
        shp_files.append(data_dict_path)

        self._zip_files(self.output_filepath, shp_files)


def spatial_to_file_factory(
    params: ToFileParamsSpatial,
    out_dir: str,
    datastore_metadata: DatastoreResourceMetadata,
) -> List[SpatialHandler]:
    handler_map = {
        "shp": SpatialToShp,
        "csv": SpatialToCsv,
        # Defaults to SpatialToSpatial if not found
    }

    handlers = []
    for target_epsg in params.target_epsgs:
        for target_format in params.target_formats:

            output_filepath = os.path.join(
                out_dir, f"{datastore_metadata.name} - {target_epsg}.{target_format}"
            )

            handler = handler_map.get(target_format, SpatialToSpatial)(
                source_epsg=params.source_epsg,
                target_epsg=target_epsg,
                output_filepath=output_filepath,
                datastore_metadata=datastore_metadata,
            )

            handlers.append(handler)

    return handlers


def non_spatial_to_file_factory(
    params: ToFileParamsNonSpatial,
    out_dir: str,
    datastore_metadata: DatastoreResourceMetadata,
) -> List[NonSpatialHandler]:
    return [
        NonSpatialHandler(
            output_filepath=os.path.join(
                out_dir, f"{datastore_metadata.name}.{target_format}"
            ),
            target_format=target_format,
            datastore_metadata=datastore_metadata,
        )
        for target_format in params.target_formats
    ]


# class ToFileSpatialHandler:

#     def __init__(
#         self,
#         params: ToFileParamsSpatial,
#         resource: Dict,
#         datastore_resource: Dict,
#     ):
#         self.params = params
#         self.resource = resource
#         self.datastore_resource = datastore_resource

#     @staticmethod
#     def _python_type_to_fiona_type(python_type: str) -> str:
#         _CKAN_TO_FIONA_TYPE_MAP = {
#             "text": "str",
#             "date": "str",
#             "timestamp": "str",
#             "float": "float",
#             "int": "int",
#             "numeric": "float",
#             "time": "str",
#         }
#         no_chars = "".join([char for char in python_type if not char.isdigit()])
#         return _CKAN_TO_FIONA_TYPE_MAP[no_chars]

#     def _truncate_long_field_names(self, schema) -> Tuple[Dict, Optional[Dict]]:
#         field_ids = [field["id"] for field in self.datastore_resource["fields"]]

#         if not any([len(field_id) for field_id in field_ids]):
#             return schema, None

#         col_map = {field_id: field_id for field_id in field_ids}
#         schema["properties"]

#         non_geometry_fields = [
#             field
#             for field in self.datastore_resource["fields"]
#             if field["id"] != "geometry"
#         ]
#         file_count = 1
#         for field in non_geometry_fields:
#             name = f'{field["id"][:7]}{file_count}'
#             col_map[field["id"]] = name
#             # TODO: why are we stripping fiona types for python types? which is more suited
#             # to the geospatial context (I'd expect probably the geospatial types?)
#             schema["properties"][name] = _fiona_type_to_python_type(field["type"])
#             file_count += 1
#         return schema, col_map

#     def _get_fiona_schema(
#         self, datastore_resource, target_format: SPATIAL_TARGET_FORMAT
#     ) -> Tuple[Dict, Dict]:

#         # Get Point, Line, or Polygon from the first row of data
#         geom_type_map = {
#             "Point": "MultiPoint",
#             "LineString": "MultiLineString",
#             "Polygon": "MultiPolygon",
#             "MultiPoint": "MultiPoint",
#             "MultiLineString": "MultiLineString",
#             "MultiPolygon": "MultiPolygon",
#         }
#         # and convert to multi (ex point to multipoint) and single quotes to double quotes
#         geometry_type = geom_type_map[
#             json.loads(datastore_resource["records"][0]["geometry"])["type"]
#         ]

#         # Get all the field data types (other than geometry)
#         # Map them to fiona data types
#         fields_metadata = {
#             field["id"]: _CKAN_TO_FIONA_TYPE_MAP[
#                 "".join([char for char in field["type"] if not char.isdigit()])
#             ]
#             for field in datastore_resource["fields"]
#             if field["id"] != "geometry"
#         }
#         schema = {"geometry": geometry_type, "properties": fields_metadata}
#         col_map = None

#         ################
#         if target_format == "shp":
#             # By default, shp colnames are renamed FIELD_#
#             # ... if their name is more than 10 characters long
#             # We dont like that, so we truncate all fieldnames
#             # ... w concat'd increasing integer so no duplicates
#             # ... but only if there are colnames >= 10 chars
#             # We make a csv mapping truncated to full colnames

#             schema, col_map = self._truncate_long_field_names(
#                 schema, datastore_resource
#             )

#         return schema, col_map

#     def _get_schema(self) -> Dict[str, Any]:
#         geom_type_map = {
#             "Point": "MultiPoint",
#             "LineString": "MultiLineString",
#             "Polygon": "MultiPolygon",
#             "MultiPoint": "MultiPoint",
#             "MultiLineString": "MultiLineString",
#             "MultiPolygon": "MultiPolygon",
#         }
#         geometry = geom_type_map[
#             json.loads(self.datastore_resource["recourds"][0])["type"]
#         ]
#         non_geometry_fields = [
#             field
#             for field in self.datastore_resource["fields"]
#             if field["id"] != "geometry"
#         ]
#         properties = {
#             field["id"]: self._python_type_to_fiona_type(field["type"])
#             for field in non_geometry_fields
#         }
#         return {
#             "geometry": geometry,
#             "properties": properties,
#         }

#     def _get_transform_generator(csv_reader, source_epsg, target_epsg):
#         """Handles transforming coordinate system"""
#         for row in csv_reader:
#             geom = json.loads(row["geometry"])
#             converted_geom = transform_geom(
#                 from_epsg(source_epsg), from_epsg(target_epsg), geom
#             )
#             row["geometry"] = converted_geom
#             """
#             TODO/NOTE/ISSUE:
#             - for csv-type output: we want to yield the row
#             - for geospatial type objects:
#                 - we want to convert to a {type: Feature, properties:row, geometry: geometry} dict
#             - for shp files  output we may need to truncate names using col-map

#             Soln: pass these concerns to downstream generator
#             """
#             yield row

#     @staticmethod
#     def _row_to_geospatial_obj(row: Dict) -> Dict:
#         geometry = row["geometry"]
#         properties = {}
#         for key, value in row.items():
#             mapped_key = col_map.get(key, key)
#             properties[mapped_key] = value
#         return {
#             "type": "Feature",
#             "properties": properties,
#             "geometry": geometry,
#         }

#     @staticmethod
#     def _get_output_format_generator(generator, target_format: SPATIAL_TARGET_FORMAT):
#         """
#         Responsible for any convertions needed to a different `target_format`
#         """
#         converters: Dict[SPATIAL_TARGET_FORMAT, Callable] = {
#             # "csv": lambda x: x,
#             "geojson": lambda x: x,
#             "gpkg": lambda x: x,
#             "shp": lambda x: x,
#         }
#         converter = converters.get(target_format)
#         for row in generator:
#             if converter is not None:
#                 yield converter(row)
#             yield row

#     def _create_cached_file(
#         self,
#         csv_reader: csv.DictReader,
#         output_filepath: str,
#         source_epsg: EPSG_TYPES,
#         target_epsg: EPSG_TYPES,
#         target_format: SPATIAL_TARGET_FORMAT,
#     ) -> str:
#         schema = self._get_schema()
#         # transform coordinate system (if needed)
#         crs_transformed = self._get_transform_generator(
#             csv_reader, source_epsg, target_epsg
#         )
#         # transform object that ultimately gets written to file
#         formatted_for_file = self._get_output_format_generator(
#             crs_transformed, target_format
#         )

#         # What about csv?

#         with fiona.open(
#             output_filepath,
#             "w",
#             schema=schema,
#             driver=FIONA_DRIVERS[target_format],
#             crs=from_epsg(target_epsg),
#         ) as outlayer:
#             outlayer.writerecords(row_generator)
#         return output_filepath

#     def to_file(self, csv_file_path: str) -> Dict[str, str]:
#         # csv_file_path = to_csv_dump(self.params.resource_id, csv_generator)
#         output = {}
#         for target_epsg in self.params.target_espgs:
#             for target_format in self.params.target_formats:
#                 filepath = self._create_cached_file(
#                     csv.DictReader(csv_file_path),
#                     self.params.source_epsg,
#                     target_epsg,
#                     target_format,
#                 )
#                 output[f"{target_epsg}-{target_format}"] = filepath
#         return output


# def _to_file_spatial(
#     output_dir: str,
#     dump_filepath: str,
#     resource_metadata: Dict,
#     datastore_resource: Dict,
#     data: ToFileParamsSpatial,
# ) -> Dict[SPATIAL_TARGET_FORMAT, str]:
#     logging.info("[ckanext-iotrans] Geometric iotrans transformation started")

#     field_ids = [field["id"] for field in datastore_resource["fields"]]

#     FIONA_DRIVERS = {
#         "shp": "ESRI Shapefile",
#         "geojson": "GeoJSON",
#         "gpkg": "GPKG",
#     }
#     for target_epsg in data.target_epsgs:
#         for target_format in data.target_formats:
#             logging.info(
#                 "[ckanext-iotrans] starting {}-{}".format(
#                     target_format, str(target_epsg)
#                 )
#             )

#             # TODO .shp -> .zip... this feels wrong, perhaps refactor
#             file_extension = ".zip" if target_format == ".shp" else target_format
#             output_filepath = generic.get_filepath(
#                 output_dir, resource_metadata["name"], target_epsg, file_extension
#             )

#             csv_reader = csv.DictReader(dump_filepath, fieldnames=field_ids)
#             next(csv_reader)  # header

#             # TODO: we `transform_dump_epsg` even if taret_epsg is equivalent to
#             # source epsg to ensure consistent formatting
#             # Is this really necessary?
#             generator = generic.transform_epsg_generator(
#                 csv_reader, source_epsg=data.source_epsg, target_epsg=target_epsg
#             )

#             schema, col_map = _get_fiona_schema(datastore_resource, target_format)

#             generator = generic.dump_to_geospatial_generator(
#                 csv_reader,
#                 # field_ids,
#                 target_format,
#                 data.source_epsg,
#                 target_epsg,
#                 col_map=col_map,
#             )

#             if target_format == "csv":
#                 name, output_filepath = generic.write_to_csv(
#                     output_filepath, field_ids, generator
#                 )
#             elif target_format == "shp":
#                 name, output_filepath = generic.write_to_zipped_shapefile(
#                     field_ids,
#                     output_dir,
#                     resource_metadata,
#                     output_filepath,
#                     col_map,
#                 )

#             # if...
#             else:

#                 ## AFAICT the only difference between shp and non-shp:
#                 # - `schema` (vs. `working_schema`)
#                 # - what is happening with col_map

#                 """
#                 branches: csv, shp, other
#                 Options here:
#                 - each format gets its own function
#                 - template pattern
#                 -
#                 """

#                 schema, col_map = _get_fiona_schema(datastore_resource, target_format)

#                 generator = generic.dump_to_geospatial_generator(
#                     dump_filepath,
#                     field_ids,
#                     target_format,
#                     data.source_epsg,
#                     target_epsg,
#                     col_map=col_map,
#                 )

#                 with fiona.open(
#                     output_filepath,
#                     "w",
#                     schema=schema,
#                     driver=FIONA_DRIVERS[target_format],
#                     crs=from_epsg(target_epsg),
#                 ) as outlayer:
#                     outlayer.writerecords(generator)

#                 if target_format.lower() == "shp":
#                     # Shapefiles are special

#                     # By default, shapefiles are made of many files
#                     # We zip those files in a single zip

#                     # By default, shp colnames are renamed FIELD_#
#                     # ... if their name is more than 10 characters long

#                     # We dont like that, so we truncate all fieldnames
#                     # ... w concat'd increasing integer so no duplicates
#                     # ... but only if there are colnames >= 10 chars
#                     # We make a csv mapping truncated to full colnames

#                     output_filepath = generic.write_to_zipped_shapefile(
#                         field_ids,
#                         output_dir,
#                         resource_metadata,
#                         output_filepath,
#                         col_map,
#                     )

#                 output = generic.append_to_output(
#                     output, target_format, target_epsg, output_filepath
#                 )
