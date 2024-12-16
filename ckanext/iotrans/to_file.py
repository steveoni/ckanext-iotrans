"""
to_file.py
Contains main algorithms for the to_file CKAN Action.

# Overview:
## the CKAN action function itself (not this module) performs:
    - input validation
    - calls ckan actions
    - formatting of responses back to end-users (e.g. validation errors, json etc.)
    - (effectively anything CKAN specific so _this_ module doesn't need to have any
      dependency on CKAN apis)
## This module:
- types and utils
- a class structure defining a Template Method pattern for handling to_file for
    2 dimensions:
    - spatial vs. non-spatial
    - target format
    - see here for detail on Template Method pattern:
      https://refactoring.guru/design-patterns/template-method
- Factory methods to produce concrete instances of the implemented Template classes
https://refactoring.guru/design-patterns/creational-patterns
    - in other words: functions that return lists of objects that implement to_file
      individually for their respective spatial/non-spatial, source_format, and
      target_format combinations.

# Class Structure:
- the `to_file` action returns `Dict[str, str]`. There are 2 outputs here per cached
  file produced (the key and the value)
  - the key: something like `shp-2952` -> some identifier that includes what file type
    and, if spatial, what projection.
  - the value: the filepath on disk where the cached file lives.
- For each spatial/non-spatial, source format, and target format, combination we need
  to generate these 2 outputs.
- This is formalized in the ToFileHandler abstract class which requires `name` (key) and
  `to_file` functions be defined.
    - So a new handler type can only be added if it implements this ABC and defines name
      and `to_file` functions

## Non-Spatial
(implementation here is fairly self-evident in NonSpatialHandler)

## Spatial
There is a basic structure that is common to all to_file algorithims, regardless of
format, source epsg, target epsg.

To limit repeat code, we use a Template Method. The template method is defined in
`SpatialHandler.to_file`:

Sub-classes of `SpatialHandler` do not override `to_file` and instead are expected to
implement hooks or defaults that are called by `SpatialHandler.to_file` (ie. the things
that do differ between epsg/format).

"""

import csv
import json
import os
import re
import shutil
import sys
import xml.etree.cElementTree as ET
from abc import ABC
from contextlib import contextmanager
from typing import Callable, Dict, Generator, List, Literal, Optional, TypedDict
from zipfile import ZipFile

import fiona
import fiona.transform
from fiona.crs import from_epsg
from pydantic import BaseModel

#####################
# Types             #
#####################

EPSG = Literal[4326, 2952]

SPATIAL_TARGET_FORMAT = Literal["shp", "geojson", "gpkg", "csv"]
NON_SPATIAL_TARGET_FORMAT = Literal["csv", "json", "xml"]


class ToFileParamsSpatial(BaseModel):
    resource_id: str
    # TODO is this really a list of possible source targets? how do we know which is which?
    source_epsg: EPSG
    target_epsgs: List[EPSG]
    target_formats: List[SPATIAL_TARGET_FORMAT]


class ToFileParamsNonSpatial(BaseModel):
    resource_id: str
    target_formats: List[NON_SPATIAL_TARGET_FORMAT]


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
    geometry_type: Optional[GeometryType]


#####################
# Constants         #
#####################

FIONA_DRIVERS = {
    "shp": "ESRI Shapefile",
    "geojson": "GeoJSON",
    "gpkg": "GPKG",
}


def geometry_to_json(geom: fiona.Geometry) -> str:
    """_geometry_to_json

    :param geom: the fiona geometry
    :type geom: Geometry
    :return: geojson compliant JSON string
    :rtype: str
    """
    geom_dict = dict(geom)

    # GeoJSON spec does not indicate a case for `null` to be valid json (only mentions
    # it to be a list of geometries or DNE in the json at all.)
    # So if it is explicitly None, remove it before jsonifying
    if "geometries" in geom_dict and geom_dict.get("geometries") is None:
        del geom_dict["geometries"]

    return json.dumps(geom_dict)


#####################
# Classes           #
#####################


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
            "csv": self._to_csv,
            "json": self._to_json,
            "xml": self._to_xml,
        }
        handler = handlers.get(self.target_format)
        if handler is None:
            raise ValueError(f"{self.target_format} is not an accepted target format")
        return handler(row_generator)

    def _to_csv(
        self,
        row_generator: Generator[Dict, None, None],
    ) -> None:
        with open(self.output_filepath, "w") as csv_file:
            writer = csv.DictWriter(
                csv_file,
                fieldnames=[field["id"] for field in self.datastore_metadata["fields"]],
            )
            writer.writeheader()
            writer.writerows(row_generator)
        return self.output_filepath

    @staticmethod
    def _row_to_json_generator(
        csv: Generator[Dict, None, None]
    ) -> Generator[str, None, None]:
        for row in csv:
            yield json.dumps(row)

    @staticmethod
    def _join_with_char(
        generator: Generator[str, None, None], char: str
    ) -> Generator[str, None, None]:
        buffer = None
        for item in generator:
            if buffer is not None:
                yield f"{buffer}{char}"
            buffer = item
        if buffer is not None:
            yield buffer

    def _to_json(
        self,
        row_generator: Generator[Dict, None, None],
    ) -> None:
        with open(self.output_filepath, "w") as json_file:
            # write starting bracket
            json_file.write("[")
            json_file.writelines(
                self._join_with_char(self._row_to_json_generator(row_generator), ",")
            )
            json_file.write("]\n")
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
                    if not (keyname[0].isalpha() or keyname[0] == "_"):
                        keyname = f"_{keyname}"
                    ET.SubElement(xml_row, keyname).text = str(value)
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


class SpatialHandler(ToFileHandler, ABC):

    # From https://fiona.readthedocs.io/en/latest/manual.html#geometry-types
    _MULTI_GEOM_MAPPING = {
        "Point": "MultiPoint",
        "LineString": "MultiLineString",
        "Polygon": "MultiPolygon",
        "3D Point": "3D MultiPoint",
        "3D LineString": "3D MultiLineString",
        "3D Polygon": "3D MultiPolygon",
        # already multi
        "MultiPoint": "MultiPoint",
        "MultiLineString": "MultiLineString",
        "MultiPolygon": "MultiPolygon",
        "3D MultiPoint": "3D MultiPoint",
        "3D MultiLineString": "3D MultiLineString",
        "3D MultiPolygon": "3D MultiPolygon",
        "3D GeometryCollection": "3D MultiGeometryCollection",
        "GeometryCollection": "GeometryCollection",
    }

    def __init__(
        self,
        source_epsg: EPSG,
        target_epsg: EPSG,
        target_format: SPATIAL_TARGET_FORMAT,
        output_filepath: str,
        datastore_metadata: DatastoreResourceMetadata,
    ):
        self.source_epsg = source_epsg
        self.target_epsg = target_epsg
        self.target_format = target_format
        self.output_filepath = output_filepath
        self.datastore_metadata = datastore_metadata

    def name(self) -> str:
        return f"{self.target_format}-{self.target_epsg}"

    @property
    def field_ids(self):
        return [field["id"] for field in self.datastore_metadata["fields"]]

    def to_file(self, input_row_generator: Generator[Dict, None, None]):
        transformed_geom = self.transform_geom(input_row_generator)
        formatted_row = self.format_row(transformed_geom)
        output_path = self.save_to_file(formatted_row)
        return output_path

    def _geom_to_multigeom(self, geom: fiona.Geometry) -> fiona.Geometry:
        multi_geom_type = self._MULTI_GEOM_MAPPING[geom.type]
        if geom.type == multi_geom_type:
            # already a multi-type
            return geom
        return fiona.Geometry(
            **{
                **dict(geom),
                "type": multi_geom_type,
                "coordinates": [geom.coordinates],
            }
        )

    def transform_geom(
        self, row_generator: Generator[Dict, None, None]
    ) -> Generator[Dict, None, None]:
        for row in row_generator:
            geom = json.loads(row["geometry"])
            converted_geom: fiona.Geometry = fiona.transform.transform_geom(
                from_epsg(self.source_epsg), from_epsg(self.target_epsg), geom
            )
            multi_geom = self._geom_to_multigeom(converted_geom)
            # TODO jsonification should happen later
            # geom_json = geometry_to_json(converted_geom)
            row["geometry"] = multi_geom
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
    def __init__(
        self,
        source_epsg: EPSG,
        target_epsg: EPSG,
        target_format: SPATIAL_TARGET_FORMAT,
        output_filepath: str,
        datastore_metadata: DatastoreResourceMetadata,
    ):
        assert target_format == "csv"
        super().__init__(
            source_epsg, target_epsg, target_format, output_filepath, datastore_metadata
        )

    def format_row(
        self, row_generator: Generator[Dict, None, None]
    ) -> Generator[Dict, None, None]:
        for row in row_generator:
            # JSON stringify geometry so it gets written correctly to csv
            row["geometry"] = geometry_to_json(row["geometry"])
            yield row

    def save_to_file(self, row_generator):
        csv.field_size_limit(sys.maxsize)
        with open(self.output_filepath, "w", encoding="utf-8") as csv_file:
            writer = csv.DictWriter(csv_file, fieldnames=self.field_ids)
            writer.writeheader()
            writer.writerows(row_generator)
        return self.output_filepath


class SpatialToSpatial(SpatialHandler):

    #############################
    # Private                   #
    #############################

    @staticmethod
    def _python_type_to_fiona_type(python_type: str) -> str:
        # TODO can we replace with fiona.FIELD_TYPES_MAP
        # See also prop_type
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

        # geometry of type X gets mapped to MultiX, why?:
        # - the `schema` param to fiona.open(...) requires _one_ geometry type to be
        #   specified
        # - this is likely because shp and gpkg (?) files only permit one type of geometry
        #   per collection:
        #   - shp: https://www.esri.com/content/dam/esrisites/sitecore-archive/Files/Pdfs/library/whitepapers/pdfs/shapefile.pdf
        #     "All the non-Null shapes in a shapefile are required to be of the same
        #      shape type"
        #   - gpkg: <TODO: confirm>
        geometry = geom_type_map[self.datastore_metadata["geometry_type"]]

        return {
            "geometry": geometry,
            "properties": properties,
        }

    #############################
    # Hooks                     #
    #############################

    def name(self) -> str:
        return f"{self.target_format}-{self.target_epsg}"

    def format_row(self, row_generator):
        # TODO use real Properties and Feature fiona objects instead of Dicts
        col_map = self._get_col_map()
        for row in row_generator:
            geometry: fiona.Geometry = row["geometry"]
            properties = {}
            for key, value in [(k, v) for k, v in row.items() if k != "geometry"]:
                mapped_key = col_map.get(key, key)
                properties[mapped_key] = value
            yield {
                "type": "Feature",
                "properties": properties,
                # "geometry": geometry,
                # TODO maybe?
                "geometry": dict(geometry),
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
        return self.output_filepath


class SpatialToShp(SpatialToSpatial):
    def __init__(
        self,
        source_epsg: EPSG,
        target_epsg: EPSG,
        target_format: SPATIAL_TARGET_FORMAT,
        output_filepath: str,
        datastore_metadata: DatastoreResourceMetadata,
    ):
        assert target_format == "shp"
        super().__init__(
            source_epsg, target_epsg, target_format, output_filepath, datastore_metadata
        )

    #############################
    # Private                   #
    #############################

    def _get_col_map(self) -> Dict[str, str]:
        # TODO docs on why we truncate to 10 chars
        if not any(len(field) > 10 for field in self.field_ids):
            return {}
        return {field: f"{field[:7]}{i+1}" for i, field in enumerate(self.field_ids)}

    def _write_fields_file(self, path):
        col_map = self._get_col_map()
        with open(path, "w", encoding="utf-8") as fields_file:
            writer = csv.DictWriter(fields_file, fieldnames=("field", "name"))
            writer.writeheader()
            col_map = col_map
            writer.writerows(
                {"field": col_map.get(field_id, field_id), "name": field_id}
                for field_id in self.field_ids
                if field_id != "geometry"
            )

    def _zip_files(self, output_filepath: str, files: List[str]) -> None:

        out_path = f"{os.path.splitext(output_filepath)[0]}.zip"
        with ZipFile(out_path, "w") as zipfile:
            for file in files:
                zipfile.write(file, arcname=os.path.basename(file))

        return out_path

    @staticmethod
    @contextmanager
    def _temp_directory(path: str) -> Generator[str, None, None]:
        # Will fail if path exists (which is probably safer: we don't want to
        # accidentally overwrite files, fail-fast instead)
        os.mkdir(path)
        try:
            yield path
        finally:
            shutil.rmtree(path)

    #############################
    # Hooks                     #
    #############################

    def save_to_file(self, row_generator):
        schema = self._get_schema()

        with self._temp_directory(
            f"{os.path.splitext(self.output_filepath)[0]}"
        ) as shp_folder:
            with fiona.open(
                shp_folder,
                "w",
                schema=schema,
                driver=FIONA_DRIVERS[self.target_format],
                crs=from_epsg(self.target_epsg),
            ) as outlayer:
                outlayer.writerecords(row_generator)

            # collect shape files
            shp_files = [
                os.path.join(shp_folder, file)
                for file in os.listdir(shp_folder)
                if os.path.splitext(file)[1] in [".shp", ".cpg", ".dbf", ".prj", ".shx"]
            ]

            # write fields.csv (documenting column maps)
            data_dict_path = os.path.join(
                shp_folder, f"{self.datastore_metadata['name']} fields.csv"
            )
            self._write_fields_file(data_dict_path)
            shp_files.append(data_dict_path)

            zip_filepath = self._zip_files(self.output_filepath, shp_files)

        return zip_filepath


#####################
# Factory Methods   #
#####################


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
                out_dir, f"{datastore_metadata['name']} - {target_epsg}.{target_format}"
            )

            handler = handler_map.get(target_format, SpatialToSpatial)(
                source_epsg=params.source_epsg,
                target_epsg=target_epsg,
                target_format=target_format,
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
                out_dir, f"{datastore_metadata['name']}.{target_format}"
            ),
            target_format=target_format,
            datastore_metadata=datastore_metadata,
        )
        for target_format in params.target_formats
    ]
