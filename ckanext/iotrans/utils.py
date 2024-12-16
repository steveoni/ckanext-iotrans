"""Utils functions for iotrans.py
"""

import codecs
import csv
import json
import os
import re
import sys
import xml.etree.cElementTree as ET
from typing import Any, Dict, Generator
from zipfile import ZipFile

import ckan.plugins.toolkit as tk
from fiona.crs import from_epsg
from fiona.transform import transform_geom

from .to_file import EPSG, geometry_to_json


def is_falsey(arg: Any) -> bool:
    return arg in ["false", "False", False]


def dump_generator(resource_id, fieldnames, context):
    """reads a CKAN datastore_search calls, returns a python generator"""
    # init some vars
    chunk = 20000
    i = 0

    while True:
        # get a chunk of records from datastore resource
        records = tk.get_action("datastore_search")(
            context,
            {
                "resource_id": resource_id,
                "limit": chunk,
                "offset": chunk * i,
            },
        )["records"]

        if len(records):
            for record in records:
                yield record
            i += 1
            continue

        else:
            break


def get_filepath(dir_path, resource_name, epsg, file_format):
    """Gets a filepath using input resource name, and desired format/epsg"""

    epsg_suffix = " - " + str(epsg) if epsg else ""
    return os.path.join(
        dir_path, "{0}{1}.{2}".format(resource_name, epsg_suffix, file_format.lower())
    )


def write_to_json(dump_filepath, output_filepath, datastore_resource, context):
    """Stream into a JSON file by running datastore_search over and over"""
    # TODO why are we re-downloading instead of using the dump_filepath?
    with codecs.open(output_filepath, "w", encoding="utf-8") as jsonfile:
        # write starting bracket
        jsonfile.write("[")

        # grab first chunk of records
        chunk_size = 20000
        iteration = 0
        data_chunk = tk.get_action("datastore_search")(
            context,
            {
                "resource_id": datastore_resource["resource_id"],
                "limit": chunk_size,
            },
        )
        # as long as there is more to grab, grab the next chunk
        while len(data_chunk["records"]):

            for record in data_chunk["records"]:
                jsonfile.write(json.dumps(record))
                jsonfile.write(", ")
            iteration += 1
            data_chunk = tk.get_action("datastore_search")(
                context,
                {
                    "resource_id": datastore_resource["resource_id"],
                    "limit": chunk_size,
                    "offset": chunk_size * iteration,
                },
            )

    with codecs.open(output_filepath, "rb+", encoding="utf-8") as jsonfile:
        # remove last ", "
        jsonfile.seek(-2, 2)
        jsonfile.truncate()

    with codecs.open(output_filepath, "a", encoding="utf-8") as jsonfile:
        # add last closing ]
        jsonfile.write("]")


def write_to_xml(dump_filepath, output_filepath, chunk_size=5000):
    """Stream into an XML file"""
    XML_ENCODING = "utf-8"
    CSV_ENCODING = "utf-8"

    with open(output_filepath, "w", encoding=XML_ENCODING) as xml_file:
        with codecs.open(dump_filepath, "r", encoding=CSV_ENCODING) as csvfile:
            dictreader = csv.DictReader(csvfile)

            root_tag = "DATA"
            xml_file.write(f'<?xml version="1.0" encoding="{XML_ENCODING}"?>\n')
            xml_file.write(f"<{root_tag}>")
            i = 0

            # chunk writes to disk based on chunk_size so that:
            # 1. we don't do it all in one batch and end up w/ a MemoryError
            # 2. we don't perform disk io for every single record which is inefficient
            chunk = []

            for csv_row in dictreader:
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


def iotrans_auth_function(context, data_dict=None):
    """CKAN auth function - requires authorized uses for certain actions"""
    if context.get("auth_user_obj", False):
        return {"success": True}
    elif not context.get("auth_user_obj", None):
        return {
            "success": False,
            "msg": "This endpoint is for authorized accounts only",
        }
