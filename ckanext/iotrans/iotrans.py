"""the to_file() and prune() functions
These function are the top level logic for this extension's CKAN actions
"""

import json
import logging
import os
import tempfile
from datetime import datetime

import ckan.plugins.toolkit as tk
import fiona
from ckan.common import config
from fiona.crs import from_epsg
from memory_profiler import profile
from pydantic import ValidationError

from . import utils
from .to_file import (
    DatastoreResourceMetadata,
    ToFileParamsNonSpatial,
    ToFileParamsSpatial,
    non_spatial_to_file_factory,
    spatial_to_file_factory,
)


@tk.side_effect_free
def to_file(context, data_dict):
    """
    inputs:
        resource_id: CKAN datastore resource ID
        source_epsg: source EPSG of resource ID, if data is spatial
        target_epsgs: list of desired EPSGs of output files, if data is spatial
        target_formats: list of desired file formats

    a spatial datasets needs a geometry column
    assumes geometry column in dataset contains geometry
    assumes geometry objects within a dataset are all the same geometry type
        ex: (all Point, all Line, or all Polygon)

    outputs:
        writes desired files to folder in /tmp
        returns a list of filepaths, where the outputs are stored on disk
    """
    is_spatial = True
    try:
        data = ToFileParamsSpatial(**data_dict)
    except ValidationError as spatial_valid_error:
        try:
            data = ToFileParamsNonSpatial(**data_dict)
            is_spatial = False
        except ValidationError as non_spatial_valid_error:
            raise tk.ValidationError(
                {
                    # TODO: concat non_spatial_valid_error and spatial_valid_error
                    "constraints": ["invalid params for both spatial and non-spatial"]
                }
            ) from spatial_valid_error

    # Make sure the resource id provided is for a datastore resource
    resource_metadata = tk.get_action("resource_show")(
        context, {"id": data.resource_id}
    )
    if utils.is_falsey(resource_metadata.get("datastore_active", None)):
        raise tk.ValidationError(
            {"constraints": [f"{data.resource_id} is not a datastore resource!"]}
        )

    datastore_resource = tk.get_action("datastore_search")(
        context, {"resource_id": data.resource_id}
    )
    if len(datastore_resource["records"]) < 1:
        raise tk.ValidationError(
            {"constraints": [f"Datastore resource {data.resource_id} is empty"]}
        )

    # Download all rows to a local csv in a temporary directory. Effictively a local
    # cache on disk. 2 reasons:
    # 1. We can't hold files this large in memory typically
    # 2. We need to re-use these data multiple times; we can at least limit db queries
    #    to a constant rathern than N times (where N is the number of outputs we target)
    temp_dir = tempfile.mkdtemp(dir=config.get("ckan.storage_path"))
    dump_filepath = utils.get_filepath(
        temp_dir,
        resource_metadata["name"],
        data_dict.get("source_epsg", None),
        "jsonlines",
    )
    fieldnames = [field["id"] for field in datastore_resource["fields"]]
    generator = utils.dump_generator(
        data.resource_id,
        fieldnames,
        context,
    )
    utils.write_to_jsonlines(dump_filepath, generator)

    geometry_type = (
        json.loads(datastore_resource["records"][0]["geometry"])["type"]
        if is_spatial
        else None
    )
    datastore_metadata: DatastoreResourceMetadata = {
        "fields": datastore_resource["fields"],
        "geometry_type": geometry_type,
        "name": resource_metadata["name"],
    }

    # Depending on whether spatial or not, we need a factory method that will generate
    # all the output handlers. These factory methods should be the same shape (params +
    # return types)
    handler_factory = (
        spatial_to_file_factory if is_spatial else non_spatial_to_file_factory
    )
    out_dir = os.path.join(temp_dir, "output")
    os.makedirs(out_dir)
    handlers = handler_factory(
        params=data,
        out_dir=out_dir,
        datastore_metadata=datastore_metadata,
    )

    # Iterate through handlers produced by the factory for each we run to_file
    # (1 'handler' = 1 output file)
    output = {}
    for handler in handlers:
        with open(dump_filepath, "r") as csv_file:
            row_generator = utils.json_lines_reader(csv_file)
            output[handler.name()] = handler.to_file(row_generator)
    return output


@tk.side_effect_free
def prune(context, data_dict):

    # Taken from:
    # https://github.com/open-data-toronto/iotrans/blob/master/iotrans/utils.py
    # Deletes input file or a directory as long as its in correct dir

    if not data_dict.get("path", None):
        raise tk.ValidationError(
            {"constraints": ["Input 'path' of dir/file to delete required!"]}
        )

    path = data_dict["path"]

    storage_path = config.get("ckan.storage_path")
    if not data_dict.get("path", None).startswith(storage_path):
        raise tk.ValidationError(
            {
                "constraints": [
                    "This action is meant for deleting folders in {}".format(
                        storage_path
                    )
                ]
            }
        )

    if os.path.isdir(path):
        # Empty the contents of the folder before removing the directory
        for f in os.listdir(path):
            os.remove(os.path.join(path, f))

        os.rmdir(path)
    else:
        os.remove(path)

    logging.info("[ckanext-iotrans] pruned ".format(path))
