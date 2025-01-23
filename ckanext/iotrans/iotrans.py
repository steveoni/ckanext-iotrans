"""the to_file() and prune() functions
These function are the top level logic for this extension's CKAN actions
"""

import json
import logging
import os
import tempfile
from typing import Callable, List

import ckan.plugins.toolkit as tk
from ckan.common import config
from pydantic import ValidationError

from .utils import generic, json_lines
from .utils.to_file import (
    DatastoreResourceMetadata,
    ToFileHandler,
    ToFileParamsNonSpatial,
    ToFileParamsSpatial,
    non_spatial_to_file_factory,
    spatial_to_file_factory,
)
from ckan.lib.jobs import enqueue
from ckan.plugins.toolkit import enqueue_job
from ckanext.iotrans.utils.process import process_to_file

@tk.side_effect_free
def to_file(context, data_dict):
    """
    Enqueue a background job to process the `to_file` task.
    """
    job = enqueue_job(process_to_file, [context, 
                                        data_dict],
                                        title=f"To_file trannsformtion-{data_dict.get('resource_id')}",
                                        rq_kwargs={"timeout": 18000})
    return {"job_id": job.id, "status": "queued"}


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
