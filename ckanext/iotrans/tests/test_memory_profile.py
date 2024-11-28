"""
For profiling memory usage.
Note: this may not be totally appropriate as a `test` per-se, however, at the time of
writing it is the most ergonomic way to:
- scaffold some temporary ckan resources
- load into a ckan context
- run a ckan action
- cleanup any temporarily created resources
"""

import pytest
import ckan.tests.helpers as helpers
import requests
from memory_profiler import memory_usage, profile

from pathlib import Path
import csv

from werkzeug.datastructures import FileStorage

from typing import Generator, List
import yaml


@pytest.fixture(scope="session")
def solarto_csv(tmp_path_factory):

    # TODO at some point this link to a prod csv resource should be replaced with a
    # more appropriate static file
    tmp_path = tmp_path_factory.mktemp("iotrans") / "solarto.csv"
    response = requests.get(
        (
            "https://ckan0.cf.opendata.inter.prod-toronto.ca/dataset/solarto/resource"
            "/6b49daae-8797-4153-b106-ccc404ec95b3/download/solarto-map%20-%204326.csv"
        ),
        timeout=20,
    )
    response.raise_for_status()

    with open(tmp_path, "wb") as file:
        file.write(response.content)

    response = requests.get(
        (
            "https://raw.githubusercontent.com/open-data-toronto/operations/"
            "cce693edcc79a15b0bf70549217ac5d02b8bedef/dags/datasets/files_to_datastore/"
            "solarto.yaml"
        ),
        timeout=20,
    )
    response.raise_for_status()
    parsed_yaml = yaml.safe_load(response.content)
    solarto_fields = parsed_yaml["solarto"]["resources"]["solarto-map"]["attributes"]

    return tmp_path, solarto_fields


def chunk_csv(file_name: Path, row_chunk_size: int) -> Generator[List, None, None]:
    # set to some smaller number to run on a subset of chunks if we don't want to wait
    # for the full resource to be created
    lim = float("inf")
    with open(file_name, "r") as csvfile:
        reader = csv.DictReader(csvfile)
        # header
        next(reader)

        chunk = []
        for row in reader:
            chunk.append(row)
            if len(chunk) >= row_chunk_size:
                yield chunk
                if lim <= 0:
                    break  # TODO remove
                lim -= 1
                chunk = []
        # Remaining rows
        if chunk:
            yield chunk


@pytest.fixture(scope="session")
def large_resource(sysadmin, package, solarto_csv):
    solarto_csv_path, solarto_fields = solarto_csv

    context = {
        "user": sysadmin["name"],
        "auth_user_obj": sysadmin,
    }
    resource = helpers.call_action(
        "resource_create",
        context=context,
        package_id=package["id"],
        name="test_fixture_resource",
        format="CSV",
        description="description of test resource. this resource should be cleaned up (deleted) by test fixtures",
        upload=FileStorage(
            stream=open(solarto_csv_path, "rb"), filename=solarto_csv_path.name
        ),
    )
    datastore_record = helpers.call_action(
        "datastore_create",
        context=context,
        resource_id=resource["id"],
        force=True,
        fields=solarto_fields,
    )

    for rows in chunk_csv(solarto_csv_path, 100):
        for i in range(len(rows)):
            if "_id" in rows[i]:
                del rows[i]["_id"]
            rows[i] = {k: v if v != "" else None for k, v in rows[i].items()}
        helpers.call_action(
            "datastore_upsert",
            resource_id=resource["id"],
            method="insert",
            records=rows,
            force=True,
        )

    try:
        yield resource
    finally:
        helpers.call_action("resource_delete", context, id=resource["id"])
        # TODO - probably should also call `datastore_delete` here for the created
        # datastore record?


@pytest.mark.profiling
@pytest.mark.parametrize(
    "target_format",
    (
        # "csv",
        # "gpkg",
        # "geojson"
        "shp",
    ),
)
def test_profile_to_file(target_format, sysadmin, large_resource):
    context = {"user": sysadmin["name"]}
    data = {
        "resource_id": large_resource["id"],
        "source_epsg": 4326,
        "target_epsgs": [4326, 2952],
        "target_formats": [target_format],
    }
    now_str = datetime.now().strftime("%Y_%m_%d_%H_%M")

    with open(f"memory_profiler_{now}.log", "w+") as log_file:

        @profile(stream=log_file)
        def to_file_wrapper():
            return helpers.call_action("to_file", context=context, **data)

        to_file_wrapper()
        # mem, val = memory_usage((to_file_wrapper, (), {}), retval=True)
        breakpoint()
    # TODO: some assertion about anticipated memory consumption


from datetime import datetime


@pytest.mark.profiling
def test_profile_to_file(sysadmin):
    context = {"user": sysadmin["name"]}
    solar_to = "a9153284-9b60-43c3-a8a5-31c65b9f38a7"
    # select id, name from resource where package_id in (select id from package where name='tps-police-divisions');
    tps_boundaries = "627c9199-050f-4380-83ec-b3017e0a34b7"

    data = {
        "resource_id": tps_boundaries,
        "source_epsg": 4326,
        "target_epsgs": [4326, 2952],
        "target_formats": ["shp"],
    }
    now_str = datetime.now().strftime("%Y_%m_%d_%H_%M")

    with open(f"memory_profiler_{now_str}.log", "w+") as log_file:

        @profile(stream=log_file)
        def to_file_wrapper():
            return helpers.call_action("to_file", context=context, **data)

        to_file_wrapper()
        # mem, val = memory_usage((to_file_wrapper, (), {}), retval=True)
        breakpoint()
    # TODO: some assertion about anticipated memory consumption
