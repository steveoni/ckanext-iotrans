"""
For profiling memory usage.
Note: this may not be totally appropriate as a `test` per-se, however, at the time of
writing it is the most ergonomic way to:
- scaffold some temporary ckan resources
- load into a ckan context
- run a ckan action
- cleanup any temporarily created resources
"""

import csv
import json
import os
import random
from datetime import datetime, timedelta
from pathlib import Path
from typing import Generator, List

import ckan.tests.helpers as helpers
import pytest
from memory_profiler import profile
from werkzeug.datastructures import FileStorage


@pytest.fixture(scope="session")
def large_geospatial_csv() -> None:
    """Generates a CSV file with fake data.

    Args:
        file_name (str): The name of the output CSV file.
        num_rows (int): The number of rows to generate.
    """
    here = os.path.dirname(os.path.abspath(__file__))
    hide_dir = os.path.join(here, "..", "..", "..", ".hide")
    os.makedirs(hide_dir, exist_ok=True)
    from pathlib import Path

    csv_file_name = Path(os.path.join(hide_dir, "fake_data.csv"))

    fields = [
        {"id": "date", "type": "text"},
        {"id": "value", "type": "float4"},
        {"id": "geometry", "type": "text"},
    ]
    fieldnames = [field["id"] for field in fields]

    if csv_file_name.exists():
        return csv_file_name, fields

    random.seed(6)
    num_rows = 4700000  # 4.7 million Point geoms (4.7M rows)
    with open(csv_file_name, "w", newline="") as csvfile:
        fieldnames = ["date", "value", "geometry"]
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)

        writer.writeheader()

        for _ in range(num_rows):
            start_date = datetime.now() - timedelta(days=3650)
            random_date = start_date + timedelta(days=random.randint(0, 3650))
            date_str = random_date.strftime("%Y-%m-%d")

            value = round(random.uniform(0.0, 1000.0), 2)

            longitude = round(random.uniform(-180.0, 180.0), 6)
            latitude = round(random.uniform(-90.0, 90.0), 6)
            geometry = {"type": "Point", "coordinates": [longitude, latitude]}

            writer.writerow(
                {"date": date_str, "value": value, "geometry": json.dumps(geometry)}
            )
    return csv_file_name, fields


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
def large_resource(sysadmin, package, large_geospatial_csv):
    large_csv_path, large_csv_fields = large_geospatial_csv

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
            stream=open(large_csv_path, "rb"), filename=large_csv_path.name
        ),
    )
    helpers.call_action(
        "datastore_create",
        context=context,
        resource_id=resource["id"],
        force=True,
        fields=large_csv_fields,
    )

    iters = 0
    for rows in chunk_csv(large_csv_path, 50000):
        for i in range(len(rows)):
            if "_id" in rows[i]:
                del rows[i]["_id"]
            rows[i] = {k: v if v != "" else None for k, v in rows[i].items()}
        print(f"calling datastore_upsert {iters}")
        helpers.call_action(
            "datastore_upsert",
            resource_id=resource["id"],
            method="insert",
            records=rows,
            force=True,
        )
        iters += 1

    try:
        yield resource
    finally:
        try:
            helpers.call_action("datastore_delete", context, resource_id=resource["id"])
        finally:
            helpers.call_action("resource_delete", context, id=resource["id"])


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
@pytest.mark.skip
def test_profile_to_file(target_format, sysadmin, large_resource):
    context = {"user": sysadmin["name"]}
    data = {
        "resource_id": large_resource["id"],
        "source_epsg": 4326,
        "target_epsgs": [4326, 2952],
        "target_formats": [target_format],
    }
    now_str = datetime.now().isoformat

    with open(f"memory_profiler_{now_str}.log", "w+") as log_file:

        @profile(stream=log_file)
        def to_file_wrapper():
            return helpers.call_action("to_file", context=context, **data)

        to_file_wrapper()
        # mem, val = memory_usage((to_file_wrapper, (), {}), retval=True)
    # TODO: some assertion about anticipated memory consumption


from datetime import datetime


@pytest.mark.profiling
@pytest.mark.skip
def test_profile_to_file_by_resource_id(sysadmin):
    context = {"user": sysadmin["name"]}
    # solar_to = "a9153284-9b60-43c3-a8a5-31c65b9f38a7"
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
    # TODO: some assertion about anticipated memory consumption
