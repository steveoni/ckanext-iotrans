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

import six
import ckan.tests.factories as factories


# @pytest.fixture(scope="session")
@pytest.fixture()
def large_csv(tmp_path_factory):
    tmp_path = tmp_path_factory.mktemp("iotrans") / "solarto.csv"

    # tmp_path = "./"

    # TODO at some point this link to a prod csv resource should be replaced with a
    # more appropriate static file
    url = (
        "https://ckan0.cf.opendata.inter.prod-toronto.ca/dataset/solarto/resource"
        "/6b49daae-8797-4153-b106-ccc404ec95b3/download/solarto-map%20-%204326.csv"
    )
    response = requests.get(url, timeout=20)
    response.raise_for_status()

    with open(tmp_path, "wb") as file:
        file.write(response.content)

    return tmp_path


# @pytest.fixture(scope="session")
@pytest.fixture()
def monkeypatch_session():
    with pytest.MonkeyPatch.context() as mp:
        yield mp


import os

from ckan.lib import uploader as ckan_uploader

from werkzeug.datastructures import FileStorage


# @pytest.fixture(scope="session")
@pytest.fixture()
def large_resource(sysadmin, package, large_csv, monkeypatch_session, ckan_config):
    tmp_dir = os.path.dirname(large_csv)
    context = {
        "user": sysadmin["name"],
        "auth_user_obj": sysadmin,
    }
    # resource = factories.Resource(
    #     package_id=package["id"],
    #     name="test_fixture_resource",
    #     format="CSV",
    #     description="description of test resource. this resource should be cleaned up (deleted) by test fixtures",
    #     upload=open(large_csv, "rb"),
    # )
    resource = helpers.call_action(
        "resource_create",
        context=context,
        package_id=package["id"],
        name="test_fixture_resource",
        format="CSV",
        description="description of test resource. this resource should be cleaned up (deleted) by test fixtures",
        # files={"upload": open(large_csv, "rb")},
        upload=FileStorage(stream=open(large_csv, "rb"), filename=large_csv.name),
    )
    helpers.call_action(
        "datastore_create",
        context=context,
        resource_id=resource["id"],
        force=True,
    )
    breakpoint()

    try:
        yield resource
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
def test_profile_to_file(target_format, sysadmin, large_resource):
    context = {"user": sysadmin["name"]}
    data = {
        "resource_id": large_resource["id"],
        "target_formats": [target_format],
    }

    @profile
    def to_file_wrapper():
        return helpers.call_action("to_file", context=context, **data)

    to_file_wrapper()
    mem = memory_usage((to_file_wrapper, (), {}))
    print(mem)
    breakpoint()
    # TODO: some assertion about anticipated memory consumption
