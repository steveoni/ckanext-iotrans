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

from ckan.plugins.toolkit import call_action


@pytest.mark.usefixtures("with_request_context")
class TestApiController(object):
    def test_resource_create_upload_file(self, app, monkeypatch, tmpdir, ckan_config):
        monkeypatch.setitem(ckan_config, "ckan.storage_path", str(tmpdir))
        monkeypatch.setattr(ckan_uploader, "_storage_path", str(tmpdir))

        user = factories.User()
        pkg = factories.Dataset(creator_user_id=user["id"])

        url = url_for(
            controller="api",
            action="action",
            logic_function="resource_create",
            ver="/3",
        )
        env = {"REMOTE_USER": six.ensure_str(user["name"])}

        content = six.ensure_binary("upload-content")
        upload_content = six.BytesIO(content)
        postparams = {
            "name": "test-flask-upload",
            "package_id": pkg["id"],
            "upload": (upload_content, "test-upload.txt"),
        }

        resp = app.post(
            url,
            data=postparams,
            environ_overrides=env,
            content_type="multipart/form-data",
        )
        result = resp.json["result"]
        assert "upload" == result["url_type"]
        assert len(content) == result["size"]


# @pytest.fixture(scope="session")
@pytest.fixture()
def large_csv(tmp_path_factory):
    tmp_path = tmp_path_factory.mktemp("iotrans") / "solarto.csv"

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


# @pytest.fixture(scope="session")
@pytest.fixture()
def large_resource(sysadmin, package, large_csv, monkeypatch_session, ckan_config):
    tmp_dir = os.path.dirname(large_csv)
    monkeypatch_session.setitem(ckan_config, "ckan.storage_path", str(tmp_dir))
    monkeypatch_session.setattr(ckan_uploader, "_storage_path", str(tmp_dir))
    context = {
        "user": sysadmin["name"],
        "auth_user_obj": sysadmin,
    }
    resource = call_action(
        "resource_create",
        context=context,
        package_id=package["id"],
        name="test_fixture_resource",
        format="CSV",
        description="description of test resource. this resource should be cleaned up (deleted) by test fixtures",
        files={"upload": open(large_csv, "rb")},
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
