import pytest
from typing import Dict, Generator

import ckan.tests.factories as factories
import ckan.tests.helpers as helpers
from ckan.model import Session, User

@pytest.fixture(scope="session")
def sysadmin() -> Generator[Dict, None, None]:
    sysadmin = factories.Sysadmin()
    yield sysadmin
    # unfortunately there is no user_purge action and user_delete is a
    # soft-delete. So instead we manually delete the user.
    # Not ideal as we may bypass any plugins modifying this behavior
    user_id = sysadmin["id"]
    user = Session.query(User).get(user_id)
    Session.delete(user)
    Session.commit()


@pytest.fixture(scope="session")
def package(sysadmin: Dict) -> Generator[Dict, None, None]:
    context = {"user": sysadmin["name"]}
    kwargs = {
        "name": "iotrans-test-dataset",
        "notes": "Test dataset from scheming plugin. This should be cleaned up "
        "and removed automatically by test fixtures.",
        "title": "Scheming Test Dataset",
        # DB depends:
        "dataset_category": "Document",
        "owner_division": "311 Toronto",
        "refresh_rate": "Daily",
    }
    package_dict = helpers.call_action("package_create", context=context, **kwargs)
    yield package_dict
    helpers.call_action("dataset_purge", context=context, id=package_dict["id"])


# @pytest.fixture(scope="session")
@pytest.fixture
def resource(sysadmin, package):
    context = {"user":sysadmin["name"]}
    data = {
        "package_id": package["id"],
        "name": "test_fixture_resource",
        "description": "description of test resource. this resource should be cleaned up (deleted) by test fixtures",
        "format": "test_Format",
        "url": "test.example.com",
    }
    resource = helpers.call_action("resource_create", context=context, **data)
    yield resource
    helpers.call_action("resource_delete", context, id=resource["id"])