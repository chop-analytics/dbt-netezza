# Override the dbt project fixture to prevent schema creation and deletion for Netezza 
# instances without schemas enabled
# Source: https://docs.getdbt.com/docs/contributing/testing-a-new-adapter
import os
import pytest  # type: ignore
import warnings

from dbt.events.functions import setup_event_logger
from dbt.tests.fixtures.project import (
    project_root,
    profiles_root,
    profiles_yml,
    dbt_project_yml,
    packages_yml,
    selectors_yml,
    adapter,
    project_files,
    shared_data_dir,
    test_data_dir,
    logs_dir,
    test_config,
    TestProjInfo
)

@pytest.fixture(scope="class")
def unique_schema(request, prefix) -> str:
    return os.getenv('DBT_TEST_NZ_SCHEMA')

@pytest.fixture(scope="class")
def project(
    project_root,
    profiles_root,
    request,
    unique_schema,
    profiles_yml,
    dbt_project_yml,
    packages_yml,
    selectors_yml,
    adapter,
    project_files,
    shared_data_dir,
    test_data_dir,
    logs_dir,
    test_config,
):
    # Logbook warnings are ignored so we don't have to fork logbook to support python 3.10.
    # This _only_ works for tests in `tests/` that use the project fixture.
    warnings.filterwarnings("ignore", category=DeprecationWarning, module="logbook")
    setup_event_logger(logs_dir)
    orig_cwd = os.getcwd()
    os.chdir(project_root)
    # Return whatever is needed later in tests but can only come from fixtures, so we can keep
    # the signatures in the test signature to a minimum.
    project = TestProjInfo(
        project_root=project_root,
        profiles_dir=profiles_root,
        adapter_type=adapter.type(),
        test_dir=request.fspath.dirname,
        shared_data_dir=shared_data_dir,
        test_data_dir=test_data_dir,
        test_schema=unique_schema,
        database=adapter.config.credentials.database,
        test_config=test_config,
    )

    yield project

    os.chdir(orig_cwd)
