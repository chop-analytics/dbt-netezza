# Override the dbt project fixture to prevent schema creation and deletion for Netezza
# instances without schemas enabled
# Source: https://docs.getdbt.com/docs/contributing/testing-a-new-adapter
import os
import pytest
import warnings
from argparse import Namespace

from dbt.events.functions import setup_event_logger, cleanup_event_logger
from dbt.tests.fixtures.project import TestProjInfo
from dbt.tests.util import run_sql_with_adapter, relation_from_name, get_manifest


@pytest.fixture(scope="class")
def unique_schema(request, prefix) -> str:
    return os.getenv("DBT_TEST_NZ_SCHEMA", "ADMIN")


@pytest.fixture(scope="class")
def project_cleanup_extra_relations() -> list[tuple[str, str]]:
    return []


@pytest.fixture(scope="class")
def project_cleanup(adapter, project_root, project_cleanup_extra_relations):
    # Wait until tests complete
    yield

    # Get project manifest if it exists
    manifest = get_manifest(project_root)
    if not manifest:
        return

    # Find all table or view relations and drop them
    relations = [
        ("view" if node.config.materialized == "view" else "table", node.name)
        for node_name, node in manifest.nodes.items()
        if node_name.split(".")[0] in ["model", "seed", "snapshot"]
    ]
    relations += project_cleanup_extra_relations
    drop_statements = [
        f"drop {relation_type} {relation_from_name(adapter, relation)}"
        for relation_type, relation in relations
    ]
    sql = ";\n".join(drop_statements)
    run_sql_with_adapter(adapter, sql)


@pytest.fixture(scope="class")
def project(
    clean_up_logging,
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
    project_cleanup,
):
    # Logbook warnings are ignored so we don't have to fork logbook to support python 3.10.
    # This _only_ works for tests in `tests/` that use the project fixture.
    warnings.filterwarnings("ignore", category=DeprecationWarning, module="logbook")
    log_flags = Namespace(
        LOG_PATH=logs_dir,
        LOG_FORMAT="json",
        LOG_FORMAT_FILE="json",
        USE_COLORS=False,
        USE_COLORS_FILE=False,
        LOG_LEVEL="info",
        LOG_LEVEL_FILE="debug",
        DEBUG=False,
        LOG_CACHE_EVENTS=False,
        QUIET=False,
        LOG_FILE_MAX_BYTES=1000000,
    )
    setup_event_logger(log_flags)
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
    cleanup_event_logger()
