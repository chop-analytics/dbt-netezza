import pytest
from dbt.tests.adapter.catalog.relation_types import CatalogRelationTypes


@pytest.mark.skip("Adapter does not support materialized views.")
class TestCatalogRelationTypes(CatalogRelationTypes):
    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {"seeds": {"datetimedelim": " "}}
