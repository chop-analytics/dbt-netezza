import pytest
from dbt.tests.adapter.utils.data_types.test_type_bigint import BaseTypeBigInt
from dbt.tests.adapter.utils.data_types.test_type_boolean import BaseTypeBoolean
from dbt.tests.adapter.utils.data_types.test_type_float import BaseTypeFloat
from dbt.tests.adapter.utils.data_types.test_type_int import BaseTypeInt
from dbt.tests.adapter.utils.data_types.test_type_numeric import BaseTypeNumeric
from dbt.tests.adapter.utils.data_types.test_type_string import BaseTypeString
from dbt.tests.adapter.utils.data_types.test_type_timestamp import BaseTypeTimestamp


class TestTypeBigIntNetezza(BaseTypeBigInt):
    pass


class TestTypeBooleanNetezza(BaseTypeBoolean):
    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {"seeds": {"boolstyle": "TRUE_FALSE"}}


class TestTypeFloatNetezza(BaseTypeFloat):
    pass


class TestTypeIntNetezza(BaseTypeInt):
    pass


class TestTypeNumericNetezza(BaseTypeNumeric):
    pass


@pytest.mark.skip(
    "Netezza requires `varchar` length to be specified and `text` type results in CLOB error."
)
class TestTypeStringNetezza(BaseTypeString):
    pass


class TestTypeTimestampNetezza(BaseTypeTimestamp):
    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {"seeds": {"datetimedelim": " "}}
