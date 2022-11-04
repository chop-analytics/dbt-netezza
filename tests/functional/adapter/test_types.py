from dbt.tests.adapter.utils.data_types.test_type_bigint import BaseTypeBigInt
from dbt.tests.adapter.utils.data_types.test_type_float import BaseTypeFloat
from dbt.tests.adapter.utils.data_types.test_type_int import BaseTypeInt
from dbt.tests.adapter.utils.data_types.test_type_numeric import BaseTypeNumeric
from dbt.tests.adapter.utils.data_types.test_type_string import BaseTypeString
from dbt.tests.adapter.utils.data_types.test_type_timestamp import BaseTypeTimestamp


class TestTypeBigIntNetezza(BaseTypeBigInt):
    pass


class TestTypeFloatNetezza(BaseTypeFloat):
    pass


class TestTypeIntNetezza(BaseTypeInt):
    pass


class TestTypeNumericNetezza(BaseTypeNumeric):
    pass


# Fails due to type
class TestTypeStringNetezza(BaseTypeString):
    pass


# Requires seed datetimedelim fix
class TestTypeTimestampNetezza(BaseTypeTimestamp):
    pass
