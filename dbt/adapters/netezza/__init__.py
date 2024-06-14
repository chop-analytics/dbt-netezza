from dbt.adapters.netezza.connections import NetezzaConnectionManager  # noqa
from dbt.adapters.netezza.connections import NetezzaCredentials
from dbt.adapters.netezza.impl import NetezzaAdapter

from dbt.adapters.base.plugin import AdapterPlugin
from dbt.include.netezza import PACKAGE_PATH


Plugin = AdapterPlugin(
    adapter=NetezzaAdapter,  # type: ignore
    credentials=NetezzaCredentials,
    include_path=PACKAGE_PATH,
)
