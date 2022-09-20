from dataclasses import dataclass
from dbt.adapters.base.relation import BaseRelation, Policy


@dataclass
class NetezzaQuotePolicy(Policy):
    database: bool = False
    schema: bool = False
    identifier: bool = False


@dataclass(frozen=True, eq=False, repr=False)
class NetezzaRelation(BaseRelation):
    quote_policy: NetezzaQuotePolicy = NetezzaQuotePolicy()

    @staticmethod
    def add_ephemeral_prefix(name: str):
        # Netezza reserves '_' name prefix for system catalogs
        return f"dbt__cte__{name}"
