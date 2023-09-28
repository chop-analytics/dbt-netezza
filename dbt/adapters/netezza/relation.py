from dataclasses import dataclass, field

from dbt.adapters.base.relation import BaseRelation, Policy
from dbt.contracts.relation import ComponentName


@dataclass
class NetezzaQuotePolicy(Policy):
    database: bool = False
    schema: bool = False
    identifier: bool = False


@dataclass(frozen=True, eq=False, repr=False)
class NetezzaRelation(BaseRelation):
    quote_policy: Policy = field(default_factory=lambda: NetezzaQuotePolicy())

    def _is_exactish_match(self, field: ComponentName, value: str) -> bool:
        # Remove requirement for dbt_created due to dbt bug with cache preservation
        # of that property
        if self.quote_policy.get_part(field) is False:
            return self.path.get_lowered_part(field) == value.lower()
        else:
            return self.path.get_part(field) == value

    @staticmethod
    def add_ephemeral_prefix(name: str):
        # Netezza reserves '_' name prefix for system catalogs
        return f"dbt__cte__{name}"
