from dbt.adapters.base.column import Column
from dataclasses import dataclass


@dataclass
class NetezzaColumn(Column):
    TYPE_LABELS = {
        **Column.TYPE_LABELS,
        "STRING": "varchar(2000)",
    }
