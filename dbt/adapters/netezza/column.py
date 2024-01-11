from dbt.adapters.base.column import Column
from dataclasses import dataclass


@dataclass
class NetezzaColumn(Column):
    TYPE_LABELS = {
        **Column.TYPE_LABELS,
        "STRING": "varchar(2000)",
    }

    # Override to ignore data type length
    def is_string(self) -> bool:
        return self.dtype.lower() == "text" or any(
            self.dtype.lower().startswith(dtype)
            for dtype in ["char", "nchar", "varchar", "nvarchar"]
        )

    # Override to ignore data type precision
    def is_numeric(self) -> bool:
        return any(
            self.dtype.lower().startswith(dtype) for dtype in ["numeric", "decimal"]
        )
