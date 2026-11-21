from dataclasses import dataclass

from provisa_sdk import table


@dataclass
class Row:
    id: int
    name: str


@table
def rows() -> list[Row]:
    """Every row."""
    return []
