from dataclasses import dataclass

from provisa_sdk import Rows, table


@dataclass
class Row:
    id: int
    name: str


@dataclass
class Inputs:
    region: str = "EU"


@table
def rows(inputs: Inputs) -> Rows[Row]:
    """Rows of a region."""
    return []
