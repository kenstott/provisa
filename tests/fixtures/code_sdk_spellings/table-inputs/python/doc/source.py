from dataclasses import dataclass
from collections.abc import Iterable


@dataclass
class Row:
    id: int
    name: str


@dataclass
class Inputs:
    region: str = "EU"


def rows(inputs: Inputs) -> Iterable[Row]:
    """Rows of a region.

    @table
    """
    return []
