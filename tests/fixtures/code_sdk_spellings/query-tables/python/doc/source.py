from dataclasses import dataclass
from provisa_services import TableRef


@dataclass
class Row:
    id: int
    name: str


@dataclass
class Match:
    left_id: int
    right_id: int


def pairs(left: TableRef[Row], right: TableRef[Row]) -> list[Match]:
    """Rows of two tables that belong together.

    @query
    """
    return []
