from dataclasses import dataclass

from provisa_sdk import query


@dataclass
class Row:
    id: int
    name: str


@query
def lookup(name: str) -> list[Row]:
    """Rows of one name."""
    return []
