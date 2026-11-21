from dataclasses import dataclass


@dataclass
class Row:
    id: int
    name: str


def lookup(name: str) -> list[Row]:
    """Rows of one name.

    @query
    """
    return []
