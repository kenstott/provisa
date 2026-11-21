from dataclasses import dataclass


@dataclass
class Row:
    id: int
    name: str


def rows() -> list[Row]:
    """Every row.

    @table
    """
    return []
