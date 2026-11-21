from dataclasses import dataclass

from provisa_sdk import command


@dataclass
class Row:
    id: int
    name: str


@command(insert="rows")
def insert_rows(rows: list[Row]) -> int:
    """The insert of rows."""
    return 0
