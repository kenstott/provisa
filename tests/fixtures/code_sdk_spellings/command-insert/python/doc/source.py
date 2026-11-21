from dataclasses import dataclass


@dataclass
class Row:
    id: int
    name: str


def insert_rows(rows: list[Row]) -> int:
    """The insert of rows.

    @command
    @insert rows
    """
    return 0
