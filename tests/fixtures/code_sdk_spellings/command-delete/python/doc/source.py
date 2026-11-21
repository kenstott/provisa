from dataclasses import dataclass


@dataclass
class Row:
    id: int
    name: str


def delete_rows(keys: list[int]) -> int:
    """The delete of rows.

    @command
    @delete rows
    """
    return 0
