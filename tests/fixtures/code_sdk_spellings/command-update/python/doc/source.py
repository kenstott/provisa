from dataclasses import dataclass


@dataclass
class Row:
    id: int
    name: str


@dataclass
class Changes:
    name: str | None = None


def update_rows(keys: list[int], changes: Changes) -> int:
    """The update of rows.

    @command
    @update rows
    """
    return 0
