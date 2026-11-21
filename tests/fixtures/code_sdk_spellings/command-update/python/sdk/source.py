from dataclasses import dataclass

from provisa_sdk import command


@dataclass
class Row:
    id: int
    name: str


@dataclass
class Changes:
    name: str | None = None


@command(update="rows")
def update_rows(keys: list[int], changes: Changes) -> int:
    """The update of rows."""
    return 0
