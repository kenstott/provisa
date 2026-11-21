from dataclasses import dataclass

from provisa_sdk import command


@dataclass
class Row:
    id: int
    name: str


@command(delete="rows")
def delete_rows(keys: list[int]) -> int:
    """The delete of rows."""
    return 0
