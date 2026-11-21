from dataclasses import dataclass

from provisa_sdk import table
from provisa_services import ReadContext


@dataclass
class Row:
    id: int
    name: str


@table(loader=100)
def rows(context: ReadContext) -> list[Row]:
    """Rows of the keys, 100 at a time."""
    return []
