from dataclasses import dataclass
from provisa_services import ReadContext


@dataclass
class Row:
    id: int
    name: str


def rows(context: ReadContext) -> list[Row]:
    """Rows of the keys, 100 at a time.

    @table
    @loader 100
    """
    return []
