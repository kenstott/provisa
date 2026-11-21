from dataclasses import dataclass

from provisa_sdk import table
from provisa_services import ReadContext


@dataclass
class Row:
    id: int
    name: str


@table
async def rows(context: ReadContext) -> list[Row]:
    """Rows the read asks for."""
    return []
