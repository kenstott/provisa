from dataclasses import dataclass
from provisa_services import ReadContext


@dataclass
class Row:
    id: int
    name: str


async def rows(context: ReadContext) -> list[Row]:
    """Rows the read asks for.

    @table
    """
    return []
