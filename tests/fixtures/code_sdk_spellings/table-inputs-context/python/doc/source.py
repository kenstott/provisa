from dataclasses import dataclass
from collections.abc import AsyncIterator

from provisa_services import ReadContext


@dataclass
class Row:
    id: int
    name: str


@dataclass
class Inputs:
    region: str = "EU"


async def rows(inputs: Inputs, context: ReadContext) -> AsyncIterator[Row]:
    """Rows of a region, as the read asks.

    @table
    """
    yield Row(id=1, name="a")
