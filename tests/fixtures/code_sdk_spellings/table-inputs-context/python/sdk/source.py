from dataclasses import dataclass

from provisa_sdk import AsyncStream, table
from provisa_services import ReadContext


@dataclass
class Row:
    id: int
    name: str


@dataclass
class Inputs:
    region: str = "EU"


@table
async def rows(inputs: Inputs, context: ReadContext) -> AsyncStream[Row]:
    """Rows of a region, as the read asks."""
    yield Row(id=1, name="a")
