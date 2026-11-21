from dataclasses import dataclass
from provisa_services import ReadContext


@dataclass
class Row:
    id: int
    name: str


@dataclass
class Inputs:
    region: str = "EU"


def rows(inputs: Inputs, context: ReadContext) -> list[Row]:
    """Rows of every key in one batch.

    @table
    @loader
    """
    return []
