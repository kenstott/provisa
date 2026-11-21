from dataclasses import dataclass
from provisa_services import ReadContext


@dataclass
class Row:
    id: int
    name: str


@dataclass
class Inputs:
    region: str = "EU"


def rows(context: ReadContext, inputs: Inputs) -> list[Row]:
    """@table"""
    return []
