from dataclasses import dataclass

from provisa_sdk import table
from provisa_services import ReadContext


@dataclass
class Row:
    id: int
    name: str


@dataclass
class Inputs:
    region: str = "EU"


@table
def rows(context: ReadContext, inputs: Inputs) -> list[Row]:
    return []
