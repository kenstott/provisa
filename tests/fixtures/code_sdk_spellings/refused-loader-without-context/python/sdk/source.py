from dataclasses import dataclass

from provisa_sdk import table


@dataclass
class Row:
    id: int
    name: str


@dataclass
class Inputs:
    region: str = "EU"


@table(loader=True)
def rows(inputs: Inputs) -> list[Row]:
    return []
