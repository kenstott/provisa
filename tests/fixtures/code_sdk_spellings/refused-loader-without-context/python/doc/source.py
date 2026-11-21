from dataclasses import dataclass


@dataclass
class Row:
    id: int
    name: str


@dataclass
class Inputs:
    region: str = "EU"


def rows(inputs: Inputs) -> list[Row]:
    """@table
    @loader
    """
    return []
