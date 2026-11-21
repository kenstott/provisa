from dataclasses import dataclass

from provisa_sdk import Stream, stream


@dataclass
class Row:
    id: int
    name: str


@dataclass
class Inputs:
    region: str = "EU"


@stream
def ticks(inputs: Inputs) -> Stream[Row]:
    """Each row as it arrives."""
    yield Row(id=1, name="a")
