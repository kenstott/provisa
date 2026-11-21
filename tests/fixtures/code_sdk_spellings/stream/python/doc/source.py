from dataclasses import dataclass
from collections.abc import Iterator


@dataclass
class Row:
    id: int
    name: str


@dataclass
class Inputs:
    region: str = "EU"


def ticks(inputs: Inputs) -> Iterator[Row]:
    """Each row as it arrives.

    @stream
    """
    yield Row(id=1, name="a")
