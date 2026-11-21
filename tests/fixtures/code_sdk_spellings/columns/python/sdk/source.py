from dataclasses import dataclass

from provisa_sdk import Batch, Results, columns


@dataclass
class Row:
    id: int
    name: str


@dataclass
class Upper:
    upper: str


@columns
def shout(names: Batch[str]) -> Results[Upper]:
    """Each name in capitals."""
    return [Upper(upper=n.upper()) for n in names]
