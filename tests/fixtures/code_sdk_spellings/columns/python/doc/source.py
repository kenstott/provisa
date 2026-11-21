from dataclasses import dataclass


@dataclass
class Row:
    id: int
    name: str


@dataclass
class Upper:
    upper: str


def shout(names: list[str]) -> list[Upper]:
    """Each name in capitals.

    @columns
    """
    return [Upper(upper=n.upper()) for n in names]
