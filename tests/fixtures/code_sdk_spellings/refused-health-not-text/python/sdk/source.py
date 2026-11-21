from dataclasses import dataclass

from provisa_sdk import health


@dataclass
class Row:
    id: int
    name: str


@health
def status() -> int:
    return 1
