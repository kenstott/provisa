from dataclasses import dataclass
from typing import Any

from provisa_sdk import table


@dataclass
class Row:
    id: int
    name: str


@table
def rows() -> list[dict[str, Any]]:
    return []
