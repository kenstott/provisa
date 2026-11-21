from dataclasses import dataclass
from typing import Any


@dataclass
class Row:
    id: int
    name: str


def rows() -> list[dict[str, Any]]:
    """@table"""
    return []
