from dataclasses import dataclass


@dataclass
class Row:
    id: int
    name: str


def rows() -> list[Row]:
    """@tabel"""
    return []
