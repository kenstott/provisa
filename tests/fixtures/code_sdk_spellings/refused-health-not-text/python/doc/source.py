from dataclasses import dataclass


@dataclass
class Row:
    id: int
    name: str


def status() -> int:
    """@health"""
    return 1
