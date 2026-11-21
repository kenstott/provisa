from dataclasses import dataclass


@dataclass
class Row:
    id: int
    name: str


def start(values: dict[str, str]) -> None:
    """Open the client.

    @on-start
    """


def stop() -> None:
    """Close the client.

    @on-stop
    """


def status() -> str:
    """@health"""
    return "ok"


def rows() -> list[Row]:
    """@table"""
    return []
