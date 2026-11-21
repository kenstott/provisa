from dataclasses import dataclass

from provisa_sdk import health, on_start, on_stop, table


@dataclass
class Row:
    id: int
    name: str


@on_start
def start(values: dict[str, str]) -> None:
    """Open the client."""


@on_stop
def stop() -> None:
    """Close the client."""


@health
def status() -> str:
    return "ok"


@table
def rows() -> list[Row]:
    return []
