from dataclasses import dataclass

from provisa_sdk import change
from provisa_services import TableRef


@dataclass
class Row:
    id: int
    name: str


@dataclass
class Changed:
    left_id: int
    right_id: int
    removed: bool
    seq: int


@change(table="pairs", key=["left_id", "right_id"], removed="removed", cursor="seq")
def pairs_changes(
    left: TableRef[Row], right: TableRef[Row], since: int | None = None
) -> list[Changed]:
    """Matches changed since ``since``."""
    return []
