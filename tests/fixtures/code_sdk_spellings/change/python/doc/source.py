from dataclasses import dataclass
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


def pairs_changes(
    left: TableRef[Row], right: TableRef[Row], since: int | None = None
) -> list[Changed]:
    """Matches changed since ``since``.

    @change table=pairs
    @change key=left_id,right_id
    @change removed=removed
    @change cursor=seq
    """
    return []
