from dataclasses import dataclass

from provisa_sdk import command


@dataclass
class Row:
    id: int
    name: str


@command
def cancel(order_id: int) -> int:
    """Cancel one order."""
    return 1
