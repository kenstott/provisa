from dataclasses import dataclass


@dataclass
class Row:
    id: int
    name: str


def cancel(order_id: int) -> int:
    """Cancel one order.

    @command
    """
    return 1
