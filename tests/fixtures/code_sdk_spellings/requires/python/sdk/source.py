from dataclasses import dataclass

from provisa_sdk import requires, table


@dataclass
class Row:
    id: int
    name: str


@requires("REGION")
@requires("API_KEY", secret=True)
@table
def rows() -> list[Row]:
    """Rows from the API."""
    return []
