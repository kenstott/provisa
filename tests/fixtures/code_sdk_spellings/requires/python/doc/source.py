from dataclasses import dataclass


@dataclass
class Row:
    id: int
    name: str


def rows() -> list[Row]:
    """Rows from the API.

    @table
    @requires API_KEY:secret
    @requires REGION
    """
    return []
