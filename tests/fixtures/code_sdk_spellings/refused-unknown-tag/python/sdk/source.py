from dataclasses import dataclass

import provisa_sdk


@dataclass
class Row:
    id: int
    name: str


@provisa_sdk.tabel
def rows() -> list[Row]:
    return []
