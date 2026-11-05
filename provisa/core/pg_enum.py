# Copyright (c) 2026 Kenneth Stott
# Canary: 8d3f1c62-47ae-4e0b-b5d9-6a2c91e07f34
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A PostgreSQL source's enum type, as the source declares it (REQ-2171): the numbers a client is
told and the labels in their order. Only the source holds these: an engine reads an enum as its
label, and its label is all a value of one is anywhere else."""

# Requirements: REQ-2167, REQ-2171

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any

_ARRAY_SPECIAL = frozenset('{},"\\')


@dataclass(frozen=True)
class SourceEnum:
    schema: str
    name: str
    oid: int
    array_oid: int
    labels: tuple[str, ...]

    @property
    def shown(self) -> str:
        """The type as PostgreSQL's messages name it, on the default search path."""
        return self.name if self.schema == "public" else f"{self.schema}.{self.name}"


#: (schema, type name) -> the enum.
SourceEnums = Mapping[tuple[str, str], SourceEnum]


def named(enums: SourceEnums, schema: str | None, name: str) -> SourceEnum | None:
    """The enum a statement names: with its schema, or with none if one type has that name."""
    if schema:
        return enums.get((schema, name))
    held = [enum for (_, type_name), enum in enums.items() if type_name == name]
    return held[0] if len(held) == 1 else None


def of_oid(enums: SourceEnums, oid: int) -> SourceEnum | None:
    return next((enum for enum in enums.values() if enum.oid == oid), None)


def _element(label: str) -> str:
    plain = (
        label
        and label.upper() != "NULL"
        and not _ARRAY_SPECIAL.intersection(label)
        and not any(c.isspace() for c in label)
    )
    if plain:
        return label
    return '"' + label.replace("\\", "\\\\").replace('"', '\\"') + '"'


def array_text(value: Any) -> Any:
    """An array of labels in PostgreSQL's text form (array_out); a NULL stays one."""
    if value is None or isinstance(value, str):
        return value
    return "{" + ",".join("NULL" if v is None else _element(str(v)) for v in value) + "}"
