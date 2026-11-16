# Copyright (c) 2026 Kenneth Stott
# Canary: a93b952a-f6c3-43f7-9427-7ce5a0ee6e96
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""What a discovery run knows of each table and column, and which columns it compares.

Columns are compared, not tables, and only within a type group (:func:`type_groups`), and within a
group only where their classes intersect (:func:`classes_meet`). A column's classes are every kind
the classifier matched, or only the steward's decided class where one is decided; a column with
none has the class :data:`UNKNOWN`.

A table that requires a parameter (REQ-1986) is never read: it takes part on what its registration
says of it (:attr:`TableFacts.parameters`). Its columns have the class :data:`NOT_READ`, which
meets every class, since no value was read to tell them apart.
"""

# Requirements: REQ-413, REQ-2147

from __future__ import annotations

import re
from dataclasses import dataclass, field

#: The class of a column the classifier matched no kind for (REQ-413): two such columns meet.
UNKNOWN = "unknown"
#: The class of every column of a table that is never read (it requires a parameter): no value
#: was read, so nothing is known of its classes and it is compared with every class.
NOT_READ = "not_read"

#: The type groups, each compared only with itself. A timestamp type is a group of its own.
WHOLE = "whole_number"
DECIMAL = "decimal"
TEXT = "text"
UUID = "uuid"
DATE = "date"
TIMESTAMP = "timestamp"
TIMESTAMPTZ = "timestamptz"
GROUPS: tuple[str, ...] = (WHOLE, DECIMAL, TEXT, UUID, DATE, TIMESTAMP, TIMESTAMPTZ)

_WHOLE_TYPES = frozenset({"smallint", "integer", "bigint"})
_ALONE = {"uuid": UUID, "date": DATE, "timestamp": TIMESTAMP, "timestamptz": TIMESTAMPTZ}
# A sized exact decimal, as a registered column records it (REQ-1961): numeric(precision,scale).
_NUMERIC = re.compile(r"^numeric(?:\s*\(\s*(\d+)\s*(?:,\s*(\d+)\s*)?\))?$")


def type_groups(data_type: str, *, subtype: str | None, array_depth: int) -> frozenset[str]:
    """The type groups a registered column is compared in (REQ-413): whole numbers (integer types
    and decimals with no decimal places); decimals (exact decimals, those with no decimal places
    included, so such a column is in both); text (the character types: the IR's ``text`` with no
    subtype); uuid; date; each timestamp type alone. Every other column -- boolean, floating point,
    JSON, binary, array, geometry, vector, and a subtype held as text -- is in none."""
    if array_depth > 0:
        return frozenset()
    spelled = data_type.strip().lower()
    if spelled in _WHOLE_TYPES:
        return frozenset({WHOLE})
    numeric = _NUMERIC.match(spelled)
    if numeric is not None:
        precision, scale = numeric.group(1), numeric.group(2)
        # numeric(p) has scale 0 in SQL; an unsized numeric's scale is not known to be 0.
        zero_scale = precision is not None and (scale is None or int(scale) == 0)
        return frozenset({WHOLE, DECIMAL}) if zero_scale else frozenset({DECIMAL})
    if spelled == "text":
        # A subtype on a text base (inet, xml, a range, a geometry) is not a character type.
        return frozenset({TEXT}) if subtype is None else frozenset()
    group = _ALONE.get(spelled)
    return frozenset() if group is None else frozenset({group})


@dataclass(frozen=True)
class ColumnFacts:
    """One registered column as a run sees it."""

    name: str
    data_type: str
    description: str | None
    tags: tuple[str, ...]
    #: Its classes: the decided class alone, or every kind matched, or ``(UNKNOWN,)``.
    classes: tuple[str, ...]
    #: Whether one of its classes is a sensitive kind: its class is sent, never its values.
    sensitive: bool
    #: Distinct non-null values read from the table; never sent for a sensitive column.
    samples: tuple[str, ...]
    is_primary_key: bool
    groups: frozenset[str]
    #: The parameter this column is, as a caller names it, when a read must give it a value.
    required_parameter: str | None = None


@dataclass(frozen=True)
class TableFacts:
    """One registered table as a run sees it: every column, those not compared included."""

    table_id: int
    domain_id: str
    schema_name: str
    table_name: str
    description: str | None
    tags: tuple[str, ...]
    #: The recorded modeling role, ``fact`` or ``dimension``; None where none is recorded.
    role: str | None
    columns: tuple[ColumnFacts, ...] = field(default_factory=tuple)

    @property
    def parameters(self) -> tuple[tuple[str, str], ...]:
        """The parameters a read of the table must give, each with its type. A table with any is
        never read by discovery: no row count, no profile, no value of it is known."""
        return tuple(
            (c.required_parameter, c.data_type)
            for c in self.columns
            if c.required_parameter is not None
        )

    @property
    def reduced(self) -> bool:
        """Whether discovery works on reduced information for this table: it requires a
        parameter, so only what its registration says is known."""
        return bool(self.parameters)

    def column(self, name: str) -> ColumnFacts | None:
        return next((c for c in self.columns if c.name == name), None)

    @property
    def primary_key(self) -> tuple[str, ...]:
        return tuple(c.name for c in self.columns if c.is_primary_key)


def classes_of(matched: set[str], decided: set[str]) -> tuple[str, ...]:
    """A column's classes (REQ-413): only the steward's decided class where one is decided (an
    accepted classification); else every kind the classifier matched; a column with none has the
    class unknown."""
    if decided:
        return tuple(sorted(decided))
    if matched:
        return tuple(sorted(matched))
    return (UNKNOWN,)


def classes_meet(a: ColumnFacts, b: ColumnFacts) -> bool:
    """Whether two columns' classes intersect: unknown meets unknown. A column never read
    (:data:`NOT_READ`) meets every column: no value was read that could tell them apart."""
    if NOT_READ in a.classes or NOT_READ in b.classes:
        return True
    return bool(set(a.classes) & set(b.classes))


def comparable(a: ColumnFacts, b: ColumnFacts, group: str) -> bool:
    """Whether two columns are compared in ``group``: both in it, and their classes intersect."""
    return group in a.groups and group in b.groups and classes_meet(a, b)
