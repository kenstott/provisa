# Copyright (c) 2026 Kenneth Stott
# Canary: f62d0e8b-3c59-4a47-9b1e-7d3f2a0c58b4
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The one streaming processor contract every transport conforms to (REQ-940).

A processor is PURE: a stream of schema-declared rows in → a stream of schema-declared rows out, no
side effects. The schema is validated at BOTH ends — input rows before they leave Provisa, output
rows as they return — so a misbehaving external processor cannot inject an off-schema field or a
wrong-typed value into the pipeline. Validation is FAIL-LOUD (project rule): a violation raises
``SchemaViolation`` rather than dropping or coercing the row.
"""

from __future__ import annotations

import datetime as _dt
from abc import ABC, abstractmethod
from collections.abc import Iterable, Iterator
from dataclasses import dataclass
from decimal import Decimal as _Decimal

from provisa.core.ir_types import to_ir

# Canonical IR type name -> the Python types accepted for that column. The declared type is the SAME
# IR vocabulary the whole platform speaks (provisa.core.ir_types, REQ-846/932) — a column's data_type
# IS an IR name — so a contract validates like-for-like against the relation it describes; there is no
# parallel GraphQL/SQL type system here, only IR (GraphQL/SQL are edge projections). A declared type
# is normalized through ``to_ir`` first, so native/alias spellings (varchar, int4, jsonb, …) resolve.
# Temporal scalars arrive as ISO strings over NDJSON framing but as date/time objects from an
# in-process (python impl_kind) transform, so both are accepted.
_TYPE_ACCEPTS: dict[str, tuple[type, ...]] = {
    "smallint": (int,),
    "integer": (int,),
    "bigint": (int,),
    "boolean": (bool,),
    "float": (float, int),  # an int is a valid float
    "double": (float, int),
    "numeric": (float, int, _Decimal),
    "text": (str,),
    "uuid": (str,),
    "date": (str, _dt.date),  # datetime.datetime subclasses datetime.date, so date covers both
    "timestamp": (str, _dt.date),
    "time": (str, _dt.time),
    "bytea": (bytes, str),  # raw bytes in-process; base64/hex string over the wire
}
# IR types that accept ANY value shape (structured JSON, incl. dict/list/bool). Validation of these
# is presence-only — the type check is skipped, so a bool/object is not spuriously rejected.
_ANY_TYPES: frozenset[str] = frozenset({"json"})


def _accepts_for(dtype: str) -> tuple[type, ...] | None:
    """Accepted Python types for a declared column type, or None when the type is unconstrained.

    The declared type is normalized to canonical IR (``to_ir``) so native/alias spellings resolve;
    ``json`` and any type outside the IR vocabulary are treated as accept-any (presence-only)."""
    try:
        ir = to_ir(dtype)
    except ValueError:
        return None  # not an IR type — opaque, accept any value (still presence-checked)
    if ir in _ANY_TYPES:
        return None
    return _TYPE_ACCEPTS.get(ir)


@dataclass(frozen=True)
class Field:  # REQ-940
    """One declared column in a processor schema."""

    name: str
    type: str = "text"  # IR type name (provisa.core.ir_types); default is the IR text scalar


@dataclass(frozen=True)
class Schema:  # REQ-940
    """An ordered set of declared fields; the row contract at one end of a processor."""

    fields: tuple[Field, ...]

    @classmethod
    def of(cls, *fields: tuple[str, str] | str) -> "Schema":
        out = []
        for f in fields:
            out.append(Field(f, "text") if isinstance(f, str) else Field(f[0], f[1]))
        return cls(tuple(out))

    @property
    def names(self) -> tuple[str, ...]:
        return tuple(f.name for f in self.fields)


class SchemaViolation(ValueError):  # REQ-940
    """A row does not conform to the declared schema (unknown field, missing field, or wrong type)."""


def _validate_row(row: dict, schema: Schema, *, where: str) -> dict:
    declared = {f.name: f.type for f in schema.fields}
    extra = set(row) - set(declared)
    if extra:
        raise SchemaViolation(f"{where}: unexpected field(s) {sorted(extra)} not in schema")
    for name, dtype in declared.items():
        if name not in row:
            raise SchemaViolation(f"{where}: missing field {name!r}")
        value = row[name]
        if value is None:
            continue  # nullability is not part of this minimal contract
        accepts = _accepts_for(dtype)
        # bool is an int subclass — reject a bool where a non-boolean IR type is declared.
        if accepts is not None and (
            not isinstance(value, accepts)
            or (to_ir(dtype) != "boolean" and isinstance(value, bool))
        ):
            raise SchemaViolation(
                f"{where}: field {name!r} expected {dtype}, got {type(value).__name__}"
            )
    return row


def validate_rows(rows: Iterable[dict], schema: Schema, *, where: str) -> Iterator[dict]:
    """Lazily validate every row against ``schema``; raise on the first violation (REQ-940)."""
    for row in rows:
        yield _validate_row(row, schema, where=where)


class TransportAdapter(ABC):  # REQ-940
    """A processor transport: schema-validated rows in → schema-validated rows out.

    Concrete adapters (shell/HTTP/gRPC) own framing, streaming, and invocation; they MUST validate
    input rows before sending and output rows on return via :func:`validate_rows`, so every transport
    enforces the same contract identically."""

    @abstractmethod
    def process(
        self, rows: Iterable[dict], *, schema_in: Schema, schema_out: Schema
    ) -> Iterator[dict]:
        """Stream ``rows`` (validated against ``schema_in``) through the external processor and yield
        its output rows (validated against ``schema_out``)."""
        raise NotImplementedError
