# Copyright (c) 2026 Kenneth Stott
# Canary: a894ce98-2309-43b3-a88d-9d93e3c6826b
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The one rule for writing a value into SQL text as a literal, per dialect.

A value is bound as a driver parameter wherever the target takes one. Where it does not, it is
written with :func:`sql_literal`, which knows each dialect's string escaping: a doubled quote is
the whole rule in standard SQL (PostgreSQL, DuckDB, Trino), while ClickHouse, the MySQL family,
Snowflake, BigQuery and Databricks also read a backslash as an escape. The string rendering is
sqlglot's own per-dialect generator. A value a dialect cannot hold -- a NUL character, a NaN or
infinity where the dialect has no spelling for it, a type with no literal form -- is refused by
name, never written approximately.
"""

from __future__ import annotations

import datetime as _dt
import json
import math
import uuid
from decimal import Decimal
from typing import Any

#: Dialects whose SQL strings follow the standard rule but which sqlglot does not name.
_STANDARD_STRING_DIALECTS = {"pinot": "trino"}

#: NaN and infinity, where the dialect spells them.
_NON_FINITE = {
    "trino": {"nan": "nan()", "inf": "infinity()", "-inf": "-infinity()"},
    "postgres": {
        "nan": "'NaN'::float8",
        "inf": "'Infinity'::float8",
        "-inf": "'-Infinity'::float8",
    },
    "duckdb": {"nan": "'nan'::DOUBLE", "inf": "'inf'::DOUBLE", "-inf": "'-inf'::DOUBLE"},
}


class UnencodableLiteral(ValueError):
    """A value the dialect cannot hold as a SQL literal."""

    def __init__(self, value: Any, dialect: str, reason: str) -> None:
        self.dialect = dialect
        super().__init__(
            f"a {type(value).__name__} value cannot be written as a {dialect} literal: {reason}"
        )


def _string(text: str, dialect: str) -> str:
    import sqlglot.expressions as exp

    if "\x00" in text:
        raise UnencodableLiteral(text, dialect, "it holds a NUL character")
    return exp.Literal.string(text).sql(dialect=_STANDARD_STRING_DIALECTS.get(dialect, dialect))


def _typed(text: str, sql_type: str, dialect: str) -> str:
    """``CAST('<text>' AS <type>)`` in the dialect's own spelling."""
    import sqlglot.expressions as exp

    if "\x00" in text:
        raise UnencodableLiteral(text, dialect, "it holds a NUL character")
    cast = exp.cast(exp.Literal.string(text), sql_type)
    return cast.sql(dialect=_STANDARD_STRING_DIALECTS.get(dialect, dialect))


def _float(value: float, dialect: str) -> str:
    if math.isfinite(value):
        return repr(value)
    name = "nan" if math.isnan(value) else ("inf" if value > 0 else "-inf")
    spelled = _NON_FINITE.get(dialect, {}).get(name)
    if spelled is None:
        raise UnencodableLiteral(value, dialect, f"{name} has no literal here")
    return spelled


def _bytes(value: bytes, dialect: str) -> str:
    if dialect in ("trino", "duckdb"):
        return f"X'{value.hex()}'"
    if dialect == "postgres":
        return (
            f"'\\x{value.hex()}'"  # bytea's hex input format; standard strings keep the backslash
        )
    raise UnencodableLiteral(value, dialect, "binary values have no literal here")


def sql_literal(value: Any, dialect: str) -> str:  # noqa: PLR0911 -- one return per value kind
    """``value`` as a SQL literal of ``dialect`` (a sqlglot dialect name, or ``pinot``).

    Raises :class:`UnencodableLiteral` for a value the dialect cannot hold, and for a type with
    no literal form."""
    import sqlglot.expressions as exp

    if value is None:
        return "NULL"
    if isinstance(value, bool):  # before int: a bool is an int
        return exp.Boolean(this=value).sql(dialect=_STANDARD_STRING_DIALECTS.get(dialect, dialect))
    if isinstance(value, int):
        return str(value)
    if isinstance(value, float):
        return _float(value, dialect)
    if isinstance(value, Decimal):
        if not value.is_finite():
            raise UnencodableLiteral(value, dialect, "it is not a finite number")
        return str(value)
    if isinstance(value, str):
        return _string(value, dialect)
    if isinstance(value, _dt.datetime):  # before date: a datetime is a date
        return _typed(value.isoformat(), "TIMESTAMP", dialect)
    if isinstance(value, _dt.date):
        return _typed(value.isoformat(), "DATE", dialect)
    if isinstance(value, _dt.time):
        return _typed(value.isoformat(), "TIME", dialect)
    if isinstance(value, uuid.UUID):
        return _string(str(value), dialect)
    if isinstance(value, (bytes, bytearray, memoryview)):
        return _bytes(bytes(value), dialect)
    if isinstance(value, (dict, list)):
        return _string(json.dumps(value), dialect)
    raise UnencodableLiteral(value, dialect, "the type has no literal form")
