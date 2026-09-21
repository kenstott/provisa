# Copyright (c) 2026 Kenneth Stott
# Canary: a3f8d1e2-6b4c-4f9a-8d2e-1c7b5a9f3e60
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""ClickHouse native landing terminals (REQ-1730/REQ-1633): the DDL/type-mapping/land helpers
``ClickHouseFederationRuntime.attach_landed_source``/``land_table`` are built on. ClickHouse had
neither terminal before this — REQ-1633's own audit flagged it as one of only two engines
(ClickHouse, MssqlWarehouseRuntime) implementing none of the three landing terminals at all.

``_CHBackend.command()`` (clickhouse_runtime.py) takes a bare SQL string, no bind parameters — every
value below is formatted as a SQL literal directly into the statement, unlike the DBAPI-parameterized
stores (Snowflake/BigQuery/Databricks) that hand a cursor a separate params tuple.
"""

from __future__ import annotations

import datetime
import decimal
import json
import re
from typing import TYPE_CHECKING, Any

from provisa.core.ir_types import to_ir

if TYPE_CHECKING:
    from provisa.federation.clickhouse_runtime import _CHBackend

# (database, table) — ClickHouse is a flat namespace (module doc, clickhouse_runtime.py): there is
# no catalog/schema split, so a landed replica's physical address is just these two parts.
Parts = tuple[str, str]

# IR type -> ClickHouse column type. A landed replica carries the source's own values; integers and
# decimals map to a safe wide ClickHouse type. JSON lands as String (ClickHouse's own JSON type is
# still experimental as of 24.3, the pinned demo image) — never silently narrowed.
_IR_TO_CLICKHOUSE: dict[str, str] = {
    "smallint": "Nullable(Int16)",
    "integer": "Nullable(Int32)",
    "bigint": "Nullable(Int64)",
    "text": "Nullable(String)",
    "boolean": "Nullable(Bool)",
    "float": "Nullable(Float32)",
    "double": "Nullable(Float64)",
    "numeric": "Nullable(Decimal(38, 9))",
    "date": "Nullable(Date)",
    "timestamp": "Nullable(DateTime64(3))",
    "time": "Nullable(String)",
    "uuid": "Nullable(UUID)",
    "bytea": "Nullable(String)",
    "json": "Nullable(String)",
}


def ddl_type(ir_type: str) -> str:
    """ClickHouse column type for a canonical IR type name — raises on an unknown type (never a
    silent widen), mirroring the store-DDL discipline of the other engines' own ``ddl_type``."""
    canonical = to_ir(ir_type)
    ch_type = _IR_TO_CLICKHOUSE.get(canonical)
    if ch_type is None:
        raise ValueError(
            f"no ClickHouse type mapping for IR type {ir_type!r} (canonical {canonical!r})"
        )
    return ch_type


_VALID_IDENTIFIER = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


def identifier(raw: str) -> str:
    """A database/table/column name ClickHouse accepts unquoted — hyphens fold to underscores;
    anything else outside the identifier alphabet is a caller error, never silently mangled."""
    name = raw.replace("-", "_")
    if not _VALID_IDENTIFIER.match(name):
        raise ValueError(f"invalid ClickHouse identifier: {raw!r}")
    return name


def qualified(parts: Parts) -> str:
    return ".".join(f'"{part}"' for part in parts)


def ensure_namespace(backend: "_CHBackend", database: str) -> None:
    backend.command(f'CREATE DATABASE IF NOT EXISTS "{database}"')


def existing_columns(backend: "_CHBackend", parts: Parts) -> list[str]:
    """The table's current column names in position order, ``[]`` if it does not exist (ClickHouse's
    own ``system.columns`` reports nothing for an absent table rather than erroring, unlike
    ``information_schema`` on some other engines)."""
    database, table = parts
    rows, _ = backend.query(
        "SELECT name FROM system.columns WHERE database = "
        f"{_lit(database)} AND table = {_lit(table)} ORDER BY position"
    )
    return [str(r[0]) for r in rows]


def create_ddl(
    parts: Parts, columns: list[tuple[str, str]], pk_columns: tuple[str, ...] = ()
) -> str:
    """MergeTree, ClickHouse's own general-purpose table engine (REQ-1730): ``ORDER BY`` is
    mandatory even with no declared primary key — ``tuple()`` (no sort order) is MergeTree's own
    documented way to say that, not a Provisa convention."""
    cols = ", ".join(f'"{name}" {ddl_type(ir_type)}' for name, ir_type in columns)
    order_by = ", ".join(f'"{c}"' for c in pk_columns) if pk_columns else ""
    order_clause = f"ORDER BY ({order_by})" if order_by else "ORDER BY tuple()"
    return f"CREATE TABLE IF NOT EXISTS {qualified(parts)} ({cols}) ENGINE = MergeTree() {order_clause}"


def reconcile_clickhouse_native(
    backend: "_CHBackend",
    *,
    parts: Parts,
    columns: list[tuple[str, str]],
    pk_columns: list[str] | None = None,
) -> str:
    """Converge the landed replica to ``columns`` (DDL only, no data). Returns
    ``created`` | ``kept`` | ``recreated``: an existing matching table survives a restart; a
    drifted one (column set/order changed) is recreated and its data re-lands on the next refresh.
    ClickHouse's MergeTree ``ORDER BY`` is set once at creation and cannot be altered in place, so
    a primary-key change also forces a recreate, same as the column-set check."""
    database, _ = parts
    ensure_namespace(backend, database)
    want = [name for name, _ in columns]
    have = existing_columns(backend, parts)
    if not have:
        backend.command(create_ddl(parts, columns, tuple(pk_columns or ())))
        return "created"
    if have == want:
        return "kept"
    backend.command(f"DROP TABLE IF EXISTS {qualified(parts)}")
    backend.command(create_ddl(parts, columns, tuple(pk_columns or ())))
    return "recreated"


def _lit(value: Any) -> str:
    """A Python value as a ClickHouse SQL literal — ``_CHBackend.command`` takes a bare string, no
    bind parameters, so every value is inlined directly (unlike the DBAPI-parameterized stores)."""
    if value is None:
        return "NULL"
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, (int, float, decimal.Decimal)):
        return str(value)
    if isinstance(value, (datetime.datetime, datetime.date, datetime.time)):
        return f"'{value.isoformat()}'"
    if isinstance(value, (dict, list)):
        return f"'{json.dumps(value, default=str).replace(chr(39), chr(39) * 2)}'"
    return "'{}'".format(str(value).replace("'", "''").replace("\\", "\\\\"))


def land_clickhouse_native(
    backend: "_CHBackend",
    *,
    parts: Parts,
    columns: list[tuple[str, str]],
    rows: list[dict],
    shape: str,
) -> str:
    """Land ``rows`` into the replica at ``parts``: REPLACE truncates then inserts (a full refresh
    of the CONTENTS, never of the table — its DDL is the reconcile's), APPEND only inserts. CDC is
    not a landing shape this store implements: refusing loudly here beats a replica that silently
    drifts from its source (same discipline as Snowflake's/BigQuery's own ``land_*_native``)."""
    if shape == "cdc":
        raise NotImplementedError(
            "ClickHouse native landing has no CDC shape; use replace or append"
        )
    if shape not in ("replace", "append"):
        raise ValueError(f"unknown landing shape {shape!r}")
    fq = qualified(parts)
    if shape == "replace":
        backend.command(f"TRUNCATE TABLE {fq}")
    if rows:
        names = [name for name, _ in columns]
        collist = ", ".join(f'"{n}"' for n in names)
        values = ", ".join("(" + ", ".join(_lit(r.get(n)) for n in names) + ")" for r in rows)
        backend.command(f"INSERT INTO {fq} ({collist}) VALUES {values}")
    return fq
