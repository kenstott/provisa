# Copyright (c) 2026 Kenneth Stott
# Canary: 988e1f99-b77b-45d4-99a2-7fa50b679fde
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.

"""Phase 0 gate: assert the SQLAlchemy Core metadata in schema_admin.py /
schema_org.py mirrors the authoritative DDL (schema.sql + billing + audit).

Parses the SQL to extract table -> {columns, primary key} and compares against
the metadata: same tables per bucket, same column names, same PK columns, and
type *family* parity (PG-specific types map to their portable substitutes:
JSONB/TEXT[] -> JSON, SERIAL -> int, BIGSERIAL -> bigint, TIMESTAMPTZ ->
timestamp, BYTEA -> binary, DOUBLE PRECISION -> float).
"""

from __future__ import annotations

import re
from pathlib import Path

import pytest
from sqlalchemy import (
    JSON,
    BigInteger,
    Boolean,
    Date,
    DateTime,
    Float,
    Integer,
    LargeBinary,
    Text,
    Uuid,
)

from provisa.audit.query_log import AUDIT_SCHEMA_SQL
from provisa.core import schema_admin, schema_org

SCHEMA_SQL = Path(__file__).parents[2] / "provisa" / "core" / "schema.sql"

# The org/user/invite registry is authored ONLY as portable SQLAlchemy metadata
# (schema_admin.REGISTRY_TABLES, created via metadata.create_all on the platform
# control plane, which may be any SQLAlchemy backend). It has no raw-SQL DDL to
# mirror, so it is excluded from SQL<->metadata parity and asserted structurally.
REGISTRY_ONLY_TABLES = {
    "orgs",
    "user_profiles",
    "user_org_memberships",
    "local_users",
    "org_invites",
    # Billing tables are now portable metadata too (schema_admin), created via
    # metadata.create_all — no raw SQL DDL to mirror.
    "tenants",
    "tenant_config",
}

# No admin/platform table has raw SQL DDL any longer — all are metadata-authoritative.
ADMIN_TABLES_IN_SCHEMA_SQL: set[str] = set()

# Tables authored ONLY as portable SQLAlchemy metadata (created via metadata.create_all),
# with no raw SQL DDL to mirror — excluded from SQL<->metadata parity on either module.
# events / event_status: the event-loop control plane (REQ-933..942), portable metadata created
# via metadata.create_all on either pg or sqlite — no raw SQL DDL to mirror.
METADATA_ONLY_TABLES = REGISTRY_ONLY_TABLES | {
    "query_sla_log",
    "source_catalog_cache",
    "events",
    "event_status",
}

_CONSTRAINT_KW = {
    "unique",
    "primary",
    "check",
    "foreign",
    "constraint",
    "exclude",
}


def _strip_schema_prefix(name: str) -> str:
    return name.split(".")[-1]


def _split_top_level(body: str) -> list[str]:
    """Split a CREATE TABLE body on top-level commas (ignoring parens and single-quoted literals)."""
    parts, depth, cur, in_quote = [], 0, [], False
    for ch in body:
        if ch == "'":
            in_quote = not in_quote
        elif not in_quote and ch == "(":
            depth += 1
        elif not in_quote and ch == ")":
            depth -= 1
        if ch == "," and depth == 0 and not in_quote:
            parts.append("".join(cur))
            cur = []
        else:
            cur.append(ch)
    if "".join(cur).strip():
        parts.append("".join(cur))
    return parts


def _sql_type_family(coldef: str) -> str:
    # Isolate the type spec: drop the leading column name, then truncate at the
    # first non-type clause so column names, DEFAULT expressions (e.g.
    # gen_random_uuid()), and CHECK literals can't be misread as the type.
    parts = coldef.strip().split(None, 1)
    spec = parts[1] if len(parts) > 1 else parts[0]
    spec_u = spec.upper()
    for kw in (
        " DEFAULT ",
        " CHECK",
        " REFERENCES",
        " GENERATED",
        " NOT NULL",
        " NULL",
        " UNIQUE",
        " PRIMARY",
    ):
        idx = spec_u.find(kw)
        if idx != -1:
            spec_u = spec_u[:idx]
    d = spec_u
    # order matters: check array and specific types first
    if "[]" in d:
        return "json"
    if "JSONB" in d or "JSON" in d:
        return "json"
    if "BIGSERIAL" in d:
        return "bigint"
    if "SERIAL" in d:
        return "int"
    if "UUID" in d:
        return "uuid"
    if "BYTEA" in d:
        return "binary"
    if "BOOLEAN" in d or re.search(r"\bBOOL\b", d):
        return "bool"
    if "DOUBLE PRECISION" in d or re.search(r"\bREAL\b", d) or re.search(r"\bFLOAT\b", d):
        return "float"
    if "TIMESTAMP" in d:
        return "timestamp"
    if re.search(r"\bDATE\b", d):
        return "date"
    if re.search(r"\bBIGINT\b", d):
        return "bigint"
    if re.search(r"\bINTEGER\b", d) or re.search(r"\bINT\b", d):
        return "int"
    if "TEXT" in d or "VARCHAR" in d or "CHAR" in d:
        return "text"
    raise AssertionError(f"unrecognized SQL type in column def: {coldef!r}")


def _metadata_type_family(col) -> str:
    t = col.type
    if isinstance(t, JSON):
        return "json"
    if isinstance(t, Uuid):
        return "uuid"
    if isinstance(t, LargeBinary):
        return "binary"
    if isinstance(t, Boolean):
        return "bool"
    if isinstance(t, Float):
        return "float"
    if isinstance(t, DateTime):
        return "timestamp"
    if isinstance(t, Date):
        return "date"
    if isinstance(t, BigInteger):
        return "bigint"
    if isinstance(t, Integer):
        return "int"
    if isinstance(t, Text):
        return "text"
    raise AssertionError(f"unrecognized metadata type for column {col.name}: {t!r}")


class ParsedTable:
    def __init__(self) -> None:
        self.columns: dict[str, str] = {}  # name -> type family
        self.pk: set[str] = set()


def _strip_comments(sql: str) -> str:
    return "\n".join(re.sub(r"--.*$", "", line) for line in sql.splitlines())


def _parse_sql(*sql_blobs: str) -> dict[str, ParsedTable]:
    tables: dict[str, ParsedTable] = {}
    combined = _strip_comments("\n".join(sql_blobs))

    # CREATE TABLE [IF NOT EXISTS] <name> ( <body> );
    create_re = re.compile(
        r"CREATE\s+TABLE\s+(?:IF\s+NOT\s+EXISTS\s+)?([\w.]+)\s*\((.*?)\)\s*;",
        re.IGNORECASE | re.DOTALL,
    )
    for m in create_re.finditer(combined):
        name = _strip_schema_prefix(m.group(1))
        pt = tables.setdefault(name, ParsedTable())
        for raw in _split_top_level(m.group(2)):
            item = raw.strip()
            if not item:
                continue
            first = item.split()[0].lower().strip('"')
            if first in _CONSTRAINT_KW:
                # table-level constraint
                if first == "primary":
                    cols = re.findall(r"\(([^)]*)\)", item)
                    if cols:
                        pt.pk.update(c.strip().strip('"') for c in cols[0].split(","))
                continue
            col_name = first
            pt.columns[col_name] = _sql_type_family(item)
            if re.search(r"\bPRIMARY\s+KEY\b", item, re.IGNORECASE):
                pt.pk.add(col_name)

    # ALTER TABLE <name> ADD COLUMN [IF NOT EXISTS] <col> <type...>
    alter_re = re.compile(
        r"ALTER\s+TABLE\s+([\w.]+)\s+ADD\s+COLUMN\s+(?:IF\s+NOT\s+EXISTS\s+)?"
        r"(\w+)\s+([^;]+?)(?:;|$)",
        re.IGNORECASE,
    )
    for m in alter_re.finditer(combined):
        name = _strip_schema_prefix(m.group(1))
        if name not in tables:
            continue
        col_name = m.group(2)
        # stop the type text before a trailing statement fragment
        type_text = m.group(3).split("\n")[0]
        tables[name].columns[col_name] = _sql_type_family(f"{col_name} {type_text}")

    return tables


@pytest.fixture(scope="module")
def parsed() -> dict[str, ParsedTable]:
    return _parse_sql(SCHEMA_SQL.read_text(), AUDIT_SCHEMA_SQL)


def _bucket(parsed: dict[str, ParsedTable], admin: bool) -> set[str]:
    return {n for n in parsed if (n in ADMIN_TABLES_IN_SCHEMA_SQL) == admin}


@pytest.mark.parametrize(
    "meta_module, is_admin",
    [(schema_org, False), (schema_admin, True)],
)
def test_table_set_matches(parsed, meta_module, is_admin):
    expected = _bucket(parsed, is_admin)
    # Metadata-only tables have no parsed SQL counterpart; compare on the raw-DDL-mirrored set.
    actual = set(meta_module.metadata.tables.keys()) - METADATA_ONLY_TABLES
    assert actual == expected, (
        f"table mismatch (admin={is_admin}): missing={expected - actual}, extra={actual - expected}"
    )


@pytest.mark.parametrize("meta_module", [schema_org, schema_admin])
def test_columns_and_pk_match(parsed, meta_module):
    for tname, table in meta_module.metadata.tables.items():
        if tname in METADATA_ONLY_TABLES:
            continue  # metadata-authoritative; no SQL counterpart
        pt = parsed[tname]
        meta_cols = set(table.columns.keys())
        sql_cols = set(pt.columns.keys())
        assert meta_cols == sql_cols, (
            f"{tname} column mismatch: missing={sql_cols - meta_cols}, extra={meta_cols - sql_cols}"
        )
        meta_pk = {c.name for c in table.primary_key.columns}
        assert meta_pk == pt.pk, f"{tname} PK mismatch: sql={pt.pk} meta={meta_pk}"


@pytest.mark.parametrize("meta_module", [schema_org, schema_admin])
def test_type_families_match(parsed, meta_module):
    for tname, table in meta_module.metadata.tables.items():
        if tname in METADATA_ONLY_TABLES:
            continue  # metadata-authoritative; no SQL counterpart
        pt = parsed[tname]
        for col in table.columns:
            expected = pt.columns[col.name]
            actual = _metadata_type_family(col)
            assert actual == expected, (
                f"{tname}.{col.name} type family: sql={expected} meta={actual}"
            )
