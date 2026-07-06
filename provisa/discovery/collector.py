# Copyright (c) 2026 Kenneth Stott
# Canary: 0f166822-96d8-4ead-871b-a59f24fc0552
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Collect table metadata from Trino and PG for LLM relationship discovery."""

from __future__ import annotations

import logging
import re
from dataclasses import dataclass
from typing import Any

import asyncpg

from provisa.compiler.naming import source_to_catalog
from provisa.otel_compat import get_tracer as _get_tracer

_tracer = _get_tracer(__name__)

log = logging.getLogger(__name__)

_SAFE_IDENT = re.compile(r"^[a-zA-Z_][a-zA-Z0-9_]*$")


def _escape_literal(value: str) -> str:
    return value.replace("'", "''")


# Requirements: REQ-018, REQ-019, REQ-167, REQ-302, REQ-413


@dataclass
class TableMeta:  # REQ-018, REQ-413
    table_id: int
    source_id: str
    domain_id: str
    schema_name: str
    table_name: str
    columns: list[dict]  # [{name, type}]
    sample_values: list[dict]  # [{col: val, ...}]


@dataclass
class DiscoveryInput:  # REQ-018, REQ-167, REQ-413
    tables: list[TableMeta]
    existing_relationships: list[dict]
    rejected_pairs: list[dict]


def _validate_ident(value: str) -> str:
    if not _SAFE_IDENT.match(value):
        raise ValueError(f"Unsafe identifier: {value!r}")
    return value


async def _try_engine_rows(engine: Any, sql: str) -> list | None:
    """Run a best-effort read through the engine seam; None if the engine/connector
    can't answer (e.g. no constraint metadata, table not sampleable). One catch point
    so callers stay blind-except-free."""
    try:
        return (await engine.execute_engine(sql)).rows
    except Exception:
        return None


async def _fetch_column_types(engine: Any, catalog: str, schema: str, table: str) -> list[dict]:
    cat = _validate_ident(catalog)
    _validate_ident(schema)
    _validate_ident(table)
    res = await engine.execute_engine(
        f"SELECT column_name, data_type "
        f"FROM {cat}.information_schema.columns "
        f"WHERE table_schema = '{schema}' AND table_name = '{table}' "
        f"ORDER BY ordinal_position"
    )
    return [{"name": row[0], "type": row[1].lower()} for row in res.rows]


async def _fetch_samples(
    engine: Any, catalog: str, schema: str, table: str, columns: list[dict], sample_size: int
) -> list[dict]:
    cat = _validate_ident(catalog)
    sch = _validate_ident(schema)
    tbl = _validate_ident(table)
    col_names = [_validate_ident(c["name"]) for c in columns[:10]]  # cap columns
    if not col_names:
        return []
    cols_sql = ", ".join(col_names)
    rows = await _try_engine_rows(
        engine, f"SELECT {cols_sql} FROM {cat}.{sch}.{tbl} LIMIT {int(sample_size)}"
    )
    if rows is None:
        log.warning("Failed to sample %s.%s.%s", cat, sch, tbl)
        return []
    return [
        {col_names[i]: str(val) if val is not None else None for i, val in enumerate(row)}
        for row in rows
    ]


async def _fetch_fk_candidates(engine: Any, catalog: str, schema: str, table: str) -> list[dict]:
    """FK candidates from TABLE_CONSTRAINTS + KEY_COLUMN_USAGE, run through the engine seam.
    Returns {constraint_name, column_name, referenced_table, referenced_column}; empty when the
    engine/connector doesn't expose constraint metadata."""
    cat = _validate_ident(catalog)
    sch = _escape_literal(schema)
    tbl = _escape_literal(table)
    rows = await _try_engine_rows(
        engine,
        f"SELECT tc.constraint_name, kcu.column_name, "
        f"ccu.table_name AS referenced_table, ccu.column_name AS referenced_column "
        f"FROM {cat}.information_schema.table_constraints tc "
        f"JOIN {cat}.information_schema.key_column_usage kcu "
        f"  ON tc.constraint_name = kcu.constraint_name "
        f"JOIN {cat}.information_schema.constraint_column_usage ccu "
        f"  ON tc.constraint_name = ccu.constraint_name "
        f"WHERE tc.table_schema = '{sch}' AND tc.table_name = '{tbl}' "
        f"  AND tc.constraint_type = 'FOREIGN KEY'",
    )
    return [
        {
            "constraint_name": row[0],
            "column_name": row[1],
            "referenced_table": row[2],
            "referenced_column": row[3],
        }
        for row in (rows or [])  # empty when the connector doesn't expose constraint metadata
    ]


async def collect_metadata(  # REQ-018, REQ-019, REQ-167, REQ-302, REQ-413
    engine: Any,
    pg_conn: asyncpg.Connection,
    scope: str,
    scope_id: str | int | None = None,
    sample_size: int = 20,
) -> DiscoveryInput:
    """Collect metadata for LLM discovery.

    scope: "table", "domain", "cross-domain"
    scope_id: table_id (int) for "table", domain_id (str) for "domain"
    """
    with _tracer.start_as_current_span("discovery.collect_metadata") as span:
        all_tables = await pg_conn.fetch(
            "SELECT id, source_id, domain_id, schema_name, table_name FROM registered_tables ORDER BY id"
        )
        all_tables = [dict(r) for r in all_tables]

        if scope == "table":
            target_table = next((t for t in all_tables if t["id"] == scope_id), None)
            if target_table is None:
                raise ValueError(f"Table {scope_id} not found")
            domain_id = target_table["domain_id"]
            tables = [t for t in all_tables if t["domain_id"] == domain_id]
        elif scope == "domain":
            tables = [t for t in all_tables if t["domain_id"] == scope_id]
            if not tables:
                raise ValueError(f"No tables found in domain {scope_id}")
        elif scope == "cross-domain":
            tables = all_tables
        else:
            raise ValueError(f"Invalid scope: {scope!r}")

        table_metas: list[TableMeta] = []
        for t in tables:
            catalog = source_to_catalog(t["source_id"])
            columns = await _fetch_column_types(engine, catalog, t["schema_name"], t["table_name"])
            samples = await _fetch_samples(
                engine, catalog, t["schema_name"], t["table_name"], columns, sample_size
            )
            table_metas.append(
                TableMeta(
                    table_id=t["id"],
                    source_id=t["source_id"],
                    domain_id=t["domain_id"],
                    schema_name=t["schema_name"],
                    table_name=t["table_name"],
                    columns=columns,
                    sample_values=samples,
                )
            )

        existing = await pg_conn.fetch(
            "SELECT source_table_id, target_table_id, source_column, target_column, cardinality "
            "FROM relationships"
        )
        existing_rels = [dict(r) for r in existing]

        result = DiscoveryInput(
            tables=table_metas,
            existing_relationships=existing_rels,
            rejected_pairs=[],
        )
        span.set_attribute("discovery.table_count", len(table_metas))
        return result


async def collect_fk_candidates(  # REQ-018, REQ-413
    engine: Any,
    pg_conn: asyncpg.Connection,
    scope: str,
    scope_id: str | int | None = None,
) -> list:
    """Return RelationshipCandidate objects derived from FK constraints, read through the
    engine seam (the engine's information_schema).

    Imported lazily to avoid circular import with analyzer.
    """
    from provisa.discovery.analyzer import RelationshipCandidate

    all_tables = await pg_conn.fetch(
        "SELECT id, source_id, domain_id, schema_name, table_name FROM registered_tables ORDER BY id"
    )
    all_tables = [dict(r) for r in all_tables]
    table_by_name: dict[str, dict] = {t["table_name"]: t for t in all_tables}

    if scope == "table":
        target = next((t for t in all_tables if t["id"] == scope_id), None)
        if target is None:
            return []
        domain_id = target["domain_id"]
        tables = [t for t in all_tables if t["domain_id"] == domain_id]
    elif scope == "domain":
        tables = [t for t in all_tables if t["domain_id"] == scope_id]
    elif scope == "cross-domain":
        tables = all_tables
    else:
        return []

    existing = {
        (r["source_table_id"], r["source_column"], r["target_table_id"], r["target_column"])
        for r in await pg_conn.fetch(
            "SELECT source_table_id, source_column, target_table_id, target_column FROM relationships"
        )
    }

    candidates: list[RelationshipCandidate] = []
    for t in tables:
        catalog = source_to_catalog(t["source_id"])
        fks = await _fetch_fk_candidates(engine, catalog, t["schema_name"], t["table_name"])
        for fk in fks:
            target = table_by_name.get(fk["referenced_table"])
            if target is None:
                continue
            key = (t["id"], fk["column_name"], target["id"], fk["referenced_column"])
            if key in existing:
                continue
            candidates.append(
                RelationshipCandidate(
                    source_table_id=t["id"],
                    source_column=fk["column_name"],
                    target_table_id=target["id"],
                    target_column=fk["referenced_column"],
                    cardinality="many_to_one",
                    confidence=1.0,
                    reasoning="Foreign key constraint",
                    suggested_name=f"{t['table_name']}-{fk['column_name']}-to-{fk['referenced_table']}",
                )
            )
    return candidates
