# Copyright (c) 2025 Kenneth Stott
# Canary: 8f8ec523-0921-4866-889d-9a3f38256e46
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""FastAPI app factory with startup hooks for config load and schema generation."""

from __future__ import annotations

import os
from contextlib import asynccontextmanager
from pathlib import Path

import asyncpg
import trino
import yaml
from fastapi import FastAPI

from provisa.api.data.endpoint import router as data_router
from provisa.compiler.introspect import ColumnMetadata, introspect_tables
from provisa.compiler.schema_gen import SchemaInput, generate_schema
from provisa.compiler.sql_gen import CompilationContext, build_context
from provisa.core.config_loader import load_config, parse_config_dict
from provisa.core.db import create_pool, init_schema


class AppState:
    """Shared application state populated at startup."""

    pg_pool: asyncpg.Pool | None = None
    trino_conn: trino.dbapi.Connection | None = None
    schemas: dict[str, object] = {}  # role_id → GraphQLSchema
    contexts: dict[str, CompilationContext] = {}  # role_id → CompilationContext


state = AppState()


async def _load_and_build(config_path: str | None = None) -> None:
    """Load config, introspect Trino, build schemas for all roles."""
    if config_path is None:
        config_path = os.environ.get("PROVISA_CONFIG", "config/provisa.yaml")

    path = Path(config_path)
    if not path.exists():
        return

    with open(path) as f:
        raw_config = yaml.safe_load(f)

    # Connect to PG
    pg_host = os.environ.get("PG_HOST", "localhost")
    pg_port = int(os.environ.get("PG_PORT", "5432"))
    pg_database = os.environ.get("PG_DATABASE", "provisa")
    pg_user = os.environ.get("PG_USER", "provisa")
    pg_password = os.environ.get("PG_PASSWORD", "provisa")

    state.pg_pool = await create_pool(
        pg_host, pg_port, pg_database, pg_user, pg_password,
    )

    # Init schema
    schema_sql_path = Path(__file__).parent.parent / "core" / "schema.sql"
    if schema_sql_path.exists():
        schema_sql = schema_sql_path.read_text()
        await init_schema(state.pg_pool, schema_sql)

    # Connect to Trino
    trino_host = os.environ.get("TRINO_HOST", "localhost")
    trino_port = int(os.environ.get("TRINO_PORT", "8080"))
    state.trino_conn = trino.dbapi.connect(
        host=trino_host,
        port=trino_port,
        user="provisa",
        catalog="postgresql",
        schema="public",
    )

    # Load config into PG (and create Trino catalogs)
    config = parse_config_dict(raw_config)
    async with state.pg_pool.acquire() as conn:
        await load_config(config, conn, state.trino_conn)

    # Introspect and build schemas per role
    async with state.pg_pool.acquire() as conn:
        tables = await _fetch_tables(conn)
        relationships = await _fetch_relationships(conn)
        naming_rules = [
            dict(r) for r in await conn.fetch(
                "SELECT pattern, replacement FROM naming_rules"
            )
        ]
        domains = [
            dict(r) for r in await conn.fetch("SELECT id, description FROM domains")
        ]
        sources = {
            r["id"]: dict(r) for r in await conn.fetch("SELECT * FROM sources")
        }
        roles = [
            dict(r) for r in await conn.fetch(
                "SELECT id, capabilities, domain_access FROM roles"
            )
        ]

        # Introspect Trino metadata
        column_types = introspect_tables(state.trino_conn, tables, sources)
        col_types_converted: dict[int, list[ColumnMetadata]] = column_types

        for role in roles:
            si = SchemaInput(
                tables=tables,
                relationships=relationships,
                column_types=col_types_converted,
                naming_rules=naming_rules,
                role=role,
                domains=domains,
            )
            try:
                state.schemas[role["id"]] = generate_schema(si)
                state.contexts[role["id"]] = build_context(si)
            except ValueError:
                # Role has no visible tables — skip
                pass


async def _fetch_tables(conn: asyncpg.Connection) -> list[dict]:
    """Fetch registered tables with columns."""
    rows = await conn.fetch(
        "SELECT id, source_id, domain_id, schema_name, table_name, governance "
        "FROM registered_tables ORDER BY id"
    )
    tables = []
    for row in rows:
        table = dict(row)
        col_rows = await conn.fetch(
            "SELECT column_name, visible_to FROM table_columns "
            "WHERE table_id = $1 ORDER BY id",
            row["id"],
        )
        table["columns"] = [
            {"column_name": r["column_name"], "visible_to": list(r["visible_to"])}
            for r in col_rows
        ]
        tables.append(table)
    return tables


async def _fetch_relationships(conn: asyncpg.Connection) -> list[dict]:
    """Fetch relationships."""
    rows = await conn.fetch(
        "SELECT id, source_table_id, target_table_id, source_column, "
        "target_column, cardinality FROM relationships"
    )
    return [dict(r) for r in rows]


@asynccontextmanager
async def lifespan(app: FastAPI):
    """App lifespan: load config and build schemas at startup."""
    await _load_and_build()
    yield
    if state.pg_pool:
        await state.pg_pool.close()
    if state.trino_conn:
        state.trino_conn.close()


def create_app() -> FastAPI:
    """Create the FastAPI application."""
    app = FastAPI(title="Provisa", lifespan=lifespan)
    app.include_router(data_router)

    @app.get("/health")
    async def health():
        return {"status": "ok"}

    return app
