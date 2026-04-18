# Copyright (c) 2026 Kenneth Stott
# Canary: 998f7261-e877-4341-a621-634d0c8011ff
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Config loader: YAML → validate → resolve secrets → upsert PG → create Trino catalogs."""

import logging
import re
from pathlib import Path

log = logging.getLogger(__name__)


def _normalize_op_id(s: str) -> str:
    return re.sub(r"[_-]", "", s).lower()

import asyncpg
import trino
import yaml

from provisa.core.models import ProvisaConfig
from provisa.core.secrets import resolve_secrets
from provisa.core.repositories import (
    source as source_repo,
    domain as domain_repo,
    table as table_repo,
    relationship as rel_repo,
    role as role_repo,
    rls as rls_repo,
    function as function_repo,
)
from provisa.core import catalog


def parse_config(path: str | Path) -> ProvisaConfig:
    """Parse and validate a YAML config file. Does NOT resolve secrets."""
    with open(Path(path), encoding="utf-8") as f:
        raw = yaml.safe_load(f)
    return ProvisaConfig.model_validate(raw)


def parse_config_dict(data: dict) -> ProvisaConfig:
    """Parse and validate a config dict."""
    return ProvisaConfig.model_validate(data)


async def _load_config_in_txn(
    config: ProvisaConfig,
    conn: asyncpg.Connection,
    trino_conn: trino.dbapi.Connection | None,
    replace: bool = False,
) -> None:
    """Upsert full config into PG within caller's transaction scope.

    When replace=True, all existing sources/tables/domains/roles/relationships
    not present in the new config are deleted first (full replace semantics).
    """
    if replace:
        new_source_ids = [src.id for src in config.sources]
        new_domain_ids = [d.id for d in config.domains]
        new_role_ids = [r.id for r in config.roles]
        if new_source_ids:
            await conn.execute(
                "DELETE FROM registered_tables WHERE source_id != ALL($1::text[])",
                new_source_ids,
            )
            await conn.execute(
                "DELETE FROM sources WHERE id != ALL($1::text[])",
                new_source_ids,
            )
        else:
            await conn.execute("DELETE FROM registered_tables")
            await conn.execute("DELETE FROM sources")
        if new_domain_ids:
            await conn.execute(
                "DELETE FROM domains WHERE id != ALL($1::text[])",
                new_domain_ids,
            )
        else:
            await conn.execute("DELETE FROM domains")
        if new_role_ids:
            await conn.execute(
                "DELETE FROM roles WHERE id != ALL($1::text[])",
                new_role_ids,
            )
        else:
            await conn.execute("DELETE FROM roles")
        await conn.execute("DELETE FROM relationships")
        await conn.execute("DELETE FROM tracked_functions")
        await conn.execute("DELETE FROM tracked_webhooks")

    # 1. Sources
    for src in config.sources:
        await source_repo.upsert(conn, src)
        if trino_conn is not None:
            try:
                resolved_pw = resolve_secrets(src.password)
                catalog.create_catalog(trino_conn, src, resolved_pw)
            except Exception:
                pass  # catalog.create_catalog already logs warnings

    # 2. Domains
    for dom in config.domains:
        await domain_repo.upsert(conn, dom)

    # 3. Naming rules
    await conn.execute("DELETE FROM naming_rules")
    for rule in config.naming.rules:
        await conn.execute(
            "INSERT INTO naming_rules (pattern, replacement) VALUES ($1, $2)",
            rule.pattern,
            rule.replace,
        )

    # 4. Roles (before tables/RLS so FK refs exist)
    for role in config.roles:
        await role_repo.upsert(conn, role)

    # 5. Tables + columns
    sources_by_id = {src.id: src for src in config.sources}

    # Pre-load OpenAPI specs once per source (avoid repeated HTTP fetches)
    openapi_specs: dict[str, dict] = {}
    for src in config.sources:
        if src.type.value == "openapi" and src.path:
            try:
                from provisa.openapi.loader import load_spec
                openapi_specs[src.id] = load_spec(src.path)
            except Exception as _e:
                log.warning("Failed to load OpenAPI spec for %s: %s", src.id, _e)

    for tbl in config.tables:
        await table_repo.upsert(conn, tbl)
        src = sources_by_id.get(tbl.source_id)
        if src and src.type.value == "sqlite" and src.path:
            from provisa.file_source.pg_migrate import migrate_sqlite_table
            try:
                await migrate_sqlite_table(src.path, tbl.table_name, conn, tbl.schema_name, tbl.table_name)
            except Exception as _e:
                log.warning("SQLite → PG migration failed for %s.%s: %s", tbl.source_id, tbl.table_name, _e)
        elif src and src.type.value == "openapi" and src.base_url:
            spec = openapi_specs.get(src.id, {})
            if spec:
                from provisa.openapi.mapper import parse_spec
                from provisa.openapi.pg_cache import cache_openapi_table
                queries, _ = parse_spec(spec)
                match = next((q for q in queries if _normalize_op_id(q.operation_id) == _normalize_op_id(tbl.table_name)), None)
                if match:
                    default_params = {p["name"]: "" for p in match.query_params}
                    fallback_cols = [(c.name, "TEXT") for c in tbl.columns] if tbl.columns else None
                    try:
                        await cache_openapi_table(
                            src.base_url,
                            match.path,
                            default_params,
                            conn,
                            tbl.schema_name,
                            tbl.table_name,
                            match.response_schema,
                            fallback_cols,
                        )
                    except Exception as _e:
                        log.warning("OpenAPI cache failed for %s.%s: %s", tbl.source_id, tbl.table_name, _e)
                else:
                    log.warning("No matching OpenAPI operation for table %s (source %s)", tbl.table_name, tbl.source_id)

    # 5a. ANALYZE — prime federation CBO stats after tables are registered
    if trino_conn is not None:
        for src in config.sources:
            try:
                catalog.analyze_source_tables(trino_conn, src, config.tables)
            except Exception:
                pass  # analyze_source_tables already logs per-table failures

    # 6. Relationships (tables must exist first)
    for rel in config.relationships:
        await rel_repo.upsert(conn, rel)

    # 7. RLS rules (tables + roles must exist first)
    for rule in config.rls_rules:
        await rls_repo.upsert(conn, rule)

    # 8. Tracked DB functions
    for func in config.functions:
        await function_repo.upsert_function(conn, func)

    # 9. Tracked webhooks
    for wh in config.webhooks:
        await function_repo.upsert_webhook(conn, wh)


async def load_config(
    config: ProvisaConfig,
    pg_conn: asyncpg.Connection,
    trino_conn: trino.dbapi.Connection | None = None,
    replace: bool = False,
) -> None:
    """Upsert full config into PG within a transaction. Idempotent.

    Pass replace=True to delete all metadata not in the new config first
    (full replace semantics — use for install simulation / clean reloads).
    """
    async with pg_conn.transaction():
        await _load_config_in_txn(config, pg_conn, trino_conn, replace=replace)


async def load_config_from_yaml(
    path: str | Path,
    pg_conn: asyncpg.Connection,
    trino_conn: trino.dbapi.Connection | None = None,
    replace: bool = False,
) -> ProvisaConfig:
    """Parse YAML, resolve secrets in source passwords, load into PG."""
    config = parse_config(path)
    await load_config(config, pg_conn, trino_conn, replace=replace)
    return config
