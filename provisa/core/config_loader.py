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

log = logging.getLogger(__name__)


async def _fill_null_column_types(
    conn: asyncpg.Connection, source_id: str, schema: str, table: str, types: dict[str, str]
) -> None:
    """Set table_columns.data_type for columns the YAML left null, from `types`
    (column_name -> data_type). Never overrides an explicit YAML-declared type."""
    if not types:
        return
    table_id = await conn.fetchval(
        "SELECT id FROM registered_tables WHERE source_id=$1 AND schema_name=$2 AND table_name=$3",
        source_id,
        schema,
        table,
    )
    if table_id is None:
        return
    for _col, _dt in types.items():
        await conn.execute(
            "UPDATE table_columns SET data_type=$1 "
            "WHERE table_id=$2 AND column_name=$3 AND data_type IS NULL",
            _dt,
            table_id,
            _col,
        )


def _normalize_op_id(s: str) -> str:
    return re.sub(r"[_-]", "", s).lower()


def _default_params_from_spec(spec: dict, path: str) -> dict:
    """Extract enum/default values for GET query params at path for pre-population."""
    path_item = spec.get("paths", {}).get(path, {})
    raw_params = list(path_item.get("parameters", []))
    op = path_item.get("get", {})
    if op:
        raw_params = raw_params + list(op.get("parameters", []))
    defaults: dict = {}
    for p in raw_params:
        if "$ref" in p:
            ref_parts = p["$ref"].lstrip("#/").split("/")
            node = spec
            for part in ref_parts:
                node = node.get(part, {})
            p = node
        if p.get("in") != "query":
            continue
        name = p.get("name", "")
        if not name:
            continue
        schema = p.get("schema") or {}
        if "enum" in schema:
            defaults[name] = schema["enum"]
        elif "default" in schema:
            defaults[name] = schema["default"]
    return defaults


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
    # Serialize concurrent config loads to prevent deadlocks when multiple
    # processes (e.g. parallel test app lifespans) upsert the same rows.
    await conn.execute("SELECT pg_advisory_xact_lock(7261748190)")
    # __provisa__ is the synthetic source for user-created views (and other
    # UI-registered provisa-managed tables) — preserve them across config reloads
    # in replace mode; they are not declared in the YAML config.
    _SYSTEM_SOURCE_IDS = ["provisa-admin", "provisa-otel", "__provisa__"]
    _SYSTEM_DOMAIN_IDS = ["", "meta", "ops"]
    if replace:
        new_source_ids = list({src.id for src in config.sources} | set(_SYSTEM_SOURCE_IDS))
        new_domain_ids = list({d.id for d in config.domains} | set(_SYSTEM_DOMAIN_IDS))
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
            await conn.execute(
                "DELETE FROM registered_tables WHERE source_id != ALL($1::text[])",
                _SYSTEM_SOURCE_IDS,
            )
            await conn.execute(
                "DELETE FROM sources WHERE id != ALL($1::text[])",
                _SYSTEM_SOURCE_IDS,
            )
        if new_domain_ids:
            await conn.execute(
                "DELETE FROM domains WHERE id != ALL($1::text[])",
                new_domain_ids,
            )
        else:
            await conn.execute(
                "DELETE FROM domains WHERE id != ALL($1::text[])",
                _SYSTEM_DOMAIN_IDS,
            )
        if new_role_ids:
            await conn.execute(
                "DELETE FROM roles WHERE id != ALL($1::text[])",
                new_role_ids,
            )
        else:
            await conn.execute("DELETE FROM roles")
        await conn.execute("DELETE FROM relationships WHERE id NOT LIKE 'meta:%'")
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

                openapi_specs[src.id] = load_spec(resolve_secrets(src.path))
            except Exception as _e:
                log.warning("Failed to load OpenAPI spec for %s: %s", src.id, _e)

    for tbl in config.tables:
        # Enrich OpenAPI table columns with descriptions from spec before upserting
        src = sources_by_id.get(tbl.source_id)
        if src and src.type.value == "openapi" and src.base_url:
            spec = openapi_specs.get(src.id, {})
            if spec:
                from provisa.openapi.mapper import parse_spec
                from provisa.openapi.register import _schema_to_columns

                queries, _ = parse_spec(spec)
                match = next(
                    (
                        q
                        for q in queries
                        if _normalize_op_id(q.operation_id) == _normalize_op_id(tbl.table_name)
                    ),
                    None,
                )
                if match:
                    spec_cols = _schema_to_columns(match.response_schema)
                    spec_col_map = {c["name"]: c for c in spec_cols}
                    # Update table columns with descriptions from spec
                    for col in tbl.columns:
                        if col.name in spec_col_map and not col.description:
                            col.description = spec_col_map[col.name].get("description")

        await table_repo.upsert(conn, tbl)
        src = sources_by_id.get(tbl.source_id)
        if src and src.type.value == "sqlite" and src.path:
            from provisa.file_source.pg_migrate import migrate_sqlite_table

            try:
                await migrate_sqlite_table(
                    src.path, tbl.table_name, conn, tbl.schema_name, tbl.table_name
                )
            except Exception as _e:
                log.warning(
                    "SQLite → PG migration failed for %s.%s: %s", tbl.source_id, tbl.table_name, _e
                )
        elif src and src.type.value == "openapi" and src.base_url:
            resolved_base_url = resolve_secrets(src.base_url)
            spec = openapi_specs.get(src.id, {})
            if spec:
                import json as _json
                from provisa.openapi.mapper import parse_spec
                from provisa.openapi.pg_cache import cache_openapi_table
                from provisa.openapi.register import _openapi_to_provisa_type, _schema_to_columns

                queries, _ = parse_spec(spec)
                match = next(
                    (
                        q
                        for q in queries
                        if _normalize_op_id(q.operation_id) == _normalize_op_id(tbl.table_name)
                    ),
                    None,
                )
                if match:
                    default_params = _default_params_from_spec(spec, match.path)
                    fallback_cols = [(c.name, "TEXT") for c in tbl.columns] if tbl.columns else None
                    try:
                        await cache_openapi_table(
                            resolved_base_url,
                            match.path,
                            default_params,
                            conn,
                            tbl.schema_name,
                            tbl.table_name,
                            match.response_schema,
                            fallback_cols,
                        )
                    except Exception as _e:
                        log.warning(
                            "OpenAPI cache failed for %s.%s: %s", tbl.source_id, tbl.table_name, _e
                        )
                    # Register in api_sources + api_endpoints for runtime hydration
                    try:
                        await conn.execute(
                            """
                            INSERT INTO api_sources (id, type, base_url, auth)
                            VALUES ($1, 'openapi', $2, $3)
                            ON CONFLICT (id) DO UPDATE SET base_url = EXCLUDED.base_url
                            """,
                            src.id,
                            resolved_base_url,
                            None,
                        )
                        resp_col_names = {
                            c["name"] for c in _schema_to_columns(match.response_schema)
                        }
                        api_columns = [
                            {
                                "name": c["name"],
                                "type": c["type"],
                                "filterable": True,
                                **(
                                    {"object_fields": c["object_fields"]}
                                    if c.get("object_fields")
                                    else {}
                                ),
                            }
                            for c in _schema_to_columns(match.response_schema)
                        ]
                        for p in match.path_params:
                            api_columns.append(
                                {
                                    "name": p["name"],
                                    "type": _openapi_to_provisa_type(p.get("type")),
                                    "filterable": False,
                                    "param_type": "path",
                                    "param_name": p["name"],
                                }
                            )
                        for p in match.query_params:
                            if p["name"] not in resp_col_names:
                                api_columns.append(
                                    {
                                        "name": p["name"],
                                        "type": _openapi_to_provisa_type(p.get("type")),
                                        "filterable": False,
                                        "param_type": "query",
                                        "param_name": p["name"],
                                    }
                                )
                        await conn.execute(
                            """
                            INSERT INTO api_endpoints
                                (source_id, path, method, table_name, columns, ttl, default_params)
                            VALUES ($1, $2, 'GET', $3, $4::jsonb, $5, $6::jsonb)
                            ON CONFLICT (table_name) DO UPDATE SET
                                source_id     = EXCLUDED.source_id,
                                path          = EXCLUDED.path,
                                columns       = EXCLUDED.columns,
                                ttl           = EXCLUDED.ttl,
                                default_params = EXCLUDED.default_params
                            """,
                            src.id,
                            match.path,
                            tbl.table_name,
                            _json.dumps(api_columns),
                            src.cache_ttl or 300,
                            _json.dumps(default_params) if default_params else None,
                        )
                        # Backfill object_fields into table_columns — YAML columns don't carry them
                        for col_data in api_columns:
                            if col_data.get("object_fields"):
                                await conn.execute(
                                    """UPDATE table_columns tc
                                       SET object_fields = $1::jsonb
                                       FROM registered_tables rt
                                       WHERE tc.table_id = rt.id
                                         AND rt.source_id = $2
                                         AND rt.table_name = $3
                                         AND tc.column_name = $4""",
                                    _json.dumps(col_data["object_fields"]),
                                    tbl.source_id,
                                    tbl.table_name,
                                    col_data["name"],
                                )
                        # Persist column data_type from the spec. OpenAPI tables are
                        # API-backed: their cached response may be empty (so Trino can't
                        # introspect every column), and native-filter params (_nf_*) never
                        # appear in the response at all. The spec is authoritative for
                        # both. introspect_tables trusts stored types — fill any null.
                        _OAPI_TRINO = {
                            "string": "varchar",
                            "integer": "integer",
                            "number": "double",
                            "boolean": "boolean",
                            "array": "json",
                            "object": "json",
                            "jsonb": "json",
                        }
                        _oapi_types: dict[str, str] = {}
                        for _sc in _schema_to_columns(match.response_schema):
                            _oapi_types[_sc["name"]] = _OAPI_TRINO.get(_sc.get("type") or "string", "varchar")
                        for _p in match.path_params + match.query_params:
                            _pt = _OAPI_TRINO.get(_p.get("type") or "string", "varchar")
                            _oapi_types[_p["name"]] = _pt
                            _oapi_types["_nf_" + _p["name"]] = _pt
                        await _fill_null_column_types(
                            conn, tbl.source_id, tbl.schema_name, tbl.table_name, _oapi_types
                        )
                    except Exception as _e:
                        log.warning(
                            "api_endpoints registration failed for %s.%s: %s",
                            src.id,
                            tbl.table_name,
                            _e,
                        )
                else:
                    log.warning(
                        "No matching OpenAPI operation for table %s (source %s)",
                        tbl.table_name,
                        tbl.source_id,
                    )

        # Resolve column types now that the table is materialized — postgres is live in
        # Trino, sqlite has been migrated into PG, and openapi responses are cached; all
        # are exposed through Trino's normalized information_schema.columns. introspect_
        # tables trusts stored types, so fill any the YAML left null (never overrides).
        if trino_conn is not None and tbl.columns:
            from provisa.compiler.introspect import introspect_column_types
            from provisa.compiler.naming import source_to_catalog

            await _fill_null_column_types(
                conn,
                tbl.source_id,
                tbl.schema_name,
                tbl.table_name,
                introspect_column_types(
                    trino_conn, source_to_catalog(tbl.source_id), tbl.schema_name, tbl.table_name
                ),
            )

    # 5a. Purge registered_tables rows whose table_name is no longer in config (handles renames)
    tables_by_source: dict[str, list[str]] = {}
    for tbl in config.tables:
        tables_by_source.setdefault(tbl.source_id, []).append(tbl.table_name)
    for src_id, current_names in tables_by_source.items():
        await conn.execute(
            "DELETE FROM registered_tables WHERE source_id = $1 AND table_name != ALL($2::text[])",
            src_id,
            current_names,
        )

    # 5b. ANALYZE — prime federation CBO stats after tables are registered
    if trino_conn is not None:
        for src in config.sources:
            try:
                catalog.analyze_source_tables(trino_conn, src, config.tables)
            except Exception:
                pass  # analyze_source_tables already logs per-table failures

    # 6. Relationships (tables must exist first)
    # Preserve relationships whose source or target table belongs to a dynamically-registered
    # source (e.g. graphql_remote) — those are managed outside of this config file.
    # Also preserve 'meta:*' relationships seeded by _seed_meta_domain.
    current_rel_ids = [rel.id for rel in config.relationships]
    if current_rel_ids:
        await conn.execute(
            """
            DELETE FROM relationships r
            WHERE r.id != ALL($1::text[])
            AND r.id NOT LIKE 'meta:%'
            AND NOT EXISTS (
                SELECT 1 FROM registered_tables rt
                JOIN sources s ON rt.source_id = s.id
                WHERE (rt.id = r.source_table_id OR rt.id = r.target_table_id)
                AND s.type = 'graphql_remote'
            )
            """,
            current_rel_ids,
        )
    else:
        await conn.execute(
            """
            DELETE FROM relationships r
            WHERE r.id NOT LIKE 'meta:%'
            AND NOT EXISTS (
                SELECT 1 FROM registered_tables rt
                JOIN sources s ON rt.source_id = s.id
                WHERE (rt.id = r.source_table_id OR rt.id = r.target_table_id)
                AND s.type = 'graphql_remote'
            )
            """,
        )
    for rel in config.relationships:
        try:
            await rel_repo.upsert(conn, rel)
        except ValueError:
            pass  # referenced table not yet registered (dynamic source); retried after source registration

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
