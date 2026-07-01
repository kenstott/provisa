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

# Requirements: REQ-012, REQ-013, REQ-016, REQ-250, REQ-251, REQ-275, REQ-282, REQ-283, REQ-285

import logging
import re
from pathlib import Path

import asyncpg
import trino
import yaml

from provisa.core.models import Domain, ProvisaConfig, Source, Table
from provisa.core import domain_policy
from provisa.core.secrets import resolve_secrets
from provisa.openapi.mapper import OpenAPIQuery
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


# PG information_schema.data_type → Trino type name. Mirrors what Trino's
# information_schema.columns reports for a PG-backed catalog, so types read via
# the (same-connection, uncommitted-visible) asyncpg path match the Trino path.
_PG_TO_TRINO_TYPE = {
    "text": "varchar",
    "character varying": "varchar",
    "bigint": "bigint",
    "integer": "integer",
    "smallint": "smallint",
    "double precision": "double",
    "real": "real",
    "boolean": "boolean",
    "json": "json",
    "jsonb": "json",
    "date": "date",
    "timestamp without time zone": "timestamp(6)",
    "timestamp with time zone": "timestamp(6) with time zone",
    "bytea": "varbinary",
    "numeric": "decimal",
}


async def _pg_column_trino_types(
    conn: asyncpg.Connection, pg_schema: str, pg_table: str
) -> dict[str, str]:
    """Return {column_name: trino_type} for a PG table read over the same asyncpg
    connection — sees uncommitted writes (e.g. an OpenAPI cache table just created
    in this transaction), which a separate Trino JDBC connection cannot."""
    rows = await conn.fetch(
        "SELECT column_name, data_type FROM information_schema.columns "
        "WHERE table_schema=$1 AND table_name=$2",
        pg_schema,
        pg_table,
    )
    return {
        r["column_name"]: _PG_TO_TRINO_TYPE[r["data_type"]]
        for r in rows
        if r["data_type"] in _PG_TO_TRINO_TYPE
    }


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


def parse_config(path: str | Path) -> ProvisaConfig:  # REQ-250
    """Parse and validate a YAML config file. Does NOT resolve secrets."""
    with open(Path(path), encoding="utf-8") as f:
        raw = yaml.safe_load(f)
    return ProvisaConfig.model_validate(raw)


def parse_config_dict(data: dict) -> ProvisaConfig:  # REQ-250
    """Parse and validate a config dict."""
    return ProvisaConfig.model_validate(data)


_SYSTEM_SOURCE_IDS = ["provisa-admin", "provisa-otel", "__provisa__"]

_OAPI_TRINO = {
    "string": "varchar",
    "integer": "integer",
    "number": "double",
    "boolean": "boolean",
    "array": "json",
    "object": "json",
    "jsonb": "json",
}


async def _replace_mode_cleanup(
    conn: asyncpg.Connection, config: ProvisaConfig
) -> None:  # REQ-013, REQ-014
    """Delete all rows not present in the new config (full replace semantics)."""
    new_source_ids = list({src.id for src in config.sources} | set(_SYSTEM_SOURCE_IDS))
    new_domain_ids = list({d.id for d in config.domains} | set(domain_policy.system_domain_ids()))
    new_role_ids = [r.id for r in config.roles]
    keep_sources = new_source_ids if new_source_ids else _SYSTEM_SOURCE_IDS
    await conn.execute(
        "DELETE FROM registered_tables WHERE source_id != ALL($1::text[])",
        keep_sources,
    )
    await conn.execute(
        "DELETE FROM sources WHERE id != ALL($1::text[])",
        keep_sources,
    )
    keep_domains = new_domain_ids if new_domain_ids else domain_policy.system_domain_ids()
    await conn.execute(
        "DELETE FROM domains WHERE id != ALL($1::text[])",
        keep_domains,
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


async def _upsert_sources(  # REQ-012, REQ-250
    conn: asyncpg.Connection,
    trino_conn: trino.dbapi.Connection | None,
    config: ProvisaConfig,
) -> None:
    for src in config.sources:
        await source_repo.upsert(conn, src)
        if trino_conn is not None:
            try:
                resolved_pw = resolve_secrets(src.password)
                catalog.create_catalog(trino_conn, src, resolved_pw)
            except Exception:
                pass  # catalog.create_catalog already logs warnings


async def _upsert_naming_rules(conn: asyncpg.Connection, config: ProvisaConfig) -> None:
    await conn.execute("DELETE FROM naming_rules")
    for rule in config.naming.rules:
        await conn.execute(
            "INSERT INTO naming_rules (pattern, replacement) VALUES ($1, $2)",
            rule.pattern,
            rule.replace,
        )


def _load_openapi_specs(config: ProvisaConfig) -> dict[str, dict]:
    """Pre-load OpenAPI specs once per source (avoid repeated HTTP fetches)."""
    openapi_specs: dict[str, dict] = {}
    for src in config.sources:
        if src.type.value == "openapi" and src.path:
            try:
                from provisa.openapi.loader import load_spec

                openapi_specs[src.id] = load_spec(resolve_secrets(src.path))
            except Exception as _e:
                log.warning("Failed to load OpenAPI spec for %s: %s", src.id, _e)
    return openapi_specs


def _enrich_openapi_table_columns(
    tbl: Table,
    spec: dict,
) -> None:
    """Update table columns with descriptions from the OpenAPI spec (in-place)."""
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
    if not match:
        return
    spec_col_map = {c["name"]: c for c in _schema_to_columns(match.response_schema)}
    for col in tbl.columns:
        if col.name in spec_col_map and not col.description:
            col.description = spec_col_map[col.name].get("description")


async def _handle_sqlite_table(
    conn: asyncpg.Connection,
    tbl: Table,
    src: Source,
) -> None:
    from provisa.file_source.pg_migrate import migrate_sqlite_table, sqlite_column_trino_types

    assert src.path is not None
    try:
        await migrate_sqlite_table(src.path, tbl.table_name, conn, tbl.schema_name, tbl.table_name)
        await _fill_null_column_types(
            conn,
            tbl.source_id,
            tbl.schema_name,
            tbl.table_name,
            sqlite_column_trino_types(src.path, tbl.table_name),
        )
    except Exception as _e:
        log.warning("SQLite → PG migration failed for %s.%s: %s", tbl.source_id, tbl.table_name, _e)


def _build_api_columns(match: OpenAPIQuery) -> tuple[list[dict], set[str]]:
    """Build the api_columns list and resp_col_names set for an OpenAPI match."""
    from provisa.openapi.register import _openapi_to_provisa_type, _schema_to_columns

    resp_col_names: set[str] = {c["name"] for c in _schema_to_columns(match.response_schema)}
    api_columns: list[dict] = [
        {
            "name": c["name"],
            "type": c["type"],
            "filterable": True,
            **({"object_fields": c["object_fields"]} if c.get("object_fields") else {}),
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
        if p["name"] in resp_col_names:
            for col in api_columns:
                if col["name"] == p["name"]:
                    col["param_type"] = "query"
                    col["param_name"] = p["name"]
                    break
        else:
            api_columns.append(
                {
                    "name": p["name"],
                    "type": _openapi_to_provisa_type(p.get("type")),
                    "filterable": False,
                    "param_type": "query",
                    "param_name": p["name"],
                }
            )
    return api_columns, resp_col_names


async def _register_api_endpoint(
    conn: asyncpg.Connection,
    tbl: Table,
    src: Source,
    match: OpenAPIQuery,
    resolved_base_url: str,
    default_params: dict,
    api_columns: list[dict],
) -> None:
    import json as _json
    from provisa.openapi.register import _schema_to_columns

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
    await conn.execute(
        """
        INSERT INTO api_endpoints
            (source_id, path, method, table_name, columns, ttl, default_params, promotions)
        VALUES ($1, $2, 'GET', $3, $4::jsonb, $5, $6::jsonb, $7::jsonb)
        ON CONFLICT (table_name) DO UPDATE SET
            source_id     = EXCLUDED.source_id,
            path          = EXCLUDED.path,
            columns       = EXCLUDED.columns,
            ttl           = EXCLUDED.ttl,
            default_params = EXCLUDED.default_params,
            promotions    = EXCLUDED.promotions
        """,
        src.id,
        match.path,
        tbl.table_name,
        _json.dumps(api_columns),
        src.cache_ttl or 300,
        _json.dumps(default_params) if default_params else None,
        _json.dumps(getattr(tbl, "promotions", []) or []),
    )
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

    # The cache table (built above, in this transaction) is
    # authoritative for response columns — including map-typed
    # responses (e.g. status→count) the spec schema has no
    # properties for. Read it over this same connection, which
    # sees the uncommitted table; the Trino pass below cannot.
    await _fill_null_column_types(
        conn,
        tbl.source_id,
        tbl.schema_name,
        tbl.table_name,
        await _pg_column_trino_types(conn, tbl.schema_name, tbl.table_name),
    )
    # Spec is authoritative for native-filter params (_nf_*),
    # which never appear in the cached response at all.
    _oapi_types: dict[str, str] = {}
    for _sc in _schema_to_columns(match.response_schema):
        _oapi_types[_sc["name"]] = _OAPI_TRINO.get(_sc.get("type") or "string", "varchar")
    for _p in match.path_params + match.query_params:
        _pt = _OAPI_TRINO.get(_p.get("type") or "string", "varchar")
        _oapi_types[_p["name"]] = _pt
        _oapi_types["_nf_" + _p["name"]] = _pt
    await _fill_null_column_types(conn, tbl.source_id, tbl.schema_name, tbl.table_name, _oapi_types)


async def _handle_openapi_table(
    conn: asyncpg.Connection,
    tbl: Table,
    src: Source,
    spec: dict,
) -> None:
    from provisa.openapi.mapper import parse_spec
    from provisa.openapi.pg_cache import cache_openapi_table

    assert src.base_url is not None
    resolved_base_url = resolve_secrets(src.base_url)
    queries, _ = parse_spec(spec)
    match = next(
        (
            q
            for q in queries
            if _normalize_op_id(q.operation_id) == _normalize_op_id(tbl.table_name)
        ),
        None,
    )
    if not match:
        log.warning(
            "No matching OpenAPI operation for table %s (source %s)",
            tbl.table_name,
            tbl.source_id,
        )
        return
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
        log.warning("OpenAPI cache failed for %s.%s: %s", tbl.source_id, tbl.table_name, _e)
    # Register in api_sources + api_endpoints for runtime hydration
    try:
        api_columns, _ = _build_api_columns(match)
        await _register_api_endpoint(
            conn, tbl, src, match, resolved_base_url, default_params, api_columns
        )
    except Exception as _e:
        log.warning(
            "api_endpoints registration failed for %s.%s: %s",
            src.id,
            tbl.table_name,
            _e,
        )


async def _upsert_single_table(
    conn: asyncpg.Connection,
    trino_conn: trino.dbapi.Connection | None,
    tbl: Table,
    src: Source | None,
    openapi_specs: dict[str, dict],
) -> None:
    """Upsert one table and run source-type-specific post-upsert steps."""
    if src and src.type.value == "openapi" and src.base_url:
        spec = openapi_specs.get(src.id, {})
        if spec:
            _enrich_openapi_table_columns(tbl, spec)

    await table_repo.upsert(conn, tbl)

    if src and src.type.value == "sqlite" and src.path:
        await _handle_sqlite_table(conn, tbl, src)
    elif src and src.type.value == "openapi" and src.base_url:
        spec = openapi_specs.get(src.id, {})
        if spec:
            await _handle_openapi_table(conn, tbl, src, spec)

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


async def _purge_removed_tables(conn: asyncpg.Connection, config: ProvisaConfig) -> None:
    """Delete registered_tables rows whose table_name is no longer in config (handles renames)."""
    tables_by_source: dict[str, list[str]] = {}
    for tbl in config.tables:
        tables_by_source.setdefault(tbl.source_id, []).append(tbl.table_name)
    for src_id, current_names in tables_by_source.items():
        await conn.execute(
            "DELETE FROM registered_tables WHERE source_id = $1 AND table_name != ALL($2::text[])",
            src_id,
            current_names,
        )


async def _analyze_sources(
    trino_conn: trino.dbapi.Connection, config: ProvisaConfig
) -> None:  # REQ-275
    """Prime federation CBO stats after tables are registered."""
    for src in config.sources:
        try:
            catalog.analyze_source_tables(trino_conn, src, config.tables)
        except Exception:
            pass  # analyze_source_tables already logs per-table failures


async def _upsert_tables(  # REQ-013, REQ-016, REQ-251
    conn: asyncpg.Connection,
    trino_conn: trino.dbapi.Connection | None,
    config: ProvisaConfig,
    openapi_specs: dict[str, dict],
) -> None:
    sources_by_id = {src.id: src for src in config.sources}

    for tbl in config.tables:
        src = sources_by_id.get(tbl.source_id)
        await _upsert_single_table(conn, trino_conn, tbl, src, openapi_specs)

    await _purge_removed_tables(conn, config)

    if trino_conn is not None:
        await _analyze_sources(trino_conn, config)


async def _upsert_relationships(
    conn: asyncpg.Connection, config: ProvisaConfig
) -> None:  # REQ-018, REQ-019, REQ-020
    """Delete stale relationships and upsert config-declared ones."""
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


async def _load_config_in_txn(  # REQ-012, REQ-013, REQ-016, REQ-041, REQ-250
    config: ProvisaConfig,
    conn: asyncpg.Connection,
    trino_conn: trino.dbapi.Connection | None,
    replace: bool = False,
) -> None:
    """Upsert full config into PG within caller's transaction scope.

    When replace=True, all existing sources/tables/domains/roles/relationships
    not present in the new config are deleted first (full replace semantics).
    """
    # Resolve domain policy before any registration so repos/compilers read one source of truth.
    domain_policy.configure(config.naming.use_domains, config.naming.default_domain)

    # Serialize concurrent config loads to prevent deadlocks when multiple
    # processes (e.g. parallel test app lifespans) upsert the same rows.
    await conn.execute("SELECT pg_advisory_xact_lock(7261748190)")

    if replace:
        await _replace_mode_cleanup(conn, config)

    # 1. Sources
    await _upsert_sources(conn, trino_conn, config)

    # 2. Domains
    if domain_policy.single_domain():
        # Seed the implicit single-domain bucket so registered_tables FK resolves.
        await domain_repo.upsert(conn, Domain(id=config.naming.default_domain))
    for dom in config.domains:
        await domain_repo.upsert(conn, dom)

    # 3. Naming rules
    await _upsert_naming_rules(conn, config)

    # 4. Roles (before tables/RLS so FK refs exist)
    for role in config.roles:
        await role_repo.upsert(conn, role)

    # 5. Tables + columns
    openapi_specs = _load_openapi_specs(config)
    _validate_table_kafka_sinks(config)
    _validate_table_live_delivery(config)
    await _upsert_tables(conn, trino_conn, config, openapi_specs)

    # 6. Relationships (tables must exist first)
    # Preserve relationships whose source or target table belongs to a dynamically-registered
    # source (e.g. graphql_remote) — those are managed outside of this config file.
    # Also preserve 'meta:*' relationships seeded by _seed_meta_domain.
    await _upsert_relationships(conn, config)

    # 7. RLS rules (tables + roles must exist first)
    for rule in config.rls_rules:
        await rls_repo.upsert(conn, rule)

    # 8. Tracked DB functions
    for func in config.functions:
        await function_repo.upsert_function(conn, func)

    # 9. Tracked webhooks
    for wh in config.webhooks:
        await function_repo.upsert_webhook(conn, wh)

    # 10. Policy sweep: dynamically-registered rows (openapi/hasura/graphql_remote) are not
    # in this config file, so the model validator can't catch them. In single-domain mode any
    # surviving row with a foreign domain_id is a hard error — re-register the offending source.
    if domain_policy.single_domain():
        await _validate_existing_domains(conn, config.naming.default_domain)


def _validate_table_kafka_sinks(config) -> None:
    """Validate kafka_sink fields on all tables (REQ-176–180)."""
    valid_triggers = {"change_event", "schedule", "manual", "poll"}
    for table in config.tables:
        if table.kafka_sink is None:
            continue
        if not table.kafka_sink.topic:
            raise ValueError(f"Table {table.table_name!r}: kafka_sink.topic is required")
        if not table.kafka_sink.triggers:
            raise ValueError(f"Table {table.table_name!r}: kafka_sink.triggers must not be empty")
        for t in table.kafka_sink.triggers:
            if t not in valid_triggers:
                raise ValueError(f"Table {table.table_name!r}: unknown kafka_sink trigger {t!r}")


# CDC-capable source types (delivery=cdc) — those with a real push provider in
# the subscription registry: PostgreSQL (LISTEN/NOTIFY triggers), Debezium and
# generic Kafka (Kafka consumers), and MongoDB (change streams). Every other
# source uses delivery=poll (watermark polling routed through Trino).
_CDC_SUPPORTED_SOURCE_TYPES = {"postgresql", "debezium", "kafka", "mongodb"}


def _validate_table_live_delivery(config) -> None:
    """Validate live delivery config on all tables (REQ-282–287)."""
    for table in config.tables:
        if table.live is None:
            continue
        if table.live.delivery == "poll" and not table.live.watermark_column:
            raise ValueError(
                f"Table {table.table_name!r}: live.delivery=poll requires watermark_column"
            )
        if table.live.delivery == "cdc":
            source = next((s for s in config.sources if s.id == table.source_id), None)
            if source and getattr(source, "type", None) not in _CDC_SUPPORTED_SOURCE_TYPES:
                raise ValueError(
                    f"Table {table.table_name!r}: live.delivery=cdc not supported for source type "
                    f"{getattr(source, 'type', 'unknown')!r}"
                )


async def _validate_existing_domains(conn: asyncpg.Connection, default_domain: str) -> None:
    rows = await conn.fetch(
        """
        SELECT source_id, schema_name, table_name, domain_id
        FROM registered_tables
        WHERE domain_id <> '' AND domain_id <> ALL($1::text[])
        """,
        [default_domain, "meta", "ops"],
    )
    if rows:
        offenders = ", ".join(
            f"{r['source_id']}.{r['schema_name']}.{r['table_name']}={r['domain_id']!r}"
            for r in rows
        )
        raise RuntimeError(
            f"naming.use_domains=false permits only domain {default_domain!r}; "
            f"re-register these sources: {offenders}"
        )


async def load_config(  # REQ-012, REQ-016, REQ-250
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


async def load_config_from_yaml(  # REQ-012, REQ-016, REQ-250
    path: str | Path,
    pg_conn: asyncpg.Connection,
    trino_conn: trino.dbapi.Connection | None = None,
    replace: bool = False,
) -> ProvisaConfig:
    """Parse YAML, resolve secrets in source passwords, load into PG."""
    config = parse_config(path)
    await load_config(config, pg_conn, trino_conn, replace=replace)
    return config
