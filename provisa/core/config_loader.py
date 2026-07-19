# Copyright (c) 2026 Kenneth Stott
# Canary: 998f7261-e877-4341-a621-634d0c8011ff
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Config loader: YAML → validate → resolve secrets → upsert PG → create the engine catalogs."""

# Requirements: REQ-012, REQ-013, REQ-016, REQ-250, REQ-251, REQ-275, REQ-282, REQ-283, REQ-285
# complexity-gate: allow-ble=6 reason="per-source config registration is best-effort: source-driver register, OpenAPI spec load, SQLite migration post-step, OpenAPI cache, api_endpoints register, and CBO analyze each log their own failure and continue, so one bad source never fails the whole config load"

import logging
import re
from pathlib import Path
from typing import TYPE_CHECKING, Any

import yaml
from sqlalchemy import column as _sa_column
from sqlalchemy import delete as _delete
from sqlalchemy import insert, or_, select, update
from sqlalchemy import table as _sa_table

from provisa.core.models import ControlPlaneConfig, Domain, ProvisaConfig, Source, Table
from provisa.core import domain_policy
from provisa.core.schema_org import (
    api_endpoints,
    api_sources,
    domains,
    naming_rules,
    registered_tables,
    relationships,
    roles,
    sources,
    table_columns,
    tracked_functions,
    tracked_webhooks,
)
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

if TYPE_CHECKING:
    from provisa.core.database import Connection

log = logging.getLogger(__name__)

# Lightweight reference to PG's information_schema.columns — a DB system view (not a
# provisa metadata table), read over the same connection to see uncommitted writes.
_information_schema_columns = _sa_table(
    "columns",
    _sa_column("column_name"),
    _sa_column("data_type"),
    _sa_column("table_schema"),
    _sa_column("table_name"),
    schema="information_schema",
)


async def _fill_null_column_types(
    conn: "Connection", source_id: str, schema: str, table: str, types: dict[str, str]
) -> None:
    """Set table_columns.data_type for columns the YAML left null, from `types`
    (column_name -> data_type). Never overrides an explicit YAML-declared type."""
    if not types:
        return
    result = await conn.execute_core(
        select(registered_tables.c.id).where(
            registered_tables.c.source_id == source_id,
            registered_tables.c.schema_name == schema,
            registered_tables.c.table_name == table,
        )
    )
    row = result.fetchone()
    table_id = row[0] if row is not None else None
    if table_id is None:
        return
    for _col, _dt in types.items():
        await conn.execute_core(
            update(table_columns)
            .where(
                table_columns.c.table_id == table_id,
                table_columns.c.column_name == _col,
                table_columns.c.data_type.is_(None),
            )
            .values(data_type=_dt)
        )


# PG information_schema.data_type → the engine type name. Mirrors what the engine's
# information_schema.columns reports for a PG-backed catalog, so types read via
# the (same-connection, uncommitted-visible) asyncpg path match the engine path.
_PG_TO_PHYSICAL_TYPE = {
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


async def _pg_column_types(conn: "Connection", pg_schema: str, pg_table: str) -> dict[str, str]:
    """Return {column_name: column_type} for a PG table read over the same asyncpg
    connection — sees uncommitted writes (e.g. an OpenAPI cache table just created
    in this transaction), which a separate the engine JDBC connection cannot."""
    result = await conn.execute_core(
        select(
            _information_schema_columns.c.column_name,
            _information_schema_columns.c.data_type,
        ).where(
            _information_schema_columns.c.table_schema == pg_schema,
            _information_schema_columns.c.table_name == pg_table,
        )
    )
    rows = [dict(r._mapping) for r in result.fetchall()]
    return {
        r["column_name"]: _PG_TO_PHYSICAL_TYPE[r["data_type"]]
        for r in rows
        if r["data_type"] in _PG_TO_PHYSICAL_TYPE
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


def load_control_plane(config_path: str | Path | None) -> ControlPlaneConfig:  # REQ-837
    """Read just the ``control_plane`` config section (or defaults).

    The control-plane database connections must be available before the full
    config is loaded (the admin UI needs the DB on first start, possibly before a
    config file exists), so this is parsed independently of ``parse_config``. It
    is the config layer — env/secret resolution happens here, not in callers."""
    if config_path and Path(config_path).exists():
        with open(Path(config_path), encoding="utf-8") as f:
            raw = yaml.safe_load(f) or {}
        return ControlPlaneConfig.model_validate(raw.get("control_plane", {}))
    return ControlPlaneConfig()


def parse_config_dict(data: dict) -> ProvisaConfig:  # REQ-250
    """Parse and validate a config dict.

    Resolve secret references (``${provider:ref}``) on the raw dict BEFORE pydantic
    validation so they work for every field, not just the string fields re-resolved
    at use time. Without this, a ``${env:PG_PORT}`` in an ``int`` field (a source
    ``port``) reaches pydantic as the literal template and fails int-parsing.
    Resolution is idempotent, so the later per-field ``resolve_secrets(...)`` calls
    stay no-ops.
    """
    from provisa.core.secrets import resolve_secrets_in_dict

    return ProvisaConfig.model_validate(resolve_secrets_in_dict(data))


_SYSTEM_SOURCE_IDS = ["provisa-admin", "provisa-otel", "__provisa__"]

_OAPI_PHYSICAL = {
    "string": "varchar",
    "integer": "integer",
    "number": "double",
    "boolean": "boolean",
    "array": "json",
    "object": "json",
    "jsonb": "json",
}


async def _replace_mode_cleanup(
    conn: "Connection", config: ProvisaConfig
) -> None:  # REQ-013, REQ-014
    """Delete all rows not present in the new config (full replace semantics)."""
    new_source_ids = list({src.id for src in config.sources} | set(_SYSTEM_SOURCE_IDS))
    new_domain_ids = list({d.id for d in config.domains} | set(domain_policy.system_domain_ids()))
    new_role_ids = [r.id for r in config.roles]
    keep_sources = new_source_ids if new_source_ids else _SYSTEM_SOURCE_IDS
    await conn.execute_core(
        _delete(registered_tables).where(registered_tables.c.source_id.not_in(keep_sources))
    )
    await conn.execute_core(_delete(sources).where(sources.c.id.not_in(keep_sources)))
    keep_domains = new_domain_ids if new_domain_ids else domain_policy.system_domain_ids()
    await conn.execute_core(_delete(domains).where(domains.c.id.not_in(keep_domains)))
    if new_role_ids:
        await conn.execute_core(_delete(roles).where(roles.c.id.not_in(new_role_ids)))
    else:
        await conn.execute_core(_delete(roles))
    await conn.execute_core(_delete(relationships).where(relationships.c.id.notlike("meta:%")))
    await conn.execute_core(_delete(tracked_functions))
    await conn.execute_core(_delete(tracked_webhooks))


async def _upsert_sources(  # REQ-012, REQ-250
    conn: "Connection",
    engine: Any,
    config: ProvisaConfig,
) -> None:
    for src in config.sources:
        await source_repo.upsert(conn, src)
        # Provision the source on the bound engine through the abstraction (the engine makes a
        # catalog; native engines attach lazily). No direct the engine reference here.
        if engine is not None:
            try:
                engine.register_source(src, resolve_secrets(src.password))
            except Exception:
                pass  # register_source / catalog.create_catalog already log warnings


async def _upsert_naming_rules(conn: "Connection", config: ProvisaConfig) -> None:
    await conn.execute_core(_delete(naming_rules))
    for rule in config.naming.rules:
        await conn.execute_core(
            insert(naming_rules).values(pattern=rule.pattern, replacement=rule.replace)
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


def _sqlite_lands(engine: Any, src: Source) -> bool:
    """True iff the engine reaches a sqlite source only by LANDING it into the control-plane store
    (FETCH/DIRECT → MATERIALIZED, e.g. Trino reads a PG replica), so it must be migrated in. An
    engine that ATTACHes sqlite in place (DuckDB) reads the file directly — no migration, which
    would also fail on a non-PG control plane (CREATE SCHEMA, etc.). Engine unknown → land (the default)."""
    if engine is None:
        return True
    from provisa.federation.engine import UnreachableSource
    from provisa.federation.strategy import Strategy, federate

    # ``engine`` may be the EngineRuntime wrapper or the bare FederationEngine; federate needs the
    # latter (it reads ``.connectors``). Unwrap the runtime's ``.engine`` when present.
    fed = (
        engine
        if getattr(engine, "connectors", None) is not None
        else getattr(engine, "engine", engine)
    )
    try:
        return federate(src, fed) is Strategy.MATERIALIZED
    except UnreachableSource:
        return False


async def _handle_sqlite_table(
    conn: "Connection",
    tbl: Table,
    src: Source,
    *,
    land: bool,
) -> None:
    """Post-register a sqlite table. Column types are ALWAYS resolved from the sqlite file at
    registration (design-time — no runtime typing), so the schema has real types whether the engine
    attaches the source in place (DuckDB) or reads a landed replica. ``land`` additionally migrates
    the rows into the control-plane store — only for engines that cannot read the file live (Trino
    FETCH); it is skipped for ATTACH engines (and would fail on a non-PG control plane anyway)."""
    from provisa.file_source.pg_migrate import migrate_sqlite_table, sqlite_column_types

    assert src.path is not None
    try:
        if land:
            await migrate_sqlite_table(
                src.path, tbl.table_name, conn, tbl.schema_name, tbl.table_name
            )
        await _fill_null_column_types(
            conn,
            tbl.source_id,
            tbl.schema_name,
            tbl.table_name,
            sqlite_column_types(src.path, tbl.table_name),
        )
    except Exception as _e:
        log.warning(
            "SQLite registration post-step failed for %s.%s: %s", tbl.source_id, tbl.table_name, _e
        )


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
    conn: "Connection",
    tbl: Table,
    src: Source,
    match: OpenAPIQuery,
    resolved_base_url: str,
    default_params: dict,
    api_columns: list[dict],
) -> None:
    from provisa.openapi.register import _schema_to_columns

    await conn.upsert(
        api_sources,
        {"id": src.id, "type": "openapi", "base_url": resolved_base_url, "auth": None},
        index_elements=["id"],
        update_columns=["base_url"],
    )
    await conn.upsert(
        api_endpoints,
        {
            "source_id": src.id,
            "path": match.path,
            "method": "GET",
            "table_name": tbl.table_name,
            "columns": api_columns,
            "ttl": src.cache_ttl or 300,
            "default_params": default_params if default_params else None,
            "promotions": getattr(tbl, "promotions", []) or [],
        },
        index_elements=["table_name"],
        update_columns=["source_id", "path", "columns", "ttl", "default_params", "promotions"],
    )
    for col_data in api_columns:
        if col_data.get("object_fields"):
            await conn.execute_core(
                update(table_columns)
                .where(
                    table_columns.c.table_id
                    == select(registered_tables.c.id)
                    .where(
                        registered_tables.c.source_id == tbl.source_id,
                        registered_tables.c.table_name == tbl.table_name,
                    )
                    .scalar_subquery(),
                    table_columns.c.column_name == col_data["name"],
                )
                .values(object_fields=col_data["object_fields"])
            )
    # Persist column data_type from the spec. OpenAPI tables are
    # API-backed: their cached response may be empty (so the engine can't
    # introspect every column), and native-filter params (_nf_*) never
    # appear in the response at all. The spec is authoritative for
    # both. introspect_tables trusts stored types — fill any null.

    # The cache table (built above, in this transaction) is
    # authoritative for response columns — including map-typed
    # responses (e.g. status→count) the spec schema has no
    # properties for. Read it over this same connection, which
    # sees the uncommitted table; the engine pass below cannot.
    await _fill_null_column_types(
        conn,
        tbl.source_id,
        tbl.schema_name,
        tbl.table_name,
        await _pg_column_types(conn, tbl.schema_name, tbl.table_name),
    )
    # Spec is authoritative for native-filter params (_nf_*),
    # which never appear in the cached response at all.
    _oapi_types: dict[str, str] = {}
    for _sc in _schema_to_columns(match.response_schema):
        _oapi_types[_sc["name"]] = _OAPI_PHYSICAL.get(_sc.get("type") or "string", "varchar")
    for _p in match.path_params + match.query_params:
        _pt = _OAPI_PHYSICAL.get(_p.get("type") or "string", "varchar")
        _oapi_types[_p["name"]] = _pt
        _oapi_types["_nf_" + _p["name"]] = _pt
    await _fill_null_column_types(conn, tbl.source_id, tbl.schema_name, tbl.table_name, _oapi_types)


async def _handle_openapi_table(
    conn: "Connection",
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
    conn: "Connection",
    engine: Any,
    tbl: Table,
    src: Source | None,
    openapi_specs: dict[str, dict],
) -> None:
    """Upsert one table and run source-type-specific post-upsert steps."""
    # Semantic-SQL naming authority (REQ-471): a source the engine cannot ATTACH in place is
    # materialized into the store, so its registered/physical name is a semantic alias — not a
    # source-physical name — and MUST be normalized through the central naming authority so all
    # data in the store follows one convention. Attach sources keep their source-physical name
    # (it must match the real table). Never rename inline — always route through apply_sql_name.
    from provisa.compiler.naming import apply_sql_name
    from provisa.federation.strategy import engine_attaches

    if src is not None and engine is not None and not engine_attaches(engine, src.type.value):
        tbl.table_name = apply_sql_name(tbl.table_name)

    if src and src.type.value == "openapi" and src.base_url:
        spec = openapi_specs.get(src.id, {})
        if spec:
            _enrich_openapi_table_columns(tbl, spec)

    await table_repo.upsert(conn, tbl)

    if src and src.type.value == "sqlite" and src.path:
        await _handle_sqlite_table(conn, tbl, src, land=_sqlite_lands(engine, src))
    elif src and src.type.value == "openapi" and src.base_url:
        spec = openapi_specs.get(src.id, {})
        if spec:
            await _handle_openapi_table(conn, tbl, src, spec)

    # Resolve column types from the FEDERATION ENGINE's own metadata — the single
    # introspection seam (EngineRuntime.introspect_columns): the engine reads its normalized
    # information_schema, DuckDB DESCRIBEs the attached source. No engine is referenced
    # directly here. introspect_tables trusts stored types, so fill any the YAML left
    # null (never overrides); an engine that can't introspect returns {} and YAML stands.
    if engine is not None and src is not None and tbl.columns:
        await _fill_null_column_types(
            conn,
            tbl.source_id,
            tbl.schema_name,
            tbl.table_name,
            engine.introspect_columns(src, tbl.schema_name, tbl.table_name),
        )


async def _purge_removed_tables(conn: "Connection", config: ProvisaConfig) -> None:
    """Delete registered_tables rows whose table_name is no longer in config (handles renames)."""
    tables_by_source: dict[str, list[str]] = {}
    for tbl in config.tables:
        tables_by_source.setdefault(tbl.source_id, []).append(tbl.table_name)
    for src_id, current_names in tables_by_source.items():
        await conn.execute_core(
            _delete(registered_tables).where(
                registered_tables.c.source_id == src_id,
                registered_tables.c.table_name.not_in(current_names),
            )
        )


async def _analyze_sources(engine: Any, config: ProvisaConfig) -> None:  # REQ-275
    """Prime federation CBO stats after tables are registered — through the engine seam."""
    for src in config.sources:
        try:
            engine.analyze(src, config.tables)
        except Exception:
            pass  # engine.analyze / analyze_source_tables already log per-table failures


async def _upsert_tables(  # REQ-013, REQ-016, REQ-251
    conn: "Connection",
    engine: Any,
    config: ProvisaConfig,
    openapi_specs: dict[str, dict],
) -> None:
    sources_by_id = {src.id: src for src in config.sources}

    for tbl in config.tables:
        src = sources_by_id.get(tbl.source_id)
        await _upsert_single_table(conn, engine, tbl, src, openapi_specs)

    await _purge_removed_tables(conn, config)

    if engine is not None:
        await _analyze_sources(engine, config)


async def _upsert_relationships(
    conn: "Connection", config: ProvisaConfig
) -> None:  # REQ-018, REQ-019, REQ-020
    """Delete stale relationships and upsert config-declared ones."""
    current_rel_ids = [rel.id for rel in config.relationships]
    graphql_remote_exists = (
        select(1)
        .select_from(registered_tables.join(sources, registered_tables.c.source_id == sources.c.id))
        .where(
            or_(
                registered_tables.c.id == relationships.c.source_table_id,
                registered_tables.c.id == relationships.c.target_table_id,
            ),
            sources.c.type == "graphql_remote",
        )
        .exists()
    )
    if current_rel_ids:
        await conn.execute_core(
            _delete(relationships).where(
                relationships.c.id.not_in(current_rel_ids),
                relationships.c.id.notlike("meta:%"),
                ~graphql_remote_exists,
            )
        )
    else:
        await conn.execute_core(
            _delete(relationships).where(
                relationships.c.id.notlike("meta:%"),
                ~graphql_remote_exists,
            )
        )
    for rel in config.relationships:
        try:
            await rel_repo.upsert(conn, rel)
        except ValueError:
            pass  # referenced table not yet registered (dynamic source); retried after source registration


async def _load_config_in_txn(  # REQ-012, REQ-013, REQ-016, REQ-041, REQ-250
    config: ProvisaConfig,
    conn: "Connection",
    engine: Any = None,
    replace: bool = False,
) -> None:
    """Upsert full config into PG within caller's transaction scope.

    When replace=True, all existing sources/tables/domains/roles/relationships
    not present in the new config are deleted first (full replace semantics).
    """
    # Resolve domain policy before any registration so repos/compilers read one source of truth.
    domain_policy.configure(config.naming.use_domains, config.naming.default_domain)

    # Serialize concurrent config loads to prevent deadlocks when multiple
    # processes (e.g. parallel test app lifespans) upsert the same rows. Taken through the DB
    # abstraction — a no-op on single-writer backends (SQLite) that need no cross-process lock.
    await conn.advisory_xact_lock(7261748190)

    if replace:
        await _replace_mode_cleanup(conn, config)

    # 1. Sources
    await _upsert_sources(conn, engine, config)

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
    _validate_change_signal(config)
    _validate_probe_type(config)
    _validate_watermark_columns(config)
    await _upsert_tables(conn, engine, config, openapi_specs)

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
        await function_repo.upsert_function(conn, func, return_schema=func.return_schema)

    # 9. Tracked webhooks. Config is the trusted source of truth, so a config-declared webhook is
    # pre-approved (REQ-209): without an 'executed' creation_request the schema gate in
    # app_loaders would silently exclude it from GraphQL forever (DB functions load ungated).
    from provisa.core.repositories import creation_request as cr_repo

    for wh in config.webhooks:
        await function_repo.upsert_webhook(conn, wh)
        await cr_repo.ensure_executed(conn, "webhook", wh.name, "config")

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


# REQ-824: non-PG RDBMS have no native push mechanism; they reach CDC only through a
# source-level Debezium transport block. This is also the exhaustive set of source
# types on which a cdc block is meaningful, alongside "postgresql" (native LISTEN/NOTIFY).
_CDC_DEBEZIUM_SOURCE_TYPES = {"mysql", "mariadb", "sqlserver", "oracle"}
_CDC_BLOCK_ALLOWED_SOURCE_TYPES = _CDC_DEBEZIUM_SOURCE_TYPES | {"postgresql"}

# REQ-814: which live strategies each source type can use. Dispatch is on strategy,
# not source_type; this matrix capability-gates strategy by the source's real push
# ability. Any pollable federated SQL source may use "poll".
_STRATEGIES_BY_SOURCE_TYPE: dict[str, set[str]] = {
    "postgresql": {"poll", "native", "debezium", "kafka"},
    "mongodb": {"poll", "native"},
    "kafka": {"kafka"},
    **{t: {"poll", "debezium", "kafka"} for t in _CDC_DEBEZIUM_SOURCE_TYPES},
}
# Strategies whose delta-transport is inherited from Source.cdc (REQ-824), and so
# require the source to declare a cdc block — except on a "kafka" source type, whose
# transport is the source's own Kafka connection.
_TRANSPORT_STRATEGIES = {"debezium", "kafka"}


def _allowed_strategies(source_type: str | None) -> set[str]:
    # Default: only watermark polling through the engine (any federated SQL source).
    return _STRATEGIES_BY_SOURCE_TYPE.get(source_type or "", {"poll"})


def _validate_table_live_delivery(config) -> None:
    """Validate live change-feed config on all tables (REQ-282–287, REQ-813, REQ-814, REQ-824)."""
    for source in config.sources:
        # REQ-824: a source-level cdc block only makes sense on CDC-capable RDBMS sources.
        if getattr(source, "cdc", None) is not None:
            stype = getattr(source, "type", None)
            if stype not in _CDC_BLOCK_ALLOWED_SOURCE_TYPES:
                raise ValueError(
                    f"Source {source.id!r}: cdc transport config not supported for source type "
                    f"{stype!r} (only PostgreSQL and Debezium-captured RDBMS)"
                )

    sources_by_id = {s.id: s for s in config.sources}
    for table in config.tables:
        if table.live is None:
            continue
        strategy = table.live.strategy
        source = sources_by_id.get(table.source_id)
        stype = getattr(source, "type", None) if source else None

        if strategy == "poll" and not table.live.watermark_column:
            raise ValueError(
                f"Table {table.table_name!r}: live.strategy=poll requires watermark_column"
            )
        if strategy == "kafka" and table.live.kafka is None and stype != "kafka":
            raise ValueError(
                f"Table {table.table_name!r}: live.strategy=kafka requires a kafka params block"
            )
        if source is None:
            continue
        # REQ-814: capability-gate strategy by source type.
        if strategy not in _allowed_strategies(stype):
            raise ValueError(
                f"Table {table.table_name!r}: live.strategy={strategy!r} not supported for source "
                f"type {stype!r} (allowed: {sorted(_allowed_strategies(stype))})"
            )
        # REQ-824: debezium/kafka transport on an RDBMS source is inherited from the
        # source's cdc block — require it. (A "kafka" source type carries its own transport.)
        if (
            strategy in _TRANSPORT_STRATEGIES
            and stype in _CDC_DEBEZIUM_SOURCE_TYPES | {"postgresql"}
            and getattr(source, "cdc", None) is None
        ):
            raise ValueError(
                f"Table {table.table_name!r}: live.strategy={strategy} on source {source.id!r} "
                f"({stype}) requires source-level cdc transport (bootstrap_servers/topic_prefix)"
            )


# REQ-925: canonical IR types a watermark may take. A watermark drives WHERE wm > cursor
# incremental reads, so it MUST be monotonic non-decreasing: an incrementing integer or a
# temporal column. Text/float/boolean/uuid/bytea/numeric are rejected — they give no reliable
# ordering for delta reads. (numeric is excluded: a scale/rounding column is not an increment.)
_MONOTONIC_WATERMARK_IR = frozenset({"smallint", "integer", "bigint", "timestamp", "date", "time"})


def _validate_watermark_columns(config) -> None:  # REQ-924, REQ-925
    """A table's watermark must be one of its OWN columns (REQ-924) and monotonic (REQ-925).

    The watermark is the top-level ``Table.watermark_column`` or, when set on live config, the table's
    ``live.watermark_column``. It is rejected when it names a column the table does not have, or
    when that column's type is not a monotonic (integer/temporal) IR type. Type is checked only
    when the column's ``data_type`` is known — introspection fills it for sources whose columns are
    reflected at startup, and the selection is re-validated then; existence is always checked when
    the table declares columns."""
    from provisa.core.ir_types import to_ir  # noqa: PLC0415

    for table in config.tables:
        live = getattr(table, "live", None)
        watermark = table.watermark_column or (live.watermark_column if live is not None else None)
        if not watermark:
            continue
        columns = list(table.columns or [])
        if not columns:
            continue  # columns filled by introspection later; re-validated at selection then
        col = next((c for c in columns if c.name == watermark), None)
        if col is None:
            raise ValueError(
                f"Table {table.table_name!r}: watermark_column {watermark!r} is not a column of "
                f"the table (watermark must name an existing column, REQ-924)"
            )
        if col.data_type is None:
            continue  # type not yet resolved (deferred introspection); re-checked at selection
        try:
            ir = to_ir(col.data_type)
        except ValueError:
            ir = None
        if ir not in _MONOTONIC_WATERMARK_IR:
            raise ValueError(
                f"Table {table.table_name!r}: watermark_column {watermark!r} has type "
                f"{col.data_type!r} which is not monotonic non-decreasing; a watermark must be a "
                f"timestamp/date/time or an incrementing integer (REQ-925)"
            )


def _validate_change_signal(config) -> None:  # REQ-932
    """Capability-gate change_signal. Push transports (debezium/kafka) require the source's cdc
    block (a "kafka" source type carries its own transport). The signal resolves table → source."""
    from provisa.core.change_signal import resolve  # noqa: PLC0415

    sources_by_id = {s.id: s for s in config.sources}
    for table in config.tables:
        source = sources_by_id.get(table.source_id)
        source_signal = getattr(source, "change_signal", None) if source else None
        sig = resolve(getattr(table, "change_signal", None), source_signal)
        if sig not in ("debezium", "kafka"):
            continue
        stype = getattr(source, "type", None) if source else None
        stype_val = getattr(stype, "value", stype)
        if stype_val == "kafka":
            continue
        if source is None or getattr(source, "cdc", None) is None:
            raise ValueError(
                f"Table {table.table_name!r}: change_signal={sig} requires source-level cdc "
                f"transport on {table.source_id!r} (bootstrap_servers/topic_prefix)"
            )


def _validate_probe_type(config) -> None:  # REQ-982
    """Capability-gate probe_type. A table's probe_type must be supported by its source's capability
    class (probe_capabilities); ttl cadence forces none. Delegates to ``resolve_probe_type``, which
    raises on an unsupported type or a ttl+explicit-type mismatch — surfaced as a config error."""
    from provisa.core.change_signal import resolve  # noqa: PLC0415
    from provisa.events.probes import resolve_probe_type  # noqa: PLC0415

    sources_by_id = {s.id: s for s in config.sources}
    for table in config.tables:
        if getattr(table, "probe_type", None) is None:
            continue  # unset → resolved per class at wiring time; nothing to reject
        source = sources_by_id.get(table.source_id)
        source_signal = getattr(source, "change_signal", None) if source else None
        sig = resolve(getattr(table, "change_signal", None), source_signal)
        stype = getattr(source, "type", None) if source else None
        stype_val = getattr(stype, "value", stype)
        try:
            resolve_probe_type(
                table.probe_type,
                source_type=str(stype_val),
                change_signal=sig,
                has_watermark=getattr(table, "watermark_column", None) is not None,
            )
        except ValueError as exc:
            raise ValueError(f"Table {table.table_name!r}: {exc}") from exc


async def _validate_existing_domains(conn: "Connection", default_domain: str) -> None:
    result = await conn.execute_core(
        select(
            registered_tables.c.source_id,
            registered_tables.c.schema_name,
            registered_tables.c.table_name,
            registered_tables.c.domain_id,
        ).where(
            registered_tables.c.domain_id != "",
            registered_tables.c.domain_id.not_in([default_domain, "meta", "ops"]),
        )
    )
    rows = [dict(r._mapping) for r in result.fetchall()]
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
    pg_conn: "Connection",
    engine: Any = None,
    replace: bool = False,
) -> None:
    """Upsert full config into PG within a transaction. Idempotent.

    ``engine`` is the EngineRuntime: it provisions each source (the engine catalog / native
    attach) and supplies engine-native column types — the ONLY engine touchpoint, so no
    the engine connection is passed here. Pass replace=True to delete all metadata not in the
    new config first (full replace semantics — use for install simulation / clean reloads).
    """
    async with pg_conn.transaction():
        await _load_config_in_txn(config, pg_conn, engine, replace=replace)


async def load_config_from_yaml(  # REQ-012, REQ-016, REQ-250
    path: str | Path,
    pg_conn: "Connection",
    engine: Any = None,
    replace: bool = False,
) -> ProvisaConfig:
    """Parse YAML, resolve secrets in source passwords, load into PG."""
    config = parse_config(path)
    await load_config(config, pg_conn, engine, replace=replace)
    return config
