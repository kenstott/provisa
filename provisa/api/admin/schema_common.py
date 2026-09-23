# Copyright (c) 2026 Kenneth Stott
# Canary: 01f851fb-a7f6-4972-b8f1-2643187fccde
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Shared leaf helpers for the admin GraphQL schema.

Strawberry types, admin-context resolution, creation-request queueing, and the
source-management operations (pool/engine registration, govdata, view MV sync)
used by both the Query and Mutation resolvers. No dependency on those classes.
"""

# complexity-gate: allow-ble=2 reason="two genuine boundaries, not grandfathering: (1) _validate_govdata_api_key TESTS an external credential — any failure to connect means the key is invalid, which it REPORTS to the user as MutationResult(success=False); narrowing would let an unexpected JVM/JDBC failure crash the mutation instead of reporting an invalid key. (2) _register_source_on_engine is best-effort engine provisioning; register_source logs its own warnings and provisioning is non-fatal (the source stays usable direct-routed), matching the established convention at config_loader.py:248."

import logging
from typing import TYPE_CHECKING, Optional, cast

import strawberry
from sqlalchemy import select, update
from strawberry.types.info import Info as StrawberryInfo

from provisa.core.schema_org import (
    registered_tables,
    sources,
)

if TYPE_CHECKING:
    from provisa.core.database import Connection

from provisa.security.rights import has_platform_bypass
from provisa.api.admin._guards import require_active_org_id
from provisa.api.admin.types import (
    MutationResult,
    SourceInput,
)

from provisa.api.admin.schema_helpers import (
    _get_pool,
)
from provisa.api.admin._table_ops import (
    _build_column_models,
    _ensure_view_column_types,
)

# Public re-export surface: these underscore-prefixed helpers were relocated here in the
# god-file split and are imported by other modules/tests from schema_common. Listing them in
# __all__ marks them as intentional exports so linters do not flag them as unused.
__all__ = [
    "CreationRequestType",
    "_add_source_pool",
    "_analyze_source_on_engine",
    "_build_column_models",
    "_configure_govdata_env",
    "_ensure_view_column_types",
    "_drop_source_on_engine",
    "_fire_catalog_indexing",
    "_prime_govdata_cache",
    "_queue_creation_request",
    "_rebuild_relationship_input",
    "_rebuild_table_input",
    "_register_source_on_engine",
    "_remove_view_mv",
    "_resolve_admin_context",
    "_sync_view_mv",
    "_upsert_source_with_domains",
    "_validate_govdata_api_key",
]


@strawberry.type
class CreationRequestType:  # REQ-434, REQ-063
    id: int
    request_type: str
    capability: str
    requested_by: Optional[str]
    status: str
    rejection_reason: Optional[str]
    payload_json: str


def _rebuild_relationship_input(payload: dict):
    from provisa.api.admin.types import RelationshipInput

    return RelationshipInput(**payload)


def _rebuild_table_input(payload: dict):
    from provisa.api.admin.types import (
        ColumnInput,
        ColumnPresetInput,
        TableInput,
        UniqueConstraintInput,
    )

    data = dict(payload)
    data["columns"] = [ColumnInput(**c) for c in payload.get("columns", [])]
    data["column_presets"] = [ColumnPresetInput(**c) for c in payload.get("column_presets", [])]
    data["unique_constraints"] = [
        UniqueConstraintInput(**u) for u in payload.get("unique_constraints", [])
    ]  # REQ-1792: an MCP-proposed table can declare these; a GraphQL-queued view/table already did
    return TableInput(**data)


def _rebuild_source_input(payload: dict):  # REQ-1792
    from provisa.api.admin.types import SourceCdcConfigInput, SourceInput

    data = dict(payload)
    if data.get("cdc") is not None:
        data["cdc"] = SourceCdcConfigInput(**data["cdc"])
    return SourceInput(**data)


async def _queue_creation_request(  # REQ-434
    info, request_type: str, capability: str, input
) -> MutationResult:
    """Persist a governed create the caller is not authorized to perform (REQ-434)."""
    import dataclasses

    from provisa.api.admin.capabilities import _identity_from_info
    from provisa.core.repositories import creation_request as cr_repo

    payload = dataclasses.asdict(input)
    identity = _identity_from_info(info)
    requested_by = getattr(identity, "user_id", None) if identity is not None else None
    pool = await _get_pool()
    async with pool.acquire() as conn:
        rid = await cr_repo.create(
            cast("Connection", conn), request_type, capability, payload, requested_by
        )
    return MutationResult(
        success=True,
        message=(
            f"Queued as creation request #{rid} — awaiting a user holding "
            f"{capability!r} to execute or reject it."
        ),
        code="schema.creation_request_queued",
        params={"id": rid, "capability": capability},
    )


def _resolve_admin_context(info: StrawberryInfo) -> tuple[str, bool]:
    """Return (active_org_id, is_admin) for the current request identity."""
    from provisa.api.admin.capabilities import _resolved_capabilities
    from provisa.api.app import state as _state

    request = info.context["request"]
    active_org_id = require_active_org_id(request)
    identity = getattr(request.state, "identity", None)
    caps = _resolved_capabilities(identity, _state) if identity else set()
    is_admin = has_platform_bypass(caps)  # REQ-1337: rights only, never the platform_admin role id
    return active_org_id, is_admin


async def _validate_govdata_api_key(input: SourceInput) -> Optional[MutationResult]:
    """Return a failure MutationResult if the govdata API key is invalid, else None."""
    if not input.username:
        return MutationResult(
            success=False,
            message="AskAmerica API Key is required",
            code="schema.askamerica_key_required",
        )
    import asyncio as _asyncio
    import logging as _vlog
    from provisa.core.models import GovDataSource as _GDS, GovDataSubject as _GDSubj
    from provisa.core.secrets import resolve_secrets as _rs_v
    from provisa.govdata.source import connect as _gd_v

    def _validate() -> None:
        gds = _GDS(
            id=input.id,
            subject=_GDSubj.all,
            govdata_schemas=["fec"],
            domain_id="default",
            api_key=_rs_v(input.username),
        )
        conn = _gd_v(gds)
        conn.getMetaData().getDatabaseProductName()

    try:
        loop = _asyncio.get_running_loop()
        await loop.run_in_executor(None, _validate)
    except Exception as _ve:
        _vlog.getLogger(__name__).warning("govdata API key validation failed: %s", _ve)
        return MutationResult(
            success=False,
            message=f"Invalid AskAmerica API Key: {_ve}",
            code="schema.invalid_askamerica_key",
            params={"error": str(_ve)},
        )
    return None


async def _stage_kaggle_if_needed(input: SourceInput) -> Optional[MutationResult]:  # REQ-1819
    """The ONE place a Kaggle-derived source's actual files get downloaded — called from inside
    create_source itself, the single resolver every creation path (the admin form's own createSource
    call, MCP chat's create_source_now, and a queued propose_source request executed later on
    Requests-page approval) funnels through.

    Before this existed, staging was a separate step (stageKaggleDataset) a caller had to remember
    to run first and hand the resulting directory into `path` — reachable but not required, so a
    caller that skipped it (confirmed live: an MCP chat proposal) created a Source pointing at a
    directory nothing had ever populated. Centralizing it here means no caller's `path` is ever
    trusted for a Kaggle-hinted source: this always re-stages (stage_dataset is idempotent — it
    overwrites the same directory) and OVERWRITES `input.path` with the real result, so skipping
    the old separate step is no longer possible from any path, present or future.

    Detected by federation_hints_json carrying kaggle_owner/kaggle_ref (stashed by
    KaggleFormSection.tsx at creation, or set the same way by an MCP proposal) — not by
    input.type, since a caller could set anything there.

    Must run inside `bound_to_request_org()` (same requirement as search_kaggle_datasets) — the
    fixed KAGGLE_TOKEN_SECRET_NAME secret resolves against whichever org's vault is bound."""
    import json as _json

    hints = _json.loads(input.federation_hints_json or "{}")
    owner, ref = hints.get("kaggle_owner"), hints.get("kaggle_ref")
    if not owner or not ref:
        return None

    from provisa.core.secrets import resolve_secrets
    from provisa.kaggle.downloader import (
        KAGGLE_TOKEN_SECRET_NAME,
        UnsupportedKaggleDataset,
        stage_dataset,
    )

    token = resolve_secrets(f"${{secret:{KAGGLE_TOKEN_SECRET_NAME}}}")
    try:
        staged_root = await stage_dataset(token, owner, ref)
    except UnsupportedKaggleDataset as exc:
        return MutationResult(
            success=False,
            message=f"Kaggle dataset {owner}/{ref}: {exc}",
            code="schema.kaggle_stage_failed",
            params={"owner": owner, "ref": ref},
        )
    except Exception as exc:
        logging.getLogger(__name__).exception(
            "_stage_kaggle_if_needed: staging failed for %s/%s", owner, ref
        )
        return MutationResult(
            success=False,
            message=f"Kaggle dataset {owner}/{ref} could not be staged: {exc}",
            code="schema.kaggle_stage_failed",
            params={"owner": owner, "ref": ref, "error": str(exc)},
        )
    input.path = str(staged_root)
    return None


async def _upsert_source_with_domains(pool, model, input: SourceInput) -> None:
    """Upsert the source model and update allowed_domains in the DB."""
    from provisa.core.repositories import source as source_repo

    async with pool.acquire() as conn:
        await source_repo.upsert(conn, model)
        _domains = [d for d in (input.allowed_domains or []) if d.strip()]
        if _domains:
            await conn.execute_core(
                update(sources).where(sources.c.id == input.id).values(allowed_domains=_domains)
            )


def _configure_govdata_env(input: SourceInput) -> None:
    """Set AWS environment variables required for govdata access."""
    import os as _os
    from provisa.core.secrets import resolve_secrets as _rs

    # Overwrite, never setdefault: the credential the caller registered the source with is the
    # authoritative one. Deferring to an ambient AWS_ACCESS_KEY_ID silently authenticates govdata
    # with whatever unrelated key happens to be in the environment, and is inconsistent with the
    # endpoint below, which already overwrites.
    _os.environ["AWS_ACCESS_KEY_ID"] = _rs(input.username)
    if input.password:
        _os.environ["AWS_SECRET_ACCESS_KEY"] = _rs(input.password)
    if input.host:
        _os.environ["AWS_ENDPOINT_OVERRIDE"] = _rs(input.host)


async def _add_source_pool(state, input: SourceInput) -> None:
    """Register a direct connection pool for the source if a driver exists."""
    from provisa.executor.drivers.registry import has_driver
    from provisa.core.secrets import resolve_secrets

    if not has_driver(input.type):
        return
    # REQ-012: a failed direct connection must surface (no silent swallow), so the
    # caller can reject registration instead of persisting a dead source.
    # Warehouse connection extras (Databricks http_path, Snowflake account/warehouse, …) the standard
    # args can't carry (REQ-986/987/988). The admin input carries them as federationHintsJson; a
    # config-built input object passes the already-parsed dict.
    from provisa.api.admin._row_mappers import _federation_hints_from_input

    hints = getattr(input, "federation_hints", None) or _federation_hints_from_input(input)
    # REQ-1726: a file-embedded source (sqlite today) has no host of its own — the Sources form
    # writes its file path into `path`, never `host`/`database`. SQLAlchemy's sqlite dialect takes
    # the file path as the URL's "database" segment and must have NO host component at all
    # (sqlalchemy_driver.py's URL.create renders a bare host as an extra path segment before the
    # file path, e.g. "sqlite://localhost/./demo/files/x.sqlite"), so the "localhost" default below
    # — sensible for every network-addressed type — actively breaks a file-embedded one. `path` is
    # never set for a source type that already carries host/database, so branching on it changes
    # nothing for them.
    file_path = getattr(input, "path", None)
    await state.source_pools.add(
        source_id=input.id,
        source_type=input.type,
        host="" if file_path else (resolve_secrets(input.host) if input.host else "localhost"),
        port=input.port,
        database=file_path or input.database,
        user=input.username,
        password=resolve_secrets(input.password),
        extra={k: resolve_secrets(v) for k, v in hints.items()},
    )


async def _synthesize_mapping_dsl_tables(pool, model) -> None:  # REQ-1730
    """Fill in ``model.mapping["tables"]`` entries a redis source's UI-registered tables never got.

    REQ-250/251's Trino table-description files are written from ``source.mapping.tables`` — a
    config-declared source populates it at provisioning, but the UI's Register Table flow never
    writes back to it (DuckDB needs no static description file; it introspects redis directly via
    ``redis/fetch.py``). Reprovisioning such a source on Trino (REQ-1730's cross-engine swap, or
    any engine change that (re)provisions an existing source) then produces an EMPTY description
    file and every one of its tables 404s as TABLE_NOT_FOUND, even though DuckDB serves it fine.
    Synthesize the same entry ``redis/fetch.py``'s own fallback convention already assumes for an
    undeclared table (``required_key_pattern``, key column "key", hash values) from what IS
    already known — ``registered_tables``/``table_columns`` — for any table not already declared.

    Prometheus deliberately gets none of this: its Trino connector has no table-description
    mechanism at all (only ``prometheus.uri``/``prometheus.http.additional-headers``/
    ``prometheus.read-timeout`` are real catalog properties — verified directly against a live
    coordinator, which rejects a ``prometheus.table-description-dir`` property outright) and
    exposes a FIXED per-metric schema (``labels MAP(VARCHAR,VARCHAR), timestamp, value``) with no
    per-label flat columns under any configuration. A registered label column (e.g. "job") is only
    reachable there as ``labels['job']`` — a physical-SQL rewrite the compiler would have to make,
    not something a table-description entry (which ``provisa.prometheus.source.
    generate_table_definitions`` produces but nothing ever wires to a real Trino property) can fix.
    """
    if model.type.value != "redis":
        return
    from provisa.core.schema_org import table_columns
    from provisa.redis.fetch import KEY_COLUMN
    from provisa.redis.source import DEFAULT_SCHEMA, ValueType, required_key_pattern

    declared = {t.get("name") for t in model.mapping.get("tables", [])}
    async with pool.acquire() as conn:
        res = await conn.execute_core(
            select(registered_tables.c.id, registered_tables.c.table_name).where(
                registered_tables.c.source_id == model.id
            )
        )
        rows = res.fetchall()
        for row in rows:
            if row.table_name in declared:
                continue
            cols_res = await conn.execute_core(
                select(table_columns.c.column_name, table_columns.c.data_type).where(
                    table_columns.c.table_id == row.id
                )
            )
            cols = [
                {"name": c.column_name, "data_type": c.data_type or "VARCHAR"}
                for c in cols_res.fetchall()
                if c.column_name != KEY_COLUMN
            ]
            model.mapping.setdefault("tables", []).append(
                {
                    "name": row.table_name,
                    "key_pattern": required_key_pattern(row.table_name, DEFAULT_SCHEMA),
                    "key_column": KEY_COLUMN,
                    "value_type": ValueType.HASH,
                    "columns": cols,
                }
            )


async def _cache_prometheus_label_columns(pool, state, model) -> None:  # REQ-1730
    """Record which registered columns of a prometheus table are labels, keyed by the physical
    (catalog, table) Trino queries address it as.

    Trino's prometheus connector has no table-description mechanism (see
    ``_synthesize_mapping_dsl_tables``'s docstring) and exposes a FIXED per-metric schema —
    ``labels MAP(VARCHAR,VARCHAR), timestamp, value`` — so a registered label column (e.g. "job")
    is only reachable there as ``labels['job']``. ``TrinoBackend.transpile_physical`` reads this
    cache to rewrite a bare label reference for exactly the (catalog, table) pairs known to be
    prometheus; DuckDB needs no rewrite (it introspects prometheus directly and already exposes the
    label as a flat column)."""
    if model.type.value != "prometheus":
        return
    from provisa.core.schema_org import table_columns

    catalog = state.source_catalogs.get(model.id, model.id)
    cache: dict[tuple[str, str], set[str]] = getattr(state, "prometheus_label_columns", None) or {}
    async with pool.acquire() as conn:
        res = await conn.execute_core(
            select(registered_tables.c.id, registered_tables.c.table_name).where(
                registered_tables.c.source_id == model.id
            )
        )
        for row in res.fetchall():
            cols_res = await conn.execute_core(
                select(table_columns.c.column_name).where(table_columns.c.table_id == row.id)
            )
            labels = {
                c.column_name
                for c in cols_res.fetchall()
                if c.column_name not in ("timestamp", "value")
            }
            cache[(catalog, row.table_name)] = labels
    state.prometheus_label_columns = cache


def _register_source_on_engine(state, model, input: SourceInput) -> None:
    """Provision the source on the bound engine (mirrors config_loader path)."""
    from provisa.core.secrets import resolve_secrets

    try:
        state.federation_engine.register_source(
            model,
            # REQ-1695: the model's password is the PERSISTED reference, so the engine is handed
            # the same credential every later reader of this source resolves. Reading it off the
            # raw form input instead would register the literal the operator typed and leave the
            # engine holding a value the control-plane row does not name.
            resolve_secrets(model.password) if model.password else "",
            # REQ-1266: the physical catalog the caller just published for this source. Registering
            # under the bare name put a non-default org's source at a catalog its own queries — which
            # address the org-prefixed name — could not find.
            catalog_name=state.source_catalogs[input.id],
        )
    except Exception as _cat_err:
        logging.getLogger(__name__).warning(
            "engine source provisioning for %r failed: %s", input.id, _cat_err
        )


def _drop_source_on_engine(state, source_id: str) -> None:
    """Deprovision a source's dynamic catalog on the bound engine — the exact mirror of
    ``_register_source_on_engine`` for deletion, which ``delete_source`` never had at all (REQ-1730).
    Confirmed live: a deleted source's Trino catalog was NEVER dropped, so every ephemeral e2e
    source ever created (this reboot-harness's own tests, hundreds of runs) accumulated forever in
    Trino's persistent dynamic-catalog store — reproduced directly via ``docker logs
    provisa-trino-1``, which showed dozens of long-orphaned per-source catalogs (many pointing at
    containers that no longer exist, spamming `Connection refused`) still being reloaded on every
    coordinator restart, and `docker inspect`'s RestartCount at 42. Best-effort and non-fatal, the
    same posture as registration: a source with no catalog (never provisioned, or already gone)
    just no-ops (``DROP CATALOG IF EXISTS``, core/catalog.py's own `drop_catalog`)."""
    try:
        state.federation_engine.drop_source(
            source_id, catalog_name=state.source_catalogs.get(source_id)
        )
    except Exception as _cat_err:
        logging.getLogger(__name__).warning(
            "engine source deprovisioning for %r failed: %s", source_id, _cat_err
        )
    # REQ-1690 (Amended 2026-09-19, delete-time stop): a pgwire-replica source (files/sharepoint/
    # splunk) keeps its Calcite server running in pgwire_replica._ENDPOINTS, keyed by this same
    # source_id, for the life of this process — untouched by drop_source above (that's the
    # Trino/dynamic-catalog seam, not this one). Confirmed live: a `files` source deleted and
    # recreated under the same id kept serving the FIRST server's model.json, so the "new" source
    # never picked up a changed path or format. No-ops when no server was ever started.
    from provisa.federation.pgwire_replica import stop_endpoint

    try:
        stop_endpoint(source_id)
    except Exception as _pgwire_err:
        logging.getLogger(__name__).warning(
            "pgwire endpoint teardown for %r failed: %s", source_id, _pgwire_err
        )


async def _analyze_source_on_engine(state, pool, model, input: SourceInput) -> None:
    """Fire engine ANALYZE on all registered tables for this source (errors swallowed)."""

    class _TblRef:
        def __init__(self, source_id: str, schema_name: str, table_name: str) -> None:
            self.source_id = source_id
            self.schema_name = schema_name
            self.table_name = table_name

    async with pool.acquire() as _conn:
        _res = await _conn.execute_core(
            select(registered_tables.c.schema_name, registered_tables.c.table_name).where(
                registered_tables.c.source_id == input.id
            )
        )
        rows = _res.fetchall()
    table_refs = [_TblRef(input.id, r.schema_name, r.table_name) for r in rows]
    if table_refs:
        # REQ-1266: analyze the catalog the source is registered at, not the bare name.
        state.federation_engine.analyze(
            model, table_refs, catalog_name=state.source_catalogs[input.id]
        )


def _prime_govdata_cache(input: SourceInput) -> None:
    """Schedule a background task to prime the govdata metadata cache."""
    import asyncio as _asyncio
    from provisa.core.models import GovDataSource as _GDS, GovDataSubject as _GDSubj
    from provisa.core.secrets import resolve_secrets as _rs2
    from provisa.govdata.source import prime_source as _prime

    _gds = _GDS(
        id=input.id,
        subject=_GDSubj.all,
        govdata_schemas=[s.strip().lower() for s in input.database.split(",") if s.strip()],
        domain_id="default",
        api_key=_rs2(input.username),
    )
    _schemas = [s.strip().lower() for s in input.database.split(",") if s.strip()]

    async def _prime_task() -> None:
        loop = _asyncio.get_running_loop()
        await loop.run_in_executor(None, _prime, _gds, _schemas)

    _asyncio.create_task(_prime_task())


def _fire_catalog_indexing(state, pool, input: SourceInput) -> None:
    """Schedule background catalog indexing for NL table search (REQ-464)."""
    import asyncio as _asyncio
    from provisa.discovery.catalog_cache import index_source as _index_source

    _asyncio.create_task(
        _index_source(
            input.id,
            pool,
            state.federation_engine,
            state.source_pools,
            state.source_types,
            state,
        )
    )


def _sync_view_mv(
    table_name: str,
    view_sql: str,
    refresh_interval: int,
    change_signal: str | None = None,
    *,
    debounce_quiet: float = 0.0,
    debounce_max_delay: float | None = None,
    consistency: str = "shared",  # REQ-879
    preprocess: str | None = None,  # REQ-957
    bitemporal_mode: str | None = None,  # REQ-1162: None | "snapshot" | "delta"
    bitemporal_key: list[str] | None = None,  # REQ-1162: entity key (required for delta)
    persist: str = "replace",  # REQ-965: replace | append | upsert
    primary_key: list[str] | None = None,  # REQ-970: row identity
    incremental: bool = False,  # REQ-969: incremental maintenance
    calendar: str | None = None,  # REQ-962: periodic-snapshot calendar (None = not periodic)
    grain: str | None = None,  # REQ-962/1168: nesting grain or nth-weekday spec
    allowed_lateness: float = 0.0,  # REQ-961: seal-deadline slack (s)
    expected_events: list[str] | None = None,  # REQ-961: preflight freshness contract
    business_day_grain: bool = False,  # REQ-962: gate windows to business days
) -> None:
    """Register or update an MVDefinition for a materialized user-defined view."""
    # REQ-879: consistency tier is a closed set — reject anything else loudly (no silent default).
    if consistency not in ("shared", "distributed"):
        raise ValueError(
            f"invalid MV consistency {consistency!r}: expected 'shared' or 'distributed'"
        )
    from provisa.api.app import state
    from provisa.mv.bitemporal import BitemporalSpec  # REQ-1162
    from provisa.mv.models import MVDefinition, MVStatus
    from provisa.core.change_signal import resolve, to_freshness_mode  # REQ-932
    from provisa.mv.determinism import check_view_determinism  # REQ-964
    from provisa.mv.preprocess import validate_preprocess  # REQ-957

    # REQ-1162: build the bitemporal spec from the declared mode/key (None = ordinary MV). Spec
    # construction validates the mode and that delta has a key — a bad declaration fails loud here.
    bitemporal = (
        BitemporalSpec(key=tuple(bitemporal_key or []), mode=bitemporal_mode)
        if bitemporal_mode
        else None
    )
    # REQ-1162: a bitemporal MV manages its own append-only persistence (refresh.py routes it to
    # _refresh_bitemporal ahead of the persist dispatch, ignoring `persist` entirely) — reject a
    # non-default `persist` here instead of silently dropping it, so a declaration never implies
    # replace/upsert semantics the MV will not actually get.
    if bitemporal is not None and persist != "replace":
        raise ValueError(
            f"MV {table_name!r}: persist={persist!r} is incompatible with bitemporal_mode "
            f"={bitemporal_mode!r} — bitemporal MVs are append-only by construction; omit "
            "persist (or leave it at its default) instead of declaring append/upsert"
        )

    # REQ-957/964: a preprocess hook must be deterministic + safe — purity-checked here so a bad hook
    # is rejected at registration, never wired into the loop where it would ripple non-determinism.
    validate_preprocess(preprocess)

    # REQ-964 (proof obligation 1): an MV's SQL must be deterministic — recompute-to-current
    # and replay demand it. Reject volatile SQL (now()/random/…) at registration; the engine's
    # dialect parses the check (None → sqlglot default parse, still catches volatile funcs).
    dialect = getattr(getattr(state, "engine", None), "dialect", None)
    ok, reason = check_view_determinism(view_sql, dialect)
    if not ok:
        raise ValueError(f"non-deterministic MV {table_name!r}: {reason}")

    mv_id = f"view-{table_name}"
    existing = state.mv_registry.get(mv_id)
    # REQ-932: derive the refresh gate from change_signal. A user view has no backing source, so
    # resolve falls to the global default. Push signals return None → keep ttl until CDC-apply.
    freshness = to_freshness_mode(resolve(change_signal, None)) or "ttl"
    # Target the store the ACTIVE engine actually materializes into — a DuckDB engine attaches its
    # store as ``mat_store``, not ``postgresql``. Hardcoding the latter fails the refresh with
    # "Catalog with name postgresql does not exist".
    target_catalog, target_schema = state.federation_engine.materialize_store_target(state.org_id)
    mv = MVDefinition(
        id=mv_id,
        source_tables=[],
        target_catalog=target_catalog,
        target_schema=target_schema,
        target_table=f"mv_{table_name}",
        refresh_interval=refresh_interval,
        enabled=True,
        sql=view_sql,
        expose_in_sdl=False,
        status=existing.status if existing is not None else MVStatus.STALE,
        freshness_mode=freshness,
        debounce_quiet=debounce_quiet,  # REQ-963
        debounce_max_delay=debounce_max_delay,  # REQ-963
        consistency=consistency,  # REQ-879
        preprocess=preprocess,  # REQ-957
        bitemporal=bitemporal,  # REQ-1162
        persist=persist,  # REQ-965
        primary_key=list(primary_key or []),  # REQ-970
        incremental=incremental,  # REQ-969
        calendar=calendar,  # REQ-962
        grain=grain,  # REQ-962/1168
        allowed_lateness=allowed_lateness,  # REQ-961
        expected_events=expected_events,  # REQ-961
        business_day_grain=business_day_grain,  # REQ-962
    )
    state.mv_registry.register(mv)


def _remove_view_mv(table_name: str) -> None:
    """Remove a materialized view definition when materialize is toggled off."""
    from provisa.api.app import state

    state.mv_registry.unregister(f"view-{table_name}")


async def activate_view_mv(table_name: str) -> None:
    """Bring a freshly-created materialized view online WITHOUT a restart. Call AFTER the MV is
    registered and the schemas are rebuilt (so ``mv.sql`` is compiled to a physical plan):

      1. Materialize it immediately — a first refresh, so it lands FRESH with rows instead of sitting
         STALE/never until something else triggers it.
      2. (Re)register its event-loop poll job — ``wire_event_loop`` otherwise runs only at boot, so a
         runtime-created MV would have no refresh cadence until the next restart. Re-wired with
         ``seed=False`` (idempotent, replace_existing) so it does NOT re-land every source.

    Both callees own their own failure handling — ``refresh_mv`` marks the MV failed (never raises)
    and ``wire_event_loop`` is best-effort (never raises into its caller) — so a failure here leaves
    the MV STALE with the manual Refresh still available, without swallowing errors at this layer."""
    import logging as _logging

    from provisa.api.app import state
    from provisa.mv.refresh import refresh_mv

    _log = _logging.getLogger(__name__)
    mv = state.mv_registry.get(f"view-{table_name}")
    if mv is None:
        return
    # refresh_mv catches its own exceptions and marks the MV refresh-failed — no guard needed here.
    # REQ-879: pass the shared control-plane catalog so a fleet coordinates the refresh (atomic claim).
    await refresh_mv(state.federation_engine, mv, state.mv_registry, store=state.tenant_db)

    scheduler = getattr(state, "_scheduler", None)
    if scheduler is not None:
        from provisa.events.app_wiring import wire_event_loop

        # wire_event_loop is best-effort and never raises into its caller.
        await wire_event_loop(scheduler, state=state, log=_log, seed=False)


# -- source password persistence (REQ-1695) ------------------------------------------------

#: REQ-1695: what a source's password is stored under in the ORG vault. ``source_`` is a literal
#: prefix rather than decoration: it makes the name valid under ``secrets_store.validate_name``
#: (which demands a leading letter or underscore) whatever the source id starts with, and it says
#: on the Secrets screen what the value is for.
_SOURCE_SECRET_PREFIX = "source_"
_SOURCE_SECRET_SUFFIX = "_password"


def source_password_secret_name(source_id: str) -> str:  # REQ-1695
    """The org-vault name holding ``source_id``'s password.

    A source id may carry hyphens, dots and slashes; a secret name may carry none of them
    (``secrets_store.NAME``), so every character outside the grammar becomes an underscore. The
    mapping is not injective -- ``a-b`` and ``a.b`` normalize alike -- and does not need to be:
    two sources with ids that differ only in punctuation cannot coexist in one control plane,
    because ``sources.id`` is the primary key and the catalog derivation
    (``source_to_catalog``) already collapses hyphens the same way.
    """
    import re

    from provisa.core.secrets_store import validate_name

    normalized = re.sub(r"[^A-Za-z0-9_]", "_", source_id)
    return validate_name(f"{_SOURCE_SECRET_PREFIX}{normalized}{_SOURCE_SECRET_SUFFIX}")


async def persist_source_password(info: StrawberryInfo, source_id: str, password: str) -> str:
    """Store ``password`` where a credential belongs and return what ``sources.password_ref`` holds.

    Three cases, and no fourth (REQ-1695):

    * empty -- the source needs no password, and the empty string is what the column carries.
    * a value written in the reference grammar (it contains ``${``) -- the operator already said
      where the credential lives, so it is stored VERBATIM. Putting it in the vault would store the
      reference text as if it were a credential and resolve to the reference on the way out.
    * a literal -- the credential itself, which never lands in the control-plane row. It goes into
      the ORG vault (``secrets_store``, encrypted at rest, values unreadable by name) and the row
      gets the ``${secret:NAME}`` that names it.

    The vault is the org's, so a rotation through this door is the same ``put`` a rotation through
    the Secrets screen is: the name is the identity and the new value replaces the old.
    """
    if not password:
        return ""
    if "${" in password:
        return password
    from provisa.api.admin.capabilities import _identity_from_info
    from provisa.api.app import state
    from provisa.core import secrets_store
    from provisa.core.request_context import current_org

    assert state.admin_db is not None, "the platform control plane holds every org's vault"
    identity = _identity_from_info(info)
    name = source_password_secret_name(source_id)
    await secrets_store.put(
        state.admin_db,
        current_org.get() or state.org_id,
        name,
        password,
        owner_id=secrets_store.ORG_OWNER,
        actor=getattr(identity, "user_id", None) if identity is not None else None,
        description=f"password for source {source_id}",
    )
    return f"${{secret:{name}}}"


async def forget_source_password(source_id: str, password_ref: str) -> None:
    """Remove the vault entry a deleted source's ``password_ref`` names (REQ-1695).

    Only the entry THIS module minted: a reference the operator wrote themselves names a secret
    they own for their own reasons, and deleting a source is not permission to delete it. The
    comparison is against the generated name, which is exactly that distinction.
    """
    if password_ref != f"${{secret:{source_password_secret_name(source_id)}}}":
        return
    from provisa.api.app import state
    from provisa.core import secrets_store
    from provisa.core.request_context import current_org

    assert state.admin_db is not None, "the platform control plane holds every org's vault"
    await secrets_store.remove(
        state.admin_db,
        current_org.get() or state.org_id,
        source_password_secret_name(source_id),
        owner_id=secrets_store.ORG_OWNER,
    )
