# Copyright (c) 2026 Kenneth Stott
# Canary: 6a539c25-94a0-403e-84da-ca0217fbe84c
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""CTAS — one-time physical data move as ``CREATE TABLE schema.table AS SELECT ...`` (REQ-996..1002).

A CTAS physically creates and populates a table in a WRITABLE source. It does NOT register a Provisa
model entity — the new table is invisible to the federated model until separately introspected.

The cluster is split into pure, individually-testable contracts and one orchestrator:

- ``parse_ctas``        — recognize a CTAS statement (REQ-996), read optional catalog off the grammar.
- ``resolve_ctas``      — validate + place the target (REQ-998 uniqueness, REQ-999 catalog-names-source,
                          REQ-1000 single-writable-or-catalog-or-reject). Rejects loudly; never guesses.
- ``decide_ctas_route`` — same engine → zero-copy native CTAS; cross-engine → engine SELECT + landed
                          write (REQ-997).
- ``coerce_result_columns`` — cross-engine result schema → target DDL types via the IR hub (REQ-1002).
- ``execute_ctas``      — orchestrate the chosen route; the cross-engine path lands transactionally
                          (create-temp → load → atomic swap, REQ-1001) through ``store_writer``.
"""

from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
from typing import TYPE_CHECKING, Protocol

from provisa.core.ir_types import to_ir

if TYPE_CHECKING:
    from collections.abc import Awaitable, Callable, Sequence

    from provisa.executor.result import QueryResult


class CtasError(ValueError):
    """A CTAS was rejected — the base for every placement/validation failure below."""


class CtasNameCollision(CtasError):
    """REQ-998: schema.table already names an asset model-wide."""


class CtasCatalogMismatch(CtasError):
    """REQ-999: an explicit catalog contradicts the source schema.table resolves to."""


class CtasAmbiguousPlacement(CtasError):
    """REQ-1000: schema maps to >1 writable source and no catalog disambiguates."""


class CtasNoWritableTarget(CtasError):
    """REQ-1000: schema maps to no writable source."""


@dataclass(frozen=True)
class CtasStatement:
    """A parsed CTAS. ``catalog`` (when present) names the SOURCE, never a resolution key (REQ-999)."""

    catalog: str | None
    schema: str
    table: str
    select_sql: str


@dataclass(frozen=True)
class WritableSource:
    """A source that can receive a CTAS target, plus the names its catalog may be written as."""

    source_id: str
    source_type: str
    # Accepted spellings for the optional grammar catalog (REQ-999): source id, engine catalog, type.
    catalog: str = ""

    @property
    def names(self) -> frozenset[str]:
        return frozenset(n for n in (self.source_id, self.catalog, self.source_type) if n)


@dataclass(frozen=True)
class ResolvedTarget:
    """The validated, placed CTAS target (REQ-998/999/1000 all satisfied)."""

    schema: str
    table: str
    source: WritableSource


class CtasRoute(str, Enum):
    ZERO_COPY = "zero_copy"  # same engine: engine-native CTAS pushdown (REQ-997)
    CROSS_ENGINE = "cross_engine"  # engine runs SELECT, rows land in the target (REQ-997)


def parse_ctas(sql: str) -> CtasStatement | None:
    """Recognize ``CREATE TABLE [catalog.]schema.table AS SELECT ...`` (REQ-996), else return None.

    Standard three-part ``catalog.schema.table`` so SQLGlot parses it natively; the optional catalog
    is read off ``exp.Table.catalog`` (REQ-999). A plain ``CREATE TABLE (cols)`` (no ``AS SELECT``) is
    NOT a CTAS and yields None so the caller's normal DDL path handles it.
    """
    import sqlglot
    import sqlglot.expressions as exp

    node = sqlglot.parse_one(sql, read="postgres")
    if not isinstance(node, exp.Create) or (node.kind or "").upper() != "TABLE":
        return None
    select = node.expression
    if select is None:  # CREATE TABLE with column defs, not a CTAS
        return None
    table_node = node.this
    if isinstance(table_node, exp.Schema):
        table_node = table_node.this
    if not isinstance(table_node, exp.Table):
        return None
    schema = table_node.db
    table = table_node.name
    if not schema:
        raise CtasError(
            f"CTAS target {table!r} must be schema-qualified (schema.table) — schema.table is the "
            "global identifier (REQ-998)"
        )
    return CtasStatement(
        catalog=table_node.catalog or None,
        schema=schema,
        table=table,
        select_sql=select.sql(dialect="postgres"),
    )


def resolve_ctas(
    stmt: CtasStatement,
    existing_assets: frozenset[str] | set[str],
    schema_sources: dict[str, list[WritableSource]],
) -> ResolvedTarget:
    """Validate and place a CTAS target (REQ-998/999/1000). Rejects loudly; never silently picks.

    - REQ-998: reject if ``schema.table`` already names a model asset.
    - REQ-1000: the schema's writable source is the placement; >1 needs a catalog, 0 is rejected.
    - REQ-999: an explicit catalog must NAME the resolved source — match proceeds, mismatch errors.
    """
    key = f"{stmt.schema}.{stmt.table}"
    if key in existing_assets:
        raise CtasNameCollision(
            f"{key!r} already exists in model — schema.table must be globally unique (REQ-998)"
        )
    candidates = schema_sources.get(stmt.schema, [])
    if not candidates:
        raise CtasNoWritableTarget(
            f"schema {stmt.schema!r} maps to no writable source — cannot place CTAS target (REQ-1000)"
        )
    if stmt.catalog:
        matched = [s for s in candidates if stmt.catalog in s.names]
        if not matched:
            available = sorted({n for s in candidates for n in s.names})
            raise CtasCatalogMismatch(
                f"catalog {stmt.catalog!r} does not match the writable source(s) for schema "
                f"{stmt.schema!r} (available: {available}) — catalog names the source (REQ-999)"
            )
        if len(matched) > 1:
            raise CtasAmbiguousPlacement(
                f"catalog {stmt.catalog!r} matches multiple writable sources for schema "
                f"{stmt.schema!r}: {[s.source_id for s in matched]} (REQ-1000)"
            )
        return ResolvedTarget(stmt.schema, stmt.table, matched[0])
    if len(candidates) > 1:
        raise CtasAmbiguousPlacement(
            f"schema {stmt.schema!r} maps to multiple writable sources "
            f"{[s.source_id for s in candidates]}; use catalog to disambiguate (REQ-1000)"
        )
    return ResolvedTarget(stmt.schema, stmt.table, candidates[0])


def decide_ctas_route(select_engine_key: str | None, target_engine_key: str) -> CtasRoute:
    """REQ-997: same engine → zero-copy native CTAS; otherwise engine runs SELECT, rows land.

    Keys are opaque engine identities. A cross-source or federated SELECT (``select_engine_key`` None
    or differing) is never treated as same-engine — the safe default is the cross-engine land path.
    """
    if select_engine_key is not None and select_engine_key == target_engine_key:
        return CtasRoute.ZERO_COPY
    return CtasRoute.CROSS_ENGINE


def coerce_result_columns(
    column_names: Sequence[str],
    column_types: Sequence[str] | None,
    source_platform: str | None,
) -> list[tuple[str, str]]:
    """REQ-1002: map the cross-engine SELECT result schema to canonical IR target-DDL types.

    Complex engine types (ROW/array/map) collapse to their IR name and variable-scale DECIMAL narrows
    to ``numeric`` through the IR hub (``to_ir``); ``ensure_table`` then renders per target dialect.
    Missing result types are a real gap — raise rather than default a column to text.
    """
    if column_types is None:
        raise CtasError(
            "cross-engine CTAS requires result column types to build target DDL (REQ-1002)"
        )
    if len(column_types) != len(column_names):
        raise CtasError(
            f"result schema mismatch: {len(column_names)} names vs {len(column_types)} types"
        )
    return [(name, to_ir(t, source_platform)) for name, t in zip(column_names, column_types)]


class _LandFn(Protocol):
    async def __call__(
        self, *, schema: str, table: str, columns: list[tuple[str, str]], rows: list[dict]
    ) -> str: ...


async def execute_ctas(
    stmt: CtasStatement,
    resolved: ResolvedTarget,
    route: CtasRoute,
    *,
    run_engine_ctas: Callable[[str], Awaitable[int]],
    run_select: Callable[[], Awaitable[QueryResult]],
    land: _LandFn,
    source_platform: str | None,
) -> int:
    """Run the CTAS on its chosen route (REQ-996). Returns the affected row count (-1 = unknown).

    ZERO_COPY: hand a native ``CREATE TABLE schema.table AS <select>`` to the source engine — the
    engine both reads and writes, zero-copy (REQ-997). CROSS_ENGINE: the engine runs the governed
    SELECT, the result schema is coerced (REQ-1002), and rows land transactionally in the target
    (REQ-1001) via ``land``.
    """
    if route is CtasRoute.ZERO_COPY:
        ctas_sql = f"CREATE TABLE {resolved.schema}.{resolved.table} AS {stmt.select_sql}"
        return await run_engine_ctas(ctas_sql)
    result = await run_select()
    columns = coerce_result_columns(result.column_names, result.column_types, source_platform)
    rows = [dict(zip(result.column_names, r)) for r in result.rows]
    await land(schema=resolved.schema, table=resolved.table, columns=columns, rows=rows)
    return len(rows)


# --- pgwire adapter: build the live model view from app state and orchestrate (REQ-996..1002) ------


def _build_model_view(ctx, engine) -> tuple[set[str], dict[str, list[WritableSource]]]:
    """Derive the global asset set (REQ-998) and schema→writable-source map (REQ-1000) from the
    role's compilation context. A source is writable iff ``resolve_write_path`` finds a route."""
    from provisa.executor.writable import resolve_write_path

    existing: set[str] = set()
    schema_sources: dict[str, list[WritableSource]] = {}
    seen: dict[str, set[str]] = {}
    for meta in ctx.tables.values():
        existing.add(f"{meta.schema_name}.{meta.table_name}")
        if resolve_write_path(meta.source_type, engine) is None:
            continue
        if meta.source_id in seen.get(meta.schema_name, set()):
            continue
        seen.setdefault(meta.schema_name, set()).add(meta.source_id)
        schema_sources.setdefault(meta.schema_name, []).append(
            WritableSource(
                source_id=meta.source_id,
                source_type=meta.source_type,
                catalog=meta.catalog_name or meta.source_id,
            )
        )
    return existing, schema_sources


def _source_sqlalchemy_dsn(state, source_id: str) -> str:
    """A SQLAlchemy DSN for a relational target source, built from its config (REQ-997 land face).

    Raises loudly on a non-relational target — the landing write face is SQLAlchemy-only, so a
    warehouse/lake target with no relational DSN is a real gap, never silently substituted."""
    from provisa.core.secrets import resolve_secrets

    src = next((s for s in state.config.sources if s.id == source_id), None)
    if src is None:
        raise CtasError(f"no config for target source {source_id!r}")
    scheme = src.type.value
    if scheme == "sqlite":
        return f"sqlite:///{src.database}"
    if scheme not in ("postgresql", "mysql", "mariadb"):
        raise CtasError(
            f"cross-engine CTAS land into source type {scheme!r} is unsupported — the landing write "
            f"face is relational (REQ-997)"
        )
    host = resolve_secrets(src.host) if src.host else "localhost"
    pw = resolve_secrets(src.password) if src.password else ""
    return f"{scheme}://{src.username}:{pw}@{host}:{src.port}/{src.database}"


async def _select_source_ids(select_sql: str, role_id: str, state) -> set[str]:
    """The source ids the CTAS SELECT reads (for same-engine routing, REQ-997)."""
    from provisa.compiler.rls import RLSContext
    from provisa.compiler.stage2 import (
        apply_governance,
        build_governance_context,
        extract_sources,
    )

    ctx = state.contexts[role_id]
    gov_ctx = build_governance_context(
        role_id,
        state.rls_contexts.get(role_id, RLSContext.empty()),
        state.masking_rules,
        ctx,
        getattr(state, "tables", []),
        role=state.roles.get(role_id),
    )
    return extract_sources(apply_governance(select_sql, gov_ctx), gov_ctx, ctx)


async def run_ctas(sql: str, role_id: str) -> str | None:
    """pgwire entry: execute a CTAS end-to-end, or return None when ``sql`` is not a CTAS.

    Governance is preserved — the SELECT side always runs through the governed pipeline. Requires the
    role's ``ddl`` capability (a CTAS both creates and writes). Returns the PG ``SELECT n`` tag."""
    stmt = parse_ctas(sql)
    if stmt is None:
        return None

    from provisa.api.app import state
    from provisa.federation import store_writer
    from provisa.pgwire._pipeline import _govern_and_route, execute_pgwire_sql

    if role_id not in state.contexts:
        raise PermissionError(f"No schema for role {role_id!r}")
    role = state.roles.get(role_id)
    if role is None:
        raise PermissionError(f"Unknown role {role_id!r}")
    if "ddl" not in (role.get("capabilities") or []):
        raise PermissionError(f"Role {role_id!r} lacks 'ddl' capability")

    engine = state.federation_engine
    existing, schema_sources = _build_model_view(state.contexts[role_id], engine)
    resolved = resolve_ctas(stmt, existing, schema_sources)

    select_sources = await _select_source_ids(stmt.select_sql, role_id, state)
    select_key = next(iter(select_sources)) if len(select_sources) == 1 else None
    route = decide_ctas_route(select_key, resolved.source.source_id)

    async def _run_engine_ctas(ctas_select_sql: str) -> int:
        # Same-engine: govern the SELECT, then push a native CTAS around the physical SELECT.
        from provisa.pgwire._pipeline import require_governed_plan

        plan = await _govern_and_route(stmt.select_sql, role_id)
        require_governed_plan(plan)  # REQ-1176: verify at the last moment, before the engine executes
        physical_select = plan.physical_sql if plan.physical_sql is not None else plan.sql
        ddl = f"CREATE TABLE {resolved.schema}.{resolved.table} AS {physical_select}"
        if plan.physical_sql is not None:
            await engine.execute_engine(ddl)
        else:
            await engine.execute_native(state.source_pools, plan.source_id, ddl, plan.exec_params)
        return -1

    async def _run_select():
        return await execute_pgwire_sql(stmt.select_sql, role_id)

    async def _land(*, schema: str, table: str, columns, rows) -> str:
        dsn = _source_sqlalchemy_dsn(state, resolved.source.source_id)
        return await store_writer.land_ctas(
            dsn, schema=schema, table=table, columns=columns, rows=rows
        )

    n = await execute_ctas(
        stmt,
        resolved,
        route,
        run_engine_ctas=_run_engine_ctas,
        run_select=_run_select,
        land=_land,
        source_platform=getattr(engine, "dialect", None),
    )
    return f"SELECT {n if n >= 0 else 0}"
