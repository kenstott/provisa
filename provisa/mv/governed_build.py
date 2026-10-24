# Copyright (c) 2026 Kenneth Stott
# Canary: c05399be-4e60-4380-aae3-6742ddd7028a
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The SELECT a build of a SQL-defined view runs (REQ-133, REQ-1912, REQ-1921, REQ-1922).

Every build of a view — the scheduled event-loop compute, a refresh, a bitemporal append — reads
its inputs with the statement this module gives, and only through it.

Where the platform declares regions, a build is its region's own work, governed as the
organisation's administrator with that region as its region attribute
(:mod:`provisa.core.region_admin`; REQ-1921, A VIEW MAY NAME A REGION): the view's SQL as defined
goes through the first stage of the one pipeline (``govern_statement``) as that identity, so each
region's copy holds what an administrator in that region may see. Its inputs are then made
readable exactly as a read's are (``query_residency.ensure_resident``): an input another region
keeps a replica of is read from that replica there, one it reads in place is read in place under
the same governance, and one that cannot be read is refused naming its region — never fetched
here unfiltered. With no platform regions the build is governed the same way, as the
administrator in the one default region: its definition is read as a direct statement reads it,
so a search table it reads is read under its arguments (REQ-1922).
"""

# Requirements: REQ-133, REQ-1912, REQ-1921, REQ-1922, REQ-2108, REQ-2131

from __future__ import annotations

from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from provisa.mv.models import MVDefinition


async def view_build_sql(mv: "MVDefinition", engine: Any) -> str:
    """The engine statement a build of ``mv`` lands from (the landing itself is the system's
    write): its definition through the one pipeline as the administrator, its inputs made readable
    as the administrator's read's are. ``engine`` is the FederationEngine it lands through."""
    from provisa.api.app import state
    from provisa.federation.query_residency import prepare_engine_residency

    from provisa.transpiler.router import Route

    require_built_here(mv)
    plan = await _governed_plan(state, mv)
    if plan.route != Route.DIRECT:
        # Its inputs are made readable as the administrator's read's are, before it runs.
        await prepare_engine_residency(state, plan)
        assert plan.physical_sql is not None  # an ENGINE plan always carries it
    if plan.columns is not None:
        # REQ-2131: a column transform's outputs are computed by its function, not the engine:
        # the build's rows are read and transformed through the one pipeline's terminal, however
        # the definition routes, and the engine lands the view from them. Recomputed at each
        # build, so at each refresh.
        return await _transformed_rows_sql(state, mv, plan, engine)
    if plan.route == Route.DIRECT:
        return await _staged_direct_build(state, mv, plan, engine)
    return plan.physical_sql


async def _typed_columns(mv: "MVDefinition", plan: Any) -> list[tuple[str, str]]:
    shape = await plan.result_shape()
    if shape is None:
        raise ViewNotBuildable(mv.id, "its result columns cannot be typed from the model")
    return [(name, str(declared)) for name, declared in shape]


async def _transformed_shape_sql(mv: "MVDefinition", plan: Any, engine: Any) -> str:
    """A statement with the view's columns, typed, and no rows: its shape, with nothing run."""
    import sqlglot.expressions as exp

    from provisa.core.ir_types import to_physical

    # The engine's SQL dialect as SQLAlchemy names it, as command_localize renders its casts.
    physical = "postgresql" if engine.dialect == "postgres" else engine.dialect
    items = [
        exp.Alias(
            this=exp.Cast(
                this=exp.Null(),
                to=exp.DataType.build(to_physical(ir, physical), dialect=engine.dialect),
            ),
            alias=exp.to_identifier(name, quoted=True),
        )
        for name, ir in await _typed_columns(mv, plan)
    ]
    return exp.select(*items).where(exp.false()).sql(dialect=engine.dialect)


async def _transformed_rows_sql(state: Any, mv: "MVDefinition", plan: Any, engine: Any) -> str:
    """The engine statement that reads ``plan``'s rows, its column transforms applied: the rows
    landed in the org's cache schema for the build, dropped after it."""
    from provisa.api_source.engine_cache import (
        _table_ref,
        cache_location,
        ensure_cache_schema,
        org_cache_schema,
        resolved_cache_catalog,
        schedule_drop,
    )
    from provisa.executor.redirect import RedirectConfig
    from provisa.pgwire._pipeline import _execute_plan

    result = await _execute_plan(plan)
    columns = await _typed_columns(mv, plan)
    rows = [dict(zip(result.column_names, row)) for row in result.rows]
    # Where a statement's own landed rows go -- a code table's among them -- not the view store.
    loc = cache_location(mv.id, resolved_cache_catalog(engine), org_cache_schema(state))
    table = f"mv_columns_{mv.id}"
    with engine.isolated_sync() as conn:
        ensure_cache_schema(conn, loc)
    await engine.land_source_table(schema=loc.schema, table=table, columns=columns, rows=rows)
    # The build that reads it runs now; an hour outlasts it.
    schedule_drop(engine, loc, table, 3600, RedirectConfig.from_env())
    return f"SELECT * FROM {_table_ref(loc, table, engine.dialect)}"


def staged_table(mv: "MVDefinition") -> str:
    """The table a DIRECT build's rows are staged in, beside the view's own table (REQ-2108)."""
    assert mv.target_table is not None  # set by MVDefinition.__post_init__
    return f"{mv.target_table}__direct"


def lineage_sql(mv: "MVDefinition", select_sql: str) -> str:
    """The SQL a build's column lineage is read from: its statement, or for a staged DIRECT build
    (which reads only :func:`staged_table`) the view's definition, which names what it derives
    from (REQ-2108)."""
    if staged_table(mv) not in select_sql:
        return select_sql
    definition = mv.sql or mv.semantic_sql
    assert definition is not None  # a staged build was governed from it
    return definition


async def _staged_direct_build(state: Any, mv: "MVDefinition", plan: Any, engine: Any) -> str:
    """REQ-2108: a build its definition routes DIRECT (one source reads it all) runs whole at that
    source, on any engine kind, so only its result rows come back. They are landed through the
    store's write face (``land_source_table``) into :func:`staged_table` in the view's store, and
    the engine statement every build step runs reads them from there, unchanged whatever the step
    (create, append, bitemporal version, shape probe, size guard)."""
    from provisa.mv.models import stored_table_ref
    from provisa.pgwire._pipeline import _execute_plan

    result = await _execute_plan(plan)
    columns = await _direct_columns(state, mv, plan, result, engine)
    table = staged_table(mv)
    await engine.land_source_table(
        schema=mv.target_schema,
        table=table,
        columns=columns,
        rows=[dict(zip(result.column_names, row)) for row in result.rows],
    )
    ref = stored_table_ref(
        mv.target_catalog, mv.target_schema, table, catalog_qualified=mv.target_catalog_qualified
    )
    return f"SELECT * FROM {ref}"


async def _direct_columns(
    state: Any, mv: "MVDefinition", plan: Any, result: Any, engine: Any
) -> list[tuple[str, str]]:
    """The staged table's (name, IR type) columns: the source driver's own result types where it
    reports them (``plan.dialect`` spells them). A driver that reports none (the SQLAlchemy
    drivers) leaves the shape to the engine, which reads the same definition's shape with LIMIT 0
    and no rows (REQ-2108)."""
    from provisa.core.ir_types import to_ir
    from provisa.federation.execution_auth import SystemAuth, mint_system_token

    if result.column_types is not None:
        return [
            (n, to_ir(t, plan.dialect))
            for n, t in zip(result.column_names, result.column_types, strict=True)
        ]
    shape = await engine.execute_engine(
        f"SELECT * FROM ({await view_shape_sql(mv, engine)}) AS _shape LIMIT 0",
        authorization=SystemAuth(mint_system_token(), reason=f"mv_direct_shape:{mv.id}"),
    )
    if shape.column_types is None:
        raise ViewNotBuildable(mv.id, "neither its source nor the engine reports its column types")
    return [
        (n, to_ir(t, engine.dialect))
        for n, t in zip(shape.column_names, shape.column_types, strict=True)
    ]


async def view_shape_sql(mv: "MVDefinition", engine: Any) -> str:
    """The engine statement of ``mv``'s definition, governed as :func:`view_build_sql`'s is: what
    reading its shape (``LIMIT 0``) runs, never its rows (REQ-2108: a DIRECT build would read and
    stage them; REQ-2131: a column transform's would run its function)."""
    from provisa.api.app import state
    from provisa.federation.query_residency import prepare_engine_residency

    require_built_here(mv)
    plan = await _governed_plan(state, mv, route_hint="engine")
    if plan.columns is not None:
        return await _transformed_shape_sql(mv, plan, engine)
    await prepare_engine_residency(state, plan)
    assert plan.physical_sql is not None  # an ENGINE plan always carries it
    return plan.physical_sql


async def view_build_rows(mv: "MVDefinition", engine: Any) -> list[dict]:
    """The rows a build of ``mv`` holds, read with :func:`view_build_sql`'s governance through
    the one pipeline's terminal."""
    from provisa.api.app import state
    from provisa.pgwire._pipeline import _execute_plan

    require_built_here(mv)
    result = await _execute_plan(await _governed_plan(state, mv))
    return [dict(zip(result.column_names, row)) for row in result.rows]


class ViewNotBuildable(RuntimeError):
    """A view whose build cannot be governed as its region's administrator (REQ-1921/1922)."""

    code = "mv.view_not_buildable_in_region"

    def __init__(self, view: str, why: str) -> None:
        self.view = view
        self.params = {"view": view}
        super().__init__(f"view {view!r} cannot be built in this region: {why}")


def require_built_here(mv: "MVDefinition") -> None:
    """Refuse a build of ``mv`` here when it names another region, which alone builds it
    (REQ-1921, A VIEW MAY NAME A REGION)."""
    from provisa.federation.replica_converge import builds_here

    if not builds_here(mv.region):
        raise ViewNotBuildable(
            mv.id, f"it names region {mv.region!r}, which alone builds and refreshes it"
        )


async def _governed_plan(state: Any, mv: "MVDefinition", *, route_hint: str | None = None) -> Any:
    """The one pipeline's plan for ``mv``'s definition, as the region's administrator, routed as
    a query with the same definition is (REQ-2108): DIRECT when one source reads it all, on the
    engine otherwise. ``route_hint`` "engine" asks the engine for it (its shape, see
    :func:`_direct_columns`)."""
    from provisa.core import region_admin
    from provisa.pgwire._pipeline import _govern_and_route
    from provisa.transpiler.router import Route

    if mv.semantic_sql is not None:
        semantic = mv.semantic_sql
    elif mv.join_pattern is not None:
        semantic = await _join_semantic_sql(state, mv)
    else:
        raise ViewNotBuildable(mv.id, "it has no definition in the model's terms to govern")
    plan = await _govern_and_route(
        semantic,
        region_admin.ROLE,
        session_vars=region_admin.session_vars(state),
        route_hint=route_hint,
        building=mv.id,  # the view answers none of its own build
    )
    if plan.route not in (Route.ENGINE, Route.DIRECT):
        raise ViewNotBuildable(
            mv.id, f"its definition routes {plan.route}, not to its source or the engine"
        )
    if plan.exec_params:
        raise ViewNotBuildable(mv.id, "its definition carries bound parameters")
    return plan


async def _join_semantic_sql(state: Any, mv: "MVDefinition") -> str:
    """A join-pattern view's definition in the model's terms: the same SELECT a physical build
    runs (``refresh.join_pattern_sql``), naming each table it was bound to by its name in the
    model and taking its columns from the registry."""
    from provisa.compiler.sql_rewrite import semantic_ref
    from provisa.federation.registry_view import registered_tables
    from provisa.mv.refresh import join_pattern_sql

    bound = dict(zip(mv.source_tables, mv.inputs, strict=True))
    registered = {
        (t.source_id, t.schema_name, t.table_name): t for t in await registered_tables(state)
    }
    metas = {
        (m.source_id, m.schema_name, m.original_table_name or m.table_name): m
        for m in state.view_context.tables.values()
    }

    def _identity(table: str) -> tuple[str, str, str]:
        identity = bound[table]
        return identity.source_id, identity.schema_name, identity.table_name

    async def _ref(table: str) -> str:
        source_id, schema, name = _identity(table)
        meta = metas.get((source_id, schema, name))
        if meta is None:
            raise ViewNotBuildable(mv.id, f"its table {table!r} has no name in the model")
        return semantic_ref(meta)

    async def _columns(table: str) -> list[str]:
        reg = registered.get(_identity(table))
        if reg is None:
            raise ViewNotBuildable(mv.id, f"its table {table!r} is not registered")
        return [c.name for c in reg.columns]

    return await join_pattern_sql(mv, _ref, _columns)
