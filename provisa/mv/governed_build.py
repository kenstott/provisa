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
here unfiltered. With no platform regions the build reads its inputs as before.
"""

# Requirements: REQ-133, REQ-1912, REQ-1921, REQ-1922

from __future__ import annotations

from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from provisa.mv.models import MVDefinition


async def view_build_sql(mv: "MVDefinition", engine: Any) -> str:
    """The engine statement a build of SQL-defined ``mv`` lands from (the landing itself is the
    system's write). ``engine`` is the FederationEngine; None returns the view's lowered SQL
    unaddressed (no engine to address it for)."""
    from provisa.api.app import state
    from provisa.core import region_admin

    if engine is not None and region_admin.governs_region_work():
        from provisa.federation.query_residency import prepare_engine_residency

        plan = await _governed_plan(state, mv)
        # Its inputs are made readable as the administrator's read's are, before it runs.
        await prepare_engine_residency(state, plan)
        assert plan.physical_sql is not None  # an ENGINE plan always carries it
        return plan.physical_sql
    assert mv.sql is not None
    return _lowered(state, mv.sql, engine)


async def view_build_rows(mv: "MVDefinition", engine: Any) -> list[dict]:
    """The rows a build of SQL-defined ``mv`` holds, read with :func:`view_build_sql`'s
    governance: through the one pipeline's terminal where the platform declares regions."""
    from provisa.api.app import state
    from provisa.core import region_admin

    if region_admin.governs_region_work():
        from provisa.pgwire._pipeline import _execute_plan

        result = await _execute_plan(await _governed_plan(state, mv))
    else:
        from provisa.federation.execution_auth import system_auth

        result = await engine.execute_engine(
            await view_build_sql(mv, engine), authorization=system_auth("event signal")
        )
    return [dict(zip(result.column_names, row)) for row in result.rows]


def _expanded(state: Any, physical: str) -> str:
    """``physical`` (a view's SQL lowered to the engine's catalogs) with each view it reads
    expanded. REQ-133/REQ-135: such a view (pets -> A -> B) is lowered to the __derived__
    sentinel, which is no engine catalog; each is read by the one view rule with nothing narrowed:
    its stored rows when fresh, else its own SQL."""
    from provisa.compiler.view_expand import expand_view_refs
    from provisa.mv.view_read import unnarrowed_view_bodies

    return expand_view_refs(physical, unnarrowed_view_bodies(physical, state.view_sql_map, state))


def _lowered(state: Any, physical: str, engine: Any) -> str:
    """``physical`` as the engine reads it in a deployment with no regions."""
    sql = _expanded(state, physical)
    if engine is None:
        return sql
    # REQ-1912: a table the view reads that is served from its replica is addressed there — the
    # same rewrite every statement bound for the engine gets.
    sql = engine.address_replicas(sql)
    # A view's ``current_setting('provisa.<var>')`` is resolved as every statement bound for the
    # engine is (the engine has no such function); any name the build is not given is NULL.
    from provisa.core.request_context import session_vars_for
    from provisa.pgwire._pipeline import _resolve_session_settings

    return _resolve_session_settings(sql, session_vars_for(None), engine.dialect)


class ViewNotBuildable(RuntimeError):
    """A view whose build cannot be governed as its region's administrator (REQ-1921/1922)."""

    code = "mv.view_not_buildable_in_region"

    def __init__(self, view: str, why: str) -> None:
        self.view = view
        self.params = {"view": view}
        super().__init__(f"view {view!r} cannot be built in this region: {why}")


async def _governed_plan(state: Any, mv: "MVDefinition") -> Any:
    """The one pipeline's plan for ``mv``'s definition, as the region's administrator, routed to
    the engine (what it reads lands through the engine into the view's storage)."""
    from provisa.core import region_admin
    from provisa.pgwire._pipeline import _govern_and_route
    from provisa.transpiler.router import Route

    if mv.semantic_sql is None:
        raise ViewNotBuildable(mv.id, "it has no definition in the model's terms to govern")
    plan = await _govern_and_route(
        mv.semantic_sql,
        region_admin.ROLE,
        session_vars=region_admin.session_vars(state),
        route_hint="engine",
    )
    if plan.route != Route.ENGINE:
        raise ViewNotBuildable(mv.id, f"its definition routes {plan.route}, not to the engine")
    if plan.exec_params:
        raise ViewNotBuildable(mv.id, "its definition carries bound parameters")
    return plan
