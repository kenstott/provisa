# Copyright (c) 2026 Kenneth Stott
# Canary: 2e7a4c1f-9b5d-4f8a-8c3e-6d2b4f7a9c1e
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Cypher over HTTP: role and label resolution, and a CALL subquery body's execution (Phase AU,
REQ-345–353). A statement is executed by the one pipeline terminal (``_execute_plan``), whose
routing stage holds the API and graphql_remote fetches. Leaf module (no route handlers).
"""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING, Any


from fastapi import Request

if TYPE_CHECKING:
    from provisa.cypher.label_map import CypherLabelMap  # noqa: F401
    from provisa.api.app import AppState  # noqa: F401
    from provisa.compiler.sql_gen import CompilationContext  # noqa: F401
    from provisa.core.database import Connection  # noqa: F401


from provisa.api.rest.registered_call import (
    _detect_procedure,  # noqa: F401 — re-exported for tests
    _handle_procedure,  # noqa: F401 — re-exported for tests
)

log = logging.getLogger(__name__)


def _resolve_role_id(request: Request, state: AppState) -> str:  # noqa: ARG001
    """The role the caller was authenticated as.

    REQ-486: AuthMiddleware settles the role for every request that reaches a router — from the
    identity's assignments on a secured server, from X-Provisa-Role on an unsecured one — and
    writes it to ``request.state.role``. Reading it is what keeps the graph surfaces under the
    same governance as the rest of the pipeline. Choosing a role out of ``state.roles`` instead
    both escalates (a headerless caller would get whichever role happened to sort first) and
    varies with the seeded system roles, which no caller named."""
    return request.state.role


def _build_label_map(ctx: CompilationContext, role_id: str, state: AppState) -> CypherLabelMap:
    """Build CypherLabelMap with cross-domain traversal nodes for the given role.

    REQ-1620: domain_access is the acting role's own — for a set of held roles, their meta-role's,
    the union of theirs (security/meta_role.py) — so a graph traversal's visibility and its
    SQL-level V001 check agree.

    REQ-1877: the map is a pure function of the registry, so it is built once per schema
    generation and kept (``cypher_plan.kept_label_map``), not rebuilt per request.
    """
    from provisa.api.rest.cypher_plan import kept_label_map
    from provisa.cypher.label_map import CypherLabelMap
    from provisa.security.rights import effective_domain_access_role

    domain_access = effective_domain_access_role(role_id, state.roles)["domain_access"]
    cache = state.schema_build_cache
    return kept_label_map(
        state,
        role_id,
        domain_access=domain_access,
        cross_domain=True,
        business_view=False,
        build=lambda: CypherLabelMap.from_schema(
            ctx,
            domain_access=domain_access,
            all_tables=cache.get("tables"),
            all_relationships=cache.get("relationships"),
            all_column_types=cache.get("column_types"),
            source_catalogs=state.source_catalogs,
        ),
    )


async def _execute_call_body(
    call_body: Any,
    label_map: Any,
    params: dict,
    state: Any,
    ctx: Any,
    role_id: str = "default",
) -> tuple[list[dict], dict]:
    """Full pipeline execution for a single CALL subquery body."""
    from provisa.cypher.translator import cypher_to_sql
    from provisa.cypher.graph_rewriter import apply_graph_rewrites
    from provisa.compiler.sql_rewrite import make_semantic_sql
    from provisa.compiler.directives import NO_CACHE_HINT
    from provisa.pgwire._pipeline import _govern_and_route_compiled

    sql_ast, ordered_params, graph_vars = cypher_to_sql(call_body, label_map, params)
    sql_ast = apply_graph_rewrites(sql_ast, graph_vars, label_map)
    sql_str = sql_ast.sql(dialect="postgres")
    semantic_sql = make_semantic_sql(sql_str, ctx)
    resolved_params = [params.get(name) for name in ordered_params]

    plan = await _govern_and_route_compiled(
        semantic_sql,
        role_id,
        exec_params=resolved_params or None,
        # A CALL body is one fragment of a composed result, never a cache entry of its own.
        cache_hint=NO_CACHE_HINT,
        sdl_joins=False,
    )
    from provisa.pgwire._pipeline import _execute_plan, require_governed_plan

    require_governed_plan(plan)  # REQ-1176: verify before the terminal executes plan SQL
    # The one terminal: residency, the statement on its route (the API stage's fetches already in
    # it), the audit row (REQ-074/REQ-1386).
    result = await _execute_plan(plan, state)
    return [dict(zip(result.column_names, row)) for row in result.rows], graph_vars
