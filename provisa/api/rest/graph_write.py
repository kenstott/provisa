# Copyright (c) 2026 Kenneth Stott
# Canary: 7c2e9a41-5b8d-4f06-a3e1-0d9b6f4c2e58
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The governed write every graph front door runs (REQ-670, REQ-2176; REQ-2135 amended
2026-11-22): Cypher over HTTP and Bolt, and SPARQL Update.

A front door parses its statement into write ASTs (provisa.cypher.write_translator's WriteAST: a
Cypher write by parse_cypher_write, a SPARQL update by provisa.sparql_query.update), and this
runs them through the one write planner, ``written``: its reads of what a write names through the
surface's governed read, and each translated statement through the one pipeline -- its admission
(the write right, the role's write grant and its columns, row rules, REQ-2074), its lowering to the
source's addressing and dialect, its execution, and the steps after a write (cache, views, change
events, sinks, hot copy) at the pipeline's terminal. Each row written is counted on the statement's
counters (provisa.cypher.counters).
"""

# Requirements: REQ-670, REQ-2176, REQ-2135, REQ-2074

from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Any

from fastapi import Request
from fastapi.responses import Response

from provisa.executor.result import QueryResult


async def governed_statement(sql: str, bound: list, role_id: str, state: Any) -> QueryResult:
    """One translated write statement through the one pipeline, admitted for ``role_id``."""
    from provisa.compiler.directives import NO_CACHE_HINT
    from provisa.pgwire._pipeline import _execute_plan, _govern_and_route_compiled

    plan = await _govern_and_route_compiled(
        sql,
        role_id,
        exec_params=bound or None,
        state=state,
        cache_hint=NO_CACHE_HINT,
        sdl_joins=False,
    )
    return await _execute_plan(plan, state)


@dataclass
class GraphWrite:
    """What a run of write ASTs did: its counters, each statement's result, and the last write's
    answer (the rows of its RETURN, if any)."""

    counted: Any
    wrote: list[QueryResult]
    columns: list[str]
    rows: list[list[Any]]

    @property
    def affected(self) -> int:
        return sum(r.rowcount if r.rowcount is not None else len(r.rows) for r in self.wrote)


async def run_graph_writes(
    asts: list[Any],
    label_map: Any,
    role_id: str,
    state: Any,
    provided: dict[str, Any],
    read: Any,
) -> GraphWrite:
    """Run ``asts`` in order through the one write planner for ``role_id``: ``read(cypher,
    params)`` is the surface's governed read, answering (columns, rows); ``provided`` the values
    the statements' parameters are bound to."""
    from provisa.cypher.counters import counting
    from provisa.cypher.write_translator import WriteTranslator, written

    wrote: list[QueryResult] = []

    async def write(sql: str, bound: list) -> tuple[list[str], list[list[Any]], int | None]:
        done = await governed_statement(sql, bound, role_id, state)
        wrote.append(done)
        return done.column_names, [list(row) for row in done.rows], done.rowcount

    translator = WriteTranslator(label_map)
    columns: list[str] = []
    rows: list[list[Any]] = []
    with counting() as counted:
        for ast in asts:
            columns, rows = await written(ast, translator, provided, read, write)
    return GraphWrite(counted, wrote, columns, rows)


class ReadAnswered(Exception):
    """A read a write made was answered by an error: that answer is the write's."""

    def __init__(self, response: Response) -> None:
        super().__init__(response.status_code)
        self.response = response


class CypherReads:
    """A write's reads as /data/cypher answers a read for the request's principal, each answer
    kept: the last is a RETURN's."""

    def __init__(self, request: Request, x_provisa_stats: str | None) -> None:
        self.request = request
        self.x_provisa_stats = x_provisa_stats
        self.answered: list[Response] = []

    async def __call__(self, query: str, params: dict) -> tuple[list[str], list[list[Any]]]:
        from provisa.api.rest.cypher_router import CypherRequest, _answer

        response = await _answer(
            CypherRequest(query=query, params=params), self.request, self.x_provisa_stats
        )
        self.answered.append(response)
        if response.status_code != 200:
            raise ReadAnswered(response)
        content = json.loads(bytes(response.body))
        columns = content["columns"]
        rows = [[r[c] for c in columns] if isinstance(r, dict) else r for r in content["rows"]]
        return columns, rows


def write_refusal(exc: Exception, surface: str) -> Response:
    """The answer to a write the planner or the pipeline refused, in ``surface``'s codes
    (``cypher``, ``sparql``); a refusal the app answers by name is raised on to its handler."""
    from provisa.api.errors import ApiError
    from provisa.api.rest.cypher_router import cypher_error
    from provisa.core.parameter_binding import ParameterBindingRefused
    from provisa.core.read_refusal import ReadRefused
    from provisa.cypher.params import CypherParamError
    from provisa.cypher.write_translator import CypherWriteParseError
    from provisa.gql.translator_types import CypherTranslateError

    if isinstance(exc, ReadAnswered):
        return exc.response
    if isinstance(exc, ParameterBindingRefused):
        # REQ-1986: a relationship joined on a parameter is not written, under its own code.
        return cypher_error(400, exc.code, str(exc), **exc.params)
    if isinstance(exc, (CypherWriteParseError, CypherParamError, CypherTranslateError)):
        return cypher_error(400, f"{surface}.refused", str(exc), reason=str(exc))
    if isinstance(exc, PermissionError):
        return cypher_error(403, f"{surface}.permission_denied", str(exc), reason=str(exc))
    if isinstance(exc, (ApiError, ReadRefused)):
        raise exc  # REQ-1350: a refusal by name is answered by the app's handler for it
    return cypher_error(500, f"{surface}.write_failed", f"Write failed: {exc}", error=str(exc))
