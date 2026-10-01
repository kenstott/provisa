# Copyright (c) 2026 Kenneth Stott
# Canary: fb2d704f-ac2a-4431-8e49-8e58cbb1ce28
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.
"""Renders a ``RequestSpec`` as what each transport sends (REQ-1911).

Names come from the setup contract (the table's and each column's per-transport spelling); the
cache opt-in text per transport is the one the optimistic test has always sent.
"""

from __future__ import annotations

import json

import contract_model
import setup_contract as sc
from queries import Query
from request_mix import RequestSpec

_BASE_DESCRIPTION = "One row, one column, no predicate — the per-transport overhead floor"
_SQL_HINT = "-- @provisa cache=true"
_CYPHER_HINT = "// @provisa cache=true"
_GRAPHQL_HINT = "@cached"
_GRPC_CACHE_METADATA = ("x-provisa-cache", "true")


def _sql_literal(value: str | int | float) -> str:
    if isinstance(value, str):
        return "'" + value.replace("'", "''") + "'"
    return str(value)


def _quoted_literal(value: str | int | float) -> str:
    """A literal in the GraphQL/Cypher double-quoted string form."""
    return json.dumps(value) if isinstance(value, str) else str(value)


class Renderer:
    """Renders specs against one contract; each distinct spec is rendered once. A language the
    table has no spelling for renders as None (no transport that needs it is ever sent the
    table: the contract refuses that)."""

    def __init__(self, setup: contract_model.Setup) -> None:
        self._tables = {(sid, t.name): t for sid, s in setup.sources.items() for t in s.tables}
        self._memo: dict[RequestSpec, Query] = {}

    def render(self, spec: RequestSpec) -> Query:
        query = self._memo.get(spec)
        if query is None:
            query = self._memo[spec] = self._render(spec)
        return query

    def _render(self, spec: RequestSpec) -> Query:
        table = self._tables[(spec.source, spec.table)]
        cols = [table.column(c) for c in spec.columns]
        filters = [(table.column(name), value) for name, value in spec.filters]
        # a request that joins, or carries a route hint, is only expressible where the language
        # can spell it (gRPC, REST and JSON:API have no join and no route hint)
        joined = bool(spec.joins) or spec.route is not None
        # a request that joins is only expressible where the language can spell every step
        graphql_ok = all(j.forward and j.graphql_field for j in spec.joins)
        cypher_ok = all(j.forward and j.cypher_rel for j in spec.joins)
        return Query(
            id="optimistic",
            category="optimistic",
            description=_BASE_DESCRIPTION,
            sql=self._sql(spec, table, cols, filters),
            cypher=(
                self._cypher(spec, table, cols, filters)
                if table.has("cypher") and cypher_ok
                else None
            ),
            graphql=(
                self._graphql(spec, table, cols, filters)
                if table.has("graphql") and graphql_ok
                else None
            ),
            grpc=self._grpc(spec, table, cols, filters)
            if table.has("grpc") and not joined
            else None,
            rest=self._rest(spec, table, cols, filters)
            if table.has("rest") and not joined
            else None,
            jsonapi=(
                self._jsonapi(spec, table, cols, filters)
                if table.has("jsonapi") and not joined
                else None
            ),
        )

    def _members(
        self, spec: RequestSpec, table: contract_model.Table
    ) -> list[contract_model.Table]:
        """The request's tables: its own, then each join's child."""
        return [table] + [self._tables[(j.child_source, j.child_table)] for j in spec.joins]

    @staticmethod
    def _comment_hints(spec: RequestSpec, cache_line: str, prefix: str) -> str:
        """The request's hint comment lines: the cache opt-in, then the route."""
        lines = [cache_line] if spec.cached else []
        if spec.route:
            lines.append(f"{prefix} route={spec.route}")
        return "".join(f"{line}\n" for line in lines)

    def _sql(
        self, spec: RequestSpec, table: contract_model.Table, cols: list, filters: list
    ) -> str:
        hint = self._comment_hints(spec, _SQL_HINT, "-- @provisa")
        if not spec.joins:
            where = (
                " WHERE "
                + " AND ".join(f"{c.names['sql']} = {_sql_literal(v)}" for c, v in filters)
                if filters
                else ""
            )
            return (
                f"{hint}SELECT {', '.join(c.names['sql'] for c in cols)} FROM {table.sql}"
                f"{where} LIMIT {spec.rows}"
            )
        members = self._members(spec, table)
        select = [f"t0.{c.names['sql']}" for c in cols] + [
            f"t{k}.{m.column(m.base_column).names['sql']} AS {m.name}_{m.base_column}"
            for k, m in enumerate(members[1:], 1)
        ]
        joins = "".join(
            f" JOIN {members[k].sql} t{k} ON t{k}.{members[k].column(j.child_column).names['sql']}"
            f" = t{j.parent}.{members[j.parent].column(j.parent_column).names['sql']}"
            for k, j in enumerate(spec.joins, 1)
        )
        where = (
            " WHERE " + " AND ".join(f"t0.{c.names['sql']} = {_sql_literal(v)}" for c, v in filters)
            if filters
            else ""
        )
        return (
            f"{hint}SELECT {', '.join(select)} FROM {table.sql} t0{joins}{where} LIMIT {spec.rows}"
        )

    def _cypher(
        self, spec: RequestSpec, table: contract_model.Table, cols: list, filters: list
    ) -> str:
        hint = self._comment_hints(spec, _CYPHER_HINT, "// @provisa")
        var = table.cypher_var
        where = (
            "WHERE "
            + " AND ".join(f"{var}.{c.names['cypher']} = {_quoted_literal(v)}" for c, v in filters)
            + " "
            if filters
            else ""
        )
        returns = [f"{var}.{c.names['cypher']} AS {c.name}" for c in cols]
        patterns = [f"({var}:{table.cypher_label})"]
        if spec.joins:
            members = self._members(spec, table)
            names = [var] + [f"j{k}" for k in range(1, len(members))]
            patterns = []
            for k, j in enumerate(spec.joins, 1):
                child = members[k]
                edge = f"-[:{j.cypher_rel}]->({names[k]}:{child.cypher_label})"
                if j.parent == 0 and not patterns:
                    patterns.append(f"({var}:{table.cypher_label}){edge}")
                else:
                    patterns.append(f"({names[j.parent]}){edge}")
                base = child.column(child.base_column)
                returns.append(f"{names[k]}.{base.names['cypher']} AS {child.name}_{base.name}")
        return f"{hint}MATCH {', '.join(patterns)} {where}RETURN {', '.join(returns)} LIMIT {spec.rows}"

    def _graphql(
        self, spec: RequestSpec, table: contract_model.Table, cols: list, filters: list
    ) -> str:
        hint = (f" {_GRAPHQL_HINT}" if spec.cached else "") + (
            f" @route(engine: {spec.route.upper()})" if spec.route else ""
        )
        args = f"limit: {spec.rows}"
        if filters:
            where = ", ".join(
                f"{c.names['graphql']}: {{eq: {_quoted_literal(v)}}}" for c, v in filters
            )
            args += f", where: {{{where}}}"
        members = self._members(spec, table)

        def selection(index: int) -> str:
            own = (
                [c.names["graphql"] for c in cols]
                if index == 0
                else [members[index].column(members[index].base_column).names["graphql"]]
            )
            nested = [
                f"{j.graphql_field} {{ {selection(k)} }}"
                for k, j in enumerate(spec.joins, 1)
                if j.parent == index
            ]
            return " ".join(own + nested)

        return f"query{hint} {{ {table.graphql_field}({args}) {{ {selection(0)} }} }}"

    @staticmethod
    def _grpc(spec: RequestSpec, table: contract_model.Table, cols: list, filters: list) -> dict:
        out: dict = {
            "mode": "scan",
            "type_name": table.grpc_type_name,
            "limit": spec.rows,
            "read_mask": [c.names["grpc"] for c in cols],
            "metadata": [_GRPC_CACHE_METADATA] if spec.cached else [],
        }
        if filters:
            out["filter"] = {c.names["grpc"]: v for c, v in filters}
        return out

    @staticmethod
    def _rest(spec: RequestSpec, table: contract_model.Table, cols: list, filters: list) -> dict:
        params: dict = {"limit": spec.rows, "fields": ",".join(c.names["rest"] for c in cols)}
        if filters:
            params["filter"] = json.dumps(
                [{"field": c.names["rest"], "comparator": "eq", "value": v} for c, v in filters]
            )
        return {"path": table.rest_path, "params": params}

    @staticmethod
    def _jsonapi(spec: RequestSpec, table: contract_model.Table, cols: list, filters: list) -> dict:
        params: dict = {
            "page[size]": spec.rows,
            f"fields[{table.jsonapi_type}]": ",".join(c.names["jsonapi"] for c in cols),
        }
        for c, v in filters:
            params[f"filter[{c.names['jsonapi']}]"] = v
        return {"path": table.jsonapi_path, "params": params}


def build_query(setup: contract_model.Setup, transport: str, *, cached: bool) -> Query:
    """The request when no knob applies, on ``transport``, with or without the cache opt-in."""
    source, table = sc.base_table(setup, transport)
    spec = RequestSpec(source, table.name, (table.base_column,), setup.base["rows"], cached)
    return Renderer(setup).render(spec)
