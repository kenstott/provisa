# Copyright (c) 2026 Kenneth Stott
# Canary: 3d0f7c52-8a41-4f6e-b7d9-52c1e9a4b6f3
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Fit a remote GraphQL table to what the source's credential may read (REQ-1923).

A remote may refuse a field for the credential it was given -- GitHub answers a query that
selects a field outside the token's scopes with an ``INSUFFICIENT_SCOPES`` error naming where
in the query the field sits, before it runs anything. A table that selects such a field can
never be read with that credential. When a table is registered, its query is offered to the
remote once: each field the remote refuses for the credential is taken out of the table and
reported to the caller, and the table is registered with what remains.

Only the error types a source declares as "refused for this credential" are acted on. Any other
error is the remote's answer to the probe's placeholder arguments and says nothing about the
table.
"""

# Requirements: REQ-1923
from __future__ import annotations

import httpx
from graphql import (
    DocumentNode,
    FieldNode,
    OperationDefinitionNode,
    SelectionSetNode,
    parse,
    print_ast,
)

from provisa.graphql_remote.executor import _safe_json, column_selection, table_query
from provisa.graphql_remote.introspect import _build_headers

# A remote that reports refused fields a few at a time is asked again until it reports none.
_MAX_ROUNDS = 50

_PLACEHOLDERS = {"String": "x", "ID": "x", "Int": 0, "Float": 0.0, "Boolean": False}


def _placeholder_variables(required_args: list[dict]) -> dict | None:
    """A value of the right type for each required argument, or None when one is of a type no
    placeholder exists for (an enum or input object) -- such a table is not probed."""
    variables = {}
    for arg in required_args:
        gql_type = arg["gql_type"].rstrip("!")
        if gql_type not in _PLACEHOLDERS:
            return None
        variables[arg["name"]] = _PLACEHOLDERS[gql_type]
    return variables


def _response_key(field: FieldNode) -> str:
    return field.alias.value if field.alias else field.name.value


def _prune(selection_set: SelectionSetNode, refused: set[tuple[int, int]], dropped: list) -> None:
    """Remove every field starting at a refused (line, column), and any field left selecting
    nothing because of it."""
    kept = []
    for sel in selection_set.selections:
        token = sel.loc.start_token if sel.loc else None
        if token is not None and (token.line, token.column) in refused:
            dropped.append(sel)
            continue
        sub = getattr(sel, "selection_set", None)
        if sub is not None:
            _prune(sub, refused, dropped)
            if not sub.selections:
                continue
        kept.append(sel)
    selection_set.selections = tuple(kept)


def _operation(document: DocumentNode) -> OperationDefinitionNode:
    operation = document.definitions[0]
    assert isinstance(operation, OperationDefinitionNode)
    return operation


def _row_selection(document: DocumentNode, table: dict) -> SelectionSetNode | None:
    """The selection set holding the table's columns: under the root field, then down the
    table's ``rows_path`` for a connection. None when pruning took the root field or the
    connection itself -- the credential may not read the table at all."""
    rows = _operation(document).selection_set
    for step in [table["field_name"], *(table.get("rows_path") or [])]:
        field = next(
            (s for s in rows.selections if isinstance(s, FieldNode) and s.name.value == step),
            None,
        )
        if field is None or field.selection_set is None:
            return None
        rows = field.selection_set
    return rows


def _probe_query(table: dict, columns: list[dict], page_size: int) -> str:
    """The table's query on ONE line. A remote reports a refused field by line and column;
    GitHub's column is only the field's own offset on the first line of a query, so the probe
    sends a query that has no other line."""
    selections = [column_selection(c) for c in columns]
    return " ".join(table_query(table, selections, page_size).split())


def _keep_object_fields(fields: list[dict], selection_set: SelectionSetNode | None) -> list[dict]:
    """A column's structured object fields, cut down to what its pruned selection still selects."""
    if selection_set is None:
        return fields
    selected = {s.name.value: s for s in selection_set.selections if isinstance(s, FieldNode)}
    kept = []
    for f in fields:
        sel = selected.get(f["name"])
        if sel is None:
            continue
        if f.get("fields"):
            f = {**f, "fields": _keep_object_fields(f["fields"], sel.selection_set)}
        kept.append(f)
    return kept


def _refit_columns(columns: list[dict], rows: SelectionSetNode) -> list[dict]:
    """The table's columns after pruning: one the remote refused outright is gone; one whose
    nested selection lost fields selects what is left."""
    by_key = {_response_key(s): s for s in rows.selections if isinstance(s, FieldNode)}
    refit = []
    for col in columns:
        own = _operation(parse(f"{{ {column_selection(col)} }}")).selection_set.selections[0]
        assert isinstance(own, FieldNode)
        key = _response_key(own)
        sel = by_key.get(key)
        if sel is None:
            continue
        if sel.selection_set is not None:
            col = {**col, "gql_selection": " ".join(print_ast(sel).split())}
            if col.get("gql_object_fields"):
                col["gql_object_fields"] = _keep_object_fields(
                    col["gql_object_fields"], sel.selection_set
                )
        refit.append(col)
    return refit


class QueryTooComplex(ValueError):
    """The remote will not run the table's query as selected: it costs more than the remote
    allows one query. ``reason`` is the remote's own message, which carries its numbers."""

    def __init__(self, table: str, reason: str):
        super().__init__(f"{table}: {reason}")
        self.table = table
        self.reason = reason


def _errors_of(resp: httpx.Response) -> list[dict]:
    """The GraphQL errors a response carries. A remote may refuse a query with a client-error
    status and still say why in the body (GitLab answers 422 "Query too large"), so the body is
    read whatever the status; a body that is not a GraphQL answer carries none."""
    if "json" not in resp.headers.get("content-type", ""):
        return []
    body = _safe_json(resp)
    return (body.get("errors") or []) if isinstance(body, dict) else []


def _too_complex(errors: list[dict], too_complex_messages: tuple[str, ...]) -> str | None:
    """The remote's message when it rejects the query as costing too much, else None."""
    if not too_complex_messages:
        return None
    for error in errors:
        message = str(error.get("message", ""))
        if message.startswith(too_complex_messages):
            return message
    return None


async def fit_table_to_credential(
    url: str,
    auth: dict | None,
    table: dict,
    refused_error_types: frozenset[str],
    page_size: int = 1,
    too_complex_messages: tuple[str, ...] = (),
) -> tuple[dict, list[dict]]:
    """Return the table without the fields the remote refuses this credential, and what was
    taken out -- ``[{"field": "projectsV2", "reason": <the remote's message>}]``. A table the
    credential may not read at all comes back with no columns.

    The query is offered at ``page_size``, the size a read will ask for, because a remote that
    prices a query prices its page size too. A remote that answers that the query costs too
    much -- an error whose message starts with one of ``too_complex_messages`` -- raises
    :class:`QueryTooComplex`.

    The table is returned unchanged when the source declares nothing to check for, or a
    required argument has no placeholder value to probe with.
    """
    variables = _placeholder_variables(table.get("required_args") or [])
    if not (refused_error_types or too_complex_messages) or variables is None:
        return table, []
    columns = list(table.get("columns") or [])
    query = _probe_query(table, columns, page_size)
    if table.get("rows_path"):
        variables = {**variables, "pageCursor": None}
    headers = {"Content-Type": "application/json", **_build_headers(auth)}
    omitted: list[dict] = []
    async with httpx.AsyncClient(timeout=30.0) as client:
        for _ in range(_MAX_ROUNDS):
            resp = await client.post(
                url, json={"query": query, "variables": variables}, headers=headers
            )
            errors = _errors_of(resp)
            too_complex = _too_complex(errors, too_complex_messages)
            if too_complex is not None:
                raise QueryTooComplex(table["name"], too_complex)
            resp.raise_for_status()
            refusals = [
                e for e in errors if e.get("type") in refused_error_types and e.get("locations")
            ]
            if not refusals:
                break
            refused = {(loc["line"], loc["column"]) for e in refusals for loc in e["locations"]}
            reasons = {
                (loc["line"], loc["column"]): e.get("message", "")
                for e in refusals
                for loc in e["locations"]
            }
            document = parse(query)
            dropped: list = []
            _prune(_operation(document).selection_set, refused, dropped)
            if not dropped:
                raise ValueError(
                    f"{table['name']}: the remote refused fields at {sorted(refused)} "
                    f"but no field of the query starts there: {refusals}"
                )
            for sel in dropped:
                token = sel.loc.start_token
                omitted.append(
                    {"field": _response_key(sel), "reason": reasons[(token.line, token.column)]}
                )
            rows = _row_selection(document, table)
            columns = _refit_columns(columns, rows) if rows is not None else []
            if not columns:
                break  # nothing of the table is readable with this credential
            query = _probe_query(table, columns, page_size)
        else:
            raise ValueError(
                f"{table['name']}: the remote still refuses fields after {_MAX_ROUNDS} rounds"
            )
    return {**table, "columns": columns}, omitted
