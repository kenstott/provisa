# Copyright (c) 2026 Kenneth Stott
# Canary: 6de1b6d4-6ca1-4a45-8af7-f0c06d056cc7
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""GraphQL mutation + tracked-action execution for /data/graphql (REQ-205, REQ-360).

Writable-column checks, action relationship resolution/filtering, and the direct
mutation execute path (never the engine). Extracted from endpoint.py; leaf module.
"""

from __future__ import annotations

import logging
from typing import Any


from fastapi import HTTPException

from provisa.api.errors import ApiError
from provisa.compiler.mutation_gen import compile_mutation
from provisa.api.data.action_exec import (
    bind_named_args,
    invoke_tracked_function,
    invoke_tracked_webhook,
)


log = logging.getLogger(__name__)


_ACTION_FILTER_ARGS = {"where", "order_by", "limit", "offset"}


async def _resolve_action_relationships(  # REQ-361, REQ-362
    rows: list[dict],
    selection_set,
    return_type_name: str,
    ctx,
    state,
) -> list[dict]:
    """Batch-resolve nested relationship fields on action result rows."""
    from graphql import FieldNode as _FieldNode
    from provisa.executor.serialize import _convert_value

    for sel in selection_set.selections:
        if not isinstance(sel, _FieldNode):
            continue
        rel_field = sel.name.value
        join_key = (return_type_name, rel_field)
        if join_key not in ctx.joins:
            continue

        join_meta = ctx.joins[join_key]
        src_col = join_meta.source_column
        tgt_col = join_meta.target_column
        tgt = join_meta.target

        nested_cols = []
        if sel.selection_set:
            for ns in sel.selection_set.selections:
                if isinstance(ns, _FieldNode):
                    nested_cols.append(ns.name.value)
        _is_singular = join_meta.cardinality in ("many-to-one", "one-to-one")
        if not nested_cols:
            for r in rows:
                r[rel_field] = None if _is_singular else []
            continue

        src_values = list({r[src_col] for r in rows if r.get(src_col) is not None})
        if not src_values or not state.source_pools.has(tgt.source_id):
            for r in rows:
                r[rel_field] = None if _is_singular else []
            continue

        select_cols = list({tgt_col} | set(nested_cols))
        col_list = ", ".join(f'"{c}"' for c in select_cols)
        placeholders = ", ".join(f"${i + 1}" for i in range(len(src_values)))
        sql = (
            f'SELECT {col_list} FROM "{tgt.schema_name}"."{tgt.table_name}"'
            f' WHERE "{tgt_col}" IN ({placeholders})'
        )
        result = await state.source_pools.execute(tgt.source_id, sql, src_values)
        rel_cols = result.column_names
        rel_rows = [{c: _convert_value(v) for c, v in zip(rel_cols, r)} for r in result.rows]

        if join_meta.cardinality == "many-to-one":
            rel_index = {rr[tgt_col]: {k: rr[k] for k in nested_cols if k in rr} for rr in rel_rows}
            for r in rows:
                r[rel_field] = rel_index.get(r.get(src_col))
        elif join_meta.cardinality == "one-to-one":
            # Same physical shape as one-to-many (matched by the target's join key, which may
            # collect several physical rows), but the field is singular: take the head of the
            # matched rows, per spec, rather than assuming a single physical match.
            from collections import defaultdict

            rel_index_head: dict = defaultdict(list)
            for rr in rel_rows:
                child = {k: rr[k] for k in nested_cols if k in rr}
                rel_index_head[rr[tgt_col]].append(child)
            for r in rows:
                matches = rel_index_head.get(r.get(src_col), [])
                r[rel_field] = matches[0] if matches else None
        elif join_meta.cardinality == "one-to-many":
            from collections import defaultdict

            rel_index_multi: dict = defaultdict(list)
            for rr in rel_rows:
                child = {k: rr[k] for k in nested_cols if k in rr}
                rel_index_multi[rr[tgt_col]].append(child)
            for r in rows:
                r[rel_field] = rel_index_multi.get(r.get(src_col), [])
        else:
            raise ValueError(f"unhandled relationship cardinality {join_meta.cardinality!r}")

    return rows


_WHERE_OPS = {
    "_eq": lambda v, c: v == c,
    "_neq": lambda v, c: v != c,
    "_gt": lambda v, c: v is not None and v > c,
    "_gte": lambda v, c: v is not None and v >= c,
    "_lt": lambda v, c: v is not None and v < c,
    "_lte": lambda v, c: v is not None and v <= c,
    "_in": lambda v, c: v in (c or []),
    "_nin": lambda v, c: v not in (c or []),
    "_like": lambda v, c: isinstance(v, str) and _like_match(v, c),
    "_ilike": lambda v, c: isinstance(v, str) and _like_match(v.lower(), (c or "").lower()),
}


def _filter_refused(field: str, message: str) -> ApiError:
    return ApiError(
        400, "data.command_filter_refused", f"{field}: {message}", field=field, reason=message
    )


def _require_column(rows: list[dict], column: str, field: str) -> None:
    if rows and column not in rows[0]:
        raise _filter_refused(
            field, f"{column!r} is not a column the command returns ({', '.join(rows[0])})"
        )


def _apply_action_filters(  # REQ-360
    rows: list[dict], args: dict, field: str = "command"
) -> list[dict]:
    """Apply where/order_by/limit/offset to a command's rows. What cannot be applied as written —
    an operator it does not know, a column the command does not return, an ordering it cannot
    read — is refused by name, never skipped."""
    where = args.get("where")
    if where is not None:
        if not isinstance(where, dict):
            raise _filter_refused(field, "where takes an object of column conditions")
        tests: list[tuple[str, Any, Any]] = []
        for column, condition in where.items():
            _require_column(rows, column, field)
            if not isinstance(condition, dict):
                tests.append((column, _WHERE_OPS["_eq"], condition))
                continue
            for op, cmp in condition.items():
                if op not in _WHERE_OPS:
                    raise _filter_refused(
                        field,
                        f"where operator {op!r} on {column!r} is not one of "
                        f"{', '.join(sorted(_WHERE_OPS))}",
                    )
                tests.append((column, _WHERE_OPS[op], cmp))
        rows = [r for r in rows if all(test(r.get(col), cmp) for col, test, cmp in tests)]

    order_by = args.get("order_by")
    if order_by:
        import re

        sort_keys: list[tuple[str, bool]] = []
        for spec in order_by:
            m = re.match(r"^(\w+)\s*(asc|desc)?$", str(spec).strip(), re.IGNORECASE)
            if m is None:
                raise _filter_refused(
                    field, f"order_by {spec!r} is not '<column>', '<column> asc' or '<column> desc'"
                )
            _require_column(rows, m.group(1), field)
            sort_keys.append((m.group(1), (m.group(2) or "asc").lower() == "desc"))
        for col, reverse in reversed(sort_keys):
            rows = sorted(rows, key=lambda r, c=col: (r.get(c) is None, r.get(c)), reverse=reverse)

    offset = args.get("offset")
    if offset:
        rows = rows[int(offset) :]

    limit = args.get("limit")
    if limit is not None:
        rows = rows[: int(limit)]

    return rows


def _like_match(value: str, pattern: str) -> bool:
    import re

    regex = re.escape(pattern).replace(r"\%", ".*").replace(r"\_", ".")
    return bool(re.fullmatch(regex, value, re.DOTALL))


async def _execute_action_field(  # REQ-205, REQ-208, REQ-209, REQ-360, REQ-869
    field_name: str, field_node, state, variables: dict | None, *, ctx=None, role_id=None
) -> list:
    """Execute a tracked function or webhook field, return rows list."""
    from provisa.compiler.sql_where import _extract_value

    raw_args: dict = {}
    if hasattr(field_node, "arguments") and field_node.arguments:
        for arg in field_node.arguments:
            raw_args[arg.name.value] = _extract_value(arg.value, variables)

    filter_args = {k: raw_args.pop(k) for k in list(raw_args) if k in _ACTION_FILTER_ARGS}
    args = raw_args

    fn = state.tracked_functions.get(field_name)
    if fn:
        rows = await invoke_tracked_function(
            field_name, bind_named_args(field_name, args, state, role_id), state, role_id
        )
        rows = await _maybe_resolve_relationships(
            rows, field_node, fn.get("returns", ""), ctx, state
        )
        return _apply_action_filters(rows, filter_args, field_name)

    wh = state.tracked_webhooks.get(field_name)
    if wh:
        rows = await invoke_tracked_webhook(
            field_name, bind_named_args(field_name, args, state, role_id), state, role_id
        )
        rows = await _maybe_resolve_relationships(
            rows, field_node, wh.get("returns", ""), ctx, state
        )
        return _apply_action_filters(rows, filter_args, field_name)

    raise ApiError(
        400, "data.unknown_action_field", f"Unknown action field: {field_name!r}", field=field_name
    )


async def _maybe_resolve_relationships(rows, field_node, returns_str: str, ctx, state) -> list:
    """Resolve nested relationship fields on action rows if ctx and return type are known."""
    if not ctx or not rows or not field_node.selection_set or not returns_str:
        return rows
    if "." not in returns_str:
        return rows
    parts = returns_str.split(".", 1)
    ret_schema, ret_table = parts[0], parts[-1]
    return_type_name = None
    for meta in ctx.tables.values():
        if meta.schema_name == ret_schema and meta.table_name == ret_table:
            return_type_name = meta.type_name
            break
    if return_type_name:
        rows = await _resolve_action_relationships(
            rows, field_node.selection_set, return_type_name, ctx, state
        )
    return rows


def _split_action_fields(document, state) -> tuple[list, list]:
    """Return (action_sel_list, regular_field_names) from document root selections."""
    action_sels = []
    regular_names = []
    for defn in document.definitions:
        if not hasattr(defn, "selection_set"):
            continue
        for sel in defn.selection_set.selections:
            from graphql import FieldNode as _FieldNode

            if not isinstance(sel, _FieldNode):
                continue
            fname = sel.name.value
            if fname in state.tracked_functions or fname in state.tracked_webhooks:
                action_sels.append(sel)
            else:
                regular_names.append(fname)
    return action_sels, regular_names


async def _handle_mutation(
    document, ctx, state, variables, role_id, request=None
):  # REQ-032, REQ-033, REQ-034, REQ-035, REQ-036, REQ-172, REQ-173, REQ-176
    """Handle a GraphQL mutation operation."""
    action_sels, regular_names = _split_action_fields(document, state)

    # Pure action mutation(s)
    if action_sels and not regular_names:
        data = {}
        for sel in action_sels:
            data[sel.name.value] = await _execute_action_field(
                sel.name.value, sel, state, variables, role_id=role_id
            )
        return {"data": data}

    # Mixed action + regular fields — not supported
    if action_sels and regular_names:
        raise ApiError(
            400, "data.mixed_action_table_mutation", "Cannot mix action fields with table mutations"
        )

    headers = dict(request.headers) if request else None
    try:
        mutations = compile_mutation(
            document,
            ctx,
            state.source_types,
            variables,
            headers,
        )
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))

    if not mutations:
        raise ApiError(400, "data.no_mutation_fields", "No mutation fields found")

    # ONE write path: each mutation's statement goes through the pipeline every surface's write
    # goes through — the admission (the role's write right, the written columns' writable_by,
    # its row filter on the rows touched and the rows left behind), the lowering to the source,
    # the execution, and the steps after a write (pgwire._pipeline._after_write).
    from provisa.compiler.directives import NO_CACHE_HINT
    from provisa.pgwire._pipeline import _execute_plan, _govern_and_route_compiled

    results = []
    for mutation in mutations:
        try:
            plan = await _govern_and_route_compiled(
                mutation.sql,
                role_id,
                exec_params=list(mutation.params) if mutation.params else None,
                state=state,
                cache_hint=NO_CACHE_HINT,
            )
        except PermissionError as exc:
            raise ApiError(403, "data.write_not_admitted", str(exc), role=role_id) from exc
        try:
            result = await _execute_plan(plan, state)
        except Exception as e:  # allow-ble: request boundary — a source or driver error of any type is this mutation's outcome, answered as a 500
            log.exception("Mutation execution failed")
            raise HTTPException(status_code=500, detail=str(e))
        affected = result.rowcount if result.rowcount is not None else len(result.rows)
        results.append({"affected_rows": affected})

    # Return first mutation result (single mutation support for now)
    mutation_name = None
    for d in document.definitions:
        if hasattr(d, "selection_set"):
            for sel in d.selection_set.selections:
                mutation_name = sel.name.value
                break

    return {"data": {mutation_name: results[0] if results else None}}
