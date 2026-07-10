# Copyright (c) 2026 Kenneth Stott
# Canary: a874cd53-3038-4bd6-a624-d4dae6bd845e
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

import asyncio
import logging

import httpx

from fastapi import HTTPException

from provisa.compiler.mutation_gen import (
    compile_mutation,
    inject_rls_into_mutation,
)
from provisa.api.data.action_exec import invoke_tracked_function
from provisa.security.mutation_authz import require_mutation_write
from provisa.transpiler.transpile import transpile


log = logging.getLogger(__name__)


def _check_writable_by(table_meta, columns: list[str], role_id: str):
    """Raise 403 if any column restricts write access and the role is not allowed."""
    table_cols = (
        {c["column_name"]: c for c in table_meta.columns} if hasattr(table_meta, "columns") else {}
    )
    if not table_cols:
        # Fall back to dict-style access (from state.tables)
        table_cols = {
            c.get("column_name", c.get("name", "")): c for c in getattr(table_meta, "columns", [])
        }
    for col_name in columns:
        col_meta = table_cols.get(col_name)
        if not col_meta:
            continue
        writable_by = (
            col_meta.get("writable_by", [])
            if isinstance(col_meta, dict)
            else getattr(col_meta, "writable_by", [])
        )
        if role_id not in writable_by:
            raise HTTPException(
                status_code=403,
                detail=f"Role {role_id!r} does not have write access to column {col_name!r}",
            )


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
        if not nested_cols:
            for r in rows:
                r[rel_field] = None if join_meta.cardinality == "many-to-one" else []
            continue

        src_values = list({r[src_col] for r in rows if r.get(src_col) is not None})
        if not src_values or not state.source_pools.has(tgt.source_id):
            for r in rows:
                r[rel_field] = None if join_meta.cardinality == "many-to-one" else []
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
        else:
            from collections import defaultdict

            rel_index_multi: dict = defaultdict(list)
            for rr in rel_rows:
                child = {k: rr[k] for k in nested_cols if k in rr}
                rel_index_multi[rr[tgt_col]].append(child)
            for r in rows:
                r[rel_field] = rel_index_multi.get(r.get(src_col), [])

    return rows


def _apply_action_filters(rows: list[dict], args: dict) -> list[dict]:  # REQ-360
    """Apply where/order_by/limit/offset post-processing to action result rows."""
    where = args.get("where")
    if where and isinstance(where, dict):

        def _matches(row: dict) -> bool:
            for field, condition in where.items():
                val = row.get(field)
                if isinstance(condition, dict):
                    for op, cmp in condition.items():
                        if op == "_eq" and val != cmp:
                            return False
                        elif op == "_neq" and val == cmp:
                            return False
                        elif op == "_gt" and not (val is not None and val > cmp):
                            return False
                        elif op == "_gte" and not (val is not None and val >= cmp):
                            return False
                        elif op == "_lt" and not (val is not None and val < cmp):
                            return False
                        elif op == "_lte" and not (val is not None and val <= cmp):
                            return False
                        elif op == "_in" and val not in (cmp or []):
                            return False
                        elif op == "_nin" and val in (cmp or []):
                            return False
                        elif op == "_like" and not (isinstance(val, str) and _like_match(val, cmp)):
                            return False
                        elif op == "_ilike" and not (
                            isinstance(val, str) and _like_match(val.lower(), (cmp or "").lower())
                        ):
                            return False
                else:
                    if val != condition:
                        return False
            return True

        rows = [r for r in rows if _matches(r)]

    order_by = args.get("order_by")
    if order_by and isinstance(order_by, list):
        import re

        sort_keys = []
        for spec in order_by:
            if isinstance(spec, str):
                m = re.match(r"^(\w+)\s*(asc|desc)?$", spec.strip(), re.IGNORECASE)
                if m:
                    sort_keys.append((m.group(1), (m.group(2) or "asc").lower() == "desc"))
            elif isinstance(spec, dict):
                for col, direction in spec.items():
                    sort_keys.append((col, str(direction).lower() == "desc"))
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

    _role = state.roles.get(role_id) if role_id is not None else None
    fn = state.tracked_functions.get(field_name)
    if fn:
        rows = await invoke_tracked_function(field_name, args, state, role_id)
        rows = await _maybe_resolve_relationships(
            rows, field_node, fn.get("returns", ""), ctx, state
        )
        return _apply_action_filters(rows, filter_args)

    wh = state.tracked_webhooks.get(field_name)
    if wh:
        require_mutation_write(wh, _role, field_name)
        url = wh["url"]
        method = wh["method"].upper()
        timeout = wh["timeout_ms"] / 1000
        async with httpx.AsyncClient(timeout=timeout) as client:
            resp = await client.request(method, url, json=args)
        body = resp.json()
        rows = body if isinstance(body, list) else [body]
        rows = await _maybe_resolve_relationships(
            rows, field_node, wh.get("returns", ""), ctx, state
        )
        return _apply_action_filters(rows, filter_args)

    raise HTTPException(status_code=400, detail=f"Unknown action field: {field_name!r}")


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
    document, ctx, rls, state, variables, role_id, request=None
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
        raise HTTPException(status_code=400, detail="Cannot mix action fields with table mutations")

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
        raise HTTPException(status_code=400, detail="No mutation fields found")

    results = []
    for mutation in mutations:
        # Look up by DB table name (ctx keys are GraphQL field names which may have domain prefix)
        table_meta = ctx.tables.get(mutation.table_name)
        if table_meta is None:
            for meta in ctx.tables.values():
                if meta.table_name == mutation.table_name:
                    table_meta = meta
                    break

        # Enforce writable_by column permissions
        if table_meta and mutation.mutation_type in ("insert", "update"):
            _check_writable_by(table_meta, mutation.returning_columns, role_id)

        # Inject RLS into UPDATE/DELETE
        if table_meta and rls.has_rules():
            mutation = inject_rls_into_mutation(
                mutation,
                table_meta.table_id,
                rls.rules,
            )

        # Mutations always route direct
        source_id = mutation.source_id
        if not state.source_pools.has(source_id):
            raise HTTPException(
                status_code=503,
                detail=f"No connection pool for source {source_id!r}",
            )

        dialect = state.source_dialects.get(source_id, "postgres")
        target_sql = transpile(mutation.sql, dialect)

        try:
            result = await state.federation_engine.execute_native(
                state.source_pools,
                source_id,
                target_sql,
                mutation.params,
            )
            results.append(
                {
                    "affected_rows": len(result.rows),
                }
            )
            # Invalidate cache for mutated table (REQ-080)
            if table_meta:
                await state.response_cache_store.invalidate_by_table(table_meta.table_id)
                # Mark affected MVs as stale (REQ-084)
                state.mv_registry.mark_stale(table_meta.table_name)
                # Emit dataset change event (REQ-172)
                from provisa.kafka.change_events import emit_change_event

                emit_change_event(mutation.table_name, source_id)
                # Trigger Kafka sinks for this table (REQ-176, fire-and-forget)
                from provisa.kafka.sink_executor import trigger_sinks_for_table

                asyncio.create_task(
                    trigger_sinks_for_table(mutation.table_name, state),
                )
                # Invalidate and reload hot table if applicable (Phase AD6)
                if state.hot_manager is not None:
                    from provisa.cache.hot_tables import HotTableManager

                    hot_mgr = state.hot_manager
                    assert isinstance(hot_mgr, HotTableManager)
                    if hot_mgr.is_hot(table_meta.table_name):
                        await hot_mgr.invalidate(table_meta.table_name)
                        entry = hot_mgr.get_entry(table_meta.table_name)
                        if entry is None:
                            # Find table config for reload
                            _tbl_schema = table_meta.schema_name
                            _tbl_catalog = table_meta.catalog_name
                            _pk = "id"  # default PK
                            await hot_mgr.load_table(
                                state.federation_engine,
                                table_meta.table_name,
                                _tbl_schema,
                                _tbl_catalog,
                                _pk,
                            )
        except Exception as e:
            log.exception("Mutation execution failed")
            raise HTTPException(status_code=500, detail=str(e))

    # Return first mutation result (single mutation support for now)
    mutation_name = None
    for d in document.definitions:
        if hasattr(d, "selection_set"):
            for sel in d.selection_set.selections:
                mutation_name = sel.name.value
                break

    return {"data": {mutation_name: results[0] if results else None}}
