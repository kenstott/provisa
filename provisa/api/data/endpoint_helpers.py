# Copyright (c) 2026 Kenneth Stott
# Canary: a874cd53-3038-4bd6-a624-d4dae6bd845e
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Pure formatting / parsing / stats helpers for the /data endpoint.

Leaf module: response formatting, Accept parsing, mermaid diagram building,
per-source stats, redirect params, directive merging, and probe-limit
injection. No dependency on the endpoint route handlers or source executors.
"""

# complexity-gate: allow-ble=1 allow-magic=2 reason="relocated verbatim from endpoint.py; ble is _request_timeout's env-var fallback when app state is absent; magic 5 is the mermaid node-label truncation width in _build_mermaid"

from __future__ import annotations

import json


from fastapi import HTTPException
from fastapi.responses import JSONResponse, Response

from provisa.compiler.directives import (
    extract_directives,
    extract_directives_from_sql_comments,
    merge_directives,
)
from provisa.executor.serialize import (
    serialize_rows,
    shape_transform,
)
from provisa.executor import stats as _qs_mod
from provisa.security.rights import Capability, InsufficientRightsError, check_capability
from provisa.transpiler.router import Route

import os as _os
import re as _re


def _request_timeout() -> float:
    try:
        from provisa.api.app import state

        return state.server_limits.get(
            "request_timeout", float(_os.environ.get("PROVISA_REQUEST_TIMEOUT", "60"))
        )
    except Exception:
        return float(_os.environ.get("PROVISA_REQUEST_TIMEOUT", "60"))


_ACCEPT_MAP = {
    "application/json": "json",
    "application/x-ndjson": "ndjson",
    "text/csv": "csv",
    "application/vnd.apache.parquet": "parquet",
    "application/vnd.apache.arrow.stream": "arrow",
}


def _parse_accept(accept: str | None) -> str:
    """Parse Accept header to output format name. Defaults to json."""
    if not accept:
        return "json"
    for mime, fmt in _ACCEPT_MAP.items():
        if mime in accept:
            return fmt
    return "json"


def _format_response(rows, columns, root_field, output_format, result_limit: int | None = None):
    """Serialize query results in the requested output format."""
    if output_format == "json":
        result = serialize_rows(rows, columns, root_field, result_limit=result_limit)
        return shape_transform(result, columns)

    if output_format == "ndjson":
        from provisa.executor.formats.ndjson import rows_to_ndjson

        content = rows_to_ndjson(rows, columns)
        return Response(content=content, media_type="application/x-ndjson")

    if output_format == "csv":
        from provisa.executor.formats.tabular import rows_to_csv

        content = rows_to_csv(rows, columns)
        return Response(content=content, media_type="text/csv")

    if output_format == "parquet":
        from provisa.executor.formats.tabular import rows_to_parquet

        content = rows_to_parquet(rows, columns)
        return Response(content=content, media_type="application/vnd.apache.parquet")

    if output_format == "arrow":
        from provisa.executor.formats.arrow import rows_to_arrow_ipc

        content = rows_to_arrow_ipc(rows, columns)
        return Response(content=content, media_type="application/vnd.apache.arrow.stream")

    return serialize_rows(rows, columns, root_field, result_limit=result_limit)


def _inject_probe_limit(sql: str, limit: int) -> str:
    """Inject or tighten a LIMIT clause for threshold probing.

    If the query already has a literal LIMIT, use the smaller of the two.
    If the query already has a parameterized LIMIT ($N), leave it unchanged.
    """
    # Parameterized limit already present — user-supplied, leave as-is
    if _re.search(r"\bLIMIT\s+\$\d+", sql, _re.IGNORECASE):
        return sql
    limit_match = _re.search(r"\bLIMIT\s+(\d+)", sql, _re.IGNORECASE)
    if limit_match:
        existing = int(limit_match.group(1))
        effective = min(existing, limit)
        return sql[: limit_match.start()] + f"LIMIT {effective}" + sql[limit_match.end() :]
    return sql + f" LIMIT {limit}"


def _check_role_capability(role, capability: Capability) -> None:
    """Raise HTTPException(403) if role lacks the given capability."""
    if role is None:
        return
    try:
        check_capability(role, capability)
    except InsufficientRightsError as e:
        raise HTTPException(status_code=403, detail=str(e))


def _detect_introspection(document) -> bool:
    """Return True if all root selections are introspection fields."""
    from graphql.language.ast import OperationDefinitionNode

    introspection_fields = {"__schema", "__type", "__typename"}
    for defn in document.definitions:
        if isinstance(defn, OperationDefinitionNode) and defn.selection_set:
            from graphql.language.ast import FieldNode as _FieldNode

            field_names = {
                sel.name.value
                for sel in defn.selection_set.selections
                if isinstance(sel, _FieldNode)
            }
            if field_names and field_names <= introspection_fields:
                return True
    return False


def _build_directives_with_legacy(request_query: str, document, legacy_hints: dict):
    """Build merged directives, falling back to legacy @provisa route hints."""
    _comment_directives = extract_directives_from_sql_comments(request_query)
    _ast_directives = extract_directives(document)
    directives = merge_directives(_comment_directives, _ast_directives)
    if directives.steward_hint is None and legacy_hints.get("route"):
        raw = legacy_hints["route"]
        directives.route = (
            "FEDERATED" if raw == "federated" else "DIRECT" if raw == "direct" else None
        )
    return directives


def _build_redirect_params(
    x_provisa_redirect: str | None,
    x_provisa_redirect_threshold: int | None,
    x_provisa_redirect_format: str | None,
    directives,
) -> tuple[str | None, int | None, bool]:
    """Return (redirect_format, effective_threshold, force_redirect) from headers + directives."""
    directive_redirect_format = (
        _parse_accept(directives.redirect_format) if directives.redirect_format else None
    )
    redirect_format = (
        _parse_accept(x_provisa_redirect_format)
        if x_provisa_redirect_format
        else directive_redirect_format
    )
    effective_threshold = x_provisa_redirect_threshold or directives.redirect_threshold
    force_redirect = (x_provisa_redirect or "").lower() == "true"
    if redirect_format and effective_threshold is None:
        force_redirect = True
    return redirect_format, effective_threshold, force_redirect


def _inject_stats_into_response(response, stats_dict: dict):
    """Inject provisa_stats extension into a JSON response/dict."""
    if isinstance(response, JSONResponse):
        body = json.loads(bytes(response.body))
        body.setdefault("extensions", {})["provisa_stats"] = stats_dict
        skip = {"content-length", "content-type"}
        extra = {k: v for k, v in response.headers.items() if k.lower() not in skip}
        return JSONResponse(content=body, headers=extra)
    if isinstance(response, dict):
        response.setdefault("extensions", {})["provisa_stats"] = stats_dict
    return response


def _count_rows_per_source(field_rows: list, ctx) -> dict[str, int]:
    """Count matched rows per source_id using join cardinality in the result.

    For one-to-many joins, sums the nested array lengths across all parent rows.
    For many-to-one / one-to-one joins, counts the non-null joined objects.
    The root source row count is NOT included here — callers use len(field_rows) for that.
    """
    counts: dict[str, int] = {}
    if not field_rows or not ctx or not hasattr(ctx, "joins"):
        return counts
    for (_, join_field), join_meta in ctx.joins.items():
        src_id = join_meta.target.source_id
        if join_meta.cardinality == "one-to-many":
            total = sum(
                len(row.get(join_field, []) or []) for row in field_rows if isinstance(row, dict)
            )
        else:
            total = sum(
                1 for row in field_rows if isinstance(row, dict) and row.get(join_field) is not None
            )
        counts[src_id] = counts.get(src_id, 0) + total
    return counts


def _build_mermaid(
    sources: set,
    source_types: dict,
    hydration_ms: dict[str, float],
    engine_ms: float | None,
    result_rows: int,
    root_field: str,
    join_fields: list | None = None,
    root_source_id: str | None = None,
    cache_catalog: str | None = None,
) -> str:
    """Build a Mermaid flowchart LR diagram for the federated query execution DAG.

    join_fields: list of (rel_field_name, source_id, is_cache_hit) for JOIN targets.
    root_source_id: when set, only this source_id is rendered in the main loop; other
                    sources in `sources` that appear as join targets are rendered via join_fields.
    cache_catalog: the engine catalog used for API cache — shown on cache node label.
    """
    _cache_label = f"{cache_catalog or root_source_id or 'pg'} cache"

    def _node_id(s: str) -> str:
        return s.replace("-", "_").replace(".", "_")

    lines = ["flowchart LR"]

    has_joins = bool(join_fields)
    single = len(sources) == 1 and not has_joins

    # When root_source_id is set, only render that source in the main loop.
    # Other sources (join targets from a different source) are handled via join_fields.
    render_sources = (
        {s for s in sources if s == root_source_id or root_source_id is None}
        if root_source_id is not None
        else sources
    )

    for src_id in sorted(render_sources):
        src_type = source_types.get(src_id, "")
        nid = _node_id(src_id)
        if src_type == "openapi":
            h_ms = hydration_ms.get(src_id, 0.0)
            cache_label = "cache hit" if h_ms < 5 else f"{round(h_ms)}ms"
            lines.append(f'    {nid}["{root_field}\\n({src_id})"]')
            lines.append(f'    pg_{nid}["{_cache_label}\\n{root_field}"]')
            lines.append(f'    {nid} -->|"{cache_label}"| pg_{nid}')
            if single:
                elapsed_label = f"{round(h_ms)}ms" if h_ms >= 5 else ""
                lines.append(f'    result(["{root_field}\\n{result_rows} rows"])')
                lines.append(
                    f'    pg_{nid} -->|"{elapsed_label}"| result'
                    if elapsed_label
                    else f"    pg_{nid} --> result"
                )
            else:
                engine_label = f"{round(engine_ms)}ms" if engine_ms is not None else ""
                lines.append(f'    pg_{nid} -->|"federated {engine_label}"| engine')
        else:
            if single:
                elapsed_label = f"{round(engine_ms)}ms" if engine_ms is not None else ""
                lines.append(f'    {nid}["{root_field}\\n({src_id})"]')
                lines.append(f'    result(["{root_field}\\n{result_rows} rows"])')
                lines.append(
                    f'    {nid} -->|"federated {elapsed_label}"| result'
                    if elapsed_label
                    else f"    {nid} --> result"
                )
            else:
                engine_label = f"{round(engine_ms)}ms" if engine_ms is not None else ""
                lines.append(f'    {nid}["{root_field}\\n({src_id})"]')
                lines.append(f'    {nid} -->|"federated {engine_label}"| engine')

    # Render JOIN target nodes — separate node per join target, even if same source as root.
    if join_fields:
        engine_label = f"{round(engine_ms)}ms" if engine_ms is not None else ""
        for rel_field, jt_src_id, is_hit in join_fields:
            jt_type = source_types.get(jt_src_id, "")
            jnid = _node_id(rel_field)
            if jt_type == "openapi":
                hit_label = "cache hit" if is_hit else "fetched"
                lines.append(f'    {jnid}["{rel_field}\\n({jt_src_id})"]')
                lines.append(f'    pg_{jnid}["{_cache_label}\\n{rel_field}"]')
                lines.append(f'    {jnid} -->|"{hit_label}"| pg_{jnid}')
                lines.append(f'    pg_{jnid} -->|"federated {engine_label}"| engine')
            else:
                lines.append(f'    {jnid}["{rel_field}\\n({jt_src_id})"]')
                lines.append(f'    {jnid} -->|"federated {engine_label}"| engine')

    if not single:
        lines.append('    engine{"Virtual\\nJoin"}')
        lines.append(f'    result(["{root_field}\\n{result_rows} rows"])')
        lines.append("    engine --> result")

    return "\n".join(lines)


def _grpc_cache_type(sql_type: str) -> str:
    """Map a gRPC ColumnDef SQL type to the cache-table type vocabulary (REQ-327)."""
    t = (sql_type or "").upper()
    if "INT" in t:
        return "integer"
    if t in ("DOUBLE", "REAL", "FLOAT") or "DECIMAL" in t or "NUMERIC" in t:
        return "number"
    if "BOOL" in t:
        return "boolean"
    return "string"


def _record_per_source_stats(
    root_field: str,
    sources: set,
    elapsed_ms: float,
    rows: int,
    ctx,
    state,
    decision=None,
    dataloader_sources: set | None = None,
    per_source_ms: dict[str, float] | None = None,
    engine_ms: float | None = None,
    hydration_rows: dict[str, int] | None = None,
    field_rows: list | None = None,
    physical_sql: str | None = None,
    hydration_cache_hits: set | None = None,
) -> None:
    """Emit FieldStat entries per source.

    For openapi sources in a federated join: emits two entries —
    one for hydration (HTTP fetch → PG write) and one for the engine join.
    For all other sources: one entry with the engine execution time.
    Per-source row counts use join cardinality from the result for one-to-many joins.
    """
    joined_rows = _count_rows_per_source(field_rows or [], ctx) if field_rows else {}
    for src_id in sources or set():
        source_type = getattr(state, "source_types", {}).get(src_id, "")
        if decision is None or decision.route != Route.DIRECT:
            prefix = "federated"
        else:
            prefix = "direct"
        strategy = f"{prefix}:{source_type}" if source_type else prefix
        if dataloader_sources and src_id in dataloader_sources:
            strategy += ":dataloader"

        src_rows = joined_rows.get(src_id, rows)

        if source_type == "openapi" and per_source_ms is not None and engine_ms is not None:
            hydration = per_source_ms.get(src_id, 0.0)
            h_rows = (hydration_rows or {}).get(src_id, 0)
            h_hit = hydration_cache_hits is not None and src_id in hydration_cache_hits
            _qs_mod.record(
                field=root_field,
                source=src_id,
                strategy="hydration",
                elapsed_ms=hydration,
                rows=h_rows,
                cache_hit=h_hit,
            )
            _qs_mod.record(
                field=root_field,
                source=src_id,
                strategy=strategy,
                elapsed_ms=engine_ms,
                rows=src_rows,
                physical_sql=physical_sql,
            )
        else:
            src_ms = per_source_ms.get(src_id, elapsed_ms) if per_source_ms else elapsed_ms
            _qs_mod.record(
                field=root_field,
                source=src_id,
                strategy=strategy,
                elapsed_ms=src_ms,
                rows=src_rows,
                physical_sql=physical_sql,
            )


def _append_mermaid(
    qs, compiled, ctx, root_field, per_source_ms, engine_ms, n_rows, hydration_cache_hits
):
    """Build and append a Mermaid diagram for the standard query path to qs."""
    _st = getattr(ctx, "source_types", None) or {}
    _root_meta2 = ctx.tables.get(root_field)
    _root_src_id = _root_meta2.source_id if _root_meta2 else None
    _jf2: list[tuple[str, str, bool]] = []
    _hch = hydration_cache_hits or set()
    # Only cross-source joins the query touched (compiled.sources, target != root).
    if _root_meta2:
        for (_tn2, _rf2), _jm2 in (ctx.joins or {}).items():
            _tgt = _jm2.target.source_id
            if _tn2 == _root_meta2.type_name and _tgt in compiled.sources and _tgt != _root_src_id:
                _jf2.append((_rf2, _tgt, _tgt in _hch))
    new_mermaid = _build_mermaid(
        compiled.sources,
        _st,
        per_source_ms or {},
        engine_ms,
        n_rows,
        root_field,
        join_fields=_jf2 or None,
        root_source_id=_root_src_id,
    )
    qs.mermaid = f"{qs.mermaid}\n\n{new_mermaid}" if qs.mermaid else new_mermaid
