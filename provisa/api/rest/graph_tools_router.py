# Copyright (c) 2026 Kenneth Stott
# Canary: 2e7a4c1f-9b5d-4f8a-8c3e-6d2b4f7a9c1e
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Cypher graph-tools routes: relationship imputation + Neo4j export (REQ-392).

POST /data/impute-relationships and /data/neo4j-export. Registered on the
shared cypher_router APIRouter. Extracted from cypher_router.py.
"""

# complexity-gate: allow-magic=4 reason="Neo4j-export/impute routes relocated verbatim from cypher_router.py; the literals are compound-label split arities (Domain:Table -> 2 parts) and export batch sizes"

from __future__ import annotations

import logging

from typing import Any

from fastapi import Request

from fastapi.responses import JSONResponse  # noqa: F401

from pydantic import BaseModel

from sqlalchemy import select

from provisa.core.schema_org import node_ids
from provisa.compiler.naming import apply_cql_property as _cql_prop
from provisa.api.rest.cypher_router import router
from provisa.api.rest.cypher_exec import (  # noqa: F401
    _build_label_map,
    _execute_call_body,
    _resolve_role_id,
)

log = logging.getLogger(__name__)


class ImputeRequest(BaseModel):
    nodes: list[dict]  # [{label: str, id: str}, ...]


@router.post("/data/impute-relationships")
async def impute_relationships(
    request: Request, body: ImputeRequest
) -> JSONResponse:  # REQ-345, REQ-351
    """Generate and execute all relationship queries for a set of visible graph nodes.

    Accepts the visible node set, uses label_map to determine pk columns and known
    schema relationships, executes one query per relationship pair, and returns
    merged nodes+edges in the standard cypher response format.
    """
    from provisa.api.app import state
    from provisa.cypher.assembler import assemble_rows, to_serializable
    from provisa.cypher.parser import parse_cypher

    role_id = _resolve_role_id(request, state)
    ctx = state.contexts.get(role_id)
    if ctx is None:
        return JSONResponse(status_code=503, content={"error": "Schema not loaded"})

    label_map = _build_label_map(ctx, role_id, state)
    nm_by_label = {nm.label: nm for nm in label_map.nodes.values()}

    # Collect stable node ids per label from request
    int_ids: list[int] = []
    id_to_label: dict[int, str] = {}
    for node in body.nodes:
        lbl = str(node.get("label", ""))
        nid = node.get("id")
        if lbl and nid is not None:
            try:
                i = int(nid)
                int_ids.append(i)
                id_to_label[i] = lbl
            except (ValueError, TypeError):
                pass

    def _cql_literal(v: Any) -> str:
        if isinstance(v, bool):
            return "true" if v else "false"
        if isinstance(v, (int, float)):
            return str(v)
        return "'" + str(v).replace("\\", "\\\\").replace("'", "\\'") + "'"

    # Resolve stable ids to id-column values via composite_id ("label|pk_value")
    by_label: dict[str, list[Any]] = {}
    if int_ids and state.tenant_db:
        async with state.tenant_db.acquire() as _pg_conn:
            _pg_result = await _pg_conn.execute_core(
                select(node_ids.c.id, node_ids.c.label, node_ids.c.composite_id).where(
                    node_ids.c.id.in_(int_ids)
                )
            )
            _pg_rows = [dict(r._mapping) for r in _pg_result.fetchall()]
        for _r in _pg_rows:
            _nm = nm_by_label.get(_r["label"])
            if _nm is None:
                continue
            _pk_str = _r["composite_id"].rsplit("|", 1)[-1]
            _val: Any = int(_pk_str) if _pk_str.lstrip("-").isdigit() else _pk_str
            by_label.setdefault(_r["label"], []).append(_val)

    visible_labels = set(by_label.keys())

    # Build queries for every relationship pair where both endpoints are visible
    queries: list[str] = []
    for rel in label_map.relationships.values():
        src_label = label_map.nodes[rel.source_label].label
        tgt_label = label_map.nodes[rel.target_label].label
        if src_label not in visible_labels or tgt_label not in visible_labels:
            continue
        src_nm = label_map.nodes[rel.source_label]
        tgt_nm = label_map.nodes[rel.target_label]
        src_prop = _cql_prop(src_nm.id_column)
        tgt_prop = _cql_prop(tgt_nm.id_column)
        src_ids = ", ".join(_cql_literal(i) for i in by_label[src_label])
        tgt_ids = ", ".join(_cql_literal(i) for i in by_label[tgt_label])
        queries.append(
            f"MATCH (a:{src_label})-[r:{rel.rel_type}]->(b:{tgt_label})"
            f" WHERE a.{src_prop} IN [{src_ids}] AND b.{tgt_prop} IN [{tgt_ids}]"
            f" RETURN a, r, b"
        )

    if not queries:
        return JSONResponse(content={"columns": [], "rows": []})

    all_nodes: dict[str, Any] = {}
    all_edges: dict[str, Any] = {}
    for cypher_query in queries:
        try:
            ast = parse_cypher(cypher_query)
            rows, graph_vars = await _execute_call_body(ast, label_map, {}, state, ctx, role_id)
            assembled = assemble_rows(rows, graph_vars)
        except Exception:
            log.exception("Impute query failed: %s", cypher_query)
            continue
        for row in assembled:
            for val in row.values():
                ser = to_serializable(val)
                if isinstance(ser, dict):
                    if "identity" in ser:
                        all_edges[ser["identity"]] = ser
                    elif "label" in ser:
                        key = f"{ser['label']}:{ser['id']}"
                        all_nodes[key] = ser

    from provisa.cypher.assembler import register_node_ids, register_rel_ids

    serializable_merged = [{"node": r} for r in list(all_nodes.values()) + list(all_edges.values())]
    await register_node_ids(serializable_merged, state.tenant_db)
    await register_rel_ids(serializable_merged, state.tenant_db)
    return JSONResponse(content={"columns": ["node"], "rows": serializable_merged})


class Neo4jExportRequest(BaseModel):
    url: str
    username: str
    password: str
    database: str = "neo4j"
    nodes: list[dict]
    edges: list[dict]


def _neo4j_cypher_literal(v: Any) -> str:
    """Render a Python value as a Cypher literal."""
    import json as _json

    if v is None:
        return "null"
    if isinstance(v, bool):
        return "true" if v else "false"
    if isinstance(v, (int, float)):
        return str(v)
    if isinstance(v, str):
        return _json.dumps(v)
    return _json.dumps(_json.dumps(v))


@router.post("/data/neo4j-export")
async def neo4j_export(body: Neo4jExportRequest) -> JSONResponse:
    """Forward graph nodes/edges to a Neo4j server via its HTTP transactional API."""
    import base64 as _base64
    import httpx as _httpx

    statements: list[str] = []

    for n in body.nodes:
        table_label = n.get("tableLabel", "")
        full_label = n.get("label", "")
        # Domain-union nodes omit tableLabel; reconstruct from compound label "Domain:Table"
        parts = full_label.split(":", 1) if ":" in full_label else [full_label]
        effective_table = table_label or (parts[1] if len(parts) == 2 else full_label) or "Node"
        effective_domain = parts[0] if len(parts) == 2 else ""
        node_id = n.get("id")
        props: dict = n.get("properties", {})
        set_parts = ", ".join(f"{k}: {_neo4j_cypher_literal(v)}" for k, v in props.items())
        set_str = f" SET n += {{{set_parts}}}" if set_parts else ""
        label_str = (
            f"`{effective_table}`:`{effective_domain}`"
            if effective_domain and effective_domain != effective_table
            else f"`{effective_table}`"
        )
        statements.append(f"MERGE (n:{label_str} {{_provisa_id: {node_id}}}){set_str}")

    for e in body.edges:
        start = e.get("start")
        end = e.get("end")
        rel_type = e.get("type", "REL")
        src_label = e.get("startNodeLabel", "Node")
        tgt_label = e.get("endNodeLabel", "Node")
        statements.append(
            f"MATCH (a:`{src_label}` {{_provisa_id: {start}}}), "
            f"(b:`{tgt_label}` {{_provisa_id: {end}}}) "
            f"MERGE (a)-[:`{rel_type}`]->(b)"
        )

    http_url = body.url.rstrip("/") + f"/db/{body.database}/tx/commit"
    token = _base64.b64encode(f"{body.username}:{body.password}".encode()).decode()

    errors: list[str] = []
    try:
        async with _httpx.AsyncClient() as client:
            resp = await client.post(
                http_url,
                json={"statements": [{"statement": s} for s in statements]},
                headers={
                    "Authorization": f"Basic {token}",
                    "Content-Type": "application/json",
                    "Accept": "application/json",
                },
                timeout=30.0,
            )
    except _httpx.ConnectError as exc:
        return JSONResponse(status_code=502, content={"error": f"Cannot connect to Neo4j: {exc}"})
    except _httpx.TimeoutException:
        return JSONResponse(status_code=504, content={"error": "Neo4j request timed out"})

    if resp.status_code == 401:
        return JSONResponse(status_code=401, content={"error": "Neo4j authentication failed"})
    if resp.status_code // 100 != 2:
        return JSONResponse(
            status_code=resp.status_code,
            content={"error": f"Neo4j HTTP {resp.status_code}: {resp.text[:200]}"},
        )

    data = resp.json()
    for err in data.get("errors", []):
        errors.append(err.get("message", str(err)))

    imported = len(statements) - len(errors)
    return JSONResponse(content={"imported": imported, "errors": errors})
