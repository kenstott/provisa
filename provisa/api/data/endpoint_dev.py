# Copyright (c) 2026 Kenneth Stott
# Canary: 4d5e6f7a-8b9c-0123-def0-123456789004
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Developer-facing data endpoints: compile, submit, proto, sql.

These endpoints are split from endpoint.py (which hit 1000+ lines) to keep
each module under the project's 1000-line limit.
"""

# Requirements: REQ-161, REQ-163, REQ-264, REQ-266, REQ-267, REQ-345,
#               REQ-354, REQ-355, REQ-356, REQ-357, REQ-358, REQ-359

from __future__ import annotations

import asyncio
import logging
from typing import TYPE_CHECKING

from fastapi import APIRouter, Header, HTTPException, Request
from fastapi.responses import JSONResponse, Response
from pydantic import BaseModel

if TYPE_CHECKING:
    from provisa.executor.result import QueryResult
    from provisa.cypher.label_map import CypherLabelMap

from provisa.api.admin._dev_shared import detect_target
from provisa.core import domain_policy
from provisa.compiler.rls import RLSContext
from provisa.compiler.sql_rewrite import rewrite_semantic_to_physical
from provisa.security.rights import Capability, InsufficientRightsError, check_capability

log = logging.getLogger(__name__)

router = APIRouter(prefix="/data", tags=["data"])


class SQLRequest(BaseModel):
    sql: str
    role: str = "admin"
    discovery_mode: bool = False


def _resolve_role_id(raw_request: Request, x_provisa_role: str | None, request_role: str) -> str:
    auth_role = getattr(raw_request.state, "role", None)
    return auth_role or x_provisa_role or request_role


@router.get("/proto/{role_id}")
async def proto_endpoint(role_id: str, domains: str = ""):  # REQ-525
    """Return the .proto file content for a role as text/plain.

    Pass ?domains=a,b to restrict to specific domains.
    """
    from provisa.api.app import state
    from provisa.grpc.proto_gen import generate_proto

    domain_list = [d for d in domains.split(",") if d and d != "all"]

    if domain_list:
        role = state.roles.get(role_id)
        if role is None:
            raise HTTPException(status_code=404, detail=f"No role {role_id!r}")
        if not state.schema_build_cache:
            raise HTTPException(status_code=503, detail="Schema build cache not ready")
        from provisa.api.data.sdl import _reachable_table_ids
        from provisa.compiler.schema_gen import SchemaInput

        cache = state.schema_build_cache
        tables = cache["tables"]
        relationships = cache["relationships"]
        seed_ids: set[int] = set()
        reachable: set[int] = set()
        for domain_id in domain_list:
            reachable |= _reachable_table_ids(domain_id, tables, relationships)
            seed_ids |= {t["id"] for t in tables if t["domain_id"] == domain_id}
        reachable |= seed_ids
        filtered_tables = [t for t in tables if t["id"] in reachable]
        existing = role.get("domain_access") or []
        if "*" not in existing:
            role = {
                **role,
                "domain_access": list(set(existing) | set(domain_list)),
            }
        si = SchemaInput(
            tables=filtered_tables,
            root_table_ids=seed_ids,
            relationships=relationships,
            column_types=cache["column_types"],
            naming_rules=cache["naming_rules"],
            role=role,
            domains=cache["domains"],
            source_types=state.source_types,
            domain_prefix=cache["domain_prefix"],
            physical_table_map=cache["physical_table_map"],
            functions=cache["functions"],
            webhooks=cache["webhooks"],
            enum_types=cache["enum_types"],
            governed_gql_types={
                tbl.get("gql_type_name")
                for reg in getattr(state, "graphql_remote_sources", {}).values()
                for tbl in reg.get("tables", [])
                if tbl.get("gql_type_name")
            },
        )
        try:
            proto = generate_proto(si)
        except ValueError as exc:
            raise HTTPException(status_code=404, detail=str(exc)) from exc
        return Response(content=proto, media_type="text/plain")

    if role_id not in state.proto_files:
        raise HTTPException(
            status_code=404,
            detail=f"No proto file available for role {role_id!r}",
        )
    return Response(content=state.proto_files[role_id], media_type="text/plain")


async def _execute_govdata(source_id: str, sql: str, state) -> "QueryResult":
    log.warning("_execute_govdata called: sql=%s", sql[:300])
    from sqlalchemy import select

    from provisa.core.models import GovDataSource, GovDataSubject
    from provisa.core.schema_org import sources
    from provisa.core.secrets import resolve_secrets
    from provisa.executor.result import QueryResult
    from provisa.govdata.source import execute_query

    pool = state.tenant_db
    async with pool.acquire() as conn:
        result = await conn.execute_core(
            select(sources.c.username, sources.c.database).where(sources.c.id == source_id)
        )
        _row = result.fetchone()
    if _row is None:
        raise HTTPException(status_code=404, detail=f"No govdata source {source_id!r}")
    row = dict(_row._mapping)

    api_key = resolve_secrets(row["username"] or "")
    database = row["database"] or ""

    all_govdata_schemas = [s.strip().lower() for s in database.split(",") if s.strip()]

    # Only instantiate schemas actually referenced in the SQL — instantiating all
    # schemas in the source's database list causes JDBC errors for schemas not cached locally.
    # Strip SQL quotes before regex so both "fec"."candidates" and fec.candidates are matched.
    import re as _re

    _sql_unquoted = sql.replace('"', "").replace("`", "")
    _sql_schemas = {m.group(1).lower() for m in _re.finditer(r"\b(\w+)\.\w+", _sql_unquoted)}
    govdata_schemas = [s for s in all_govdata_schemas if s in _sql_schemas] or all_govdata_schemas

    loop = asyncio.get_event_loop()

    gds = GovDataSource(
        id=source_id,
        subject=GovDataSubject.all,
        govdata_schemas=govdata_schemas,
        domain_id="default",
        api_key=api_key,
    )

    # Calcite/GovData uses lowercase schema names. Quoted identifiers ("ref"."table") are required
    # for reserved-word schemas (e.g. ref, fec). Keep quotes as generated by the compiler.
    # Also convert LIMIT n → FETCH FIRST n ROWS ONLY (Calcite Oracle syntax).
    import re as _re

    govdata_sql = sql
    govdata_sql = _re.sub(
        r"\bLIMIT\s+(\d+)\b", r"FETCH FIRST \1 ROWS ONLY", govdata_sql, flags=_re.IGNORECASE
    )
    govdata_sql = _re.sub(r"\)\s+AS\s+(\w)", r") \1", govdata_sql, flags=_re.IGNORECASE)
    # Calcite Oracle mode rejects subquery aliases entirely in some contexts.
    # Unwrap SELECT * FROM (inner) alias FETCH FIRST n ROWS ONLY → inner FETCH FIRST n ROWS ONLY
    _unwrap = _re.match(
        r"^\s*SELECT\s+\*\s+FROM\s*\(\s*(.*?)\s*\)\s+\w+\s+(FETCH\s+FIRST\s+\d+\s+ROWS\s+ONLY)\s*$",
        govdata_sql,
        flags=_re.IGNORECASE | _re.DOTALL,
    )
    if _unwrap:
        govdata_sql = f"{_unwrap.group(1).strip()} {_unwrap.group(2)}"
    rows_as_dicts = await loop.run_in_executor(None, lambda: execute_query(gds, govdata_sql))

    if not rows_as_dicts:
        return QueryResult(rows=[], column_names=[])

    column_names = list(rows_as_dicts[0].keys())
    rows = [tuple(r[c] for c in column_names) for r in rows_as_dicts]
    return QueryResult(rows=rows, column_names=column_names)


@router.post("/sql")
async def sql_endpoint(  # REQ-264, REQ-266, REQ-267
    raw_request: Request,
    request: SQLRequest,
    x_provisa_role: str | None = Header(None),
    accept: str | None = Header(None),
    x_provisa_stats: str | None = Header(None),
    x_provisa_as_of: str | None = Header(None),  # REQ-1163: read bitemporal MVs as of this time
):
    """Execute raw SQL through Stage 2 governance (REQ-264, REQ-266, REQ-267).

    Pipeline:
      1. Parse incoming SQL with SQLGlot.
      2. Construct GovernanceContext from the request role.
      3. Reject (HTTP 403) any table not in the role's schema scope.
      4. Apply Stage 2 governance: RLS, masking, visibility, ceiling.
      5. Route and execute the governed SQL.
    """
    from provisa.api.app import state
    from provisa.api.data.endpoint_helpers import _parse_accept, _format_response

    role_id = _resolve_role_id(raw_request, x_provisa_role, request.role)
    output_format = _parse_accept(accept)
    stats_enabled = (x_provisa_stats or "").lower() == "true"

    # REQ-1163: a request-level as-of validated to a safe SQL timestamp literal (400 on malformed).
    _as_of = None
    if x_provisa_as_of:
        from provisa.mv.bitemporal import parse_as_of

        try:
            _as_of = parse_as_of(x_provisa_as_of)
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=f"invalid X-Provisa-As-Of: {exc}")

    import time as _time
    from provisa.executor import stats as _qs_mod

    if stats_enabled:
        _qs_mod.begin()
    _t0 = _time.perf_counter()

    def _finalize(result, *, source: str, strategy: str, physical_sql: str | None):
        rows_as_dicts = [dict(zip(result.column_names, row)) for row in result.rows]
        if stats_enabled:
            _qs_mod.record(
                field="sql",
                source=source,
                strategy=strategy,
                elapsed_ms=(_time.perf_counter() - _t0) * 1000,
                rows=len(rows_as_dicts),
                physical_sql=physical_sql,
            )
            qs = _qs_mod.current()
            if qs is not None and output_format == "json":
                from fastapi.encoders import jsonable_encoder

                return JSONResponse(
                    jsonable_encoder(
                        {"data": {"sql": rows_as_dicts}, "provisa_stats": qs.to_dict()}
                    )
                )
            # non-json formats fall through to standard response (no stats injection)

        if output_format == "json":
            return {"data": {"sql": rows_as_dicts}}
        from provisa.compiler.sql_gen import ColumnRef

        columns = [
            ColumnRef(alias=None, column=c, field_name=c, nested_in=None)
            for c in result.column_names
        ]
        return _format_response(result.rows, columns, "sql", output_format)

    # ONE pipeline: /data/sql runs through the single governed chokepoint. execute_sql_batch splits a
    # multi-statement batch statement-aware and governs+executes EACH (last result returned) — so the
    # tail is never silently dropped, and a per-statement registered command still routes through the
    # shared function hook. Surface-specific request auth (capability gate) is a pre-check here; as_of
    # (REQ-1163) + discovery_mode are query-shaping params threaded in.
    if role_id not in state.schemas:
        raise HTTPException(status_code=400, detail=f"No schema for role {role_id!r}")
    role = state.roles.get(role_id)
    _check_sql_capabilities(role, request.discovery_mode)
    from provisa.pgwire._pipeline import execute_sql_batch

    # Request-boundary error handling (mirrors the pgwire handle_query handler): a governance denial
    # (RLS/masking/visibility/relationship guard) raises PermissionError → 403; any OTHER error from an
    # arbitrary user query — a source/engine rejection (UndefinedTable, syntax, type mismatch, ...) or a
    # parse error — is a client error → 400 with the detail, exactly as pgwire converts it to a SQLSTATE
    # error response. Converting a bad query to an error RESPONSE (not crashing the request) is the
    # documented boundary contract, not silent error handling.
    try:
        result = await execute_sql_batch(
            request.sql, role_id, state, discovery_mode=request.discovery_mode, as_of=_as_of
        )
    except PermissionError as exc:
        raise HTTPException(status_code=403, detail=str(exc))
    except HTTPException:
        raise
    except Exception as exc:  # allow-ble: request boundary — an arbitrary user query can raise ANY engine/driver exception type
        raise HTTPException(status_code=400, detail=str(exc))
    return _finalize(result, source="engine", strategy="batch", physical_sql=request.sql)


def _check_sql_capabilities(role, discovery_mode: bool) -> None:
    if role and not discovery_mode:
        try:
            check_capability(role, Capability.QUERY_DEVELOPMENT)
        except InsufficientRightsError as e:
            raise HTTPException(status_code=403, detail=str(e))


def _check_qualifier_binding(tree) -> str | None:
    """Return an error if any column's table qualifier binds to no FROM relation.

    sqlglot accepts undefined aliases (e.g. SELECT u.name with no `u` table) as
    valid syntax. Collect every table name, table alias, CTE name and derived-table
    alias across the tree, then flag column qualifiers that match none of them.
    Comparison is case-insensitive (unquoted SQL identifiers fold case), and the
    binding set is a tree-wide over-approximation, so only genuinely-undefined
    qualifiers are reported.
    """
    from sqlglot import exp

    bindings: set[str] = set()
    for t in tree.find_all(exp.Table):
        bindings.add(t.alias_or_name.lower())
    for cte in tree.find_all(exp.CTE):
        bindings.add(cte.alias_or_name.lower())
    for sub in tree.find_all(exp.Subquery):
        if sub.alias:
            bindings.add(sub.alias.lower())

    unresolved: set[str] = set()
    schema_qualified: set[str] = set()
    for col in tree.find_all(exp.Column):
        # A column carrying a schema/catalog part (schema.table.column) embeds the
        # semantic domain prefix, which is not rewritten in column position and will
        # not match the physical relation. Columns must use the bare table name.
        if col.args.get("db"):
            schema_qualified.add(col.sql(dialect="postgres"))
        elif col.table and col.table.lower() not in bindings:
            unresolved.add(col.table)
    if schema_qualified:
        names = ", ".join(sorted(schema_qualified))
        return (
            f"Schema-qualified column reference(s): {names}. Qualify columns with the "
            "bare table name only (e.g. inquiries.user_id), never the domain/schema prefix."
        )
    if unresolved:
        names = ", ".join(sorted(unresolved))
        return (
            f"Unknown table qualifier(s): {names}. Every column must be qualified by a "
            "table name that appears in the FROM/JOIN clauses (no aliases)."
        )
    return None


def _collect_nl_user_tables(ctx) -> tuple[list, dict, dict, "CypherLabelMap"]:
    """Return (all_tables, user_nodes, table_name_to_type) from a schema context."""
    from provisa.compiler.sql_gen import TableMeta as _TableMeta
    from provisa.cypher.label_map import CypherLabelMap as _CLM

    seen_type_names: set[str] = set()
    all_tables: list[_TableMeta] = []
    for tbl in ctx.tables.values():
        if tbl.type_name not in seen_type_names:
            seen_type_names.add(tbl.type_name)
            all_tables.append(tbl)
    for jm in ctx.joins.values():
        if jm.target.type_name not in seen_type_names:
            seen_type_names.add(jm.target.type_name)
            all_tables.append(jm.target)

    _lm = _CLM.from_schema(ctx)
    if domain_policy.single_domain():
        # Single-domain mode: nodes carry no domain label — include all non-traversal nodes.
        _user_nodes = {tn: nm for tn, nm in _lm.nodes.items() if not nm.traversal_only}
    else:
        _user_domains = {
            d for d in (n.domain_id for n in _lm.nodes.values()) if d not in (None, "ops", "meta")
        }
        _user_nodes = {
            tn: nm
            for tn, nm in _lm.nodes.items()
            if nm.domain_id in _user_domains and not nm.traversal_only
        }
    _table_name_to_type: dict[str, str] = {nm.table_name: tn for tn, nm in _user_nodes.items()}
    return all_tables, _user_nodes, _table_name_to_type, _lm


async def _run_table_selection(
    user_nodes: dict,
    question: str,
    sql_domain_fn,
    table_name_to_type: dict[str, str],
) -> set[str]:
    from provisa.llm.client import ProviasLLMClient

    table_list = ", ".join(
        f"{sql_domain_fn(nm.domain_id)}.{nm.table_name}" for nm in user_nodes.values()
    )
    _table_selector = ProviasLLMClient("table_selection")
    pass1_text = await _table_selector.complete(
        prompt=f"Tables: {table_list}\n\nQuestion: {question}",
        system=(
            "You are a table selector. Given a natural-language question and a list of available tables, "
            "reply with ONLY a comma-separated list of the table names (without domain prefix) that are "
            "needed to answer the question. No explanation. No punctuation other than commas."
        ),
        max_tokens=256,
    )
    selected_raw = [t.strip().split(".")[-1] for t in pass1_text.split(",") if t.strip()]
    selected_types: set[str] = {
        table_name_to_type[name] for name in selected_raw if name in table_name_to_type
    }
    if not selected_types:
        selected_types = set(user_nodes)
    return selected_types


def _build_multihop_lines(selected_types: set[str], lm, sql_domain_fn) -> list[str]:
    multihop_lines: list[str] = []
    _seen_path_keys: set[tuple[str, ...]] = set()
    for src_tn in selected_types:
        src_nm = lm.nodes[src_tn]
        for tgt_tn in selected_types:
            if src_tn == tgt_tn:
                continue
            paths = lm.find_paths(src_tn, tgt_tn, max_hops=4)
            multihop_paths = [p for p in paths if len(p) >= 2]
            if not multihop_paths:
                continue
            shortest = min(multihop_paths, key=len)
            path_key = tuple(
                f"{r.source_label}:{r.join_source_column}:{r.target_label}" for r in shortest
            )
            if path_key in _seen_path_keys:
                continue
            _seen_path_keys.add(path_key)
            node_chain = [src_nm] + [lm.nodes[r.target_label] for r in shortest]
            hops_str = " → ".join(
                f"{sql_domain_fn(n.domain_id)}.{n.table_name}" for n in node_chain
            )
            multihop_lines.append(f"Multi-hop path ({len(shortest)} hops): {hops_str}")
            for r in shortest:
                s_nm = lm.nodes[r.source_label]
                t_nm = lm.nodes[r.target_label]
                multihop_lines.append(
                    f"  JOIN {sql_domain_fn(t_nm.domain_id)}.{t_nm.table_name}"
                    f" ON {s_nm.table_name}.{r.join_source_column}"
                    f" = {t_nm.table_name}.{r.join_target_column}"
                )
    return multihop_lines


def _build_relevant_type_names(selected_types: set[str], lm) -> set[str]:
    hop_type_names: set[str] = set()
    for src_tn in selected_types:
        for tgt_tn in selected_types:
            if src_tn == tgt_tn:
                continue
            for path in lm.find_paths(src_tn, tgt_tn, max_hops=4):
                for r in path:
                    hop_type_names.add(r.source_label)
                    hop_type_names.add(r.target_label)
    return selected_types | hop_type_names


def _build_schema_block(
    all_tables: list,
    relevant_type_names: set[str],
    ctx,
    sql_domain_fn,
    multihop_lines: list[str],
) -> str:
    from provisa.compiler.sql_rewrite import semantic_table_name

    schema_lines: list[str] = []
    for tbl in all_tables:
        if tbl.type_name not in relevant_type_names:
            continue
        cols = ctx.aggregate_columns.get(tbl.table_id, [])
        col_list = ", ".join(
            ctx.physical_to_sql.get((tbl.table_id, col_name), col_name) for col_name, _ in cols
        )
        tbl_sql = semantic_table_name(tbl)
        schema_lines.append(f"Table {sql_domain_fn(tbl.domain_id)}.{tbl_sql}")
        schema_lines.append(f"  Columns: {col_list or '(unknown)'}")
        for (src_type, _), jm in ctx.joins.items():
            if src_type == tbl.type_name:
                tgt_sql = semantic_table_name(jm.target)
                schema_lines.append(
                    f"  Approved JOIN: {sql_domain_fn(jm.target.domain_id)}.{tgt_sql} "
                    f"ON {tbl_sql}.{jm.source_column} = {tgt_sql}.{jm.target_column}"
                )

    schema_block = "\n".join(schema_lines)
    if multihop_lines:
        schema_block += (
            "\n\nMulti-hop join paths (use these when tables are not directly joined):\n"
            + "\n".join(multihop_lines)
        )
    return schema_block


async def _run_sql_generation_loop(
    question: str,
    schema_block: str,
    ctx,
    gov_ctx,
    role_obj,
    raw_tables: list,
) -> tuple[str, int, str]:
    import sqlglot
    from provisa.llm.client import ProviasLLMClient
    from provisa.compiler.sql_validator import validate_sql

    sql_gen_system = (
        "You are a SQL generator for the Provisa data platform.\n"
        "Output ONLY a valid PostgreSQL SELECT statement — no explanation, no markdown, no code fences.\n\n"
        f"Available tables and approved joins:\n{schema_block}\n\n"
        "STRICT RULES — violating any rule causes a validation error:\n"
        "1. Table names: use domain.table_name exactly as listed (e.g. pet_store.pets). Never quote schema names.\n"
        "2. Column names: use ONLY the column names listed under each table, exactly as shown (case-sensitive). Never invent names.\n"
        "3. JOINs: use ONLY the 'Approved JOIN' conditions listed above, character for character. "
        "Never write a JOIN ON condition that is not in the list above.\n"
        "4. Include a LIMIT clause (default 100).\n"
        "5. Never use table aliases. In SELECT, ON, and WHERE, qualify every column "
        "with the BARE table name only — never the domain/schema prefix. Use "
        "table.column exactly as written in the approved joins (e.g. inquiries.user_id "
        "and animal_breeds.name — NOT pet_store.inquiries.user_id, NOT ab.name). "
        "The domain prefix (pet_store., shelter.) is used ONLY in FROM/JOIN table refs, "
        "never in a column reference. A FROM/JOIN clause must not introduce an alias "
        "either (write 'JOIN pet_store.pets ON ...', never 'JOIN pet_store.pets p ON ...').\n"
        "6. If the question requires a relationship between tables that does not appear in the "
        "approved joins list above, respond with exactly the token: NOT_APPLICABLE\n"
        "7. Output only the SQL statement or NOT_APPLICABLE."
    )

    _sql_gen = ProviasLLMClient("sql_generation")
    last_error: str = ""
    last_sql: str = ""
    attempt: int = 0
    current_prompt: str = question

    for attempt in range(1, 4):
        if last_error:
            current_prompt = (
                f"{question}\n\nPrevious attempt:\n{last_sql}\n\n"
                f"Error: {last_error}\nFix it and output only the corrected SQL."
            )

        last_sql = await _sql_gen.complete(
            prompt=current_prompt,
            system=sql_gen_system,
            max_tokens=1024,
        )
        last_sql = last_sql.strip()
        if last_sql == "NOT_APPLICABLE":
            return "NOT_APPLICABLE", attempt, "NOT_APPLICABLE"
        if last_sql.startswith("```"):
            last_sql = "\n".join(
                line for line in last_sql.splitlines() if not line.strip().startswith("```")
            ).strip()

        try:
            tree = sqlglot.parse_one(last_sql, read="postgres")
        except Exception as exc:
            last_error = str(exc)
            continue

        binding_error = _check_qualifier_binding(tree)
        if binding_error:
            last_error = binding_error
            continue

        normalized = rewrite_semantic_to_physical(last_sql, ctx)
        violations = validate_sql(normalized, ctx, gov_ctx, role_obj or {}, raw_tables)
        if violations:
            last_error = "; ".join(f"[{v.code}] {v.message}" for v in violations)
            continue

        last_error = ""
        break

    return last_sql, attempt, last_error


class NLToSQLRequest(BaseModel):
    question: str
    role: str = "admin"


@router.post("/nl-to-sql")
async def nl_to_sql_endpoint(  # REQ-354, REQ-355, REQ-356, REQ-357, REQ-358, REQ-359
    raw_request: Request,
    request: NLToSQLRequest,
    x_provisa_role: str | None = Header(None),
):
    """Translate a natural-language question to Semantic SQL via Claude.

    Validates the generated SQL with sqlglot and retries (up to 3 attempts)
    feeding parse errors back to Claude.
    """
    from provisa.api.app import state
    from provisa.compiler.naming import domain_to_sql_name
    from provisa.compiler.stage2 import build_governance_context

    role_id = _resolve_role_id(raw_request, x_provisa_role, request.role)
    if role_id not in state.contexts:
        raise HTTPException(status_code=400, detail=f"No schema for role {role_id!r}")

    ctx = state.contexts[role_id]
    rls = state.rls_contexts.get(role_id, RLSContext.empty())
    role_obj = state.roles.get(role_id)
    gov_ctx = build_governance_context(
        role_id,
        rls,
        state.masking_rules,
        ctx,
        getattr(state, "tables", []),
        role=role_obj,
        relationships=getattr(state, "relationships", None),
    )
    raw_tables = getattr(state, "tables", [])

    all_tables, _user_nodes, _table_name_to_type, _lm = _collect_nl_user_tables(ctx)

    def _sql_domain(domain_id: str | None) -> str:
        return domain_to_sql_name(domain_id) if domain_id else "default"

    selected_types = await _run_table_selection(
        _user_nodes, request.question, _sql_domain, _table_name_to_type
    )

    multihop_lines = _build_multihop_lines(selected_types, _lm, _sql_domain)
    relevant_type_names = _build_relevant_type_names(selected_types, _lm)
    schema_block = _build_schema_block(
        all_tables, relevant_type_names, ctx, _sql_domain, multihop_lines
    )

    last_sql, attempt, last_error = await _run_sql_generation_loop(
        request.question, schema_block, ctx, gov_ctx, role_obj, raw_tables
    )

    if last_error:
        raise HTTPException(
            status_code=422, detail=f"Could not generate valid SQL after 3 attempts: {last_error}"
        )

    return {"sql": last_sql, "attempts": attempt}


class QueryRequest(BaseModel):
    query: str
    params: dict = {}
    variables: dict | None = None
    role: str = "admin"


@router.post("/query")
async def unified_query_endpoint(  # REQ-001, REQ-267, REQ-345
    raw_request: Request,
    request: QueryRequest,
    x_provisa_role: str | None = Header(None),
):
    """Execute a GraphQL, SQL, or Cypher query; auto-detected from syntax.

    Returns { columns, rows } for Cypher/SQL.
    Returns { data } for GraphQL (native format).
    """

    role_id = _resolve_role_id(raw_request, x_provisa_role, request.role)

    target = detect_target(request.query)

    if target == "cypher":
        from provisa.api.rest.cypher_router import CypherRequest, cypher_query

        body = CypherRequest(query=request.query, params=request.params)
        return await cypher_query(body, raw_request)

    if target == "graphql":
        from provisa.api.data.endpoint import graphql_endpoint
        from provisa.api.data.endpoint import GraphQLRequest as GQLRequest

        gql_req = GQLRequest(query=request.query, variables=request.variables, role=role_id)
        return await graphql_endpoint(raw_request, gql_req)

    # SQL
    sql_req = SQLRequest(sql=request.query, role=role_id)
    return await sql_endpoint(raw_request, sql_req, x_provisa_role=x_provisa_role)
