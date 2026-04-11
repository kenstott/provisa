# Copyright (c) 2026 Kenneth Stott
# Canary: 2f87c2de-a092-4613-b94c-3899f4b2b39a
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""gRPC Arrow Flight server for Provisa (REQ-045, REQ-126).

Clients send a GraphQL query as the Flight ticket, receive Arrow record batches.
When the Zaychik Flight SQL proxy is available, results stream end-to-end
without materializing the full result in Provisa memory.

Supports three modes controlled by a connection property in the Flight SQL handshake:
  - mode=catalog: exposes the semantic layer as a read-only JDBC catalog
  - mode=approved: exposes only persisted approved queries as virtual tables
  - default: full query-execution behavior
"""

from __future__ import annotations

import asyncio
import json
import logging
import re

import pyarrow as pa
import pyarrow.flight as flight

from provisa.api.flight.catalog import (
    ApprovedQuery,
    CatalogTable,
    approved_query_to_flight_info,
    build_catalog_tables,
    catalog_table_to_arrow_schema,
    catalog_table_to_flight_info,
    fetch_approved_queries,
    fetch_approved_queries_async,
)
from provisa.compiler.parser import coerce_variable_defaults, parse_query
from provisa.compiler.rls import RLSContext, inject_rls
from provisa.compiler.sampling import apply_sampling, get_sample_size
from provisa.compiler.sql_gen import compile_query
from provisa.executor.formats.arrow import rows_to_arrow_table
from provisa.executor.direct import execute_direct
from provisa.security.rights import Capability, has_capability
from provisa.transpiler.router import Route, decide_route
from provisa.transpiler.transpile import transpile, transpile_to_trino

log = logging.getLogger(__name__)

_SQL_PREFIX = re.compile(r"^\s*(SELECT|WITH)\b", re.IGNORECASE)
_SQL_FROM = re.compile(r"\bFROM\s+(\w+)", re.IGNORECASE)
_SQL_LIMIT = re.compile(r"\bLIMIT\s+(\d+)", re.IGNORECASE)
_GQL_OP_NAME = re.compile(r"\bquery\s+(\w+)")
_WHERE_INT = re.compile(r'\b(\w+)\s*=\s*(-?\d+(?:\.\d+)?)\b')
_WHERE_STR = re.compile(r"\b(\w+)\s*=\s*'([^']*)'")
_CYPHER_PREFIX = re.compile(r"^\s*(MATCH|OPTIONAL\s+MATCH|CALL|WITH|MERGE|CREATE|RETURN)\b", re.IGNORECASE)


def _is_sql(query: str) -> bool:
    return bool(_SQL_PREFIX.match(query))


def _is_cypher(query: str) -> bool:
    return bool(_CYPHER_PREFIX.match(query))


def _parse_where_variables(sql: str) -> dict:
    """Extract key=value pairs from a SQL WHERE clause as a variables dict.

    Supports integer/float literals and single-quoted string literals.
    Used to map JDBC-style ``SELECT * FROM op WHERE k = v`` filters to
    GraphQL variable values.
    """
    variables: dict = {}
    where_match = re.search(r'\bWHERE\b(.*?)(?:\bLIMIT\b|$)', sql, re.IGNORECASE | re.DOTALL)
    if not where_match:
        return variables
    clause = where_match.group(1)
    for m in _WHERE_STR.finditer(clause):
        variables[m.group(1)] = m.group(2)
    for m in _WHERE_INT.finditer(clause):
        key = m.group(1)
        if key not in variables:
            raw = m.group(2)
            variables[key] = float(raw) if "." in raw else int(raw)
    return variables


class ProvisaFlightServer(flight.FlightServerBase):
    """Arrow Flight server that executes GraphQL queries and streams Arrow data.

    Supports three modes via handshake properties:
      - catalog: read-only JDBC catalog view of the semantic layer
      - approved: list approved persisted queries as virtual tables
      - (default): full GraphQL query execution
    """

    def __init__(self, state, location="grpc://0.0.0.0:8815", *, main_loop=None, **kwargs):
        super().__init__(location, **kwargs)
        self._state = state
        self._session_modes: dict[bytes, str] = {}  # token -> mode
        # The main event loop owns the asyncpg pools; dispatch coroutines to it.
        self._main_loop = main_loop or asyncio.get_event_loop()
        # Keep a local loop for non-pool async work.
        self._loop = asyncio.new_event_loop()

    @staticmethod
    def _parse_mode(buf: bytes | None) -> str:
        """Extract mode from handshake payload or ticket JSON."""
        if not buf:
            return "default"
        try:
            data = json.loads(buf.decode("utf-8"))
        except (json.JSONDecodeError, UnicodeDecodeError):
            return "default"
        return data.get("mode", "default")

    # ------------------------------------------------------------------
    # Flight SQL handshake
    # ------------------------------------------------------------------

    def do_handshake(self, context, payload):
        """Parse mode from handshake properties and return a session token."""
        buf = b""
        for chunk in payload:
            buf += chunk
        mode = self._parse_mode(buf)
        if mode not in ("catalog", "approved", "default"):
            raise flight.FlightServerError(
                f"Unknown mode: {mode!r}. Must be 'catalog', 'approved', or 'default'."
            )
        # Use the mode string itself as a simple session token
        token = json.dumps({"mode": mode}).encode("utf-8")
        self._session_modes[token] = mode
        return token, []

    # ------------------------------------------------------------------
    # list_flights — enumerate available data
    # ------------------------------------------------------------------

    def list_flights(self, context, criteria):
        """List available flights based on mode."""
        mode = self._mode_from_criteria(criteria)

        if mode == "catalog":
            tables = build_catalog_tables(self._state)
            for table in tables:
                yield catalog_table_to_flight_info(table)

        elif mode == "approved":
            queries = fetch_approved_queries(self._state)
            for q in queries:
                yield approved_query_to_flight_info(q)

        # Default mode: no flight listing (clients send ad-hoc queries)

    # ------------------------------------------------------------------
    # get_flight_info — metadata for a specific flight
    # ------------------------------------------------------------------

    def get_flight_info(self, context, descriptor):
        """Return FlightInfo for a descriptor.

        For catalog mode: descriptor path is [domain_id, table_name].
        For approved mode: descriptor path is ["approved", stable_id].
        """
        path = list(descriptor.path)

        # Approved mode: path = ["approved", stable_id]
        if len(path) == 2 and path[0] == b"approved":
            stable_id = path[1].decode("utf-8") if isinstance(path[1], bytes) else path[1]
            queries = fetch_approved_queries(self._state)
            for q in queries:
                if q.stable_id == stable_id:
                    return approved_query_to_flight_info(q)
            raise flight.FlightServerError(
                f"Approved query not found: {stable_id}"
            )

        # Catalog mode: path = [domain_id, table_name]
        if len(path) == 2:
            domain_id = path[0].decode("utf-8") if isinstance(path[0], bytes) else path[0]
            table_name = path[1].decode("utf-8") if isinstance(path[1], bytes) else path[1]
            tables = build_catalog_tables(self._state)
            for t in tables:
                if t.domain_id == domain_id and t.table_name == table_name:
                    return catalog_table_to_flight_info(t)
            raise flight.FlightServerError(
                f"Table not found: {domain_id}.{table_name}"
            )

        raise flight.FlightServerError(
            f"Invalid descriptor path: {path}"
        )

    # ------------------------------------------------------------------
    # get_schema — Arrow schema for a catalog table
    # ------------------------------------------------------------------

    def get_schema(self, context, descriptor):
        """Return the Arrow schema for a catalog table.

        Descriptor path: [domain_id, table_name].
        """
        path = list(descriptor.path)
        if len(path) != 2:
            raise flight.FlightServerError(
                f"get_schema requires path [domain, table], got {path}"
            )

        domain_id = path[0].decode("utf-8") if isinstance(path[0], bytes) else path[0]
        table_name = path[1].decode("utf-8") if isinstance(path[1], bytes) else path[1]

        tables = build_catalog_tables(self._state)
        for t in tables:
            if t.domain_id == domain_id and t.table_name == table_name:
                schema = catalog_table_to_arrow_schema(t)
                return flight.SchemaResult(schema)

        raise flight.FlightServerError(
            f"Table not found: {domain_id}.{table_name}"
        )

    # ------------------------------------------------------------------
    # do_get — execute query or return catalog data
    # ------------------------------------------------------------------

    def do_get(self, context, ticket):
        """Execute a query from the ticket and return Arrow record batches.

        Dispatch logic:
          1. If ticket contains 'query' → execute it; mode controls the schema:
               catalog / default → full governed schema (Stage 2 RLS/masking)
               approved          → approved query op names as virtual tables
          2. No 'query' → metadata fetch based on mode:
               catalog  → table/column listing
               approved → approved query listing
        """
        try:
            request = json.loads(ticket.ticket.decode("utf-8"))
        except (json.JSONDecodeError, UnicodeDecodeError) as e:
            raise flight.FlightServerError(f"Invalid ticket: {e}")

        mode = request.get("mode", "default")

        if request.get("query"):
            return self._execute_query(request, mode)

        if mode == "catalog":
            return self._do_get_catalog(ticket)
        if mode == "approved":
            return self._do_get_approved(ticket)

        raise flight.FlightServerError("Ticket must include 'query'")

    # ------------------------------------------------------------------
    # Private helpers
    # ------------------------------------------------------------------

    def _mode_from_criteria(self, criteria: bytes) -> str:
        """Extract mode from list_flights criteria bytes."""
        if not criteria:
            return "default"
        try:
            data = json.loads(criteria.decode("utf-8"))
            return data.get("mode", "default")
        except (json.JSONDecodeError, UnicodeDecodeError):
            return "default"

    def _do_get_catalog(self, ticket) -> flight.RecordBatchStream:
        """Return catalog metadata as Arrow record batches."""
        request = json.loads(ticket.ticket.decode("utf-8"))
        domain = request.get("domain")
        table_name = request.get("table")

        tables = build_catalog_tables(self._state)

        if domain and table_name:
            # Return schema info for a specific table as rows
            for t in tables:
                if t.domain_id == domain and t.table_name == table_name:
                    return flight.RecordBatchStream(self._build_columns_table(t))
            raise flight.FlightServerError(
                f"Table not found: {domain}.{table_name}"
            )

        # Return all tables as rows
        return flight.RecordBatchStream(self._build_catalog_table(tables, domain))

    def _do_get_approved(self, ticket) -> flight.RecordBatchStream:
        """Return approved query data as Arrow record batches."""
        request = json.loads(ticket.ticket.decode("utf-8"))
        stable_id = request.get("stable_id")

        queries = fetch_approved_queries(self._state)

        if stable_id:
            for q in queries:
                if q.stable_id == stable_id:
                    return flight.RecordBatchStream(
                        self._build_approved_query_table(q),
                    )
            raise flight.FlightServerError(
                f"Approved query not found: {stable_id}"
            )

        return flight.RecordBatchStream(
            self._build_approved_queries_table(queries),
        )

    @staticmethod
    def _build_catalog_table(
        tables: list[CatalogTable],
        domain_filter: str | None = None,
    ) -> pa.Table:
        """Build Arrow table listing catalog tables."""
        domains = []
        names = []
        descriptions = []
        for t in tables:
            if domain_filter and t.domain_id != domain_filter:
                continue
            domains.append(t.domain_id)
            names.append(t.table_name)
            descriptions.append(t.description)
        return pa.table({
            "schema_name": pa.array(domains, type=pa.utf8()),
            "table_name": pa.array(names, type=pa.utf8()),
            "description": pa.array(descriptions, type=pa.utf8()),
        })

    @staticmethod
    def _build_columns_table(cat_table: CatalogTable) -> pa.Table:
        """Build Arrow table of column metadata for a catalog table."""
        col_names = []
        col_types = []
        col_nullable = []
        col_descs = []
        for col in cat_table.columns:
            col_names.append(col.name)
            col_types.append(col.data_type)
            col_nullable.append(col.is_nullable)
            col_descs.append(col.description)
        return pa.table({
            "column_name": pa.array(col_names, type=pa.utf8()),
            "data_type": pa.array(col_types, type=pa.utf8()),
            "is_nullable": pa.array(col_nullable, type=pa.bool_()),
            "description": pa.array(col_descs, type=pa.utf8()),
        })

    @staticmethod
    def _build_approved_query_table(query: ApprovedQuery) -> pa.Table:
        """Build Arrow table for a single approved query."""
        return pa.table({
            "stable_id": [query.stable_id],
            "query_text": [query.query_text],
            "compiled_sql": [query.compiled_sql],
        })

    @staticmethod
    def _build_approved_queries_table(
        queries: list[ApprovedQuery],
    ) -> pa.Table:
        """Build Arrow table listing approved queries."""
        stable_ids = [q.stable_id for q in queries]
        query_texts = [q.query_text for q in queries]
        compiled_sqls = [q.compiled_sql for q in queries]
        return pa.table({
            "stable_id": pa.array(stable_ids, type=pa.utf8()),
            "query_text": pa.array(query_texts, type=pa.utf8()),
            "compiled_sql": pa.array(compiled_sqls, type=pa.utf8()),
        })

    def _compile_query(self, ticket_bytes):
        """Parse ticket, compile GraphQL to SQL, apply security pipeline."""
        try:
            request = json.loads(ticket_bytes.decode("utf-8"))
        except (json.JSONDecodeError, UnicodeDecodeError) as e:
            raise flight.FlightServerError(f"Invalid ticket: {e}")

        query_text = request.get("query")
        role_id = request.get("role", "admin")
        variables = request.get("variables")

        if not query_text:
            raise flight.FlightServerError("Ticket must include 'query'")

        if role_id not in self._state.schemas:
            raise flight.FlightServerError(f"No schema for role {role_id!r}")

        schema = self._state.schemas[role_id]
        ctx = self._state.contexts[role_id]
        rls = self._state.rls_contexts.get(role_id, RLSContext.empty())
        role = self._state.roles.get(role_id)

        document = parse_query(schema, query_text, variables)
        compiled_queries = compile_query(document, ctx, variables)
        if not compiled_queries:
            raise flight.FlightServerError("No query fields found")

        compiled = compiled_queries[0]

        decision = decide_route(
            sources=compiled.sources,
            source_types=self._state.source_types,
            source_dialects=self._state.source_dialects,
        )

        return document, ctx, rls, role, compiled, decision, variables

    def _do_get_sql(self, request: dict) -> flight.RecordBatchStream:
        """Execute SQL where FROM references an approved query operation name."""
        try:
            return self._do_get_sql_inner(request)
        except flight.FlightServerError:
            raise
        except Exception as exc:
            log.exception("SQL flight execution failed")
            raise flight.FlightServerError(f"SQL execution failed: {type(exc).__name__}: {exc}") from exc

    def _do_get_sql_inner(self, request: dict) -> flight.RecordBatchStream:
        sql = request.get("query", "")
        role_id = request.get("role", "admin")

        from_match = _SQL_FROM.search(sql)
        if not from_match:
            raise flight.FlightServerError("Cannot parse FROM clause")
        op_name = from_match.group(1)

        limit_match = _SQL_LIMIT.search(sql)
        limit = int(limit_match.group(1)) if limit_match else None

        approved = asyncio.run_coroutine_threadsafe(
            fetch_approved_queries_async(self._state), self._main_loop
        ).result()
        matched = None
        for q in approved:
            m = _GQL_OP_NAME.search(q.query_text or "")
            if m and m.group(1) == op_name:
                matched = q
                break

        if matched is None:
            raise flight.FlightServerError(f"Approved query not found: {op_name}")

        if role_id not in self._state.schemas:
            raise flight.FlightServerError(f"Unknown role: {role_id}")

        ctx = self._state.contexts[role_id]
        rls = self._state.rls_contexts.get(role_id, RLSContext.empty())
        role = self._state.roles.get(role_id)
        schema = self._state.schemas[role_id]

        # Fix 1: ticket-level variables (Python Flight client path)
        ticket_variables: dict = request.get("variables") or {}
        # Fix 2: WHERE clause variables (JDBC tool path)
        where_variables = _parse_where_variables(sql)
        # WHERE variables are overrides; ticket variables take precedence
        variables: dict = {**where_variables, **ticket_variables}

        document = parse_query(schema, matched.query_text, variables or None)
        # Fix 3: apply declared GraphQL defaults for any still-missing vars
        variables = coerce_variable_defaults(document, variables)
        compiled_queries = compile_query(document, ctx, variables or None)
        if not compiled_queries:
            raise flight.FlightServerError("No query fields in approved query")

        compiled = compiled_queries[0]
        decision = decide_route(
            sources=compiled.sources,
            source_types=self._state.source_types,
            source_dialects=self._state.source_dialects,
        )

        compiled_for_exec = inject_rls(compiled, ctx, rls)
        sampling = not has_capability(role, Capability.FULL_RESULTS) if role else True
        if sampling:
            compiled_for_exec = apply_sampling(compiled_for_exec, get_sample_size())

        target_sql = transpile(compiled_for_exec.sql, decision.dialect or "postgres")
        result = asyncio.run_coroutine_threadsafe(
            execute_direct(
                self._state.source_pools, decision.source_id,
                target_sql, compiled_for_exec.params,
            ),
            self._main_loop,
        ).result()
        table = rows_to_arrow_table(result.rows, compiled.columns)
        if limit is not None:
            table = table.slice(0, limit)
        return flight.RecordBatchStream(table)

    def _do_get_cypher(self, request: dict) -> flight.RecordBatchStream:
        """Execute a Cypher query ticket and return Arrow record batches."""
        import json as _json
        import sqlglot
        from provisa.cypher.parser import parse_cypher, CypherParseError
        from provisa.cypher.label_map import CypherLabelMap
        from provisa.cypher.translator import cypher_to_sql, CypherCrossSourceError, CypherTranslateError
        from provisa.cypher.graph_rewriter import apply_graph_rewrites
        from provisa.cypher.params import collect_param_names, bind_params, CypherParamError
        from provisa.cypher.assembler import assemble_rows, to_serializable
        from provisa.compiler.rls import RLSContext
        from provisa.compiler.sql_gen import make_semantic_sql, rewrite_semantic_to_trino_physical
        from provisa.compiler.stage2 import apply_governance, build_governance_context

        query_text = request.get("query", "")
        role_id = request.get("role", "admin")
        params = request.get("params") or {}

        if role_id not in self._state.contexts:
            raise flight.FlightServerError(f"No schema for role {role_id!r}")

        ctx = self._state.contexts[role_id]
        rls = self._state.rls_contexts.get(role_id, RLSContext.empty())

        try:
            ast = parse_cypher(query_text)
        except CypherParseError as exc:
            raise flight.FlightServerError(f"Cypher parse error: {exc}")

        label_map = CypherLabelMap.from_schema(ctx)

        param_names = collect_param_names(query_text)
        try:
            bind_params(param_names, params)
        except CypherParamError as exc:
            raise flight.FlightServerError(f"Cypher param error: {exc}")

        try:
            sql_ast, ordered_params, graph_vars = cypher_to_sql(ast, label_map, params)
        except (CypherCrossSourceError, CypherTranslateError) as exc:
            raise flight.FlightServerError(f"Cypher translate error: {exc}")

        sql_ast = apply_graph_rewrites(sql_ast, graph_vars, label_map)

        try:
            sql_str = sql_ast.sql(dialect="postgres")
        except Exception as exc:
            raise flight.FlightServerError(f"Cypher SQL render failed: {exc}")

        gov_ctx = build_governance_context(
            role_id, rls, self._state.masking_rules, ctx,
            getattr(self._state, "tables", []),
        )
        semantic_sql = make_semantic_sql(sql_str, ctx)
        governed_sql = apply_governance(semantic_sql, gov_ctx)
        exec_sql = rewrite_semantic_to_trino_physical(governed_sql, ctx)

        try:
            trino_sql = sqlglot.transpile(exec_sql, read="postgres", write="trino")[0]
        except Exception as exc:
            raise flight.FlightServerError(f"Cypher transpile failed: {exc}")

        resolved_params = [params.get(name) for name in ordered_params]

        trino_conn = getattr(self._state, "trino_conn", None)
        if trino_conn is None:
            raise flight.FlightServerError("Federation engine not connected")

        def _run() -> list[dict]:
            cursor = trino_conn.cursor()
            try:
                cursor.execute(trino_sql, resolved_params or [])
                cols = [d[0] for d in (cursor.description or [])]
                return [dict(zip(cols, row)) for row in cursor.fetchall()]
            finally:
                cursor.close()

        import concurrent.futures
        with concurrent.futures.ThreadPoolExecutor(max_workers=1) as pool:
            raw_rows = pool.submit(_run).result()

        assembled = assemble_rows(raw_rows, graph_vars)
        serialized = [to_serializable(r) for r in assembled]

        if not serialized:
            columns = list(graph_vars.keys()) if graph_vars else []
            empty = {col: pa.array([], type=pa.utf8()) for col in columns}
            return flight.RecordBatchStream(pa.table(empty))

        col_names = list(serialized[0].keys())
        col_data: dict[str, list] = {c: [] for c in col_names}
        for row in serialized:
            for col in col_names:
                val = row.get(col)
                col_data[col].append(
                    json.dumps(val) if isinstance(val, (dict, list)) else val
                )
        return flight.RecordBatchStream(pa.table(col_data))

    def _execute_query(self, request: dict, mode: str) -> flight.RecordBatchStream:
        """Dispatch a query to the correct handler based on language × mode.

        catalog / default + SQL  → governed schema (Stage 2 RLS/masking/visibility)
        approved         + SQL  → approved query op names as virtual tables
        any mode         + Cypher → governed Cypher pipeline
        any mode         + GraphQL → compiled GraphQL pipeline
        stable_id        → fetch approved query, detect language, re-dispatch
        """
        stable_id = request.get("stable_id")
        if stable_id:
            queries = asyncio.run_coroutine_threadsafe(
                fetch_approved_queries_async(self._state), self._main_loop
            ).result()
            matched = next((q for q in queries if q.stable_id == stable_id), None)
            if matched is None:
                raise flight.FlightServerError(f"Approved query not found: {stable_id!r}")
            new_request = {k: v for k, v in request.items() if k != "stable_id"}
            new_request["query"] = matched.query_text or ""
            return self._execute_query(new_request, "approved")

        query_text = request.get("query", "")
        if _is_cypher(query_text):
            return self._do_get_cypher(request)
        if _is_sql(query_text):
            if mode == "approved":
                return self._do_get_sql(request)
            return self._do_get_sql_governed(request)
        return self._do_get_graphql(request, mode)

    def _do_get_sql_governed(self, request: dict) -> flight.RecordBatchStream:
        """Execute SQL through Stage 2 governance against the full role schema.

        Used for catalog and default modes. Applies RLS, column masking, and
        visibility rules for the request role before execution.
        """
        import sqlglot
        import sqlglot.expressions as exp
        from provisa.compiler.rls import RLSContext
        from provisa.compiler.sql_gen import rewrite_semantic_to_physical
        from provisa.compiler.stage2 import apply_governance, build_governance_context, extract_sources
        from provisa.executor.direct import execute_direct
        from provisa.executor.trino import execute_trino

        sql = request.get("query", "")
        role_id = request.get("role", "admin")

        if role_id not in self._state.contexts:
            raise flight.FlightServerError(f"No schema for role {role_id!r}")

        ctx = self._state.contexts[role_id]
        rls = self._state.rls_contexts.get(role_id, RLSContext.empty())
        role = self._state.roles.get(role_id)

        try:
            parsed_tree = sqlglot.parse_one(sql, read="postgres")
        except Exception as exc:
            raise flight.FlightServerError(f"SQL parse error: {exc}")

        gov_ctx = build_governance_context(
            role_id, rls, self._state.masking_rules, ctx,
            getattr(self._state, "tables", []),
        )

        forbidden = [
            (f"{t.db}.{t.name}" if t.db else t.name)
            for t in parsed_tree.find_all(exp.Table)
            if (f"{t.db}.{t.name}" if t.db else t.name) not in gov_ctx.table_map
            and t.name not in gov_ctx.table_map
        ]
        if forbidden:
            raise flight.FlightServerError(
                f"Tables not accessible for role {role_id!r}: {', '.join(forbidden)}"
            )

        governed = apply_governance(sql, gov_ctx)
        sources = extract_sources(governed, gov_ctx, ctx)
        _default_source = next(
            (sid for sid, t in self._state.source_types.items() if t in ("postgresql", "mysql", "sqlite")),
            next(iter(self._state.source_pools), "pg"),
        )
        decision = decide_route(
            sources=sources or {_default_source},
            source_types=self._state.source_types,
            source_dialects=self._state.source_dialects,
        )
        physical = rewrite_semantic_to_physical(governed, ctx)

        if decision.route == Route.TRINO:
            sql_to_run = transpile_to_trino(physical)
            result = asyncio.run_coroutine_threadsafe(
                execute_trino(sql_to_run, []), self._main_loop
            ).result()
        else:
            sql_to_run = transpile(physical, decision.dialect or "postgres")
            result = asyncio.run_coroutine_threadsafe(
                execute_direct(
                    self._state.source_pools,
                    decision.source_id or _default_source,
                    sql_to_run, [],
                ),
                self._main_loop,
            ).result()

        from provisa.compiler.sql_gen import ColumnRef
        columns = [ColumnRef(field_name=c, column=c) for c in result.column_names]
        table = rows_to_arrow_table(result.rows, columns)
        return flight.RecordBatchStream(table)

    def _do_get_graphql(self, request: dict, mode: str = "default") -> flight.RecordBatchStream:
        """Execute a GraphQL query ticket and return Arrow record batches.

        In approved mode, the operation name must match an approved query entry.
        """
        if mode == "approved":
            query_text = request.get("query", "")
            m = _GQL_OP_NAME.search(query_text)
            op_name = m.group(1) if m else None
            if not op_name:
                raise flight.FlightServerError(
                    "approved mode requires a named GraphQL operation"
                )
            approved = fetch_approved_queries(self._state)
            approved_names = {
                mm.group(1)
                for q in approved
                if (mm := _GQL_OP_NAME.search(q.query_text or ""))
            }
            if op_name not in approved_names:
                raise flight.FlightServerError(
                    f"Operation {op_name!r} is not an approved query"
                )

        ticket_bytes = json.dumps(request).encode("utf-8")
        document, ctx, rls, role, compiled, decision, variables = \
            self._compile_query(ticket_bytes)

        compiled_for_exec = compiled
        sampling = not has_capability(role, Capability.FULL_RESULTS) if role else True

        if decision.route == Route.DIRECT and decision.source_id:
            compiled_for_exec = inject_rls(compiled_for_exec, ctx, rls)
            if sampling:
                compiled_for_exec = apply_sampling(compiled_for_exec, get_sample_size())
            target_sql = transpile(compiled_for_exec.sql, decision.dialect or "postgres")
            result = asyncio.run_coroutine_threadsafe(
                execute_direct(
                    self._state.source_pools, decision.source_id,
                    target_sql, compiled_for_exec.params,
                ),
                self._main_loop,
            ).result()
            table = rows_to_arrow_table(result.rows, compiled.columns)
            return flight.RecordBatchStream(table)

        compiled_for_exec = compile_query(
            document, ctx, variables, use_catalog=True,
        )[0]
        compiled_for_exec = inject_rls(compiled_for_exec, ctx, rls)
        if sampling:
            compiled_for_exec = apply_sampling(compiled_for_exec, get_sample_size())
        trino_sql = transpile_to_trino(compiled_for_exec.sql)

        if self._state.flight_client is None:
            raise flight.FlightServerError(
                "Zaychik Flight SQL proxy is not configured. "
                "Set ZAYCHIK_HOST/ZAYCHIK_PORT and ensure the service is running."
            )
        from provisa.executor.trino_flight import execute_trino_flight_stream
        arrow_schema, batch_gen = execute_trino_flight_stream(
            self._state.flight_client, trino_sql, compiled_for_exec.params,
        )
        return flight.GeneratorStream(arrow_schema, batch_gen)
