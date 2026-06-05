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

The catalog path exposes the semantic layer as a read-only JDBC catalog.
"""

from __future__ import annotations

import asyncio
import json
import logging
import re
from collections.abc import Iterable, Iterator
from typing import TYPE_CHECKING, cast

import pyarrow as pa
import pyarrow.flight as flight

from provisa.api.flight.catalog import (
    CatalogTable,
    build_catalog_tables,
    catalog_table_to_arrow_schema,
    catalog_table_to_flight_info,
)
from provisa.compiler.parser import parse_query
from provisa.compiler.rls import RLSContext, inject_rls
from provisa.compiler.sampling import apply_sampling, get_sample_size
from provisa.compiler.sql_gen import compile_query
from provisa.executor.direct import execute_direct
from provisa.executor.formats.arrow import rows_to_arrow_table
from provisa.security.rights import Capability, has_capability
from provisa.transpiler.router import Route, decide_route
from provisa.transpiler.transpile import transpile, transpile_to_trino

if TYPE_CHECKING:
    from graphql import DocumentNode, GraphQLSchema

    from provisa.api.app import AppState
    from provisa.compiler.sql_gen import CompilationContext, CompiledQuery
    from provisa.transpiler.router import RouteDecision

log = logging.getLogger(__name__)

_SQL_PREFIX = re.compile(r"^\s*(SELECT|WITH)\b", re.IGNORECASE)
_SQL_FROM = re.compile(r"\bFROM\s+(\w+)", re.IGNORECASE)
_SQL_LIMIT = re.compile(r"\bLIMIT\s+(\d+)", re.IGNORECASE)
_GQL_OP_NAME = re.compile(r"\bquery\s+(\w+)")
_WHERE_INT = re.compile(r"\b(\w+)\s*=\s*(-?\d+(?:\.\d+)?)\b")
_WHERE_STR = re.compile(r"\b(\w+)\s*=\s*'([^']*)'")
_CYPHER_PREFIX = re.compile(
    r"^\s*(MATCH|OPTIONAL\s+MATCH|CALL|WITH|MERGE|CREATE|RETURN)\b", re.IGNORECASE
)


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
    variables: dict[str, str | int | float] = {}
    where_match = re.search(r"\bWHERE\b(.*?)(?:\bLIMIT\b|$)", sql, re.IGNORECASE | re.DOTALL)
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


def _parse_limit_value(value: object) -> int | None:  # object-ok: value comes from a JSON-parsed dict with heterogeneous value types
    if value is None:
        return None
    if isinstance(value, bool) or not isinstance(value, int):
        raise flight.FlightServerError("limit must be a non-negative integer")  # pyright: ignore[reportPrivateImportUsage]  # lib omits __all__
    if value < 0:
        raise flight.FlightServerError("limit must be a non-negative integer")  # pyright: ignore[reportPrivateImportUsage]  # lib omits __all__
    return value


class ProvisaFlightServer(flight.FlightServerBase):  # pyright: ignore[reportPrivateImportUsage]  # lib omits __all__
    """Arrow Flight server that executes GraphQL queries and streams Arrow data."""

    def __init__(
        self,
        state: AppState,
        location: str = "grpc://0.0.0.0:8815",
        *,
        main_loop: asyncio.AbstractEventLoop | None = None,
        **kwargs: object,  # object-ok: forwarded verbatim to FlightServerBase.__init__ which accepts arbitrary keyword args  # pyright: ignore[reportPrivateImportUsage]  # lib omits __all__
    ) -> None:
        super().__init__(location, **kwargs)
        self._state = state
        # The main event loop owns the asyncpg pools; dispatch coroutines to it.
        self._main_loop = main_loop or asyncio.get_event_loop()
        # Keep a local loop for non-pool async work.
        self._loop = asyncio.new_event_loop()

    # ------------------------------------------------------------------
    # Flight SQL handshake
    # ------------------------------------------------------------------

    def do_handshake(
        self,
        context: flight.ServerCallContext,  # noqa: ARG002  # required by Flight override signature  # pyright: ignore[reportPrivateImportUsage]  # lib omits __all__
        payload: Iterable[bytes],
    ) -> tuple[bytes, list[object]]:
        """Parse role from handshake properties and return a session token."""
        buf = b""
        for chunk in payload:
            buf += chunk
        try:
            data = json.loads(buf.decode("utf-8")) if buf else {}
        except (json.JSONDecodeError, UnicodeDecodeError):
            data = {}
        role_id = data.get("role", "")
        token = json.dumps({"role": role_id}).encode("utf-8")
        return token, []

    # ------------------------------------------------------------------
    # list_flights — enumerate available data
    # ------------------------------------------------------------------

    def list_flights(
        self,
        context: flight.ServerCallContext,  # noqa: ARG002  # required by Flight override signature  # pyright: ignore[reportPrivateImportUsage]  # lib omits __all__
        criteria: bytes,  # noqa: ARG002  # required by Flight override signature
    ) -> Iterator[flight.FlightInfo]:  # pyright: ignore[reportPrivateImportUsage]  # lib omits __all__
        """List available flights (catalog tables)."""
        tables = build_catalog_tables(self._state)
        for table in tables:
            yield catalog_table_to_flight_info(table)

    # ------------------------------------------------------------------
    # get_flight_info — metadata for a specific flight
    # ------------------------------------------------------------------

    def get_flight_info(
        self,
        context: flight.ServerCallContext,  # noqa: ARG002  # required by Flight override signature  # pyright: ignore[reportPrivateImportUsage]  # lib omits __all__
        descriptor: flight.FlightDescriptor,  # pyright: ignore[reportPrivateImportUsage]  # lib omits __all__
    ) -> flight.FlightInfo:  # pyright: ignore[reportPrivateImportUsage]  # lib omits __all__
        """Return FlightInfo for a catalog table descriptor.  # pyright: ignore[reportPrivateImportUsage]  # lib omits __all__

        Descriptor path: [domain_id, table_name].
        """
        path = list(descriptor.path)

        if len(path) == 2:
            domain_id = path[0].decode("utf-8") if isinstance(path[0], bytes) else path[0]
            table_name = path[1].decode("utf-8") if isinstance(path[1], bytes) else path[1]
            tables = build_catalog_tables(self._state)
            for t in tables:
                if t.domain_id == domain_id and t.table_name == table_name:
                    return catalog_table_to_flight_info(t)
            raise flight.FlightServerError(f"Table not found: {domain_id}.{table_name}")  # pyright: ignore[reportPrivateImportUsage]  # lib omits __all__

        raise flight.FlightServerError(f"Invalid descriptor path: {path}")  # pyright: ignore[reportPrivateImportUsage]  # lib omits __all__

    # ------------------------------------------------------------------
    # get_schema — Arrow schema for a catalog table
    # ------------------------------------------------------------------

    def get_schema(
        self,
        context: flight.ServerCallContext,  # noqa: ARG002  # required by Flight override signature  # pyright: ignore[reportPrivateImportUsage]  # lib omits __all__
        descriptor: flight.FlightDescriptor,  # pyright: ignore[reportPrivateImportUsage]  # lib omits __all__
    ) -> flight.SchemaResult:  # pyright: ignore[reportPrivateImportUsage]  # lib omits __all__
        """Return the Arrow schema for a catalog table.

        Descriptor path: [domain_id, table_name].
        """
        path = list(descriptor.path)
        if len(path) != 2:
            raise flight.FlightServerError(f"get_schema requires path [domain, table], got {path}")  # pyright: ignore[reportPrivateImportUsage]  # lib omits __all__

        domain_id = path[0].decode("utf-8") if isinstance(path[0], bytes) else path[0]
        table_name = path[1].decode("utf-8") if isinstance(path[1], bytes) else path[1]

        tables = build_catalog_tables(self._state)
        for t in tables:
            if t.domain_id == domain_id and t.table_name == table_name:
                schema = catalog_table_to_arrow_schema(t)
                return flight.SchemaResult(schema)  # pyright: ignore[reportPrivateImportUsage]  # lib omits __all__

        raise flight.FlightServerError(f"Table not found: {domain_id}.{table_name}")  # pyright: ignore[reportPrivateImportUsage]  # lib omits __all__

    # ------------------------------------------------------------------
    # do_get — execute query or return catalog data
    # ------------------------------------------------------------------

    def do_get(
        self,
        context: flight.ServerCallContext,  # noqa: ARG002  # required by Flight override signature  # pyright: ignore[reportPrivateImportUsage]  # lib omits __all__
        ticket: flight.Ticket,  # pyright: ignore[reportPrivateImportUsage]  # lib omits __all__
    ) -> flight.RecordBatchStream | flight.GeneratorStream:  # pyright: ignore[reportPrivateImportUsage]  # lib omits __all__
        """Execute a query from the ticket and return Arrow record batches.

        Dispatch logic:
          1. If ticket contains 'query' → execute it through the governed pipeline.
          2. No 'query' → catalog metadata fetch (table/column listing).
        """
        try:
            request = json.loads(ticket.ticket.decode("utf-8"))
        except (json.JSONDecodeError, UnicodeDecodeError) as e:
            raise flight.FlightServerError(f"Invalid ticket: {e}") from e  # pyright: ignore[reportPrivateImportUsage]  # lib omits __all__

        if request.get("query"):
            return self._execute_query(request)

        return self._do_get_catalog(ticket)

    # ------------------------------------------------------------------
    # Private helpers
    # ------------------------------------------------------------------

    def _do_get_catalog(self, ticket: flight.Ticket) -> flight.RecordBatchStream:  # pyright: ignore[reportPrivateImportUsage]  # lib omits __all__
        """Return catalog metadata as Arrow record batches."""
        request = json.loads(ticket.ticket.decode("utf-8"))
        domain = request.get("domain")
        table_name = request.get("table")

        tables = build_catalog_tables(self._state)

        if domain and table_name:
            # Return schema info for a specific table as rows
            for t in tables:
                if t.domain_id == domain and t.table_name == table_name:
                    return flight.RecordBatchStream(self._build_columns_table(t))  # pyright: ignore[reportPrivateImportUsage]  # lib omits __all__
            raise flight.FlightServerError(f"Table not found: {domain}.{table_name}")  # pyright: ignore[reportPrivateImportUsage]  # lib omits __all__

        # Return all tables as rows
        return flight.RecordBatchStream(self._build_catalog_table(tables, domain))  # pyright: ignore[reportPrivateImportUsage]  # lib omits __all__

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
        return pa.table(
            {
                "schema_name": pa.array(domains, type=pa.utf8()),
                "table_name": pa.array(names, type=pa.utf8()),
                "description": pa.array(descriptions, type=pa.utf8()),
            }
        )

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
        return pa.table(
            {
                "column_name": pa.array(col_names, type=pa.utf8()),
                "data_type": pa.array(col_types, type=pa.utf8()),
                "is_nullable": pa.array(col_nullable, type=pa.bool_()),
                "description": pa.array(col_descs, type=pa.utf8()),
            }
        )

    def _compile_query(
        self, ticket_bytes: bytes
    ) -> tuple[
        DocumentNode,
        CompilationContext,
        RLSContext,
        dict[str, object] | None,
        CompiledQuery,
        RouteDecision,
        dict[str, object] | None,
    ]:
        """Parse ticket, compile GraphQL to SQL, apply security pipeline."""
        try:
            request = json.loads(ticket_bytes.decode("utf-8"))
        except (json.JSONDecodeError, UnicodeDecodeError) as e:
            raise flight.FlightServerError(f"Invalid ticket: {e}") from e  # pyright: ignore[reportPrivateImportUsage]  # lib omits __all__

        query_text = request.get("query")
        role_id = request.get("role", "admin")
        variables = request.get("variables")

        if not query_text:
            raise flight.FlightServerError("Ticket must include 'query'")  # pyright: ignore[reportPrivateImportUsage]  # lib omits __all__

        if role_id not in self._state.schemas:
            raise flight.FlightServerError(f"No schema for role {role_id!r}")  # pyright: ignore[reportPrivateImportUsage]  # lib omits __all__

        schema = cast("GraphQLSchema", self._state.schemas[role_id])
        ctx = self._state.contexts[role_id]
        rls = self._state.rls_contexts.get(role_id, RLSContext.empty())
        role = self._state.roles.get(role_id)

        document = parse_query(schema, query_text, variables)
        compiled_queries = compile_query(document, ctx, variables)
        if not compiled_queries:
            raise flight.FlightServerError("No query fields found")  # pyright: ignore[reportPrivateImportUsage]  # lib omits __all__

        compiled = compiled_queries[0]

        decision = decide_route(
            sources=compiled.sources,
            source_types=self._state.source_types,
            source_dialects=self._state.source_dialects,
            source_dsns=getattr(self._state, "source_dsns", None),
        )

        return document, ctx, rls, role, compiled, decision, variables

    def _do_get_cypher(self, request: dict[str, object]) -> flight.RecordBatchStream:  # pyright: ignore[reportPrivateImportUsage]  # lib omits __all__
        """Execute a Cypher query ticket and return Arrow record batches."""
        import concurrent.futures

        import sqlglot

        from provisa.compiler.rls import RLSContext
        from provisa.compiler.sql_gen import (
            make_semantic_sql,
            rewrite_semantic_to_trino_physical,
        )
        from provisa.compiler.stage2 import apply_governance, build_governance_context
        from provisa.cypher.assembler import assemble_rows, to_serializable
        from provisa.cypher.graph_rewriter import apply_graph_rewrites
        from provisa.cypher.label_map import CypherLabelMap
        from provisa.cypher.params import (
            CypherParamError,
            bind_params,
            collect_param_names,
        )
        from provisa.cypher.parser import CypherParseError, parse_cypher
        from provisa.cypher.translator import (
            CypherCrossSourceError,
            CypherTranslateError,
            cypher_to_sql,
        )

        query_text = str(request.get("query", ""))
        role_id = str(request.get("role", "admin"))
        params_obj = request.get("params") or {}
        params: dict[str, object] = params_obj if isinstance(params_obj, dict) else {}

        if role_id not in self._state.contexts:
            raise flight.FlightServerError(f"No schema for role {role_id!r}")  # pyright: ignore[reportPrivateImportUsage]  # lib omits __all__

        ctx = self._state.contexts[role_id]
        rls = self._state.rls_contexts.get(role_id, RLSContext.empty())

        try:
            ast = parse_cypher(query_text)
        except CypherParseError as exc:
            raise flight.FlightServerError(f"Cypher parse error: {exc}") from exc  # pyright: ignore[reportPrivateImportUsage]  # lib omits __all__

        label_map = CypherLabelMap.from_schema(ctx)

        param_names = collect_param_names(query_text)
        try:
            bind_params(param_names, params)
        except CypherParamError as exc:
            raise flight.FlightServerError(f"Cypher param error: {exc}") from exc  # pyright: ignore[reportPrivateImportUsage]  # lib omits __all__

        try:
            sql_ast, ordered_params, graph_vars = cypher_to_sql(ast, label_map, params)
        except (CypherCrossSourceError, CypherTranslateError) as exc:
            raise flight.FlightServerError(f"Cypher translate error: {exc}") from exc  # pyright: ignore[reportPrivateImportUsage]  # lib omits __all__

        sql_ast = apply_graph_rewrites(sql_ast, graph_vars, label_map)

        try:
            sql_str = sql_ast.sql(dialect="postgres")
        except Exception as exc:
            raise flight.FlightServerError(f"Cypher SQL render failed: {exc}") from exc  # pyright: ignore[reportPrivateImportUsage]  # lib omits __all__

        gov_ctx = build_governance_context(
            role_id,
            rls,
            self._state.masking_rules,
            ctx,
            getattr(self._state, "tables", []),
        )
        semantic_sql = make_semantic_sql(sql_str, ctx)
        governed_sql = apply_governance(semantic_sql, gov_ctx)
        exec_sql = rewrite_semantic_to_trino_physical(governed_sql, ctx)

        try:
            trino_sql = sqlglot.transpile(exec_sql, read="postgres", write="trino")[0]
        except Exception as exc:
            raise flight.FlightServerError(f"Cypher transpile failed: {exc}") from exc  # pyright: ignore[reportPrivateImportUsage]  # lib omits __all__

        resolved_params = [params.get(name) for name in ordered_params]

        trino_conn = getattr(self._state, "trino_conn", None)
        if trino_conn is None:
            raise flight.FlightServerError("Federation engine not connected")  # pyright: ignore[reportPrivateImportUsage]  # lib omits __all__

        def _run() -> list[dict[str, object]]:
            cursor = trino_conn.cursor()
            try:
                cursor.execute(trino_sql, resolved_params or [])
                cols = [d[0] for d in (cursor.description or [])]
                return [dict(zip(cols, row, strict=False)) for row in cursor.fetchall()]
            finally:
                cursor.close()

        with concurrent.futures.ThreadPoolExecutor(max_workers=1) as pool:
            raw_rows = pool.submit(_run).result()

        assembled = assemble_rows(raw_rows, graph_vars)
        serialized = [to_serializable(r) for r in assembled]

        if not serialized:
            columns = list(graph_vars.keys()) if graph_vars else []
            empty = {col: pa.array([], type=pa.utf8()) for col in columns}
            return flight.RecordBatchStream(pa.table(empty))  # pyright: ignore[reportPrivateImportUsage]  # lib omits __all__

        col_names = list(serialized[0].keys())
        col_data: dict[str, list[object]] = {c: [] for c in col_names}
        for row in serialized:
            for col in col_names:
                val = row.get(col)
                col_data[col].append(json.dumps(val) if isinstance(val, (dict, list)) else val)
        return flight.RecordBatchStream(pa.table(col_data))  # pyright: ignore[reportPrivateImportUsage]  # lib omits __all__

    def _execute_query(
        self, request: dict[str, object]
    ) -> flight.RecordBatchStream | flight.GeneratorStream:  # pyright: ignore[reportPrivateImportUsage]  # lib omits __all__
        """Dispatch a query to the correct handler based on language."""
        query_text = str(request.get("query", ""))
        if _is_cypher(query_text):
            return self._do_get_cypher(request)
        if _is_sql(query_text):
            return self._do_get_sql_governed(request)
        return self._do_get_graphql(request)

    def _do_get_sql_governed(self, request: dict[str, object]) -> flight.RecordBatchStream:  # pyright: ignore[reportPrivateImportUsage]  # lib omits __all__
        """Execute SQL through Stage 2 governance against the full role schema.

        Applies RLS, column masking, and visibility rules for the request role
        before execution.
        """
        import sqlglot
        import sqlglot.expressions as exp

        from provisa.compiler.rls import RLSContext
        from provisa.compiler.sql_gen import rewrite_semantic_to_physical
        from provisa.compiler.stage2 import (
            apply_governance,
            build_governance_context,
            extract_sources,
        )
        from provisa.executor.direct import execute_direct
        from provisa.executor.trino_flight import execute_trino_flight_arrow

        sql = str(request.get("query", ""))
        role_id = str(request.get("role", "admin"))

        if role_id not in self._state.contexts:
            raise flight.FlightServerError(f"No schema for role {role_id!r}")  # pyright: ignore[reportPrivateImportUsage]  # lib omits __all__

        ctx = self._state.contexts[role_id]
        rls = self._state.rls_contexts.get(role_id, RLSContext.empty())
        role = self._state.roles.get(role_id)

        if role and not has_capability(role, Capability.AD_HOC_QUERY):
            raise flight.FlightServerError(  # pyright: ignore[reportPrivateImportUsage]  # lib omits __all__
                f"Role {role_id!r} lacks required capability: ad_hoc_query"
            )

        try:
            parsed_tree = sqlglot.parse_one(sql, read="postgres")
        except Exception as exc:
            raise flight.FlightServerError(f"SQL parse error: {exc}") from exc  # pyright: ignore[reportPrivateImportUsage]  # lib omits __all__

        gov_ctx = build_governance_context(
            role_id,
            rls,
            self._state.masking_rules,
            ctx,
            getattr(self._state, "tables", []),
        )

        forbidden = [
            (f"{t.db}.{t.name}" if t.db else t.name)
            for t in parsed_tree.find_all(exp.Table)
            if (f"{t.db}.{t.name}" if t.db else t.name) not in gov_ctx.table_map
            and t.name not in gov_ctx.table_map
        ]
        if forbidden:
            raise flight.FlightServerError(  # pyright: ignore[reportPrivateImportUsage]  # lib omits __all__
                f"Tables not accessible for role {role_id!r}: {', '.join(forbidden)}"
            )

        governed = apply_governance(sql, gov_ctx)
        sources = extract_sources(governed, gov_ctx, ctx)
        _default_source = next(
            (
                sid
                for sid, t in self._state.source_types.items()
                if t in ("postgresql", "mysql", "sqlite")
            ),
            next(iter(self._state.source_pools), "pg"),  # type: ignore[call-overload]  # preexisting: SourcePool iteration relied on implicit Any before _state was typed; behavior preserved
        )
        decision = decide_route(
            sources=sources or {_default_source},
            source_types=self._state.source_types,
            source_dialects=self._state.source_dialects,
            source_dsns=getattr(self._state, "source_dsns", None),
        )
        physical = rewrite_semantic_to_physical(governed, ctx)

        if decision.route == Route.TRINO:
            sql_to_run = transpile_to_trino(physical)
            if self._state.flight_client is None:
                raise flight.FlightServerError(  # pyright: ignore[reportPrivateImportUsage]  # lib omits __all__
                    "Zaychik Flight SQL proxy is not configured. "
                    "Set ZAYCHIK_HOST/ZAYCHIK_PORT and ensure the service is running."
                )
            table = execute_trino_flight_arrow(self._state.flight_client, sql_to_run, [])
            return flight.RecordBatchStream(table)  # pyright: ignore[reportPrivateImportUsage]  # lib omits __all__
        else:
            sql_to_run = transpile(physical, decision.dialect or "postgres")
            result = asyncio.run_coroutine_threadsafe(
                execute_direct(
                    self._state.source_pools,
                    decision.source_id or _default_source,
                    sql_to_run,
                    [],
                ),
                self._main_loop,
            ).result()

        from provisa.compiler.sql_gen import ColumnRef

        columns = [
            ColumnRef(field_name=c, column=c, alias=None, nested_in=None)
            for c in result.column_names
        ]
        table = rows_to_arrow_table(result.rows, columns)
        return flight.RecordBatchStream(table)  # pyright: ignore[reportPrivateImportUsage]  # lib omits __all__

    def _do_get_graphql(
        self, request: dict[str, object]
    ) -> flight.RecordBatchStream | flight.GeneratorStream:  # pyright: ignore[reportPrivateImportUsage]  # lib omits __all__
        """Execute a GraphQL query ticket and return Arrow record batches."""
        role_id = str(request.get("role", ""))
        _role = self._state.roles.get(role_id)
        if _role and not has_capability(_role, Capability.AD_HOC_QUERY):
            raise flight.FlightServerError(  # pyright: ignore[reportPrivateImportUsage]  # lib omits __all__
                f"Role {role_id!r} lacks required capability: ad_hoc_query"
            )

        ticket_bytes = json.dumps(request).encode("utf-8")
        document, ctx, rls, role, compiled, decision, variables = self._compile_query(ticket_bytes)

        compiled_for_exec = compiled
        sampling = not has_capability(role, Capability.FULL_RESULTS) if role else True

        if (
            decision.route == Route.DIRECT
            and decision.source_id
            and self._state.source_pools.has(decision.source_id)
        ):
            compiled_for_exec = inject_rls(compiled_for_exec, ctx, rls)
            if sampling:
                compiled_for_exec = apply_sampling(compiled_for_exec, get_sample_size())
            target_sql = transpile(compiled_for_exec.sql, decision.dialect or "postgres")
            result = asyncio.run_coroutine_threadsafe(
                execute_direct(
                    self._state.source_pools,
                    decision.source_id,
                    target_sql,
                    compiled_for_exec.params,
                ),
                self._main_loop,
            ).result()
            table = rows_to_arrow_table(result.rows, compiled.columns)
            return flight.RecordBatchStream(table)  # pyright: ignore[reportPrivateImportUsage]  # lib omits __all__

        compiled_for_exec = compile_query(
            document,
            ctx,
            variables,
            use_catalog=True,
        )[0]
        compiled_for_exec = inject_rls(compiled_for_exec, ctx, rls)
        if sampling:
            compiled_for_exec = apply_sampling(compiled_for_exec, get_sample_size())
        trino_sql = transpile_to_trino(compiled_for_exec.sql)

        if self._state.flight_client is None:
            raise flight.FlightServerError(  # pyright: ignore[reportPrivateImportUsage]  # lib omits __all__
                "Zaychik Flight SQL proxy is not configured. "
                "Set ZAYCHIK_HOST/ZAYCHIK_PORT and ensure the service is running."
            )
        from provisa.executor.trino_flight import execute_trino_flight_stream

        arrow_schema, batch_gen = execute_trino_flight_stream(
            self._state.flight_client,
            trino_sql,
            compiled_for_exec.params,
        )
        return flight.GeneratorStream(arrow_schema, batch_gen)  # pyright: ignore[reportPrivateImportUsage]  # lib omits __all__
