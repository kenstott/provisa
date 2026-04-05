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

import json
import logging

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
)
from provisa.compiler.parser import parse_query
from provisa.compiler.rls import RLSContext, inject_rls
from provisa.compiler.sampling import apply_sampling, get_sample_size
from provisa.compiler.sql_gen import compile_query
from provisa.executor.formats.arrow import rows_to_arrow_table
from provisa.executor.trino import execute_trino
from provisa.executor.direct import execute_direct
from provisa.security.rights import Capability, has_capability
from provisa.transpiler.router import Route, decide_route
from provisa.transpiler.transpile import transpile, transpile_to_trino

log = logging.getLogger(__name__)


class ProvisaFlightServer(flight.FlightServerBase):
    """Arrow Flight server that executes GraphQL queries and streams Arrow data.

    Supports three modes via handshake properties:
      - catalog: read-only JDBC catalog view of the semantic layer
      - approved: list approved persisted queries as virtual tables
      - (default): full GraphQL query execution
    """

    def __init__(self, state, location="grpc://0.0.0.0:8815", **kwargs):
        super().__init__(location, **kwargs)
        self._state = state
        self._session_modes: dict[bytes, str] = {}  # token -> mode

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
        """Execute a query from the ticket and return Arrow record batches."""
        mode = self._parse_mode(ticket.ticket)

        if mode == "catalog":
            return self._do_get_catalog(ticket)
        if mode == "approved":
            return self._do_get_approved(ticket)

        return self._do_get_default(ticket)

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

    def _do_get_default(self, ticket):
        """Original full-execution do_get path."""
        document, ctx, rls, role, compiled, decision, variables = \
            self._compile_query(ticket.ticket)

        compiled_for_exec = compiled
        sampling = not has_capability(role, Capability.FULL_RESULTS) if role else True

        if decision.route == Route.DIRECT and decision.source_id:
            compiled_for_exec = inject_rls(compiled_for_exec, ctx, rls)
            if sampling:
                compiled_for_exec = apply_sampling(compiled_for_exec, get_sample_size())
            target_sql = transpile(compiled_for_exec.sql, decision.dialect or "postgres")
            import asyncio
            loop = asyncio.new_event_loop()
            try:
                result = loop.run_until_complete(
                    execute_direct(
                        self._state.source_pools, decision.source_id,
                        target_sql, compiled_for_exec.params,
                    )
                )
            finally:
                loop.close()
            table = rows_to_arrow_table(result.rows, compiled.columns)
            return flight.RecordBatchStream(table)

        # Trino path — recompile with catalog-qualified names
        compiled_for_exec = compile_query(
            document, ctx, variables, use_catalog=True,
        )[0]
        compiled_for_exec = inject_rls(compiled_for_exec, ctx, rls)
        if sampling:
            compiled_for_exec = apply_sampling(compiled_for_exec, get_sample_size())
        trino_sql = transpile_to_trino(compiled_for_exec.sql)

        # Streaming via Zaychik (true end-to-end Arrow, no materialization)
        if self._state.flight_client is not None:
            from provisa.executor.trino_flight import execute_trino_flight_stream
            try:
                arrow_schema, batch_gen = execute_trino_flight_stream(
                    self._state.flight_client, trino_sql, compiled_for_exec.params,
                )
                return flight.GeneratorStream(arrow_schema, batch_gen)
            except Exception:
                log.warning("Flight SQL streaming failed, using REST path")

        # REST path — execute via Trino REST, stream as Arrow batches
        result = execute_trino(
            self._state.trino_conn, trino_sql, compiled_for_exec.params,
        )
        table = rows_to_arrow_table(result.rows, compiled.columns)
        return flight.RecordBatchStream(table)
