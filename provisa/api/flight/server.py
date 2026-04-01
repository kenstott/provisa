# Copyright (c) 2025 Kenneth Stott
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""gRPC Arrow Flight server for Provisa (REQ-045).

Clients send a GraphQL query as the Flight ticket, receive Arrow record batches.
"""

from __future__ import annotations

import json

import pyarrow as pa
import pyarrow.flight as flight

from provisa.compiler.parser import parse_query
from provisa.compiler.rls import RLSContext, inject_rls
from provisa.compiler.sampling import apply_sampling, get_sample_size
from provisa.compiler.sql_gen import compile_query
from provisa.executor.formats.arrow import rows_to_arrow_table
from provisa.executor.trino import execute_trino
from provisa.executor.trino_flight import execute_trino_flight_arrow  # noqa: F401
from provisa.executor.direct import execute_direct
from provisa.security.rights import Capability, has_capability
from provisa.transpiler.router import Route, decide_route
from provisa.transpiler.transpile import transpile, transpile_to_trino


class ProvisaFlightServer(flight.FlightServerBase):
    """Arrow Flight server that executes GraphQL queries and streams Arrow data."""

    def __init__(self, state, location="grpc://0.0.0.0:8815", **kwargs):
        super().__init__(location, **kwargs)
        self._state = state

    def do_get(self, context, ticket):
        """Execute a query from the ticket and return Arrow record batches."""
        # Ticket is JSON: {"query": "...", "role": "admin", "variables": {...}}
        try:
            request = json.loads(ticket.ticket.decode("utf-8"))
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

        compiled_for_exec = compiled
        if decision.route == Route.DIRECT and decision.source_id:
            compiled_for_exec = inject_rls(compiled_for_exec, ctx, rls)
            if not has_capability(role, Capability.FULL_RESULTS) if role else True:
                compiled_for_exec = apply_sampling(compiled_for_exec, get_sample_size())
            target_sql = transpile(compiled_for_exec.sql, decision.dialect or "postgres")
            # Arrow Flight is async context — use sync execution for now
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
        else:
            compiled_for_exec = compile_query(
                document, ctx, variables, use_catalog=True,
            )[0]
            compiled_for_exec = inject_rls(compiled_for_exec, ctx, rls)
            if not has_capability(role, Capability.FULL_RESULTS) if role else True:
                compiled_for_exec = apply_sampling(compiled_for_exec, get_sample_size())
            trino_sql = transpile_to_trino(compiled_for_exec.sql)

            # Use Flight SQL for native Arrow when available
            if self._state.flight_client is not None:
                table = execute_trino_flight_arrow(
                    self._state.flight_client, trino_sql, compiled_for_exec.params,
                )
                return flight.RecordBatchStream(table)

            result = execute_trino(
                self._state.trino_conn, trino_sql, compiled_for_exec.params,
            )

        table = rows_to_arrow_table(result.rows, compiled.columns)
        return flight.RecordBatchStream(table)
