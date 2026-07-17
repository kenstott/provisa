# Copyright (c) 2026 Kenneth Stott
# Canary: 6c7c7c80-b434-4cb6-8935-f26b8cba2448
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Provisa airport Flight server (REQ-1098).

Serves the DuckDB `airport` community extension's Flight application protocol so
an external DuckDB client can::

    INSTALL airport FROM community; LOAD airport;
    ATTACH 'grpc://host:port' AS provisa (TYPE AIRPORT);
    SELECT ... FROM provisa.<schema>.<table>;

READ-ONLY increment: catalog discovery (list_schemas, catalog_version,
endpoints, get_flight_info) + governed do_get SELECT. Governance (RLS, masking,
column visibility, row cap) is enforced because do_get runs the shared
``_govern_and_route`` pipeline via EngineRuntime — engine-agnostic.

Role: taken from the gRPC ``authorization: Bearer <role>`` header (DuckDB airport
secret ``auth_token``). Absent → PROVISA_AIRPORT_DEFAULT_ROLE (documented dev
default for unauthenticated access); absent too → the call is refused.

Not yet implemented (later increments, wired to fail with a clear protocol error
so nothing half-works): predicate/projection pushdown, column_statistics,
flight_info time-travel, create_transaction, DML (do_exchange/do_put), DDL.
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import threading
from typing import TYPE_CHECKING, Any

import pyarrow as pa
import pyarrow.flight as flight

from provisa.api.airport import wire
from provisa.api.airport.query import governed_table_scan_arrow

if TYPE_CHECKING:
    from provisa.api.app import AppState

log = logging.getLogger(__name__)

_CATALOG_VERSION = 1  # is_fixed catalog for the read-only MVP (no live schema mutation)


class _HeaderMiddleware(flight.ServerMiddleware):  # pyright: ignore[reportPrivateImportUsage]
    """Captures the incoming gRPC call headers so handlers can read the role."""

    def __init__(self, headers: dict[str, list[str]]) -> None:
        self.headers = headers


class _HeaderMiddlewareFactory(
    flight.ServerMiddlewareFactory  # pyright: ignore[reportPrivateImportUsage]
):
    def start_call(self, info: Any, headers: dict[str, list[str]]) -> _HeaderMiddleware:  # noqa: ARG002
        return _HeaderMiddleware(headers)


def _err(msg: str) -> Exception:
    return flight.FlightServerError(msg)  # pyright: ignore[reportPrivateImportUsage]


class ProvisaAirportServer(
    flight.FlightServerBase  # pyright: ignore[reportPrivateImportUsage]
):
    """Flight server speaking the DuckDB airport dialect over the governed pipeline."""

    def __init__(
        self,
        state: AppState,
        host: str,
        port: int,
        *,
        main_loop: asyncio.AbstractEventLoop,
    ) -> None:
        super().__init__(
            f"grpc://0.0.0.0:{port}",
            middleware={"headers": _HeaderMiddlewareFactory()},
        )
        self._state = state
        self._main_loop = main_loop
        # Advertised endpoint location — where the client sends do_get. Reuse the same
        # gRPC connection by advertising this server's reachable address.
        self._location = f"grpc://{host}:{port}"
        self._scan_cache: dict[tuple[str, str, str], pa.Table] = {}
        self._cache_lock = threading.Lock()

    # ------------------------------------------------------------------ role
    def _role(self, context: flight.ServerCallContext) -> str:  # pyright: ignore[reportPrivateImportUsage]
        mw = context.get_middleware("headers")
        token = ""
        if mw is not None:
            for key in ("authorization", "Authorization"):
                vals = mw.headers.get(key)
                if vals:
                    raw = vals[0]
                    token = raw[7:].strip() if raw.lower().startswith("bearer ") else raw.strip()
                    break
        if not token:
            # Documented dev default for unauthenticated access (REQ-1098). No token AND
            # no configured default → refuse, rather than silently assume a privileged role.
            token = os.environ.get("PROVISA_AIRPORT_DEFAULT_ROLE", "")
        if not token:
            raise _err(
                "airport: no role — attach with a bearer auth_token secret or set "
                "PROVISA_AIRPORT_DEFAULT_ROLE"
            )
        if token not in self._state.contexts:
            raise _err(f"airport: unknown role {token!r}")
        return token

    # ------------------------------------------------------------- catalog
    def _catalog_for_role(self, role_id: str) -> list[tuple[str, str, str]]:
        """(schema, table, sql_ref) for the federated data tables visible to the role.

        Enumerated from the role's compilation context (which is already visibility-scoped:
        an analyst's context omits admin-only tables/columns). Restricted to tables backed by
        a queryable external source pool — this excludes Provisa's internal system surfaces
        (the 'meta' registry and otel/results/iceberg catalogs, whose sources are not external
        data pools). Streaming (kafka) sources are out of the static-scan read MVP.
        """
        from provisa.compiler.naming import domain_to_sql_name
        from provisa.compiler.sql_rewrite import semantic_table_name

        ctx = self._state.contexts[role_id]
        seen: set[tuple[str, str]] = set()
        out: list[tuple[str, str, str]] = []
        for field_name, meta in getattr(ctx, "tables", {}).items():
            # ctx.tables also holds derived _aggregate/_connection/_group_by variants — skip.
            if field_name.endswith(("_aggregate", "_connection", "_group_by", "GroupBy")):
                continue
            if not self._state.source_pools.has(meta.source_id):
                continue  # internal/system source (meta/otel/results/iceberg) — not user data
            if self._state.source_types.get(meta.source_id) == "kafka" or meta.source_type == "kafka":
                continue  # streaming source — not a static scan (out of read-MVP scope)
            schema = domain_to_sql_name(meta.domain_id) or "default"
            table = semantic_table_name(meta)
            key = (schema, table)
            if key in seen:
                continue
            seen.add(key)
            out.append((schema, table, f'SELECT * FROM "{schema}"."{table}"'))
        return out

    def _lookup(self, role_id: str, schema: str, table: str) -> str:
        for s, t, sql_ref in self._catalog_for_role(role_id):
            if s == schema and t == table:
                return sql_ref
        raise _err(f"airport: table not found for role {role_id!r}: {schema}.{table}")

    def _scan_cached(self, role_id: str, schema: str, table: str, sql_ref: str) -> pa.Table:
        """Governed scan of a table, cached per (role, schema, table). list_schemas populates
        the cache so the do_get that follows streams a schema byte-identical to what the client
        planned with (REQ-1098 MVP snapshot semantics; a later increment adds live re-reads)."""
        key = (role_id, schema, table)
        with self._cache_lock:
            cached = self._scan_cache.get(key)
        if cached is not None:
            return cached
        tbl = governed_table_scan_arrow(self._state, self._main_loop, sql_ref, role_id)
        with self._cache_lock:
            self._scan_cache[key] = tbl
        return tbl

    def _table_flight_info(
        self, schema: str, table: str, arrow_schema: pa.Schema, comment: str = ""
    ) -> flight.FlightInfo:  # pyright: ignore[reportPrivateImportUsage]
        descriptor = flight.FlightDescriptor.for_path(schema, table)  # pyright: ignore[reportPrivateImportUsage]
        ticket = flight.Ticket(self._ticket_json(schema, table))  # pyright: ignore[reportPrivateImportUsage]
        # No location — the client reuses the ATTACH connection for do_get (matches airport-go's
        # inline FlightInfo, which carries an empty endpoint location).
        endpoint = flight.FlightEndpoint(ticket, [])  # pyright: ignore[reportPrivateImportUsage]
        app_meta = wire._encode(
            {
                "type": "table",
                "schema": schema,
                "catalog": "",
                "name": table,
                "comment": comment,
                "input_schema": None,
                "action_name": None,
                "description": None,
                "extra_data": None,
            }
        )
        return flight.FlightInfo(  # pyright: ignore[reportPrivateImportUsage]
            arrow_schema, descriptor, [endpoint], -1, -1, app_metadata=app_meta
        )

    @staticmethod
    def _ticket_json(schema: str, table: str) -> bytes:
        # airport TicketData (JSON) — schema+table only, matching airport-go EncodeTableTicket
        # (no catalog field for the single unnamed catalog).
        return json.dumps({"schema": schema, "table": table}).encode("utf-8")

    # ------------------------------------------------------------- DoAction
    def do_action(  # pyright: ignore[reportPrivateImportUsage]
        self,
        context: flight.ServerCallContext,  # pyright: ignore[reportPrivateImportUsage]
        action: flight.Action,  # pyright: ignore[reportPrivateImportUsage]
    ):
        atype = action.type
        body = action.body.to_pybytes() if action.body else b""
        if atype == "list_schemas":
            return [flight.Result(self._do_list_schemas(context))]  # pyright: ignore[reportPrivateImportUsage]
        if atype == "catalog_version":
            return [
                flight.Result(  # pyright: ignore[reportPrivateImportUsage]
                    wire.build_catalog_version_response(_CATALOG_VERSION, is_fixed=True)
                )
            ]
        if atype == "endpoints":
            return [flight.Result(self._do_endpoints(context, body))]  # pyright: ignore[reportPrivateImportUsage]
        if atype == "flight_info":
            return [flight.Result(self._do_flight_info(context, body))]  # pyright: ignore[reportPrivateImportUsage]
        if atype == "create_transaction":
            # No transaction coordinator (read-only MVP). The airport protocol's defined
            # response for "no tx manager" is a nil identifier — reads proceed without a txn
            # (airport-go handleCreateTransaction). NOT a silent no-op: it is the protocol's
            # explicit no-coordination reply. DML increments will add a real coordinator.
            self._role(context)  # authorize
            return [flight.Result(wire._encode({"identifier": None}))]  # pyright: ignore[reportPrivateImportUsage]
        if atype == "get_transaction_status":
            return [
                flight.Result(  # pyright: ignore[reportPrivateImportUsage]
                    wire._encode({"status": "", "exists": False})
                )
            ]
        # Verbs reserved for later increments — refuse explicitly (never silent no-op) so a
        # client feature that needs them fails with a clear protocol error, not wrong data.
        if atype in (
            "column_statistics",
            "table_function_flight_info",
            "create_schema",
            "drop_schema",
            "create_table",
            "drop_table",
            "add_column",
            "remove_column",
            "rename_column",
            "rename_table",
            "change_column_type",
        ):
            raise _err(f"airport: action {atype!r} not supported by the read-only Provisa MVP")
        raise _err(f"airport: unknown action {atype!r}")

    def _do_list_schemas(self, context: flight.ServerCallContext) -> bytes:  # pyright: ignore[reportPrivateImportUsage]
        role_id = self._role(context)
        # Group tables by airport schema (= domain). The governed scan (run once here, cached)
        # yields each table's Arrow schema — already visibility-filtered for the role.
        by_schema: dict[str, list[tuple[str, str, pa.Schema]]] = {}
        for schema, table, sql_ref in self._catalog_for_role(role_id):
            tbl = self._scan_cached(role_id, schema, table, sql_ref)
            by_schema.setdefault(schema, []).append((schema, table, tbl.schema))

        schema_payloads: list[dict[str, Any]] = []
        for schema in sorted(by_schema):
            protos: list[bytes] = []
            for _s, table, ars in by_schema[schema]:
                fi = self._table_flight_info(schema, table, ars)
                protos.append(fi.serialize())
            serialized, sha256 = wire.serialize_schema_contents(protos)
            schema_payloads.append(
                {
                    "name": schema,
                    "description": "",
                    "serialized": serialized,
                    "sha256": sha256,
                    # Matches airport-go: only the catalog's DefaultSchemaName is marked default;
                    # a governed domain never is.
                    "is_default": False,
                }
            )
        return wire.build_list_schemas_response(
            schema_payloads, _CATALOG_VERSION, is_fixed=True
        )

    def _descriptor_path(self, body: bytes) -> tuple[str, str]:
        req = wire.decode_action_request(body)
        desc_bytes = req.get("descriptor")
        if not desc_bytes:
            raise _err("airport: request missing descriptor")
        if isinstance(desc_bytes, str):
            desc_bytes = desc_bytes.encode("latin-1")
        desc = flight.FlightDescriptor.deserialize(desc_bytes)  # pyright: ignore[reportPrivateImportUsage]
        path = [p.decode("utf-8") if isinstance(p, bytes) else p for p in desc.path]
        if len(path) != 2:
            raise _err(f"airport: descriptor path must be [schema, table], got {path}")
        return path[0], path[1]

    def _do_endpoints(self, context: flight.ServerCallContext, body: bytes) -> bytes:  # pyright: ignore[reportPrivateImportUsage]
        self._role(context)  # authorize
        schema, table = self._descriptor_path(body)
        ticket = flight.Ticket(self._ticket_json(schema, table))  # pyright: ignore[reportPrivateImportUsage]
        endpoint = flight.FlightEndpoint(  # pyright: ignore[reportPrivateImportUsage]
            ticket, [flight.Location(self._location)]  # pyright: ignore[reportPrivateImportUsage]
        )
        return wire.build_endpoints_response([endpoint.serialize()])

    def _do_flight_info(self, context: flight.ServerCallContext, body: bytes) -> bytes:  # pyright: ignore[reportPrivateImportUsage]
        role_id = self._role(context)
        schema, table = self._descriptor_path(body)
        fi = self._flight_info_for(role_id, schema, table)
        return fi.serialize()

    # ------------------------------------------------------------- GetFlightInfo
    def get_flight_info(  # pyright: ignore[reportPrivateImportUsage]
        self,
        context: flight.ServerCallContext,  # pyright: ignore[reportPrivateImportUsage]
        descriptor: flight.FlightDescriptor,  # pyright: ignore[reportPrivateImportUsage]
    ) -> flight.FlightInfo:  # pyright: ignore[reportPrivateImportUsage]
        role_id = self._role(context)
        path = [p.decode("utf-8") if isinstance(p, bytes) else p for p in descriptor.path]
        if len(path) != 2:
            raise _err(f"airport: descriptor path must be [schema, table], got {path}")
        return self._flight_info_for(role_id, path[0], path[1])

    def _flight_info_for(
        self, role_id: str, schema: str, table: str
    ) -> flight.FlightInfo:  # pyright: ignore[reportPrivateImportUsage]
        sql_ref = self._lookup(role_id, schema, table)
        tbl = self._scan_cached(role_id, schema, table, sql_ref)
        return self._table_flight_info(schema, table, tbl.schema)

    # ------------------------------------------------------------- DoGet
    def do_get(  # pyright: ignore[reportPrivateImportUsage]
        self,
        context: flight.ServerCallContext,  # pyright: ignore[reportPrivateImportUsage]
        ticket: flight.Ticket,  # pyright: ignore[reportPrivateImportUsage]
    ) -> flight.RecordBatchStream:  # pyright: ignore[reportPrivateImportUsage]
        role_id = self._role(context)
        try:
            td = json.loads(ticket.ticket.decode("utf-8"))
        except (json.JSONDecodeError, UnicodeDecodeError) as e:
            raise _err(f"airport: invalid ticket: {e}") from e
        schema = td.get("schema")
        table = td.get("table")
        if not schema or not table:
            raise _err("airport: ticket must carry schema and table")
        sql_ref = self._lookup(role_id, schema, table)
        tbl = self._scan_cached(role_id, schema, table, sql_ref)
        return flight.RecordBatchStream(tbl)  # pyright: ignore[reportPrivateImportUsage]
