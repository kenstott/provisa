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
    INSERT INTO provisa.<schema>.<table> VALUES (...);
    CREATE SCHEMA provisa.<domain>;

Every capability routes through Provisa's ONE governed, engine-dispatching pipeline
(``_govern_and_route`` for reads, ``_compile_govern_execute`` for writes), so
governance (RLS, masking, column visibility, row cap, writable-column ACL) applies
and the query runs on whatever engine is bound — never a Trino/duckdb hardcode.

Implemented (REQ-1098):
  * Catalog discovery: list_schemas, catalog_version, endpoints, flight_info,
    get_flight_info.
  * Governed reads with predicate + projection PUSHDOWN — the ``endpoints`` action
    carries DuckDB's ``json_filters`` + ``column_ids``, translated to a semantic
    WHERE/projection folded into the DoGet ticket so the SOURCE filters.
  * Transactions: create_transaction / get_transaction_status (best-effort
    read-committed coordinator).
  * Governed DML: INSERT via do_exchange, submitted as semantic SQL through the
    SAME governed write pipeline; gated on the target engine's writability.
  * DDL: create_schema / drop_schema mapped to the Provisa domain catalog.

Refused with a correct Flight protocol error (never a silent no-op), each justified
inline: column_statistics, table_function_flight_info, UPDATE/DELETE, create_table
and column/struct mutations — see the handlers for why each has no sound meaning for
a governed federation catalog on the active engine.

Role: taken from the gRPC ``authorization: Bearer <role>`` header (DuckDB airport
secret ``auth_token``). Absent → PROVISA_AIRPORT_DEFAULT_ROLE (documented dev
default for unauthenticated access); absent too → the call is refused.
"""

from __future__ import annotations

import asyncio
import datetime as _dt
import json
import logging
import os
import threading
from typing import TYPE_CHECKING, Any

import pyarrow as pa
import pyarrow.flight as flight

from provisa.api.airport import pushdown, wire
from provisa.api.airport.query import governed_mutation, governed_table_scan_arrow
from provisa.api.airport.transactions import AirportTransactionManager

if TYPE_CHECKING:
    from provisa.api.app import AppState

log = logging.getLogger(__name__)

_CATALOG_VERSION = 1  # is_fixed catalog for the read MVP (DDL bumps the control-plane, not this)


class _HeaderMiddleware(flight.ServerMiddleware):  # pyright: ignore[reportPrivateImportUsage]
    """Captures the incoming gRPC call headers so handlers can read role + airport metadata."""

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
        self._txn = AirportTransactionManager()

    # ------------------------------------------------------------------ role
    def _headers(self, context: flight.ServerCallContext) -> dict[str, list[str]]:  # pyright: ignore[reportPrivateImportUsage]
        mw = context.get_middleware("headers")
        return mw.headers if mw is not None else {}

    def _header(self, headers: dict[str, list[str]], name: str) -> str | None:
        for key in (name, name.lower(), name.title()):
            vals = headers.get(key)
            if vals:
                v = vals[0]
                return v.decode("utf-8") if isinstance(v, bytes) else v
        return None

    def _role(self, context: flight.ServerCallContext) -> str:  # pyright: ignore[reportPrivateImportUsage]
        headers = self._headers(context)
        token = ""
        raw = self._header(headers, "authorization")
        if raw:
            token = raw[7:].strip() if raw.lower().startswith("bearer ") else raw.strip()
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
            out.append((schema, table, self._scan_sql(ctx, meta, schema, table)))
        return out

    @staticmethod
    def _scan_sql(ctx: Any, meta: Any, schema: str, table: str) -> str:
        """Scan SELECT listing only the role's REGISTERED semantic columns — never ``SELECT *``.

        The airport catalog must advertise exactly the governed column set: SELECT * would surface
        unregistered physical columns (e.g. an audit ``created_at`` with a DB default) that are not
        part of the semantic model, leaking them on read AND causing DuckDB to send them (as NULL)
        on INSERT, overriding the source default. Projecting the registered columns keeps the
        advertised schema == the governed model on both paths.
        """
        cols = ctx.aggregate_columns.get(meta.table_id, [])
        names = [ctx.physical_to_sql.get((meta.table_id, cn), cn) for cn, _ in cols]
        if not names:
            return f'SELECT * FROM "{schema}"."{table}"'  # no registered columns → fall back
        col_sql = ", ".join(f'"{n}"' for n in names)
        return f'SELECT {col_sql} FROM "{schema}"."{table}"'

    def _source_type_for(self, role_id: str, schema: str, table: str) -> str:
        """The physical source type backing (schema, table) for the role, for the write gate."""
        from provisa.compiler.naming import domain_to_sql_name
        from provisa.compiler.sql_rewrite import semantic_table_name

        ctx = self._state.contexts[role_id]
        for field_name, meta in getattr(ctx, "tables", {}).items():
            if field_name.endswith(("_aggregate", "_connection", "_group_by", "GroupBy")):
                continue
            s = domain_to_sql_name(meta.domain_id) or "default"
            if s == schema and semantic_table_name(meta) == table:
                return self._state.source_types.get(meta.source_id) or (meta.source_type or "")
        raise _err(f"airport: table not found for role {role_id!r}: {schema}.{table}")

    def _lookup(self, role_id: str, schema: str, table: str) -> str:
        for s, t, sql_ref in self._catalog_for_role(role_id):
            if s == schema and t == table:
                return sql_ref
        raise _err(f"airport: table not found for role {role_id!r}: {schema}.{table}")

    def _scan_cached(self, role_id: str, schema: str, table: str, sql_ref: str) -> pa.Table:
        """Governed full-table scan, cached per (role, schema, table). list_schemas populates
        the cache so the do_get that follows streams a schema byte-identical to what the client
        planned with. Pushdown scans (WHERE/projection) are NOT cached — they run fresh."""
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
    def _ticket_json(
        schema: str,
        table: str,
        *,
        columns: list[str] | None = None,
        where: str | None = None,
    ) -> bytes:
        # airport TicketData (JSON). Provisa owns both ends of this ticket, so it carries the
        # ALREADY-TRANSLATED pushdown (projected column names + a semantic WHERE body) rather
        # than DuckDB's raw json_filters — do_get folds them straight into the governed SQL.
        td: dict[str, Any] = {"schema": schema, "table": table}
        if columns:
            td["columns"] = columns
        if where:
            td["where"] = where
        return json.dumps(td).encode("utf-8")

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
            self._role(context)  # authorize
            tx_id = self._txn.begin()
            return [flight.Result(wire._encode({"identifier": tx_id}))]  # pyright: ignore[reportPrivateImportUsage]
        if atype == "get_transaction_status":
            self._role(context)  # authorize
            req = wire.decode_action_request(body) if body else {}
            tx_id = req.get("transaction_id")
            if isinstance(tx_id, bytes):
                tx_id = tx_id.decode("utf-8")
            status, exists = self._txn.status(tx_id or "")
            return [flight.Result(wire._encode({"status": status, "exists": exists}))]  # pyright: ignore[reportPrivateImportUsage]
        if atype == "create_schema":
            return [flight.Result(self._do_create_schema(context, body))]  # pyright: ignore[reportPrivateImportUsage]
        if atype == "drop_schema":
            self._do_drop_schema(context, body)
            return []  # airport drop_schema returns an empty result stream on success
        if atype == "column_statistics":
            # Refused by protocol error (correct, not a no-op): the governed federation catalog
            # holds no precomputed per-column statistics, and computing DuckDB-format stats here
            # would require an UNGOVERNED full scan per metadata call — bypassing the role row-cap
            # and masking. We also do NOT advertise can_produce_statistics, so a conforming client
            # never calls this; DuckDB falls back to its own cardinality estimates.
            raise _err(
                "airport: column_statistics unsupported — a governed federation catalog exposes no "
                "precomputed column statistics (computing them would bypass governance)"
            )
        if atype == "table_function_flight_info":
            # Refused: the governed catalog advertises only base tables, never table functions —
            # there is no function whose dynamic output schema this could resolve.
            raise _err(
                "airport: table_function_flight_info unsupported — the governed catalog advertises "
                "no table functions"
            )
        if atype in (
            "create_table",
            "drop_table",
            "add_column",
            "remove_column",
            "rename_column",
            "rename_table",
            "change_column_type",
            "set_not_null",
            "drop_not_null",
            "set_default",
            "add_field",
            "rename_field",
            "remove_field",
        ):
            # Refused: these mutate a PHYSICAL table's shape. A governed federation catalog binds
            # a semantic model over heterogeneous, independently-owned sources; airport supplies
            # only an Arrow schema, with no target source, dialect, governance policy, or PK — so
            # there is no sound, unambiguous mapping. Physical-shape changes go through the admin
            # schema-mutation API (create_source/register_table/update_table) with that context.
            # (Struct-field ops add_field/rename_field/remove_field have no meaning at all for a
            # relational federation catalog.) create_schema/drop_schema DO map — to domains.
            raise _err(
                f"airport: action {atype!r} unsupported — physical schema mutation must go through "
                "Provisa's admin schema-mutation API, which carries the source/governance context "
                "the airport DDL payload lacks"
            )
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

    def _descriptor_path(self, req: dict[str, Any]) -> tuple[str, str]:
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
        role_id = self._role(context)
        req = wire.decode_action_request(body)
        schema, table = self._descriptor_path(req)
        # PUSHDOWN (REQ-1098): the airport extension delivers projection (column_ids) + predicate
        # (json_filters) in the endpoints "parameters" map. Resolve them against the table's full
        # advertised schema, translate to a semantic projection + WHERE, and fold into the ticket.
        params = req.get("parameters") or {}
        columns: list[str] | None = None
        where: str | None = None
        full_cols = list(self._scan_cached(role_id, schema, table, self._lookup(role_id, schema, table)).schema.names)
        column_ids = params.get("column_ids")
        columns = pushdown.resolve_projection(column_ids, full_cols)
        json_filters = params.get("json_filters")
        if isinstance(json_filters, bytes):
            json_filters = json_filters.decode("utf-8")
        where = pushdown.translate_filters(json_filters, columns or full_cols)
        if where or columns:
            log.info(
                "airport pushdown scan role=%s %s.%s columns=%s where=%s",
                role_id, schema, table, columns, where,
            )
        ticket = flight.Ticket(  # pyright: ignore[reportPrivateImportUsage]
            self._ticket_json(schema, table, columns=columns, where=where)
        )
        endpoint = flight.FlightEndpoint(  # pyright: ignore[reportPrivateImportUsage]
            ticket, [flight.Location(self._location)]  # pyright: ignore[reportPrivateImportUsage]
        )
        return wire.build_endpoints_response([endpoint.serialize()])

    def _do_flight_info(self, context: flight.ServerCallContext, body: bytes) -> bytes:  # pyright: ignore[reportPrivateImportUsage]
        role_id = self._role(context)
        schema, table = self._descriptor_path(wire.decode_action_request(body))
        fi = self._flight_info_for(role_id, schema, table)
        return fi.serialize()

    # ------------------------------------------------------------- DDL → domain catalog
    def _do_create_schema(self, context: flight.ServerCallContext, body: bytes) -> bytes:  # pyright: ignore[reportPrivateImportUsage]
        """create_schema → create a Provisa domain (the airport schema == Provisa domain).

        This is the one DDL verb that maps cleanly onto a governed federation catalog: a schema
        is a namespace, and Provisa's namespace is the domain. Runs through the control-plane
        domain repository on the main loop. Returns the airport create_schema response shape
        (serialized-contents map) so the extension accepts it.
        """
        role_id = self._role(context)
        self._require_admin(role_id)
        req = wire.decode_action_request(body)
        name = req.get("schema") or req.get("schema_name") or req.get("name")
        if isinstance(name, bytes):
            name = name.decode("utf-8")
        if not name:
            raise _err("airport: create_schema requires a schema name")
        comment = req.get("comment")
        if isinstance(comment, bytes):
            comment = comment.decode("utf-8")
        asyncio.run_coroutine_threadsafe(
            self._upsert_domain(str(name), comment or ""), self._main_loop
        ).result()
        serialized, sha256 = wire.serialize_schema_contents([])
        return wire._encode(
            {"sha256": sha256, "url": None, "serialized": wire._Str(serialized)}
        )

    def _do_drop_schema(self, context: flight.ServerCallContext, body: bytes) -> None:  # pyright: ignore[reportPrivateImportUsage]
        role_id = self._role(context)
        self._require_admin(role_id)
        req = wire.decode_action_request(body)
        name = req.get("name") or req.get("schema_name") or req.get("schema")
        if isinstance(name, bytes):
            name = name.decode("utf-8")
        if not name:
            raise _err("airport: drop_schema requires a schema name")
        ignore = bool(req.get("ignore_not_found"))
        deleted = asyncio.run_coroutine_threadsafe(
            self._delete_domain(str(name)), self._main_loop
        ).result()
        if not deleted and not ignore:
            raise _err(f"airport: schema {name!r} not found")

    def _require_admin(self, role_id: str) -> None:
        from provisa.security.rights import Capability, has_capability

        role = self._state.roles.get(role_id) or {}
        if not has_capability(role, Capability.TABLE_REGISTRATION):
            raise _err(f"airport: role {role_id!r} may not mutate the catalog (DDL)")

    async def _upsert_domain(self, domain_id: str, description: str) -> None:
        from provisa.core.models import Domain as DomainModel
        from provisa.core.repositories import domain as domain_repo

        pool = self._state.tenant_db
        assert pool is not None, "control-plane pool not initialized"  # live at DDL time
        async with pool.acquire() as conn:
            await domain_repo.upsert(conn, DomainModel(id=domain_id, description=description))

    async def _delete_domain(self, domain_id: str) -> bool:
        from provisa.core.repositories import domain as domain_repo

        pool = self._state.tenant_db
        assert pool is not None, "control-plane pool not initialized"  # live at DDL time
        async with pool.acquire() as conn:
            return await domain_repo.delete(conn, domain_id)

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
        columns = td.get("columns")
        where = td.get("where")
        sql_ref = self._lookup(role_id, schema, table)
        full = self._scan_cached(role_id, schema, table, sql_ref)  # full advertised schema
        if not columns and not where:
            # No pushdown — full-table governed scan (cached, schema-stable).
            return flight.RecordBatchStream(full)  # pyright: ignore[reportPrivateImportUsage]
        # Pushdown: build the semantic SELECT with source-side projection + WHERE. Injecting the
        # predicate as a semantic WHERE means it flows through the IDENTICAL governance a user's
        # /data/sql WHERE would (RLS AND-ed, masking, visibility) — the SOURCE filters and reads
        # only the projected columns, not just the DuckDB client.
        col_sql = ", ".join(f'"{c}"' for c in columns) if columns else "*"
        sql = f'SELECT {col_sql} FROM "{schema}"."{table}"'
        if where:
            sql += f" WHERE {where}"
        _trace_pushdown(sql)
        scanned = governed_table_scan_arrow(self._state, self._main_loop, sql, role_id)
        # Return the FULL advertised schema (the airport contract: DuckDB planned the scan against
        # the flight_info schema and projects client-side; a narrowed DoGet stream would mismatch).
        # Source-side projection still happened above — unprojected columns are null-filled here and
        # DuckDB never reads them (they are exactly the columns it projected out).
        return flight.RecordBatchStream(_pad_to_schema(scanned, full.schema))  # pyright: ignore[reportPrivateImportUsage]

    # ------------------------------------------------------------- DoExchange (DML)
    def do_exchange(  # pyright: ignore[reportPrivateImportUsage]
        self,
        context: flight.ServerCallContext,  # pyright: ignore[reportPrivateImportUsage]
        descriptor: flight.FlightDescriptor,  # pyright: ignore[reportPrivateImportUsage]  # noqa: ARG002
        reader,
        writer,
    ) -> None:
        role_id = self._role(context)
        headers = self._headers(context)
        operation = self._header(headers, "airport-operation")
        flight_path = self._header(headers, "airport-flight-path")
        if not operation:
            raise _err("airport: do_exchange missing airport-operation header")
        if not flight_path or flight_path.count("/") != 1:
            raise _err(f"airport: invalid airport-flight-path {flight_path!r} (want schema/table)")
        schema, table = flight_path.split("/")

        if operation == "insert":
            self._do_exchange_insert(role_id, schema, table, reader, writer)
            return
        if operation in ("update", "delete"):
            # Refused by protocol error (correct, not a no-op): the airport UPDATE/DELETE path
            # keys rows by a server-advertised `rowid` pseudo-column that the client obtained from
            # a prior scan. The governed catalog deliberately does NOT expose a physical rowid —
            # RLS row-filtering and column masking mean a stable physical row identity would leak
            # unfiltered/unmasked identity and let a client target rows it cannot see. Governed
            # UPDATE/DELETE must therefore be issued as WHERE-qualified SQL via /data/sql, where
            # the predicate itself is governed. (A conforming DuckDB client will not even emit
            # these against a table that advertises no rowid.)
            raise _err(
                f"airport: {operation} unsupported — the governed catalog exposes no rowid pseudo-"
                "column (RLS/masking make a physical row identity unsafe to expose); issue a "
                "WHERE-qualified UPDATE/DELETE through the governed SQL path instead"
            )
        raise _err(f"airport: unsupported do_exchange operation {operation!r}")

    def _do_exchange_insert(
        self, role_id: str, schema: str, table: str, reader, writer
    ) -> None:
        # Gate on the target engine/source being writable — engine-agnostic (writable.py), never a
        # Trino/duckdb hardcode. A read-only source yields a correct protocol error.
        from provisa.executor.writable import is_writable_on

        source_type = self._source_type_for(role_id, schema, table)
        if not is_writable_on(source_type, self._state.federation_engine.engine):
            raise _err(
                f"airport: source type {source_type!r} for {schema}.{table} is read-only on the "
                "active engine — INSERT refused"
            )

        # Open the bidirectional stream by sending the output schema (airport requires the server
        # to Begin before the client streams data). We return the full table schema; with no
        # RETURNING requested DuckDB reads only the trailing total_changed metadata.
        out_schema = self._scan_cached(
            role_id, schema, table, self._lookup(role_id, schema, table)
        ).schema
        writer.begin(out_schema)

        incoming = reader.read_all()
        total = incoming.num_rows
        if total:
            sql = self._build_insert_sql(schema, table, incoming)
            # ONE pipeline: submit the mutation SQL through the SAME governed write path as
            # /data/sql (writable-column ACL, RLS, write-routing all apply).
            governed_mutation(self._state, self._main_loop, sql, role_id)
        writer.write_metadata(pa.py_buffer(wire._encode({"total_changed": total})))

    @staticmethod
    def _build_insert_sql(schema: str, table: str, data: pa.Table) -> str:
        cols = list(data.schema.names)
        col_sql = ", ".join(f'"{c}"' for c in cols)
        rows = data.to_pylist()
        values = []
        for row in rows:
            values.append("(" + ", ".join(_sql_literal(row[c]) for c in cols) + ")")
        return f'INSERT INTO "{schema}"."{table}" ({col_sql}) VALUES ' + ", ".join(values)


def _pad_to_schema(scanned: pa.Table, full_schema: pa.Schema) -> pa.Table:
    """Re-shape a source-side-projected scan to the full advertised schema (null-fill absentees).

    Keeps the DoGet stream schema == the flight_info schema DuckDB planned with, while preserving
    the source-side projection (only projected columns were actually read from the source). Columns
    the client projected out are null here and are never read by DuckDB.
    """
    present = set(scanned.schema.names)
    n = scanned.num_rows
    arrays: list[pa.Array | pa.ChunkedArray] = []
    for field in full_schema:
        if field.name in present:
            arrays.append(scanned.column(field.name))
        else:
            arrays.append(pa.nulls(n, type=field.type))
    return pa.table(arrays, schema=full_schema)


def _trace_pushdown(sql: str) -> None:
    """Record the source-side pushdown SQL when PROVISA_AIRPORT_PUSHDOWN_LOG is set.

    Test-only instrumentation (the e2e asserts the SOURCE received the WHERE/projection, which is
    otherwise indistinguishable from DuckDB re-applying the predicate client-side). Gated on the
    env var so it is inert in normal operation.
    """
    path = os.environ.get("PROVISA_AIRPORT_PUSHDOWN_LOG")
    if not path:
        return
    with open(path, "a", encoding="utf-8") as fh:
        fh.write(sql + "\n")


def _sql_literal(value: Any) -> str:
    """Render a Python value (from an Arrow row) as a SQL literal for the governed INSERT.

    The literal is re-parsed and re-governed by _compile_govern_execute, so this only needs to
    be a faithful, correctly-escaped rendering — not a trust boundary.
    """
    if value is None:
        return "NULL"
    if isinstance(value, bool):
        return "TRUE" if value else "FALSE"
    if isinstance(value, (int, float)):
        return repr(value)
    if isinstance(value, (bytes, bytearray)):
        return "'\\x" + bytes(value).hex() + "'"
    if isinstance(value, (_dt.datetime, _dt.date, _dt.time)):
        return "'" + value.isoformat() + "'"
    return "'" + str(value).replace("'", "''") + "'"
