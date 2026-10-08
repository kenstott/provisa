# Copyright (c) 2026 Kenneth Stott
# Canary: 6c7c7c80-b434-4cb6-8935-f26b8cba2448
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Provisa airport Flight server (REQ-1120).

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

Implemented (REQ-1120):
  * Catalog discovery: list_schemas, catalog_version, endpoints, flight_info,
    get_flight_info.
  * Governed reads with predicate + projection PUSHDOWN — the ``endpoints`` action
    carries DuckDB's ``json_filters`` + ``column_ids``, translated to a semantic
    WHERE/projection folded into the DoGet ticket so the SOURCE filters.
  * Transactions: create_transaction / get_transaction_status (best-effort
    read-committed coordinator).
  * Governed DML: INSERT / UPDATE / DELETE via do_exchange, submitted as semantic SQL
    through the SAME governed write pipeline; gated on the target engine's writability.
    UPDATE/DELETE key rows by the table's PRIMARY KEY, advertised as the airport row
    identity (``is_rowid`` pseudo-column) so a role can only mutate rows it can see.
Refused with a correct Flight protocol error (never a silent no-op): column_statistics
and table_function_flight_info, each justified inline; and every DDL action — create_schema,
drop_schema, create_table, drop_table and the column/struct mutations — because nothing is
defined through a query protocol (provisa/compiler/definitions.py): a table or a domain is
created in the model, or in the data source and admitted into the model. UPDATE/
DELETE are refused ONLY per-table, when a table genuinely has no primary key.

Role: with authentication on, the gRPC ``authorization: Bearer <credential>`` header (DuckDB
airport secret ``auth_token``) carries a provider token or a personal access token; it is
validated and the role is derived from the identity (``x-provisa-role`` may request a held one —
the DuckDB extension's only authentication option is the bearer ``auth_token``, so a DuckDB
client acts as the role its identity resolves to).
On a deployment with no auth provider the bearer names the role; absent →
PROVISA_AIRPORT_DEFAULT_ROLE (documented dev default); absent too → the call is refused.
"""

from __future__ import annotations

import json
import logging
import os
import threading
from typing import TYPE_CHECKING, Any

import pyarrow as pa
import pyarrow.flight as flight

from provisa.api.flight.compression import generator_stream
from provisa.api.airport import pushdown, wire
from provisa.api.airport.query import (
    governed_mutation,
    governed_table_scan_schema,
    governed_table_scan_stream,
)
from provisa.api.airport.transactions import AirportTransactionManager
from provisa.core.rpc_loop import hold_loop_for_stream, run_rpc

if TYPE_CHECKING:
    from provisa.api.app import AppState

from provisa.compiler.definitions import DefinitionNotAvailable

log = logging.getLogger(__name__)

_CATALOG_VERSION = 1  # is_fixed catalog for the read MVP (DDL bumps the control-plane, not this)

# The airport row-identity pseudo-column. Advertised as an Arrow field carrying the extension's
# ``is_rowid`` metadata key (ground-truthed against airport-go v0.2.1 catalog/helpers.go
# FindRowIDColumn + the Query-farm airport docs): DuckDB hides it from ``SELECT *`` and streams it
# back on UPDATE/DELETE so the server can identify the affected rows. Provisa fills it with the
# table's PRIMARY-KEY tuple (JSON-encoded), so the only identities a role receives are the PKs of
# rows RLS already let it read — a role cannot target a row it cannot see (REQ-1120/REQ-1125).
_ROWID_FIELD = "rowid"
_ROWID_META = {b"is_rowid": b"true"}

# The Airport actions that define, alter or drop something, and the statement kind each is.
_DEFINITION_ACTIONS = {
    "create_schema": "CREATE SCHEMA",
    "drop_schema": "DROP SCHEMA",
    "create_table": "CREATE TABLE",
    "drop_table": "DROP TABLE",
    "add_column": "ALTER TABLE",
    "remove_column": "ALTER TABLE",
    "rename_column": "ALTER TABLE",
    "rename_table": "ALTER TABLE",
    "change_column_type": "ALTER TABLE",
    "set_not_null": "ALTER TABLE",
    "drop_not_null": "ALTER TABLE",
    "set_default": "ALTER TABLE",
    "add_field": "ALTER TABLE",
    "rename_field": "ALTER TABLE",
    "remove_field": "ALTER TABLE",
}


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


def _bound_batches(org_id: str, batches: Any) -> Any:
    """Pull ``batches`` with ``org_id`` bound for each pull (REQ-1266): pyarrow pulls a stream's
    batches on its own thread after do_get has returned and unbound the RPC."""
    from provisa.core.request_context import reset_current_org, set_current_org

    pulled = iter(batches)
    while True:
        token = set_current_org(org_id)
        try:
            batch = next(pulled)
        except StopIteration:
            return
        finally:
            reset_current_org(token)
        yield batch


class ProvisaAirportServer(
    flight.FlightServerBase  # pyright: ignore[reportPrivateImportUsage]
):
    """Flight server speaking the DuckDB airport dialect over the governed pipeline."""

    def __init__(
        self,
        state: AppState,
        host: str,
        port: int,
    ) -> None:
        # REQ-1900: listens on a loopback port the kernel picks; the advertised ``port`` is bound
        # by the relay start_airport_server puts in front (pyarrow's Flight server cannot share a
        # port between worker processes — see provisa/api/flight/relay.py).
        super().__init__(
            "grpc://127.0.0.1:0",
            middleware={"headers": _HeaderMiddlewareFactory()},
        )
        self._state = state
        # Advertised endpoint location — where the client sends do_get. Reuse the same
        # gRPC connection by advertising this server's reachable address.
        self._location = f"grpc://{host}:{port}"
        # Cache the byte-stable ADVERTISED schema (small, off the result path — Defect 5), not the
        # full table: flight_info / list_schemas populate it so the do_get that follows streams a
        # schema byte-identical to what the client planned with.
        self._schema_cache: dict[tuple[str, str, str], pa.Schema] = {}
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

    def _serving_org(self) -> str:
        """The org an Airport RPC is served in (REQ-1266). Airport names no org -- an ATTACH carries
        a role, never an org -- so a single-org deployment serves its one org, and under
        multitenancy the RPC is refused by name rather than given some org's catalog."""
        if getattr(self._state, "multitenancy", False):
            raise _err(
                "airport: names no org under multitenancy; query a multi-tenant deployment "
                "through a surface that names its org (Flight SQL, pgwire, HTTP)"
            )
        return self._state.org_id

    def _in_serving_org(self, body: Any) -> Any:
        """Run one RPC's ``body`` with the serving org bound on this handler thread."""
        from provisa.core.request_context import reset_current_org, set_current_org

        token = set_current_org(self._serving_org())
        try:
            return body()
        finally:
            reset_current_org(token)

    def _role(self, context: flight.ServerCallContext) -> str:  # pyright: ignore[reportPrivateImportUsage]
        """The role this call runs as.

        With authentication on, the ``authorization: Bearer`` header is a CREDENTIAL — a provider
        token or a personal access token, validated as every other transport validates it — and
        the role is derived from the validated identity (:meth:`_authenticated_role`). A role is
        never taken from the caller's say-so.

        On a deployment with no auth provider there is no identity: the bearer names the role
        at face value (the DuckDB airport secret's ``auth_token``), as ``X-Provisa-Role`` does
        over HTTP there, and PROVISA_AIRPORT_DEFAULT_ROLE serves a call that names none.
        """
        from provisa.auth.bearer import auth_active

        headers = self._headers(context)
        token = ""
        raw = self._header(headers, "authorization")
        if raw:
            token = raw[7:].strip() if raw.lower().startswith("bearer ") else raw.strip()
        try:
            secured = auth_active(self._state, "airport")
        except RuntimeError as exc:
            raise _err(str(exc)) from exc
        if secured:
            return self._authenticated_role(token, self._header(headers, "x-provisa-role"))
        if not token:
            # Documented dev default for unauthenticated access (REQ-1120). No token AND
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

    def _authenticated_role(self, credential: str, requested: str | None) -> str:
        """The role a validated credential acts as (REQ-1263, REQ-273).

        The identity's own role, or the one ``x-provisa-role`` requests when the identity holds
        it (one role, or a comma-separated set acting as its meta-role) — the rule gRPC and Flight
        follow, from the same functions (``auth.bearer``). No credential, a rejected one, or a role the identity does not
        hold is refused; PROVISA_AIRPORT_DEFAULT_ROLE is not consulted.
        """
        import jwt

        from provisa.core.connection_loop import run_on_connection_loop
        from provisa.auth.bearer import authorize_role, validate_bearer_credential

        if not credential:
            raise flight.FlightUnauthenticatedError(  # pyright: ignore[reportPrivateImportUsage]
                "airport: a bearer credential is required (the airport secret's auth_token)"
            )
        try:
            identity = run_on_connection_loop(
                validate_bearer_credential(self._state, credential, "an airport client")
            )
        except (ValueError, jwt.PyJWTError) as exc:
            # Every rejection reads the same on the wire: a caller must not learn from the
            # response whether the credential was unknown, expired or revoked.
            raise flight.FlightUnauthenticatedError(  # pyright: ignore[reportPrivateImportUsage]
                "airport: credential rejected"
            ) from exc
        try:
            role_id = authorize_role(self._state, identity, requested)
        except PermissionError as exc:
            raise flight.FlightUnauthenticatedError(f"airport: {exc}") from exc  # pyright: ignore[reportPrivateImportUsage]
        if role_id not in self._state.contexts:
            raise _err(f"airport: role {role_id!r} has no data surface")
        return role_id

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
            if (
                self._state.source_types.get(meta.source_id) == "kafka"
                or meta.source_type == "kafka"
            ):
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

    def _meta_for(self, role_id: str, schema: str, table: str) -> Any:
        """The role-scoped TableMeta backing (schema, table), or raise if not visible."""
        from provisa.compiler.naming import domain_to_sql_name
        from provisa.compiler.sql_rewrite import semantic_table_name

        ctx = self._state.contexts[role_id]
        for field_name, meta in getattr(ctx, "tables", {}).items():
            if field_name.endswith(("_aggregate", "_connection", "_group_by", "GroupBy")):
                continue
            s = domain_to_sql_name(meta.domain_id) or "default"
            if s == schema and semantic_table_name(meta) == table:
                return meta
        raise _err(f"airport: table not found for role {role_id!r}: {schema}.{table}")

    def _source_type_for(self, role_id: str, schema: str, table: str) -> str:
        """The physical source type backing (schema, table) for the role, for the write gate."""
        meta = self._meta_for(role_id, schema, table)
        return self._state.source_types.get(meta.source_id) or (meta.source_type or "")

    def _pk_for(self, role_id: str, schema: str, table: str) -> list[str]:
        """The table's PRIMARY-KEY column names for the role — the airport row identity.

        Sourced from the role's compilation context (``ctx.pk_columns``), which is populated from the
        source's own PK constraint at startup (``_resolve_pk_from_sources``) and is visibility-scoped:
        a role only ever sees the PK columns it may read. An empty list means the table has no PK, so
        no safe governed row identity can be formed and UPDATE/DELETE are refused for THAT table.
        """
        ctx = self._state.contexts[role_id]
        meta = self._meta_for(role_id, schema, table)
        return list(getattr(ctx, "pk_columns", {}).get(meta.table_id, []))

    @staticmethod
    def _rowid_field() -> pa.Field:
        return pa.field(_ROWID_FIELD, pa.string(), nullable=False, metadata=_ROWID_META)

    def _advertised_schema(self, base: pa.Schema, pk: list[str]) -> pa.Schema:
        """Append the ``is_rowid`` pseudo-column to a base scan schema when the table has a PK.

        Advertised on flight_info/list_schemas AND streamed on do_get so DuckDB plans against — and
        can echo back on UPDATE/DELETE — the row identity. No PK → base schema unchanged (the table
        advertises no rowid and DuckDB will not emit an UPDATE/DELETE against it).
        """
        if not pk:
            return base
        return pa.schema(list(base) + [self._rowid_field()])

    def _append_rowid_batch(self, batch: pa.RecordBatch, pk: list[str]) -> pa.RecordBatch:
        """Append the rowid column = JSON-encoded PK tuple per row (decoded back on UPDATE/DELETE)."""
        pk_lists = {c: batch.column(batch.schema.get_field_index(c)).to_pylist() for c in pk}
        values = [
            json.dumps([pk_lists[c][i] for c in pk], default=str) for i in range(batch.num_rows)
        ]
        arrays = list(batch.columns) + [pa.array(values, type=pa.string())]
        return pa.RecordBatch.from_arrays(
            arrays, schema=pa.schema(list(batch.schema) + [self._rowid_field()])
        )

    def _lookup(self, role_id: str, schema: str, table: str) -> str:
        for s, t, sql_ref in self._catalog_for_role(role_id):
            if s == schema and t == table:
                return sql_ref
        raise _err(f"airport: table not found for role {role_id!r}: {schema}.{table}")

    def _base_schema_cached(self, role_id: str, schema: str, table: str, sql_ref: str) -> pa.Schema:
        """The governed scan's byte-stable base Arrow schema, cached per (role, schema, table).

        Derived from the query's TYPED output columns without holding the rows (Defect 5), so
        flight_info / list_schemas advertise it and the do_get that follows streams a schema
        byte-identical to what the client planned with. The full-table scan is NOT cached — do_get
        streams data fresh."""
        key = (role_id, schema, table)
        with self._cache_lock:
            cached = self._schema_cache.get(key)
        if cached is not None:
            return cached
        base = governed_table_scan_schema(self._state, sql_ref, role_id)
        with self._cache_lock:
            self._schema_cache[key] = base
        return base

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
        # REQ-1882: the whole RPC runs on this handler thread's own loop.
        return run_rpc(
            lambda: self._in_serving_org(lambda: self._do_action_on_loop(context, action))
        )

    def _do_action_on_loop(
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
        definition = _DEFINITION_ACTIONS.get(atype)
        if definition is not None:
            # Nothing is defined through a query protocol: an Airport DDL action never becomes
            # a SQL statement, so it is refused here with the pipeline's own refusal
            # (provisa/compiler/definitions.py) — the same message every other surface answers.
            raise _err(str(DefinitionNotAvailable(definition)))
        raise _err(f"airport: unknown action {atype!r}")

    def _do_list_schemas(self, context: flight.ServerCallContext) -> bytes:  # pyright: ignore[reportPrivateImportUsage]
        role_id = self._role(context)
        # Group tables by airport schema (= domain). The governed scan (run once here, cached)
        # yields each table's Arrow schema — already visibility-filtered for the role.
        by_schema: dict[str, list[tuple[str, str, pa.Schema]]] = {}
        for schema, table, sql_ref in self._catalog_for_role(role_id):
            base = self._base_schema_cached(role_id, schema, table, sql_ref)
            pk = self._pk_for(role_id, schema, table)
            by_schema.setdefault(schema, []).append(
                (schema, table, self._advertised_schema(base, pk))
            )

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
        return wire.build_list_schemas_response(schema_payloads, _CATALOG_VERSION, is_fixed=True)

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
        # PUSHDOWN (REQ-1120): the airport extension delivers projection (column_ids) + predicate
        # (json_filters) in the endpoints "parameters" map. Resolve them against the table's full
        # advertised schema, translate to a semantic projection + WHERE, and fold into the ticket.
        params = req.get("parameters") or {}
        columns: list[str] | None = None
        where: str | None = None
        full_cols = list(
            self._base_schema_cached(
                role_id, schema, table, self._lookup(role_id, schema, table)
            ).names
        )
        column_ids = params.get("column_ids")
        columns = pushdown.resolve_projection(column_ids, full_cols)
        json_filters = params.get("json_filters")
        if isinstance(json_filters, bytes):
            json_filters = json_filters.decode("utf-8")
        where = pushdown.translate_filters(json_filters, columns or full_cols)
        if where or columns:
            log.info(
                "airport pushdown scan role=%s %s.%s columns=%s where=%s",
                role_id,
                schema,
                table,
                columns,
                where,
            )
        ticket = flight.Ticket(  # pyright: ignore[reportPrivateImportUsage]
            self._ticket_json(schema, table, columns=columns, where=where)
        )
        endpoint = flight.FlightEndpoint(  # pyright: ignore[reportPrivateImportUsage]
            ticket,
            [flight.Location(self._location)],  # pyright: ignore[reportPrivateImportUsage]
        )
        return wire.build_endpoints_response([endpoint.serialize()])

    def _do_flight_info(self, context: flight.ServerCallContext, body: bytes) -> bytes:  # pyright: ignore[reportPrivateImportUsage]
        role_id = self._role(context)
        schema, table = self._descriptor_path(wire.decode_action_request(body))
        fi = self._flight_info_for(role_id, schema, table)
        return fi.serialize()

    # ------------------------------------------------------------- DDL → domain catalog
    # ------------------------------------------------------------- GetFlightInfo
    def get_flight_info(  # pyright: ignore[reportPrivateImportUsage]
        self,
        context: flight.ServerCallContext,  # pyright: ignore[reportPrivateImportUsage]
        descriptor: flight.FlightDescriptor,  # pyright: ignore[reportPrivateImportUsage]
    ) -> flight.FlightInfo:  # pyright: ignore[reportPrivateImportUsage]
        # REQ-1882: the whole RPC runs on this handler thread's own loop.
        return run_rpc(
            lambda: self._in_serving_org(lambda: self._get_flight_info_on_loop(context, descriptor))
        )

    def _get_flight_info_on_loop(
        self,
        context: flight.ServerCallContext,  # pyright: ignore[reportPrivateImportUsage]
        descriptor: flight.FlightDescriptor,  # pyright: ignore[reportPrivateImportUsage]
    ) -> flight.FlightInfo:  # pyright: ignore[reportPrivateImportUsage]
        role_id = self._role(context)
        path = [p.decode("utf-8") if isinstance(p, bytes) else p for p in descriptor.path]
        if len(path) != 2:
            raise _err(f"airport: descriptor path must be [schema, table], got {path}")
        return self._flight_info_for(role_id, path[0], path[1])

    def _flight_info_for(self, role_id: str, schema: str, table: str) -> flight.FlightInfo:  # pyright: ignore[reportPrivateImportUsage]
        sql_ref = self._lookup(role_id, schema, table)
        base = self._base_schema_cached(role_id, schema, table, sql_ref)
        pk = self._pk_for(role_id, schema, table)
        return self._table_flight_info(schema, table, self._advertised_schema(base, pk))

    # ------------------------------------------------------------- DoGet
    def do_get(  # pyright: ignore[reportPrivateImportUsage]
        self,
        context: flight.ServerCallContext,  # pyright: ignore[reportPrivateImportUsage]
        ticket: flight.Ticket,  # pyright: ignore[reportPrivateImportUsage]
    ) -> flight.GeneratorStream:  # pyright: ignore[reportPrivateImportUsage]
        # REQ-1882: the whole RPC — and the stream it returns — runs on this handler thread's loop.
        return run_rpc(lambda: self._in_serving_org(lambda: self._do_get_on_loop(context, ticket)))

    def _do_get_on_loop(
        self,
        context: flight.ServerCallContext,  # pyright: ignore[reportPrivateImportUsage]
        ticket: flight.Ticket,  # pyright: ignore[reportPrivateImportUsage]
    ) -> flight.GeneratorStream:  # pyright: ignore[reportPrivateImportUsage]
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
        base = self._base_schema_cached(role_id, schema, table, sql_ref)  # full advertised base
        pk = self._pk_for(role_id, schema, table)
        advertised = self._advertised_schema(base, pk)

        if not columns and not where:
            # No pushdown — full-table governed scan, STREAMED (never materialized here; Defect 5).
            scan = governed_table_scan_stream(self._state, sql_ref, role_id)
        else:
            # Pushdown: build the semantic SELECT with source-side projection + WHERE. Injecting the
            # predicate as a semantic WHERE means it flows through the IDENTICAL governance a user's
            # /data/sql WHERE would (RLS AND-ed, masking, visibility) — the SOURCE filters and reads
            # only the projected columns, not just the DuckDB client.
            scan_cols = columns
            if scan_cols is not None and pk:
                # The rowid is derived from the PK, so the PK columns must be scanned even when the
                # client projected them out — otherwise the row identity could not be formed.
                scan_cols = list(dict.fromkeys(scan_cols + [c for c in pk if c not in scan_cols]))
            col_sql = ", ".join(f'"{c}"' for c in scan_cols) if scan_cols else "*"
            sql = f'SELECT {col_sql} FROM "{schema}"."{table}"'
            if where:
                sql += f" WHERE {where}"
            _trace_pushdown(sql)
            scan = governed_table_scan_stream(self._state, sql, role_id)

        # Stream each governed batch reshaped to the FULL advertised schema (the airport contract:
        # DuckDB planned against the flight_info schema and projects client-side; a narrowed stream
        # would mismatch). Source-side projection still happened above — the columns DuckDB projected
        # out are null-filled per batch and never read. The is_rowid pseudo-column (JSON PK tuple) is
        # appended per batch when the table has a PK, so DuckDB can echo it back on UPDATE/DELETE.
        # REQ-1882: pyarrow pulls the batches after do_get returns (a DIRECT scan's cursor fetches
        # on this RPC's loop), so the stream holds the loop until it ends.
        out_gen = hold_loop_for_stream(
            _bound_batches(self._serving_org(), self._reshape_batches(scan.batches, base, pk))
        )
        # REQ-1350: what the scan's answer says about itself rides ahead of the rows.
        return generator_stream(advertised, out_gen, scan.warnings)  # pyright: ignore[reportPrivateImportUsage]

    def _reshape_batches(self, batch_gen: Any, base: pa.Schema, pk: list[str]) -> Any:
        """Yield each streamed RecordBatch padded to ``base`` (+ per-batch rowid when PK) — the
        streaming analogue of _pad_to_schema + _append_rowid, one batch in memory at a time."""
        for batch in batch_gen:
            padded = _pad_batch_to_schema(batch, base)
            if pk:
                padded = self._append_rowid_batch(padded, pk)
            yield padded

    # ------------------------------------------------------------- DoExchange (DML)
    def do_exchange(  # pyright: ignore[reportPrivateImportUsage]
        self,
        context: flight.ServerCallContext,  # pyright: ignore[reportPrivateImportUsage]
        descriptor: flight.FlightDescriptor,  # pyright: ignore[reportPrivateImportUsage]  # noqa: ARG002
        reader,
        writer,
    ) -> None:
        # REQ-1882: the whole RPC runs on this handler thread's own loop.
        run_rpc(
            lambda: self._in_serving_org(lambda: self._do_exchange_on_loop(context, reader, writer))
        )

    def _do_exchange_on_loop(
        self,
        context: flight.ServerCallContext,  # pyright: ignore[reportPrivateImportUsage]
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
            self._do_exchange_pk_mutation(role_id, schema, table, operation, reader, writer)
            return
        raise _err(f"airport: unsupported do_exchange operation {operation!r}")

    def _do_exchange_insert(self, role_id: str, schema: str, table: str, reader, writer) -> None:
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
        out_schema = self._base_schema_cached(
            role_id, schema, table, self._lookup(role_id, schema, table)
        )
        writer.begin(out_schema)

        incoming = reader.read_all()
        total = incoming.num_rows
        if total:
            sql = self._build_insert_sql(schema, table, incoming)
            # ONE pipeline: submit the mutation SQL through the SAME governed write path as
            # /data/sql (writable-column ACL, RLS, write-routing all apply).
            governed_mutation(self._state, sql, role_id)
        writer.write_metadata(pa.py_buffer(wire._encode({"total_changed": total})))

    def _do_exchange_pk_mutation(
        self, role_id: str, schema: str, table: str, operation: str, reader, writer
    ) -> None:
        """Governed UPDATE/DELETE keyed by PRIMARY-KEY row identity (REQ-1120/REQ-1125).

        The airport extension echoes back the ``is_rowid`` pseudo-column (Provisa's JSON-encoded PK
        tuple) for each affected row, plus — for UPDATE — the new column values. We decode the rowids
        to PK tuples and issue a WHERE-qualified UPDATE/DELETE through the SAME governed write pipeline
        as /data/sql (``_compile_govern_execute``): writable-column ACL, RLS injection on the WHERE,
        and write-routing all apply. Because the client's rowids are only PKs RLS already let it read,
        and the governed WHERE is RLS-filtered again, a role can never mutate a row it cannot see —
        an attempt is a governed no-op (total_changed 0).
        """
        from provisa.executor.writable import is_writable_on

        pk = self._pk_for(role_id, schema, table)
        if not pk:
            # Refused ONLY per-table: no primary key → no safe row identity to form a WHERE. Not a
            # global gap — a table WITH a PK is fully supported.
            raise _err(
                f"airport: {operation} unsupported for {schema}.{table} — the table has no primary "
                "key, so no safe governed row identity can be formed"
            )
        source_type = self._source_type_for(role_id, schema, table)
        if not is_writable_on(source_type, self._state.federation_engine.engine):
            raise _err(
                f"airport: source type {source_type!r} for {schema}.{table} is read-only on the "
                f"active engine — {operation.upper()} refused"
            )

        # Begin the bidirectional stream (airport requires the server to send a schema before the
        # client streams the identity/value rows). No RETURNING chunks are emitted — DuckDB reads
        # only the trailing total_changed metadata.
        out_schema = self._base_schema_cached(
            role_id, schema, table, self._lookup(role_id, schema, table)
        )
        writer.begin(out_schema)

        incoming = reader.read_all()
        total = 0
        if incoming.num_rows:
            pk_tuples = self._decode_rowids(incoming, pk)
            if operation == "delete":
                sql = self._build_delete_sql(schema, table, pk, pk_tuples)
                total = governed_mutation(self._state, sql, role_id)
            else:
                total = self._apply_updates(role_id, schema, table, pk, pk_tuples, incoming)
        writer.write_metadata(pa.py_buffer(wire._encode({"total_changed": total})))

    @staticmethod
    def _decode_rowids(incoming: pa.Table, pk: list[str]) -> list[list[Any]]:
        """Decode the incoming ``is_rowid`` column (JSON PK tuples) back to per-row PK value lists."""
        names = incoming.schema.names
        rowid_idx = names.index(_ROWID_FIELD) if _ROWID_FIELD in names else None
        if rowid_idx is None:
            for i, field in enumerate(incoming.schema):
                if field.metadata and field.metadata.get(b"is_rowid"):
                    rowid_idx = i
                    break
        if rowid_idx is None:
            raise _err("airport: UPDATE/DELETE stream carries no rowid identity column")
        out: list[list[Any]] = []
        for raw in incoming.column(rowid_idx).to_pylist():
            decoded = json.loads(raw)
            if not isinstance(decoded, list) or len(decoded) != len(pk):
                raise _err(f"airport: malformed rowid {raw!r} for primary key {pk}")
            out.append(decoded)
        return out

    def _apply_updates(
        self,
        role_id: str,
        schema: str,
        table: str,
        pk: list[str],
        pk_tuples: list[list[Any]],
        incoming: pa.Table,
    ) -> int:
        """Issue one governed UPDATE per affected row (each row carries its own new column values)."""
        rows = incoming.to_pylist()
        set_cols = [c for c in incoming.schema.names if c != _ROWID_FIELD and c not in pk]
        if not set_cols:
            return 0  # nothing to change beyond the identity itself
        total = 0
        for row, pk_vals in zip(rows, pk_tuples):
            assignments = ", ".join(f'"{c}" = {_literal(row[c])}' for c in set_cols)
            where = " AND ".join(f'"{col}" = {_literal(val)}' for col, val in zip(pk, pk_vals))
            sql = (
                f'UPDATE "{schema}"."{table}" SET {assignments} WHERE {where} '
                f"RETURNING {', '.join(chr(34) + c + chr(34) for c in pk)}"
            )
            total += governed_mutation(self._state, sql, role_id)
        return total

    @staticmethod
    def _build_delete_sql(
        schema: str, table: str, pk: list[str], pk_tuples: list[list[Any]]
    ) -> str:
        pk_ret = ", ".join(f'"{c}"' for c in pk)
        if len(pk) == 1:
            values = ", ".join(_literal(t[0]) for t in pk_tuples)
            where = f'"{pk[0]}" IN ({values})'
        else:
            # Composite PK — OR of per-row equality tuples.
            clauses = [
                "(" + " AND ".join(f'"{c}" = {_literal(v)}' for c, v in zip(pk, t)) + ")"
                for t in pk_tuples
            ]
            where = " OR ".join(clauses)
        return f'DELETE FROM "{schema}"."{table}" WHERE {where} RETURNING {pk_ret}'

    @staticmethod
    def _build_insert_sql(schema: str, table: str, data: pa.Table) -> str:
        cols = list(data.schema.names)
        col_sql = ", ".join(f'"{c}"' for c in cols)
        rows = data.to_pylist()
        values = []
        for row in rows:
            values.append("(" + ", ".join(_literal(row[c]) for c in cols) + ")")
        return f'INSERT INTO "{schema}"."{table}" ({col_sql}) VALUES ' + ", ".join(values)


def _as_str(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, bytes):
        return value.decode("utf-8")
    return str(value)


def _arrow_schema_to_columns(schema: pa.Schema) -> list[tuple[str, str]]:
    """Map an airport create_table Arrow schema to (name, IR-type) column defs (REQ-1120).

    IR names are the ONE engine-independent vocabulary (ir_types.py); the store write face renders
    them per the target source dialect at DDL time. An Arrow type with no faithful IR mapping raises
    — never a silent ``text`` default, matching the IR hub's no-guess contract.
    """
    out: list[tuple[str, str]] = []
    for field in schema:
        out.append((field.name, _arrow_type_to_ir(field.type)))
    return out


def _arrow_type_to_ir(t: pa.DataType) -> str:
    if pa.types.is_boolean(t):
        return "boolean"
    if pa.types.is_int8(t) or pa.types.is_int16(t) or pa.types.is_uint8(t):
        return "smallint"
    if pa.types.is_int32(t) or pa.types.is_uint16(t):
        return "integer"
    if pa.types.is_int64(t) or pa.types.is_uint32(t) or pa.types.is_uint64(t):
        return "bigint"
    if pa.types.is_float32(t):
        return "float"
    if pa.types.is_float64(t):
        return "double"
    if pa.types.is_decimal(t):
        return "numeric"
    if pa.types.is_date(t):
        return "date"
    if pa.types.is_timestamp(t):
        return "timestamp"
    if pa.types.is_time(t):
        return "time"
    if pa.types.is_binary(t) or pa.types.is_large_binary(t):
        return "bytea"
    if pa.types.is_string(t) or pa.types.is_large_string(t):
        return "text"
    raise _err(f"airport: create_table Arrow type {t!r} has no IR mapping")


def _pad_batch_to_schema(batch: pa.RecordBatch, full_schema: pa.Schema) -> pa.RecordBatch:
    """Re-shape a source-side-projected scan batch to the full advertised schema (null-fill absentees).

    Keeps the DoGet stream schema == the flight_info schema DuckDB planned with, while preserving
    the source-side projection (only projected columns were actually read from the source). Columns
    the client projected out are null here and are never read by DuckDB. Applied per streamed batch
    so peak memory is one batch, not the whole scan (Defect 5).
    """
    present = set(batch.schema.names)
    n = batch.num_rows
    arrays: list[pa.Array] = []
    for field in full_schema:
        if field.name in present:
            arrays.append(batch.column(batch.schema.get_field_index(field.name)))
        else:
            arrays.append(pa.nulls(n, type=field.type))
    return pa.RecordBatch.from_arrays(arrays, schema=full_schema)


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


def _literal(value: Any) -> str:
    """A value from an Arrow row as a literal of the governed statement, which is PostgreSQL SQL
    re-parsed and re-governed by _compile_govern_execute: the dialect's one literal rule."""
    from provisa.compiler.sql_literals import sql_literal

    return sql_literal(value, "postgres")
