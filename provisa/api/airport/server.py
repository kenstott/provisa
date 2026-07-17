# Copyright (c) 2026 Kenneth Stott
# Canary: 6c7c7c80-b434-4cb6-8935-f26b8cba2448
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Provisa airport Flight server (REQ-1106).

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

Implemented (REQ-1106):
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
  * DDL: create_schema / drop_schema mapped to the Provisa domain catalog; create_table
    creates the physical table in the target domain's writable source (schema-mutation
    pipeline) so the new table joins the catalog and accepts a governed INSERT.

Refused with a correct Flight protocol error (never a silent no-op), each justified
inline: column_statistics, table_function_flight_info, and the physical column/struct
mutations (add_column / rename_table / add_field / …) — see the handlers for why each
has no sound meaning for a governed federation catalog on the active engine. UPDATE/
DELETE are refused ONLY per-table, when a table genuinely has no primary key.

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

# The airport row-identity pseudo-column. Advertised as an Arrow field carrying the extension's
# ``is_rowid`` metadata key (ground-truthed against airport-go v0.2.1 catalog/helpers.go
# FindRowIDColumn + the Query-farm airport docs): DuckDB hides it from ``SELECT *`` and streams it
# back on UPDATE/DELETE so the server can identify the affected rows. Provisa fills it with the
# table's PRIMARY-KEY tuple (JSON-encoded), so the only identities a role receives are the PKs of
# rows RLS already let it read — a role cannot target a row it cannot see (REQ-1106/REQ-1111).
_ROWID_FIELD = "rowid"
_ROWID_META = {b"is_rowid": b"true"}


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
            # Documented dev default for unauthenticated access (REQ-1106). No token AND
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

    def _append_rowid(self, tbl: pa.Table, pk: list[str]) -> pa.Table:
        """Append the rowid column = JSON-encoded PK tuple per row (decoded back on UPDATE/DELETE)."""
        pk_lists = {c: tbl.column(c).to_pylist() for c in pk}
        values = [
            json.dumps([pk_lists[c][i] for c in pk], default=str) for i in range(tbl.num_rows)
        ]
        return tbl.append_column(self._rowid_field(), pa.array(values, type=pa.string()))

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
        if atype == "create_table":
            return [flight.Result(self._do_create_table(context, body))]  # pyright: ignore[reportPrivateImportUsage]
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
            # Refused: these ALTER an existing physical table's shape. A governed federation catalog
            # binds a semantic model over heterogeneous, independently-owned sources; airport supplies
            # only a single-column Arrow schema, with no source dialect / governance policy — so there
            # is no sound, unambiguous mapping. Physical-shape ALTERs go through the admin schema-
            # mutation API (update_table) with that context. (Struct-field ops add_field/rename_field/
            # remove_field have no meaning at all for a relational federation catalog.) create_schema/
            # drop_schema map to domains; create_table maps to a governed create in the writable source.
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
            pk = self._pk_for(role_id, schema, table)
            by_schema.setdefault(schema, []).append(
                (schema, table, self._advertised_schema(tbl.schema, pk))
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
        # PUSHDOWN (REQ-1106): the airport extension delivers projection (column_ids) + predicate
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

    # ------------------------------------------------------------- DDL → physical create_table
    def _do_create_table(self, context: flight.ServerCallContext, body: bytes) -> bytes:  # pyright: ignore[reportPrivateImportUsage]
        """create_table → CREATE the physical table in the target domain's WRITABLE source, then
        register it in the governed model so it joins the airport catalog and accepts a governed
        INSERT (REQ-1106/REQ-1111).

        The airport ``schema_name`` is a Provisa DOMAIN (sql name). The physical table is created in
        that domain's single writable source (derived from the domain's existing tables — REQ-1000
        single-writable-or-reject) via the store write face (the SAME face CTAS uses to create tables
        in a writable relational source), then registered through the schema-mutation core so the ONE
        catalog serves it. Admin-capability gated, exactly like create_schema.
        """
        role_id = self._role(context)
        self._require_admin(role_id)
        req = wire.decode_action_request(body)
        schema = _as_str(req.get("schema_name") or req.get("schema"))
        table = _as_str(req.get("table_name") or req.get("name"))
        if not schema or not table:
            raise _err("airport: create_table requires schema_name and table_name")
        raw_schema = req.get("arrow_schema")
        if not raw_schema:
            raise _err("airport: create_table requires an arrow_schema")
        if isinstance(raw_schema, str):
            raw_schema = raw_schema.encode("latin-1")
        arrow_schema = pa.ipc.read_schema(pa.py_buffer(raw_schema))
        columns = _arrow_schema_to_columns(arrow_schema)

        source_id, domain_id, phys_schema = self._resolve_writable_target(role_id, schema)
        asyncio.run_coroutine_threadsafe(
            self._create_and_register_table(
                source_id, domain_id, phys_schema, table, columns
            ),
            self._main_loop,
        ).result()
        # airport create_table response = the new table's FlightInfo protobuf (matches airport-go
        # buildTableFlightInfo). The fresh table has no PK yet, so it advertises no rowid.
        return self._table_flight_info(schema, table, arrow_schema).serialize()

    def _resolve_writable_target(self, role_id: str, airport_schema: str) -> tuple[str, str, str]:
        """Resolve (source_id, domain_id, physical_schema) for a create_table into ``airport_schema``.

        The airport schema is a domain; its writable source is derived from the domain's existing
        tables (REQ-1000): exactly one writable source is the target, zero is refused (nothing to
        bind to), more than one is refused as ambiguous — never a silent pick.
        """
        from provisa.compiler.naming import domain_to_sql_name
        from provisa.executor.writable import is_writable_on

        ctx = self._state.contexts[role_id]
        engine = self._state.federation_engine.engine
        candidates: dict[str, tuple[str, str]] = {}  # source_id -> (domain_id, physical_schema)
        for field_name, meta in getattr(ctx, "tables", {}).items():
            if field_name.endswith(("_aggregate", "_connection", "_group_by", "GroupBy")):
                continue
            if (domain_to_sql_name(meta.domain_id) or "default") != airport_schema:
                continue
            # Restrict to the SAME source set the airport catalog advertises (see _catalog_for_role):
            # an external, queryable data pool that is not a streaming (kafka) source. A streaming or
            # internal/system source is never a valid physical create_table target.
            if not self._state.source_pools.has(meta.source_id):
                continue
            stype = self._state.source_types.get(meta.source_id) or (meta.source_type or "")
            if stype == "kafka":
                continue
            if not is_writable_on(stype, engine):
                continue
            candidates.setdefault(meta.source_id, (meta.domain_id, meta.schema_name))
        if not candidates:
            raise _err(
                f"airport: schema {airport_schema!r} has no writable source bound — cannot "
                "create_table (create the table in the source, or bind a writable source to the domain)"
            )
        if len(candidates) > 1:
            raise _err(
                f"airport: schema {airport_schema!r} maps to multiple writable sources "
                f"{sorted(candidates)} — ambiguous create_table target"
            )
        source_id, (domain_id, phys_schema) = next(iter(candidates.items()))
        return source_id, domain_id, phys_schema

    async def _create_and_register_table(
        self,
        source_id: str,
        domain_id: str,
        phys_schema: str,
        table: str,
        columns: list[tuple[str, str]],
    ) -> None:
        from provisa.api.admin.schema_helpers import _rebuild_schemas
        from provisa.api.app import state as app_state
        from provisa.compiler.naming import apply_convention
        from provisa.core.models import Column as ColumnModel, Table as TableModel
        from provisa.core.repositories import table as table_repo
        from provisa.executor.ctas import _source_sqlalchemy_dsn
        from provisa.federation import store_writer

        # Physical create in the writable source via the store write face — the SAME primitive CTAS
        # uses to create a table in a writable relational source (not a parallel DDL path).
        dsn = _source_sqlalchemy_dsn(app_state, source_id)
        await store_writer.ensure_table(
            dsn, schema=phys_schema, table=table, columns=columns
        )

        # Register in the governed model (schema-mutation core: table_repo.upsert + rebuild) so the
        # one catalog serves it. Columns are visible+writable to every role — a create_table author
        # publishes an open governed table; governance can be tightened afterward via the admin API.
        all_roles = list(self._state.roles.keys()) or ["admin"]
        col_models = [
            ColumnModel(
                name=name, data_type=ir_type, visible_to=all_roles, writable_by=all_roles
            )
            for name, ir_type in columns
        ]
        model = TableModel(
            source_id=source_id,
            domain_id=domain_id,
            schema_name=phys_schema,
            table_name=table,
            columns=col_models,
            alias=apply_convention(table, "apollo_graphql"),
        )
        pool = self._state.tenant_db
        assert pool is not None, "control-plane pool not initialized"  # live at DDL time
        async with pool.acquire() as conn:
            await table_repo.upsert(conn, model)
        await _rebuild_schemas()

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
        pk = self._pk_for(role_id, schema, table)
        return self._table_flight_info(schema, table, self._advertised_schema(tbl.schema, pk))

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
        pk = self._pk_for(role_id, schema, table)
        if not columns and not where:
            # No pushdown — full-table governed scan (cached, schema-stable).
            out = full
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
            scanned = governed_table_scan_arrow(self._state, self._main_loop, sql, role_id)
            # Return the FULL advertised schema (the airport contract: DuckDB planned the scan against
            # the flight_info schema and projects client-side; a narrowed DoGet stream would mismatch).
            # Source-side projection still happened above — unprojected columns are null-filled here
            # and DuckDB never reads them (they are exactly the columns it projected out).
            out = _pad_to_schema(scanned, full.schema)
        if pk:
            out = self._append_rowid(out, pk)  # the is_rowid pseudo-column DuckDB echoes on UPDATE/DELETE
        return flight.RecordBatchStream(out)  # pyright: ignore[reportPrivateImportUsage]

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
            self._do_exchange_pk_mutation(role_id, schema, table, operation, reader, writer)
            return
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

    def _do_exchange_pk_mutation(
        self, role_id: str, schema: str, table: str, operation: str, reader, writer
    ) -> None:
        """Governed UPDATE/DELETE keyed by PRIMARY-KEY row identity (REQ-1106/REQ-1111).

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
        out_schema = self._scan_cached(
            role_id, schema, table, self._lookup(role_id, schema, table)
        ).schema
        writer.begin(out_schema)

        incoming = reader.read_all()
        total = 0
        if incoming.num_rows:
            pk_tuples = self._decode_rowids(incoming, pk)
            if operation == "delete":
                sql = self._build_delete_sql(schema, table, pk, pk_tuples)
                total = governed_mutation(self._state, self._main_loop, sql, role_id)
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
            assignments = ", ".join(f'"{c}" = {_sql_literal(row[c])}' for c in set_cols)
            where = " AND ".join(
                f'"{col}" = {_sql_literal(val)}' for col, val in zip(pk, pk_vals)
            )
            sql = (
                f'UPDATE "{schema}"."{table}" SET {assignments} WHERE {where} '
                f'RETURNING {", ".join(chr(34) + c + chr(34) for c in pk)}'
            )
            total += governed_mutation(self._state, self._main_loop, sql, role_id)
        return total

    @staticmethod
    def _build_delete_sql(
        schema: str, table: str, pk: list[str], pk_tuples: list[list[Any]]
    ) -> str:
        pk_ret = ", ".join(f'"{c}"' for c in pk)
        if len(pk) == 1:
            values = ", ".join(_sql_literal(t[0]) for t in pk_tuples)
            where = f'"{pk[0]}" IN ({values})'
        else:
            # Composite PK — OR of per-row equality tuples.
            clauses = [
                "(" + " AND ".join(f'"{c}" = {_sql_literal(v)}' for c, v in zip(pk, t)) + ")"
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
            values.append("(" + ", ".join(_sql_literal(row[c]) for c in cols) + ")")
        return f'INSERT INTO "{schema}"."{table}" ({col_sql}) VALUES ' + ", ".join(values)


def _as_str(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, bytes):
        return value.decode("utf-8")
    return str(value)


def _arrow_schema_to_columns(schema: pa.Schema) -> list[tuple[str, str]]:
    """Map an airport create_table Arrow schema to (name, IR-type) column defs (REQ-1106).

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
