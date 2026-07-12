# Copyright (c) 2026 Kenneth Stott
# Canary: placeholder
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Transport contract harness (REQ-264, REQ-128, REQ-802).

Line coverage says nothing about the combinatorial matrix this platform actually
is: N sources x M consumption transports x K query languages x governance. The
highest-risk claim is that EVERY governed transport returns the SAME data and
enforces the SAME governance — a bug where RLS/column-visibility holds over REST
but leaks over Bolt is a data breach that per-file coverage cannot surface.

This harness pins that claim as a CONTRACT run over every transport, against one
shared server + dataset:

  1. Equivalence — the same logical read returns the same rows on every transport.
  2. Column visibility — the admin-only ``amount`` column NEVER reaches a
     non-admin role, on any transport.

Adding a transport = one TransportAdapter. Today: REST /data/sql (SQL over HTTP),
pgwire (SQL over the Postgres wire), Bolt (Cypher over the Bolt wire), and Arrow
Flight (SQL over the app's OWN governed Flight server — a JSON ticket carrying the
query + role, run through the governed pipeline; NOT the Zaychik/raw-Trino Flight
SQL client, which is ungoverned), and gRPC (a per-config generated proto, so the
client discovers the service + orders RPC by reflection and passes the role in
x-provisa-role metadata), and GraphQL over HTTP (POST /data/graphql; a column the
role cannot see is absent from its per-role schema, so selecting it is a validation
error — governance enforced at the schema boundary). All five governed consumption
surfaces are now covered.
"""

from __future__ import annotations

import pytest

from tests.integration.isolated_server import IsolatedServer, drop_org_schema

pytestmark = [pytest.mark.e2e]

_ORG = "transport_contract"
# sample_config.yaml: orders(id, customer_id, amount, region, status, created_at)
# with amount visible_to [admin] only, and role `analyst` restricted to the
# sales-analytics domain.
_ADMIN = "admin"
_RESTRICTED = "analyst"


# --------------------------------------------------------------------------- #
# Transport adapters — each reads the SAME logical query and normalizes the
# result to (columns, rows) so different wire shapes become comparable.
# --------------------------------------------------------------------------- #
class TransportAdapter:
    name: str

    def read(self, role: str, columns: str) -> tuple[list[str], list[tuple]]:
        """Return (column_names, rows) for `SELECT <columns> FROM orders` as `role`.

        `columns` is a comma-separated list understood by every adapter (the SQL
        adapters pass it through; the Cypher adapter maps each to o.<col>)."""
        raise NotImplementedError


class RestSqlAdapter(TransportAdapter):
    name = "rest_sql"

    def __init__(self, server: IsolatedServer) -> None:
        self._base = server.base_url

    def read(self, role, columns):
        import httpx

        with httpx.Client(base_url=self._base, timeout=60.0) as c:
            resp = c.post(
                "/data/sql",
                json={"sql": f"SELECT {columns} FROM orders ORDER BY id"},
                headers={"x-provisa-role": role},
            )
        if resp.status_code != 200:
            raise _Denied(f"rest_sql {role}: HTTP {resp.status_code} {resp.text[:200]}")
        return _parse_rest(resp.json(), columns)


class GraphqlAdapter(TransportAdapter):
    name = "graphql"
    # sample_config: domain_prefix on, sales-analytics domain -> orders is `sa__orders`.
    _FIELD = "sa__orders"

    def __init__(self, server: IsolatedServer) -> None:
        self._base = server.base_url

    def read(self, role, columns):
        import httpx

        selection = " ".join(_cols(columns))
        with httpx.Client(base_url=self._base, timeout=60.0) as c:
            resp = c.post(
                "/data/graphql",
                json={"query": f"{{ {self._FIELD} {{ {selection} }} }}"},
                headers={"x-provisa-role": role},
            )
        if resp.status_code != 200:
            raise _Denied(f"graphql {role}: HTTP {resp.status_code} {resp.text[:200]}")
        body = resp.json()
        # A field the role cannot see is not in its per-role schema -> validation error.
        if body.get("errors"):
            raise _Denied(f"graphql {role}: {body['errors']}")
        data = (body.get("data") or {}).get(self._FIELD)
        if data is None:
            raise _Denied(f"graphql {role}: null data")
        cols = _cols(columns)
        rows = [tuple(r.get(c) for c in cols) for r in data]
        return cols, rows


class PgwireAdapter(TransportAdapter):
    name = "pgwire"

    def __init__(self, server: IsolatedServer) -> None:
        self._port = server.pgwire_port

    def read(self, role, columns):
        import psycopg2

        # Trust mode: the connection username maps directly to the Provisa role_id.
        # Trust mode ignores the password but psycopg2 requires a non-empty one.
        conn = psycopg2.connect(
            host="127.0.0.1", port=self._port, user=role, password="provisa", dbname="provisa"
        )
        try:
            cur = conn.cursor()
            try:
                cur.execute(f"SELECT {columns} FROM orders ORDER BY id")
                rows = [tuple(r) for r in cur.fetchall()]
                cols = [d[0] for d in cur.description]
                return cols, rows
            except psycopg2.Error as exc:
                raise _Denied(f"pgwire {role}: {exc}") from exc
            finally:
                cur.close()
        finally:
            conn.close()


class BoltAdapter(TransportAdapter):
    name = "bolt"

    def __init__(self, server: IsolatedServer) -> None:
        self._uri = f"bolt://127.0.0.1:{server.bolt_port}"

    def read(self, role, columns):
        ret = ", ".join(f"o.{c} AS {c}" for c in _cols(columns))
        return self.run_return(role, ret)

    def run_return(self, role, return_clause: str) -> tuple[list[str], list[tuple]]:
        """Run `MATCH (o:orders) RETURN <return_clause>` as `role`, normalized to
        (columns, rows). Role travels as the Bolt auth principal (pgwire's trust
        model). Used for both the canonical read and metamorphic probes."""
        from neo4j import GraphDatabase
        from neo4j.exceptions import Neo4jError

        driver = GraphDatabase.driver(self._uri, auth=(role, ""))
        try:
            with driver.session() as sess:
                try:
                    result = sess.run(f"MATCH (o:orders) RETURN {return_clause}")
                    records = list(result)
                    keys = list(result.keys())
                except Neo4jError as exc:
                    raise _Denied(f"bolt {role}: {exc}") from exc
                rows = [tuple(rec[k] for k in keys) for rec in records]
                return keys, rows
        finally:
            driver.close()


class FlightAdapter(TransportAdapter):
    name = "flight"

    def __init__(self, server: IsolatedServer) -> None:
        self._loc = f"grpc://127.0.0.1:{server.flight_port}"

    def read(self, role, columns):
        import json

        import pyarrow.flight as flight

        # The app's OWN Flight server (not Zaychik): the ticket carries the SQL and
        # the role, and do_get runs it through the governed pipeline. Role lives in
        # the ticket, so no separate handshake is needed.
        client = flight.FlightClient(self._loc)
        ticket = flight.Ticket(
            json.dumps(
                {"query": f"SELECT {columns} FROM orders ORDER BY id", "role": role}
            ).encode()
        )
        try:
            table = client.do_get(ticket).read_all()
        except flight.FlightError as exc:
            raise _Denied(f"flight {role}: {exc}") from exc
        finally:
            client.close()
        cols = list(table.column_names)
        rows = [tuple(r[c] for c in cols) for r in table.to_pylist()]
        return cols, rows


class GrpcAdapter(TransportAdapter):
    name = "grpc"

    def __init__(self, server: IsolatedServer) -> None:
        self._target = f"127.0.0.1:{server.grpc_port}"

    def read(self, role, columns):
        import grpc
        from google.protobuf.descriptor_pool import DescriptorPool
        from google.protobuf.message_factory import GetMessageClass
        from grpc_reflection.v1alpha.proto_reflection_descriptor_database import (
            ProtoReflectionDescriptorDatabase,
        )

        wanted = _cols(columns)
        channel = grpc.insecure_channel(self._target)
        try:
            # The proto is generated per-config, so discover the service + the orders
            # server-streaming RPC dynamically via reflection rather than shipping stubs.
            db = ProtoReflectionDescriptorDatabase(channel)
            pool = DescriptorPool(db)
            svc_name = next(s for s in db.get_services() if s.endswith("Service"))
            svc = pool.FindServiceByName(svc_name)
            method = next(
                m for m in svc.methods if m.name.startswith("Query") and "rder" in m.name
            )
            req_cls = GetMessageClass(method.input_type)
            resp_cls = GetMessageClass(method.output_type)
            rpc = channel.unary_stream(
                f"/{svc.full_name}/{method.name}",
                request_serializer=req_cls.SerializeToString,
                response_deserializer=resp_cls.FromString,
            )
            try:
                responses = list(
                    rpc(req_cls(), metadata=(("x-provisa-role", role),), timeout=60)
                )
            except grpc.RpcError as exc:
                raise _Denied(f"grpc {role}: {exc.details()}") from exc
            # A governed column the role cannot see is absent from the response message.
            present = {f.name for f in resp_cls.DESCRIPTOR.fields}
            cols = [c for c in wanted if c in present]
            rows = [tuple(getattr(msg, c) for c in cols) for msg in responses]
            return cols, rows
        finally:
            channel.close()


class _Denied(Exception):
    """A transport refused the query (governance denial at the wire boundary)."""


def _cols(columns: str) -> list[str]:
    return [c.strip() for c in columns.split(",")]


def _parse_rest(body, columns: str) -> tuple[list[str], list[tuple]]:
    """Normalize the /data/sql response to (columns, rows).

    The SQL endpoint returns {"data": {<table>: [rowdict, ...]}} (serialize_rows);
    handle the {"columns", "rows"} and flat-list shapes too so the adapter is
    resilient to endpoint variants."""
    if isinstance(body, dict) and "rows" in body and "columns" in body:
        cols = list(body["columns"])
        return cols, [tuple(r) for r in body["rows"]]
    d = body["data"] if isinstance(body, dict) and "data" in body else body
    if isinstance(d, dict):
        rowdicts = next(iter(d.values())) if d else []  # {table: [rowdicts]}
    else:
        rowdicts = d
    if not rowdicts:
        return _cols(columns), []
    cols = list(rowdicts[0].keys())
    return cols, [tuple(r[c] for c in cols) for r in rowdicts]


# --------------------------------------------------------------------------- #
# Fixtures
# --------------------------------------------------------------------------- #
@pytest.fixture(scope="module")
def contract_server():
    """One Provisa server exposing every governed transport, over sample_config."""
    server = IsolatedServer(
        _ORG,
        enable_bolt=True,
        enable_pgwire=True,
        await_flight=True,
        await_grpc=True,
        config="tests/fixtures/sample_config.yaml",
    )
    server.start()
    try:
        yield server
    finally:
        server.stop_process()
        import asyncio

        asyncio.run(drop_org_schema(_ORG))


@pytest.fixture(
    params=[RestSqlAdapter, PgwireAdapter, BoltAdapter, FlightAdapter, GrpcAdapter, GraphqlAdapter],
    ids=["rest_sql", "pgwire", "bolt", "flight", "grpc", "graphql"],
)
def adapter(request, contract_server) -> TransportAdapter:
    return request.param(contract_server)


# --------------------------------------------------------------------------- #
# Contract 1 — cross-transport equivalence
# --------------------------------------------------------------------------- #
def test_transport_returns_same_rows_as_reference(adapter, contract_server):
    """Every transport returns the SAME (id, region) rows for the admin read.

    REST /data/sql is the reference; each transport must agree. A transport that
    returns different rows for the same logical query is a correctness defect the
    combinatorial matrix would otherwise hide.
    """
    _, ref_rows = RestSqlAdapter(contract_server).read(_ADMIN, "id, region")
    assert ref_rows, "reference read returned no rows — dataset not seeded"

    _, rows = adapter.read(_ADMIN, "id, region")
    assert _norm(rows) == _norm(ref_rows), (
        f"{adapter.name} rows diverge from the reference transport"
    )


# --------------------------------------------------------------------------- #
# Contract 2 — column visibility enforced on every transport
# --------------------------------------------------------------------------- #
def test_admin_only_column_never_reaches_restricted_role(adapter):
    """The admin-only `amount` column must not leak to `analyst` on ANY transport.

    A pass = either the transport rejects the query outright (denial at the wire
    boundary) OR it returns rows with no real `amount` value. The invariant is
    that a restricted role never obtains a governed value — however the transport
    chooses to enforce it.
    """
    # Positive control: admin CAN read amount — so the analyst check below is
    # discriminating governance, not a column that is simply absent for everyone.
    admin_cols, admin_rows = adapter.read(_ADMIN, "id, amount")
    if "amount" in [c.lower() for c in admin_cols]:
        aidx = [c.lower() for c in admin_cols].index("amount")
        assert any(r[aidx] not in (None, 0, "0", "") for r in admin_rows), (
            f"{adapter.name}: admin saw no real `amount` values — positive control failed"
        )

    try:
        cols, rows = adapter.read(_RESTRICTED, "id, amount")
    except _Denied:
        return  # rejected outright — the strongest form of enforcement

    # Not rejected: then `amount` must not carry a real value for the restricted role.
    if "amount" in [c.lower() for c in cols]:
        idx = [c.lower() for c in cols].index("amount")
        leaked = [r[idx] for r in rows if r[idx] not in (None, 0, "0", "")]
        assert not leaked, (
            f"{adapter.name} leaked admin-only `amount` values to {_RESTRICTED}: {leaked[:3]}"
        )


# --------------------------------------------------------------------------- #
# Contract 3 — metamorphic: governance is on the COLUMN, not the column NAME.
# --------------------------------------------------------------------------- #
# Value-preserving SQL projections that all read `amount` but rename/wrap it, so a
# governor that only pattern-matches the literal name "amount" would let the value
# through under a different output name. Each must still be blocked.
_LEAK_TRANSFORMS = ["amount AS a", "amount + 0 AS a", "(amount) AS amt2", "amount * 1 AS a"]


@pytest.fixture(
    params=[RestSqlAdapter, PgwireAdapter, FlightAdapter], ids=["rest_sql", "pgwire", "flight"]
)
def sql_adapter(request, contract_server) -> TransportAdapter:
    return request.param(contract_server)


@pytest.mark.parametrize("transform", _LEAK_TRANSFORMS, ids=lambda t: t.split(" AS ")[0])
def test_governance_survives_sql_transformation(sql_adapter, contract_server, transform):
    """A restricted role must not obtain the admin-only `amount` value even when the
    query renames/wraps it (alias, arithmetic identity, parens). Governance that
    keys on the literal column name rather than the resolved column would leak here
    — a metamorphic property the plain name-based check cannot catch.
    """
    # The real values the restricted role must never see (value-preserving transforms
    # surface the SAME numbers, so a leak is an exact set-membership hit).
    _, admin_rows = RestSqlAdapter(contract_server).read(_ADMIN, "amount")
    admin_amounts = {str(r[0]) for r in admin_rows if r[0] not in (None, 0, 0.0, "0", "")}
    assert admin_amounts, "positive control: admin has no amounts to protect"

    try:
        _, rows = sql_adapter.read(_RESTRICTED, f"id, {transform}")
    except _Denied:
        return  # transform rejected outright — governance held

    leaked = {str(r[-1]) for r in rows} & admin_amounts
    assert not leaked, (
        f"{sql_adapter.name}: transform `{transform}` leaked admin amounts "
        f"to {_RESTRICTED}: {sorted(leaked)[:3]}"
    )


# --------------------------------------------------------------------------- #
# Contract 4 — metamorphic over Cypher/Bolt (the highest-risk transport).
# --------------------------------------------------------------------------- #
# Cypher return expressions that read `amount` under a different output name, the
# Bolt analogue of the SQL transforms above.
_CYPHER_LEAK_RETURNS = ["o.amount AS a", "o.amount + 0 AS a", "o.amount * 1 AS a"]


@pytest.mark.parametrize("return_clause", _CYPHER_LEAK_RETURNS, ids=lambda r: r.split(" AS ")[0])
def test_governance_survives_cypher_transformation(contract_server, return_clause):
    """Bolt/Cypher analogue of the SQL metamorphic check: a restricted role must not
    obtain the admin-only amount over Bolt even when Cypher renames/wraps it. Closes
    the highest-risk transport (previously the thinnest governance coverage)."""
    bolt = BoltAdapter(contract_server)
    # Admin's real amounts over the SAME Bolt path (types match for the leak check).
    try:
        _, admin_rows = bolt.run_return(_ADMIN, "o.amount AS a")
    except _Denied:  # pragma: no cover - admin must be able to read amount
        pytest.fail("positive control: admin cannot read amount over Bolt")
    admin_amounts = {str(r[0]) for r in admin_rows if r[0] not in (None, 0, 0.0)}
    assert admin_amounts, "positive control: admin has no amounts to protect"

    try:
        _, rows = bolt.run_return(_RESTRICTED, f"{return_clause}")
    except _Denied:
        return  # rejected outright — governance held

    leaked = {str(r[-1]) for r in rows} & admin_amounts
    assert not leaked, (
        f"bolt: Cypher `{return_clause}` leaked admin amounts to {_RESTRICTED}: {sorted(leaked)[:3]}"
    )


# --------------------------------------------------------------------------- #
# Contract 5 — row-level governance (RLS) is applied on every transport.
# --------------------------------------------------------------------------- #
def test_rls_row_filter_applied_on_every_transport(adapter, contract_server):
    """The `analyst` role carries an RLS row filter on orders
    (region = current_setting('provisa.user_region')); admin has none. So on EVERY
    transport the analyst must NOT see the full admin row set — the read is either
    denied or returns a strict subset. Equal counts would mean the row filter was
    silently dropped for that transport (a cross-tenant row leak).
    """
    _, admin_rows = RestSqlAdapter(contract_server).read(_ADMIN, "id, region")
    assert admin_rows, "positive control: admin sees rows to be filtered from analyst"

    try:
        _, analyst_rows = adapter.read(_RESTRICTED, "id, region")
    except _Denied:
        return  # RLS predicate rejected the read outright — filter is applied

    assert len(analyst_rows) < len(admin_rows), (
        f"{adapter.name}: analyst saw {len(analyst_rows)} rows vs admin's "
        f"{len(admin_rows)} — RLS row filter was not applied on this transport"
    )


def _norm(rows: list[tuple]) -> list[tuple]:
    """Normalize rows for cross-transport comparison (stringify, sort)."""
    return sorted(tuple(str(v) for v in row) for row in rows)
