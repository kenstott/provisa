# Copyright (c) 2026 Kenneth Stott
# Canary: e8bad655-56d7-4c13-b700-a27144a47e74
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Flight SQL's catalog commands (REQ-2175): catalogs, database schemas, tables, table types and
keys, through the ADBC Flight SQL driver against a server on a loopback port, answered from the
catalog pgwire lists to the same role -- a role compiled from a model of two tables, each with a
primary key, one referring to the other. Before, GetObjects failed "'NoneType' object is not
iterable" or NOT_IMPLEMENTED."""

# Requirements: REQ-2175

from __future__ import annotations

import types

import adbc_driver_flightsql
import adbc_driver_flightsql.dbapi as flightsql
import adbc_driver_manager
import pyarrow as pa
import pyarrow.flight as flight
import pytest

from provisa.api.flight.server import ProvisaFlightServer
from provisa.compiler.context import build_context
from provisa.compiler.introspect import ColumnMetadata
from provisa.compiler.schema_gen import SchemaInput, generate_schema
from provisa.proto import FlightSql_pb2 as pb

_ROLE = "seller"
_COLUMN_TYPES = {
    1: [
        ColumnMetadata(column_name="id", data_type="integer", is_nullable=False),
        ColumnMetadata(column_name="customer_id", data_type="integer", is_nullable=True),
        ColumnMetadata(column_name="amount", data_type="numeric(10,2)", is_nullable=True),
    ],
    2: [
        ColumnMetadata(column_name="id", data_type="integer", is_nullable=False),
        ColumnMetadata(column_name="name", data_type="varchar", is_nullable=True),
    ],
}


def _table(table_id: int, name: str, columns: list[str]) -> dict:
    return {
        "id": table_id,
        "source_id": "sales-pg",
        "domain_id": "sales",
        "schema_name": "public",
        "table_name": name,
        "columns": [
            {"column_name": c, "visible_to": [_ROLE], "is_primary_key": c == "id"} for c in columns
        ],
        "write_ops": [],
        "write_returns_rows": True,
        "write_refused_forms": [],
    }


def _context():
    si = SchemaInput(
        tables=[
            _table(1, "orders", ["id", "customer_id", "amount"]),
            _table(2, "customers", ["id", "name"]),
        ],
        relationships=[
            {
                "id": "orders-customer",
                "source_table_id": 1,
                "source_column": "customer_id",
                "target_table_id": 2,
                "target_column": "id",
                "cardinality": "many-to-one",
            }
        ],
        column_types=_COLUMN_TYPES,
        naming_rules=[],
        role={"id": _ROLE, "capabilities": [], "domain_access": ["*"]},
        domains=[{"id": "sales", "description": ""}],
    )
    generate_schema(si)
    return build_context(si)


class _State:
    auth_config = None
    auth_middleware_active = False
    multitenancy = False
    org_id = "default"
    settings_overrides: dict = {"derived_roles": []}
    config = None

    def __init__(self):
        self.contexts = {_ROLE: _context()}
        self.schema_build_cache = {"column_types": _COLUMN_TYPES}


@pytest.fixture
def served(monkeypatch):
    """A ProvisaFlightServer on a loopback port, each call running as the role its
    x-provisa-role header names (recorded)."""
    roles: list[str] = []

    def _as_caller(self, request, body):  # noqa: ARG001  # the server's signature
        roles.append(str(request.get("role")))
        return body()

    monkeypatch.setattr(ProvisaFlightServer, "_as_ticket_caller", _as_caller)
    server = ProvisaFlightServer(_State(), location="grpc://127.0.0.1:0")
    try:
        yield types.SimpleNamespace(uri=f"grpc://127.0.0.1:{server.port}", roles=roles)
    finally:
        server.shutdown()


_AS_SELLER = {"adbc.flight.sql.rpc.call_header.x-provisa-role": _ROLE}


def _connect(uri: str):
    return flightsql.connect(uri, db_kwargs=_AS_SELLER)


def _command(uri: str, command) -> pa.Table:
    """``command``'s answer, fetched as a Flight SQL client does: GetFlightInfo, then DoGet."""
    from provisa.api.flight.flightsql import pack

    client = flight.connect(uri)
    options = flight.FlightCallOptions(headers=[(b"x-provisa-role", _ROLE.encode())])
    try:
        info = client.get_flight_info(flight.FlightDescriptor.for_command(pack(command)), options)
        (endpoint,) = info.endpoints
        table = client.do_get(endpoint.ticket, options).read_all()
        assert table.schema == info.schema
        return table
    finally:
        client.close()


def test_the_catalog_lists_the_roles_tables_in_its_schemas(served):
    with _connect(served.uri) as conn:
        objects = conn.adbc_get_objects(depth="tables").read_all().to_pylist()
    assert [c["catalog_name"] for c in objects] == ["provisa"]
    # The current schema, as pgwire answers current_schema() and lists it, then the domains.
    public, sales = objects[0]["catalog_db_schemas"]
    assert (public["db_schema_name"], public["db_schema_tables"]) == ("public", [])
    assert sales["db_schema_name"] == "sales"
    assert [(t["table_name"], t["table_type"]) for t in sales["db_schema_tables"]] == [
        ("customers", "TABLE"),
        ("orders", "TABLE"),
    ]
    assert set(served.roles) == {_ROLE}


def test_a_tables_columns_are_the_ones_the_role_is_served(served):
    with _connect(served.uri) as conn:
        schema = conn.adbc_get_table_schema("orders", db_schema_filter="sales")
    # As pgwire lists them: the served columns, then the link columns every table carries.
    assert schema.names == ["id", "customer_id", "amount", "_name_", "_domain_"]
    assert schema.field("id").type == pa.int32()
    assert schema.field("amount").type == pa.decimal128(10, 2)


def test_filters_and_table_types(served):
    tables = _command(
        served.uri,
        pb.CommandGetTables(db_schema_filter_pattern="sal%", table_name_filter_pattern="cust_mers"),
    )
    assert tables.column("table_name").to_pylist() == ["customers"]
    assert _command(served.uri, pb.CommandGetTables(table_name_filter_pattern="none")).num_rows == 0
    assert _command(served.uri, pb.CommandGetTables(table_types=["VIEW"])).num_rows == 0
    types_ = _command(served.uri, pb.CommandGetTableTypes())
    assert types_.column("table_type").to_pylist() == ["TABLE"]
    schemas = _command(served.uri, pb.CommandGetDbSchemas(db_schema_filter_pattern="x%"))
    assert schemas.num_rows == 0


def test_keys_are_the_models(served):
    pks = _command(served.uri, pb.CommandGetPrimaryKeys(table="orders"))
    assert [(r["column_name"], r["key_sequence"]) for r in pks.to_pylist()] == [("id", 1)]
    imported = _command(served.uri, pb.CommandGetImportedKeys(table="orders")).to_pylist()
    exported = _command(served.uri, pb.CommandGetExportedKeys(table="customers")).to_pylist()
    cross = _command(
        served.uri, pb.CommandGetCrossReference(pk_table="customers", fk_table="orders")
    ).to_pylist()
    for keys in (imported, exported, cross):
        assert [
            (k["fk_table_name"], k["fk_column_name"], k["pk_table_name"], k["pk_column_name"])
            for k in keys
        ] == [("orders", "customer_id", "customers", "id")]
    assert _command(served.uri, pb.CommandGetExportedKeys(table="orders")).num_rows == 0


def test_a_catalog_needs_a_role():
    server = ProvisaFlightServer(_State(), location="grpc://127.0.0.1:0")
    try:
        with (
            adbc_driver_flightsql.connect(f"grpc://127.0.0.1:{server.port}") as db,
            adbc_driver_manager.AdbcConnection(db) as conn,
            pytest.raises(adbc_driver_manager.Error, match="role is required"),
        ):
            conn.get_objects(adbc_driver_manager.GetObjectsDepth.CATALOGS)
    finally:
        server.shutdown()
