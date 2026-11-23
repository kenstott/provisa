# Copyright (c) 2026 Kenneth Stott
# Canary: 1c1c60e8-ccea-4248-980b-0aa1e9ecfcfe
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""REQ-1961 on a running server: the type a catalog lists for a column is the type a read of it
returns, a numeric is exact where the model records its size, and a changed size is what the
next read returns.

One table on a real PostgreSQL source, registered with a sized numeric, a bare numeric, an
integer and a smallint, read over Flight (catalog, rows, no rows, a computed column), pgwire,
SQL over HTTP, GraphQL, REST and JSON:API. A second table, registered through the admin API, has
a varchar(20) and a char(3) column: each is text in the model and is described on pgwire, and
listed in pg_attribute, with its native type's OID and length typmod. Then the model's size for the column is changed through the admin API: to a wider
scale, which the next read returns; and to a scale the stored values do not fit, which the
next read refuses naming the column rather than rounding.
"""

# Requirements: REQ-1961

from __future__ import annotations

import json
import os
import time
import urllib.request
from decimal import Decimal

import pyarrow as pa
import pyarrow.flight as fl
import pytest
import sqlalchemy as sa

from tests.integration.worker_boot_harness import WorkerBoot, _config

pytestmark = [pytest.mark.integration]

_ROLES = ["org_admin", "analyst"]
_QUERY = "SELECT id, amount, ratio, small FROM sales.ledger ORDER BY id"


def _columns(amount_type: str) -> list[dict]:
    return [
        {"name": "id", "data_type": "integer", "visible_to": _ROLES},
        {"name": "amount", "data_type": amount_type, "visible_to": _ROLES},
        {"name": "ratio", "data_type": "numeric", "visible_to": _ROLES},
        {"name": "small", "data_type": "smallint", "visible_to": _ROLES},
    ]


def _flight(port: int, sql: str) -> pa.Table:
    client = fl.connect(f"grpc://127.0.0.1:{port}")
    try:
        ticket = fl.Ticket(json.dumps({"query": sql, "role": "org_admin"}).encode())
        return client.do_get(ticket, fl.FlightCallOptions(timeout=60)).read_all()
    finally:
        client.close()


def _listed(port: int) -> pa.Schema:
    client = fl.connect(f"grpc://127.0.0.1:{port}")
    try:
        return client.get_flight_info(fl.FlightDescriptor.for_path("sales", "ledger")).schema
    finally:
        client.close()


def _post(port: int, path: str, body: dict, headers: dict | None = None) -> dict:
    request = urllib.request.Request(
        f"http://127.0.0.1:{port}{path}",
        data=json.dumps(body).encode(),
        headers={"Content-Type": "application/json", **(headers or {})},
    )
    with urllib.request.urlopen(request, timeout=60) as response:
        return json.loads(response.read())


def _declare_amount(port: int, amount_type: str) -> None:
    columns = ", ".join(
        f'{{name: "{c["name"]}", visibleTo: ["org_admin", "analyst"], dataType: "{c["data_type"]}"}}'
        for c in _columns(amount_type)
    )
    result = _post(
        port,
        "/admin/graphql",
        {
            "query": 'mutation { updateTable(input: {sourceId: "sales-pg", domainId: "sales", '
            f'schemaName: "public", tableName: "ledger", columns: [{columns}]}}) '
            "{ success message } }"
        },
    )
    assert result["data"]["updateTable"]["success"], result


def _get(port: int, path: str) -> dict:
    request = urllib.request.Request(
        f"http://127.0.0.1:{port}{path}", headers={"X-Provisa-Role": "org_admin"}
    )
    with urllib.request.urlopen(request, timeout=60) as response:
        return json.loads(response.read())


def _graphql_field(port: int, table: str) -> str:
    """The role's GraphQL root field reading ``table``."""
    answer = _get(port, "/data/introspection")
    schema = answer.get("data", answer)["__schema"]
    (root,) = [t for t in schema["types"] if t["name"] == schema["queryType"]["name"]]
    (field,) = [f["name"] for f in root["fields"] if f["name"].lower().endswith(table)]
    return field


def _until(read, wanted, timeout: float = 120.0):
    """``read()`` once it answers ``wanted`` — a model change is served once the runtime has
    reloaded it, which is not the instant the change is saved."""
    deadline = time.monotonic() + timeout
    while True:
        got = read()
        if wanted(got) or time.monotonic() > deadline:
            return got
        time.sleep(2)


def test_a_numeric_is_listed_and_read_as_the_model_declares_it_on_every_surface():
    boot = WorkerBoot(
        1,
        pg_host=os.environ.get("PG_HOST", "localhost"),
        pg_port=int(os.environ.get("PG_PORT", "5432")),
    )
    boot.create_database()
    try:
        source = sa.create_engine(boot.url, isolation_level="AUTOCOMMIT")
        with source.connect() as conn:
            conn.execute(
                sa.text(
                    "CREATE TABLE public.ledger (id integer PRIMARY KEY, amount numeric(18,2), "
                    "ratio numeric, small smallint)"
                )
            )
            conn.execute(
                sa.text(
                    "CREATE TABLE public.codes (id integer PRIMARY KEY, code varchar(20), "
                    "flag char(3))"
                )
            )
            conn.execute(sa.text("INSERT INTO public.codes VALUES (1, 'A-1', 'yes')"))
            conn.execute(
                sa.text("INSERT INTO public.ledger VALUES (1, 12.50, 0.5, 3), (2, 0.25, 0.25, 4)")
            )
        source.dispose()
        base = _config(
            os.environ.get("PG_HOST", "localhost"),
            int(os.environ.get("PG_PORT", "5432")),
            boot.database,
        )
        boot._extra_config = {
            "tables": [
                *base["tables"],
                {
                    "source_id": "sales-pg",
                    "domain_id": "sales",
                    "schema": "public",
                    "table": "ledger",
                    "columns": _columns("numeric(18,2)"),
                },
            ]
        }
        boot.start()
        boot.wait_all_ready(timeout=300)
        flight_port, http_port = boot.ports["flight"], boot.ports["http"]

        declared = {
            "id": pa.int32(),
            "amount": pa.decimal128(18, 2),
            "ratio": pa.float64(),
            "small": pa.int16(),
        }

        # The catalog lists the registered types...
        listed = _listed(flight_port)
        assert {f.name: f.type for f in listed} == declared

        # ...a read returns them, exact where the size is recorded...
        rows = _flight(flight_port, _QUERY)
        assert {f.name: f.type for f in rows.schema} == declared
        assert rows.column("amount").to_pylist() == [Decimal("12.50"), Decimal("0.25")]
        assert rows.column("ratio").to_pylist() == [0.5, 0.25]

        # ...and a read of no rows returns the same schema.
        none = _flight(
            flight_port, "SELECT id, amount, ratio, small FROM sales.ledger WHERE id < 0"
        )
        assert none.num_rows == 0
        assert {f.name: f.type for f in none.schema} == declared

        # A computed column is its engine's, exact as sent: the square of a two-digit scale has
        # four digits after the point, whatever the model says of the column squared.
        # (Over a PostgreSQL direct route the product arrives with no size and is floating point;
        # over an engine route it is the engine's exact decimal. The value is the same.)
        squared = _flight(
            flight_port, "SELECT amount * amount AS sq FROM sales.ledger WHERE id = 1"
        )
        assert [float(v) for v in squared.column("sq").to_pylist()] == [156.25]

        # pgwire returns a sized numeric as a numeric, and one the model records no size for as
        # float8 — described so and sent so (the governed statement casts it at its source).
        import psycopg

        with psycopg.connect(
            host="127.0.0.1",
            port=boot.ports["pgwire"],
            user="org_admin",
            password="x",
            dbname="provisa",
            connect_timeout=10,
            autocommit=True,
        ) as conn:
            cursor = conn.execute(_QUERY)
            described = {d.name: d.type_code for d in cursor.description or []}
            first = cursor.fetchall()[0]
            assert first[1] == Decimal("12.50")
            assert described["amount"] == 1700  # numeric
            assert described["ratio"] == 701  # float8
            assert first[2] == 0.5 and isinstance(first[2], float)

        # A computed decimal PostgreSQL reports no size for is floating point, whatever its rows
        # hold (#202); stating its size keeps it exact.
        loose = _flight(flight_port, "SELECT amount / 3 AS q FROM sales.ledger ORDER BY id")
        assert loose.schema.field("q").type == pa.float64()
        stated = _flight(
            flight_port,
            "SELECT CAST(amount / 3 AS NUMERIC(12, 6)) AS q FROM sales.ledger ORDER BY id",
        )
        assert stated.schema.field("q").type == pa.decimal128(12, 6)
        assert stated.column("q").to_pylist() == [Decimal("4.166667"), Decimal("0.083333")]

        # HTTP answers JSON, which has no decimal: a sized numeric is the string of its exact
        # digits (REQ-1867, amended 2026-11-02).
        answered = _post(http_port, "/data/sql", {"sql": _QUERY, "role": "org_admin"})
        assert [row["amount"] for row in answered["data"]["sql"]] == ["12.50", "0.25"]

        # GraphQL, REST and JSON:API answer JSON too: the same exact digits on each, and the
        # unsized numeric as the floating point its source cast it to.
        ledger = _graphql_field(http_port, "ledger")
        graphql = _post(
            http_port,
            "/data/graphql",
            {"query": f"{{ {ledger} {{ id amount ratio }} }}"},
            {"X-Provisa-Role": "org_admin"},
        )
        assert "errors" not in graphql, graphql
        assert sorted((r["id"], r["amount"], r["ratio"]) for r in graphql["data"][ledger]) == [
            (1, "12.50", 0.5),
            (2, "0.25", 0.25),
        ]
        rest = _get(http_port, "/data/rest/sales/ledger")
        rest_rows = rest["data"] if isinstance(rest, dict) else rest
        assert sorted((r["id"], r["amount"], r["ratio"]) for r in rest_rows) == [
            (1, "12.50", 0.5),
            (2, "0.25", 0.25),
        ]
        jsonapi = _get(http_port, "/data/jsonapi/sales/ledger")
        assert sorted(
            (r["attributes"]["amount"], r["attributes"]["ratio"]) for r in jsonapi["data"]
        ) == [("0.25", 0.25), ("12.50", 0.5)]

        # A varchar(n) or char(n) column is text in the model and keeps its native type: pgwire
        # describes it with that type's OID and length typmod, and pg_attribute agrees
        # (amended 2026-10-28, VARCHAR AND CHAR KEEP THEIR TYPE).
        registered = _post(
            http_port,
            "/admin/graphql",
            {
                "query": 'mutation { registerTable(input: {sourceId: "sales-pg", '
                'domainId: "sales", schemaName: "public", tableName: "codes", columns: ['
                '{name: "id", visibleTo: ["org_admin"], dataType: "integer"}, '
                '{name: "code", visibleTo: ["org_admin"], dataType: "text"}, '
                '{name: "flag", visibleTo: ["org_admin"], dataType: "text"}]}) '
                "{ success message } }"
            },
        )
        assert registered["data"]["registerTable"]["success"], registered

        def _codes_described():
            try:
                with psycopg.connect(
                    host="127.0.0.1",
                    port=boot.ports["pgwire"],
                    user="org_admin",
                    password="x",
                    dbname="provisa",
                    connect_timeout=10,
                    autocommit=True,
                ) as conn:
                    cursor = conn.execute("SELECT code, flag FROM sales.codes")
                    wire = [(d.name, d.type_code, d.display_size) for d in cursor.description or []]
                    cursor.fetchall()
                    listed = conn.execute(
                        "SELECT a.attname, a.atttypid::int, a.atttypmod FROM pg_attribute a "
                        "JOIN pg_class c ON c.oid = a.attrelid WHERE c.relname = 'codes' "
                        "AND a.attname IN ('code', 'flag') ORDER BY a.attname"
                    ).fetchall()
                    return wire, listed
            except psycopg.Error as not_yet:
                return not_yet

        wanted = (
            [("code", 1043, 20), ("flag", 1042, 3)],
            [("code", 1043, 24), ("flag", 1042, 7)],
        )
        assert _until(_codes_described, lambda got: got == wanted) == wanted

        # The model's size for the column changes: the next read is typed by the new declaration.
        _declare_amount(http_port, "numeric(18,3)")
        wider = _until(
            lambda: _flight(flight_port, _QUERY).schema.field("amount").type,
            lambda got: got == pa.decimal128(18, 3),
        )
        assert wider == pa.decimal128(18, 3)
        assert _listed(flight_port).field("amount").type == pa.decimal128(18, 3)
        assert _flight(flight_port, _QUERY).column("amount").to_pylist() == [
            Decimal("12.500"),
            Decimal("0.250"),
        ]

        # A size the stored values do not fit is refused naming the column, never rounded.
        _declare_amount(http_port, "numeric(18,1)")

        def _read_or_refusal():
            try:
                return _flight(flight_port, _QUERY)
            except fl.FlightError as refused:
                return refused

        refused = _until(_read_or_refusal, lambda got: isinstance(got, fl.FlightError))
        assert isinstance(refused, fl.FlightError), "0.25 was read as numeric(18,1)"
        assert "amount" in str(refused) and "registered" in str(refused)

        # A size no numeric can have is not saved at all.
        impossible = _post(
            http_port,
            "/admin/graphql",
            {
                "query": 'mutation { updateTable(input: {sourceId: "sales-pg", domainId: "sales", '
                'schemaName: "public", tableName: "ledger", columns: ['
                '{name: "amount", visibleTo: ["org_admin"], dataType: "numeric(5,9)"}]}) '
                "{ success message } }"
            },
        )
        assert impossible.get("errors") or not impossible["data"]["updateTable"]["success"]
    finally:
        boot.cleanup()
