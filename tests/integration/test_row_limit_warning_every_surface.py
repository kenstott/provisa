# Copyright (c) 2026 Kenneth Stott
# Canary: 3d5ac187-f930-4033-9f89-0733d3988752
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Integration: a read that a role's row limit cut short says so, on every surface (REQ-1949).

One server, a row limit of 3 for every role without full results. ``four`` holds one row past
the limit, ``three`` exactly the limit. On each surface a limited role reading ``four`` gets
three rows -- never the fourth -- and the warning in that surface's own channel; reading
``three`` it gets three rows and no warning; a role with full results gets all four and none.
"""

# Requirements: REQ-1949

from __future__ import annotations

import json
import os
import time
import urllib.error
import urllib.request

import pytest
import sqlalchemy as sa

from tests.integration.worker_boot_harness import WorkerBoot

pytestmark = [pytest.mark.integration]

_PG_HOST = os.environ.get("PG_HOST", "localhost")
_PG_PORT = int(os.environ.get("PG_PORT", "5432"))
_LIMIT = 3
_ROLES = ["org_admin", "limited", "whole"]
_CUT = "statement.rows_cut"


def _table(name: str) -> dict:
    return {
        "source_id": "sales-pg",
        "domain_id": "sales",
        "schema": "public",
        "table": name,
        "columns": [
            {"name": "id", "data_type": "integer", "visible_to": _ROLES, "is_primary_key": True},
            {"name": "label", "data_type": "varchar", "visible_to": _ROLES},
        ],
    }


@pytest.fixture(scope="module")
def server():
    reads = ["query_development"]
    boot = WorkerBoot(
        1,
        pg_host=_PG_HOST,
        pg_port=_PG_PORT,
        extra_config={
            "tables": [_table("four"), _table("three")],
            "roles": [
                {"id": "limited", "capabilities": reads, "domain_access": ["*"]},
                {
                    "id": "whole",
                    "capabilities": [*reads, "full_results"],
                    "domain_access": ["*"],
                },
            ],
        },
        env={"PROVISA_REDIRECT_ENABLED": "false", "PROVISA_DEFAULT_ROW_LIMIT": str(_LIMIT)},
    )
    boot.create_database()
    try:
        engine = sa.create_engine(boot.url, isolation_level="AUTOCOMMIT")
        with engine.connect() as conn:
            for name, rows in (("four", 4), ("three", 3)):
                conn.execute(
                    sa.text(f"CREATE TABLE public.{name} (id integer PRIMARY KEY, label text)")
                )
                conn.execute(
                    sa.text(
                        f"INSERT INTO public.{name} "
                        f"SELECT i, 'r' || i FROM generate_series(1, {rows}) AS i"
                    )
                )
        engine.dispose()
        boot.start()
        boot.wait_all_ready(timeout=300)
        yield boot
    finally:
        boot.cleanup()


# Each surface answers (rows returned, the warning codes it carried in its own channel).


def _http(boot, role: str, path: str, body: dict) -> tuple[int, dict, list[str]]:
    req = urllib.request.Request(
        f"http://127.0.0.1:{boot.ports['http']}{path}",
        data=json.dumps(body).encode(),
        headers={"Content-Type": "application/json", "x-provisa-role": role},
        method="POST",
    )
    try:
        with urllib.request.urlopen(req, timeout=120) as resp:
            said = json.loads(resp.headers.get("x-provisa-warnings") or "[]")
            return resp.status, json.loads(resp.read().decode()), [w["code"] for w in said]
    except urllib.error.HTTPError as exc:
        raise AssertionError(f"{exc.code} {exc.read().decode()}") from exc


def _sql_http(boot, role: str, table: str) -> tuple[int, list[str]]:
    _, body, codes = _http(boot, role, "/data/sql", {"sql": f"SELECT id FROM sales.{table}"})
    return len(body["data"]["sql"]), codes


def _cypher_http(boot, role: str, table: str) -> tuple[int, list[str]]:
    label = table.capitalize()
    _, body, codes = _http(boot, role, "/data/cypher", {"query": f"MATCH (n:{label}) RETURN n.id"})
    return len(body["rows"]), codes


def _pgwire(boot, role: str, table: str) -> tuple[int, list[str]]:
    import psycopg

    notices: list[str] = []
    with psycopg.connect(
        host="127.0.0.1",
        port=boot.ports["pgwire"],
        user=role,
        password="provisa",
        dbname="provisa",
        autocommit=True,
        connect_timeout=30,
    ) as conn:
        # REQ-1350: a warning is a NOTICE whose Detail is {"code", "params"} as JSON.
        conn.add_notice_handler(
            lambda diag: (
                notices.append(json.loads(diag.message_detail)["code"])
                if diag.message_detail
                else None
            )
        )
        rows = conn.execute(f"SELECT id FROM sales.{table}").fetchall()
    return len(rows), notices


def _flight(boot, role: str, table: str) -> tuple[int, list[str]]:
    import pyarrow.flight as fl

    client = fl.connect(f"grpc://127.0.0.1:{boot.ports['flight']}")
    try:
        label = table.capitalize()
        ticket = fl.Ticket(
            json.dumps({"query": f"MATCH (n:{label}) RETURN n.id", "role": role}).encode()
        )
        reader = client.do_get(ticket)
        rows, codes = 0, []
        while True:
            try:
                chunk = reader.read_chunk()
            except StopIteration:
                break
            if chunk.data is not None:
                rows += chunk.data.num_rows
            # REQ-1350/REQ-1949: warnings ride the app_metadata of a zero-row batch -- ahead of
            # the rows, and for what the rows showed, after them.
            if chunk.app_metadata is not None:
                said = json.loads(chunk.app_metadata.to_pybytes().decode())
                codes += [w["code"] for w in said.get("provisa_warnings", [])]
        return rows, codes
    finally:
        client.close()


def _grpc(boot, role: str, table: str) -> tuple[int, list[str]]:
    """A streamed Query RPC. gRPC sets its trailing metadata before the rows are sent; a warning
    the rows showed is there because the metadata was set once more when the stream ended."""
    import grpc
    from google.protobuf.message_factory import GetMessageClass

    from tests.grpc_proto_client import role_descriptor_pool

    _pool, svc = role_descriptor_pool(f"http://127.0.0.1:{boot.ports['http']}", role)
    method = next(
        m
        for m in svc.methods
        if m.name.startswith("Query")
        and table in m.name.lower()
        and not m.name.endswith(("Aggregate", "GroupBy", "Batch"))
    )
    req_cls = GetMessageClass(method.input_type)
    resp_cls = GetMessageClass(method.output_type)
    channel = grpc.insecure_channel(f"127.0.0.1:{boot.ports['grpc']}")
    try:
        rpc = channel.unary_stream(
            f"/{svc.full_name}/{method.name}",
            request_serializer=req_cls.SerializeToString,
            response_deserializer=resp_cls.FromString,
        )
        call = rpc(req_cls(), metadata=(("x-provisa-role", role),), timeout=120)
        rows = len(list(call))
        trailers = dict(call.trailing_metadata())
        said = json.loads(trailers.get("x-provisa-warnings", "[]"))
        return rows, [w["code"] for w in said]
    finally:
        channel.close()


_SURFACES = {
    "sql_http": _sql_http,
    "cypher_http": _cypher_http,
    "pgwire": _pgwire,
    "flight": _flight,
    "grpc": _grpc,
}


@pytest.mark.parametrize("surface", list(_SURFACES))
def test_a_cut_read_returns_the_limit_and_says_so(server, surface):
    rows, codes = _SURFACES[surface](server, "limited", "four")
    assert rows == _LIMIT, rows  # never the row past the limit
    assert codes.count(_CUT) == 1, codes


@pytest.mark.parametrize("surface", list(_SURFACES))
def test_a_read_that_exactly_fills_the_limit_says_nothing(server, surface):
    rows, codes = _SURFACES[surface](server, "limited", "three")
    assert rows == _LIMIT, rows
    assert _CUT not in codes and "statement.rows_cut_unchecked" not in codes, codes


@pytest.mark.parametrize("surface", list(_SURFACES))
def test_a_role_with_full_results_reads_every_row_and_is_told_nothing(server, surface):
    rows, codes = _SURFACES[surface](server, "whole", "four")
    assert rows == 4 and codes == [], (rows, codes)


@pytest.mark.parametrize("surface", ["sql_http", "pgwire"])
def test_a_statements_own_limit_within_the_role_limit_is_not_a_cut(server, surface):
    """The statement asked for two rows and got two: the role's limit cut nothing."""
    if surface == "sql_http":
        _, body, codes = _http(
            server, "limited", "/data/sql", {"sql": "SELECT id FROM sales.four LIMIT 2"}
        )
        rows = len(body["data"]["sql"])
    else:
        import psycopg

        codes = []
        with psycopg.connect(
            host="127.0.0.1",
            port=server.ports["pgwire"],
            user="limited",
            password="provisa",
            dbname="provisa",
            autocommit=True,
            connect_timeout=30,
        ) as conn:
            conn.add_notice_handler(
                lambda diag: (
                    codes.append(json.loads(diag.message_detail)["code"])
                    if diag.message_detail
                    else None
                )
            )
            rows = len(conn.execute("SELECT id FROM sales.four LIMIT 2").fetchall())
    assert rows == 2 and _CUT not in codes, (rows, codes)


def test_a_cut_read_says_so_again_each_time_it_is_asked(server):
    """A warned answer is never kept as the statement's answer, so a repeat is read and
    checked again -- the second answer carries the warning as the first did."""
    for _ in range(2):
        rows, codes = _sql_http(server, "limited", "four")
        assert rows == _LIMIT and codes.count(_CUT) == 1, (rows, codes)


def _newest_audit_row(boot, role: str) -> dict:
    """``role``'s newest audit row, waiting for the writer (it inserts in batches)."""
    engine = sa.create_engine(boot.url)
    try:
        with engine.connect() as conn:
            schema = conn.execute(
                sa.text(
                    "SELECT table_schema FROM information_schema.tables "
                    "WHERE table_name = 'query_audit_log' AND table_schema LIKE 'org%' LIMIT 1"
                )
            ).scalar_one()
        return _last_row(engine, schema, role)
    finally:
        engine.dispose()


def _last_row(engine, schema: str, role: str) -> dict:
    with engine.connect() as conn:
        row = conn.execute(
            sa.text(
                f"SELECT id, route, row_count, enforced FROM {schema}.query_audit_log "
                "WHERE role_id = :role ORDER BY id DESC LIMIT 1"
            ),
            {"role": role},
        ).first()
    return dict(row._mapping) if row is not None else {"id": 0}


def _audited(boot, role: str, read) -> dict:
    """The audit row ``read()`` wrote for ``role``: the first one newer than the newest before."""
    before = _newest_audit_row(boot, role)["id"]
    read()
    deadline = time.monotonic() + 60
    while True:
        row = _newest_audit_row(boot, role)
        if row["id"] != before:
            return row
        assert time.monotonic() < deadline, "no audit row was written for the read"
        time.sleep(0.5)


def test_the_audit_row_says_what_the_limit_did_to_the_answer(server):
    """REQ-1949: the warning is a fact about what the caller was given, so the statement's own
    audit row holds it, beside the limit itself; a whole answer's row says nothing of a cut."""
    cut = _audited(server, "limited", lambda: _sql_http(server, "limited", "four"))
    assert cut["row_count"] == _LIMIT, cut
    assert cut["enforced"]["row_limit"] == {"limit": _LIMIT, "kind": "role", "outcome": "cut"}
    whole = _audited(server, "limited", lambda: _sql_http(server, "limited", "three"))
    assert whole["row_count"] == _LIMIT and "row_limit" not in whole["enforced"], whole
    free = _audited(server, "whole", lambda: _sql_http(server, "whole", "four"))
    assert free["row_count"] == 4 and "row_limit" not in free["enforced"], free


def test_a_stream_read_from_the_source_is_checked_when_it_ends(server):
    """pgwire streams a single-source read from the source's own cursor (route ``direct``); the
    row after the limit is then asked for on a second connection while that cursor is still
    open. The route is asserted, not assumed: this case proves nothing on any other route."""
    seen: dict = {}

    def read() -> None:
        seen["rows"], seen["codes"] = _pgwire(server, "limited", "four")

    row = _audited(server, "limited", read)
    assert row["route"] == "direct", row  # served from the source, not through the engine
    assert seen["rows"] == _LIMIT and seen["codes"].count(_CUT) == 1, seen
    assert row["enforced"]["row_limit"]["outcome"] == "cut", row
