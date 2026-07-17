# Copyright (c) 2026 Kenneth Stott
# Canary: b08f4e2b-22f8-4b96-bff9-30dcc8838d4d
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""E2E: the REAL DuckDB `airport` extension reads a GOVERNED table from Provisa (REQ-1098).

Provisa SERVES the airport Flight protocol (provisa/api/airport). This test boots a real
Provisa app (uvicorn, sample_config, isolated PG) with PROVISA_AIRPORT_PORT set, then from an
in-process DuckDB with the community `airport` extension:

    INSTALL airport FROM community; LOAD airport;
    CREATE SECRET (TYPE airport, auth_token '<role>', scope 'grpc://localhost:<port>');
    ATTACH 'grpc://localhost:<port>' AS provisa (TYPE AIRPORT);
    SELECT ... FROM provisa."sales_analytics"."customers";

Governance proof (server-side): sample_config declares an RLS rule on `orders` for `analyst`
(``region = current_setting('provisa.user_region')``). The airport transport supplies no session
var, so the governed pipeline resolves the predicate to NULL — the documented deny-by-default —
and `analyst` reads ZERO order rows while `admin` (no RLS on orders) reads them all. The DuckDB
call runs in a subprocess bounded by a hard wall-clock timeout so an airport protocol mismatch
fails fast instead of hanging the suite.

This is the counterpart to test_airport_source_e2e.py, which drives Provisa as an airport
CLIENT; here Provisa is the airport SERVER.
"""

from __future__ import annotations

import json
import os
import socket
import subprocess
import sys
import time
from pathlib import Path

import pytest

pytestmark = [pytest.mark.integration]

_REPO_ROOT = Path(__file__).resolve().parents[2]
_SAMPLE_CONFIG = _REPO_ROOT / "tests" / "fixtures" / "sample_config.yaml"

# Semantic identifiers the governed pipeline advertises for sample_config's
# domain "sales-analytics" (domain_to_sql_name → "sales_analytics").
_SCHEMA = "sales_analytics"
_TABLE = "customers"

# Shares the server-side pushdown-trace file path from the fixture to the tests.
_STATE: dict = {}


def _free_port() -> int:
    s = socket.socket()
    try:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]
    finally:
        s.close()


def _tcp_reachable(host: str, port: int) -> bool:
    try:
        with socket.create_connection((host, port), timeout=0.5):
            return True
    except OSError:
        return False


@pytest.fixture(scope="module")
def airport_server_port():
    """Boot a Provisa app with the airport Flight service enabled; yield its port."""
    http_port = _free_port()
    flight_port = _free_port()
    airport_port = _free_port()

    import tempfile

    pushdown_log = tempfile.NamedTemporaryFile(  # noqa: SIM115
        prefix="airport_pushdown_", suffix=".sql", delete=False
    )
    pushdown_log.close()
    _STATE["pushdown_log"] = pushdown_log.name

    env = {
        **os.environ,
        "PG_PASSWORD": os.environ.get("PG_PASSWORD") or "provisa",
        "PROVISA_CONFIG": str(_SAMPLE_CONFIG),
        "FLIGHT_PORT": str(flight_port),
        "PROVISA_AIRPORT_PORT": str(airport_port),
        # Source-side pushdown trace — the server appends each translated pushdown SQL here so the
        # test can prove the SOURCE received the WHERE/projection (not just DuckDB re-filtering).
        "PROVISA_AIRPORT_PUSHDOWN_LOG": pushdown_log.name,
        # PYTHONPATH → import the worktree's provisa (with the airport service) in the subprocess.
        "PYTHONPATH": str(_REPO_ROOT),
    }

    logf = tempfile.NamedTemporaryFile(  # noqa: SIM115
        prefix="airport_server_", suffix=".log", delete=False
    )
    proc = subprocess.Popen(
        [sys.executable, "-m", "uvicorn", "main:app", "--host", "0.0.0.0", f"--port={http_port}"],
        cwd=str(_REPO_ROOT),
        env=env,
        stdout=logf,
        stderr=subprocess.STDOUT,
    )

    def _log_tail() -> str:
        logf.flush()
        with open(logf.name) as fh:
            return "".join(fh.readlines()[-40:])

    try:
        deadline = time.monotonic() + 90
        while time.monotonic() < deadline:
            if _tcp_reachable("localhost", airport_port):
                break
            if proc.poll() is not None:
                raise RuntimeError(
                    f"Provisa app exited early (code {proc.returncode}):\n{_log_tail()}"
                )
            time.sleep(1)
        else:
            raise RuntimeError(
                f"airport server did not bind localhost:{airport_port} within 90s:\n{_log_tail()}"
            )
        yield airport_port
    finally:
        proc.terminate()
        try:
            proc.wait(timeout=10)
        except subprocess.TimeoutExpired:
            proc.kill()
        os.unlink(logf.name)
        os.unlink(pushdown_log.name)


# Runs in a bounded subprocess (subprocess.run(timeout=...)) so a hung airport handshake can
# never hang the suite. Prints one JSON line: {"columns": [...], "rows": [[...], ...]}.
_CLIENT_SCRIPT = r"""
import json
import sys

import duckdb

port = int(sys.argv[1])
role = sys.argv[2]
schema = sys.argv[3]
table = sys.argv[4]

conn = duckdb.connect()
conn.execute("INSTALL airport FROM community")
conn.execute("LOAD airport")
loc = f"grpc://localhost:{port}"
conn.execute(
    "CREATE SECRET airport_sec (TYPE airport, auth_token ?, scope ?)", [role, loc]
)
conn.execute(f"ATTACH '{loc}' AS provisa (TYPE AIRPORT)")

rel = conn.execute(f'SELECT * FROM provisa."{schema}"."{table}" ORDER BY id')
columns = [d[0] for d in rel.description]
rows = rel.fetchall()
print(json.dumps({"columns": columns, "rows": rows}, default=str))
"""


def _run_airport_client(port: int, role: str, schema: str, table: str) -> dict:
    result = subprocess.run(
        [sys.executable, "-c", _CLIENT_SCRIPT, str(port), role, schema, table],
        capture_output=True,
        text=True,
        timeout=120,
    )
    assert result.returncode == 0, (
        f"airport client subprocess failed (role={role}):\n"
        f"stdout={result.stdout}\nstderr={result.stderr}"
    )
    return json.loads(result.stdout.strip().splitlines()[-1])


# Runs an arbitrary sequence of statements through the real DuckDB airport client. The final
# statement, if it is a query, is fetched and returned; otherwise just executed. Bounded subprocess.
_SQL_CLIENT_SCRIPT = r"""
import json
import sys

import duckdb

port = int(sys.argv[1])
role = sys.argv[2]
fetch = sys.argv[3] == "1"
stmts = json.loads(sys.argv[4])

conn = duckdb.connect()
conn.execute("INSTALL airport FROM community")
conn.execute("LOAD airport")
loc = f"grpc://localhost:{port}"
conn.execute("CREATE SECRET airport_sec (TYPE airport, auth_token ?, scope ?)", [role, loc])
conn.execute(f"ATTACH '{loc}' AS provisa (TYPE AIRPORT)")

rel = None
for s in stmts:
    rel = conn.execute(s)
if fetch and rel is not None:
    columns = [d[0] for d in rel.description]
    rows = rel.fetchall()
    print(json.dumps({"columns": columns, "rows": rows}, default=str))
else:
    print(json.dumps({"columns": [], "rows": []}, default=str))
"""


def _run_sql(port: int, role: str, stmts: list[str], *, fetch: bool = True) -> dict:
    result = subprocess.run(
        [
            sys.executable, "-c", _SQL_CLIENT_SCRIPT,
            str(port), role, "1" if fetch else "0", json.dumps(stmts),
        ],
        capture_output=True,
        text=True,
        timeout=120,
    )
    assert result.returncode == 0, (
        f"airport SQL client failed (role={role}, stmts={stmts}):\n"
        f"stdout={result.stdout}\nstderr={result.stderr}"
    )
    return json.loads(result.stdout.strip().splitlines()[-1])


def _run_sql_expect_failure(port: int, role: str, stmts: list[str]) -> str:
    """Run statements expecting a NON-zero exit (a refused-by-protocol-error op). Returns stderr."""
    result = subprocess.run(
        [sys.executable, "-c", _SQL_CLIENT_SCRIPT, str(port), role, "0", json.dumps(stmts)],
        capture_output=True,
        text=True,
        timeout=120,
    )
    assert result.returncode != 0, (
        f"expected the op to be refused, but it succeeded (role={role}, stmts={stmts}):\n"
        f"stdout={result.stdout}"
    )
    return result.stderr


def test_admin_reads_governed_table_via_duckdb_airport(airport_server_port):
    """The real DuckDB airport extension ATTACHes Provisa and reads governed rows (admin)."""
    out = _run_airport_client(airport_server_port, "admin", _SCHEMA, _TABLE)
    assert "email" in out["columns"], out["columns"]  # admin-only column visible
    assert {"id", "name", "region"}.issubset(set(out["columns"])), out["columns"]
    assert len(out["rows"]) > 0, "expected governed customer rows"


def test_governance_rls_applied_analyst_gets_zero_order_rows(airport_server_port):
    """Server-side RLS governance: analyst's orders are row-filtered to empty (deny-by-default)
    while admin reads all orders — through the real DuckDB airport client."""
    admin_orders = _run_airport_client(airport_server_port, "admin", _SCHEMA, "orders")
    analyst_orders = _run_airport_client(airport_server_port, "analyst", _SCHEMA, "orders")
    # analyst can still read a non-RLS table — proving the role isn't globally broken.
    analyst_customers = _run_airport_client(airport_server_port, "analyst", _SCHEMA, _TABLE)

    assert len(admin_orders["rows"]) > 0, "admin should read order rows"
    assert len(analyst_orders["rows"]) == 0, (
        f"RLS should deny all analyst order rows, got {len(analyst_orders['rows'])}"
    )
    assert len(analyst_orders["rows"]) < len(admin_orders["rows"])
    assert len(analyst_customers["rows"]) > 0, "analyst should read non-RLS customers"


def _read_pushdown_trace() -> str:
    path = _STATE.get("pushdown_log")
    assert path, "pushdown trace path not set by fixture"
    with open(path, encoding="utf-8") as fh:
        return fh.read()


# ---------------------------------------------------------------- Increment 2: pushdown
def test_predicate_pushdown_filters_at_source(airport_server_port):
    """A WHERE predicate is pushed to the SOURCE (not just re-applied by DuckDB).

    Proof of source-side application: the server records the translated pushdown SQL it submitted
    to the governed pipeline; the recorded SQL must carry the WHERE. Also asserts the returned
    rows are correctly filtered."""
    out = _run_sql(
        airport_server_port,
        "admin",
        [f"""SELECT id, region FROM provisa."{_SCHEMA}"."{_TABLE}" WHERE region = 'us-east'"""],
    )
    # Every returned row matches the predicate.
    region_idx = out["columns"].index("region")
    assert out["rows"], "expected at least one us-east customer"
    assert all(r[region_idx] == "us-east" for r in out["rows"]), out["rows"]
    # Source-side proof: the server pushed a WHERE on region to the governed pipeline.
    trace = _read_pushdown_trace()
    assert '"region" = ' in trace and "us-east" in trace, trace
    assert "WHERE" in trace, trace


def test_projection_pushdown_selects_only_requested_columns(airport_server_port):
    """A projection is pushed to the SOURCE: the server's scan SELECTs only the requested columns."""
    out = _run_sql(
        airport_server_port,
        "admin",
        [f"""SELECT id, name FROM provisa."{_SCHEMA}"."{_TABLE}" ORDER BY id"""],
    )
    assert set(out["columns"]) == {"id", "name"}, out["columns"]
    trace = _read_pushdown_trace()
    # The source-side SELECT projected specific columns (id, name), not SELECT *.
    assert '"id"' in trace and '"name"' in trace, trace


# ---------------------------------------------------------------- Increment 4: governed DML
def test_governed_insert_roundtrip_via_airport(airport_server_port):
    """INSERT through the airport do_exchange path lands via the governed write pipeline and is
    then readable back through the governed read path — same catalog, governance intact."""
    # A high, fixed id unused by the fixture data; the session DB snapshot is restored at teardown,
    # and DELETE is refused-by-protocol, so no explicit cleanup is possible or needed.
    row_id = 987654
    _run_sql(
        airport_server_port,
        "admin",
        [
            f"""INSERT INTO provisa."{_SCHEMA}"."{_TABLE}" (id, name, email, region)
                VALUES ({row_id}, 'Airport Insert', 'ai@example.com', 'ap-1')"""
        ],
        fetch=False,
    )
    back = _run_sql(
        airport_server_port,
        "admin",
        [f'SELECT id, name, region FROM provisa."{_SCHEMA}"."{_TABLE}" WHERE id = {row_id}'],
    )
    assert len(back["rows"]) == 1, back
    name_idx = back["columns"].index("name")
    assert back["rows"][0][name_idx] == "Airport Insert", back["rows"][0]


def test_update_delete_refused_by_protocol_error(airport_server_port):
    """UPDATE/DELETE are refused with a Flight protocol error (governed catalog exposes no rowid)."""
    for stmt in (
        f"UPDATE provisa.\"{_SCHEMA}\".\"{_TABLE}\" SET region = 'x' WHERE id = 1",
        f"DELETE FROM provisa.\"{_SCHEMA}\".\"{_TABLE}\" WHERE id = 1",
    ):
        stderr = _run_sql_expect_failure(airport_server_port, "admin", [stmt]).lower()
        assert any(k in stderr for k in ("rowid", "unsupported", "does not support", "update", "delete")), stderr


# ---------------------------------------------------------------- Increment 5: DDL
def test_create_schema_maps_to_domain(airport_server_port):
    """CREATE SCHEMA via airport creates a Provisa domain through the schema-mutation pipeline."""
    # Should not raise — the create_schema DoAction maps to a domain upsert.
    _run_sql(airport_server_port, "admin", ["CREATE SCHEMA provisa.airport_ddl_test"], fetch=False)
    # drop_schema maps to domain delete.
    _run_sql(airport_server_port, "admin", ["DROP SCHEMA provisa.airport_ddl_test"], fetch=False)


def test_create_table_refused_by_protocol_error(airport_server_port):
    """CREATE TABLE is refused with a protocol error — physical shape mutation needs the admin
    schema-mutation API's source/governance context, absent from the airport DDL payload."""
    stderr = _run_sql_expect_failure(
        airport_server_port, "admin", [f'CREATE TABLE provisa."{_SCHEMA}"."ap_new" (x INTEGER)']
    )
    assert "unsupported" in stderr.lower() or "schema-mutation" in stderr.lower(), stderr


# ---------------------------------------------------------------- Increment 3: transactions
def test_transaction_create_and_status(airport_server_port):
    """create_transaction mints an identifier; get_transaction_status reports it active — verified
    directly against the Flight action (the shape the DuckDB extension consumes around DML)."""
    import msgpack
    import pyarrow.flight as fl

    client = fl.connect(f"grpc://localhost:{airport_server_port}")
    opts = fl.FlightCallOptions(headers=[(b"authorization", b"admin")])

    created = list(client.do_action(fl.Action("create_transaction", b""), opts))
    body = created[0].body.to_pybytes()
    tx = msgpack.unpackb(body, raw=False)
    assert isinstance(tx.get("identifier"), str) and tx["identifier"], tx

    req = msgpack.packb({"transaction_id": tx["identifier"]})
    status = list(client.do_action(fl.Action("get_transaction_status", req), opts))
    st = msgpack.unpackb(status[0].body.to_pybytes(), raw=False)
    assert st["exists"] is True, st
    assert st["status"] == "active", st

    # An unknown transaction id reports not-exists (not an error).
    req2 = msgpack.packb({"transaction_id": "does-not-exist"})
    st2 = msgpack.unpackb(
        list(client.do_action(fl.Action("get_transaction_status", req2), opts))[0].body.to_pybytes(),
        raw=False,
    )
    assert st2["exists"] is False, st2


def test_column_statistics_refused_by_protocol_error(airport_server_port):
    """column_statistics is refused with a Flight protocol error (governed catalog has no stats)."""
    import pyarrow.flight as fl

    client = fl.connect(f"grpc://localhost:{airport_server_port}")
    opts = fl.FlightCallOptions(headers=[(b"authorization", b"admin")])
    with pytest.raises(fl.FlightError) as exc:
        list(client.do_action(fl.Action("column_statistics", b"\x80"), opts))
    assert "column_statistics" in str(exc.value) or "statistics" in str(exc.value).lower()


if __name__ == "__main__":
    pytest.main([__file__, "-q"])
