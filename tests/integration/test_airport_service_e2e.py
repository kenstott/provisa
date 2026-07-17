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

    env = {
        **os.environ,
        "PG_PASSWORD": os.environ.get("PG_PASSWORD") or "provisa",
        "PROVISA_CONFIG": str(_SAMPLE_CONFIG),
        "FLIGHT_PORT": str(flight_port),
        "PROVISA_AIRPORT_PORT": str(airport_port),
        # PYTHONPATH → import the worktree's provisa (with the airport service) in the subprocess.
        "PYTHONPATH": str(_REPO_ROOT),
    }

    import tempfile

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


if __name__ == "__main__":
    pytest.main([__file__, "-q"])
