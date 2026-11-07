# Copyright (c) 2026 Kenneth Stott
# Canary: 7f7c0d5c-1204-48b6-bff2-b18f0b2891ba
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The named demo ``customer360`` works as shipped (REQ-2034): its four SaaS systems land from the
rows it declares and none is contacted, its customer master hub is read live, and its tour answers
what its words say.

Its own docker-compose.yml brings the hub up on a port of this test's own. A real Provisa server
(DuckDB engine, its own control plane) loads the demo's config.yaml as is, with every outbound
request sent through a proxy of this test's that answers none and records each: Salesforce,
ServiceNow, Jira and Stripe land from replica_rows/ (REQ-2089) and no request reaches any of
them. The demo's setup builds the check's cascade. Then each tour statement, its Cypher and its
example query answer rows; the answer through the hub leaves out Larkspur Analytics, and the answer
through the corrected hub adds it and nothing else."""

# Requirements: REQ-2034, REQ-2089

from __future__ import annotations

import os
import socket
import subprocess
import sys
import threading
from pathlib import Path
from typing import Any

import httpx
import pytest
import yaml

from provisa.core.demo_bundles import store_namespaces

pytestmark = [pytest.mark.integration]

_REPO = Path(__file__).resolve().parents[2]
_DEMO = _REPO / "demo" / "named" / "customer360"
_TOUR = yaml.safe_load((_DEMO / "tour.yaml").read_text())["steps"]
_ROLE = "org_admin"
#: The hosts of the four systems' live accounts: no request may reach any of them.
_SAAS_HOSTS = ("salesforce.com", "force.com", "service-now.com", "atlassian.net", "stripe.com")
_SAAS_TABLES = {
    "accounts": "salesforce/salesforce.Account.tsv",
    "opportunities": "salesforce/salesforce.Opportunity.tsv",
    "companies": "servicenow/servicenow.core_company.tsv",
    "incidents": "servicenow/servicenow.incident.tsv",
    "bugs": "jira/jira.bug.tsv",
    "stripe_customers": "stripe/default.get_customers.tsv",
    "invoices": "stripe/default.get_invoices.tsv",
}


class RecordingProxy:
    """An HTTP(S) proxy on a leased loopback port that records the first line of every request
    (``CONNECT api.stripe.com:443 HTTP/1.1``, ``GET http://...``). A request for one of the four
    systems' hosts is refused (403); any other is passed through, so what the server fetches for
    itself (DuckDB's extensions) still arrives."""

    def __init__(self) -> None:
        from tests.port_lease import lease_ports

        (self.port,) = lease_ports(1)
        self.requests: list[str] = []
        self._sock = socket.create_server(("127.0.0.1", self.port))
        self._thread = threading.Thread(target=self._serve, daemon=True)
        self._thread.start()

    def _serve(self) -> None:
        while True:
            try:
                conn, _ = self._sock.accept()
            except OSError:
                return
            threading.Thread(target=self._handle, args=(conn,), daemon=True).start()

    def _handle(self, conn: socket.socket) -> None:
        with conn:
            conn.settimeout(30)
            try:
                head = conn.recv(65536)
            except OSError:
                return
            first = head.split(b"\r\n", 1)[0].decode("latin-1")
            self.requests.append(first)
            parts = first.split()
            if len(parts) < 2 or any(host in parts[1] for host in _SAAS_HOSTS):
                conn.sendall(
                    b"HTTP/1.1 403 Forbidden\r\nContent-Length: 0\r\nConnection: close\r\n\r\n"
                )
                return
            method, target = parts[0], parts[1]
            if method == "CONNECT":
                host, _, port = target.rpartition(":")
                upstream_addr, preface = (host, int(port)), b""
            else:
                rest = target.split("://", 1)[-1]
                hostport = rest.split("/", 1)[0]
                host, _, port = hostport.partition(":")
                upstream_addr, preface = (host, int(port or 80)), head
            try:
                upstream = socket.create_connection(upstream_addr, timeout=30)
            except OSError:
                conn.sendall(
                    b"HTTP/1.1 502 Bad Gateway\r\nContent-Length: 0\r\nConnection: close\r\n\r\n"
                )
                return
            with upstream:
                if method == "CONNECT":
                    conn.sendall(b"HTTP/1.1 200 Connection established\r\n\r\n")
                else:
                    upstream.sendall(preface)
                conn.settimeout(None)
                upstream.settimeout(None)
                back = threading.Thread(target=_pipe, args=(upstream, conn), daemon=True)
                back.start()
                _pipe(conn, upstream)
                back.join(timeout=60)

    @property
    def env(self) -> dict[str, str]:
        url = f"http://127.0.0.1:{self.port}"
        return {
            "HTTP_PROXY": url,
            "HTTPS_PROXY": url,
            "ALL_PROXY": url,
            "http_proxy": url,
            "https_proxy": url,
            "all_proxy": url,
            "NO_PROXY": "localhost,127.0.0.1,::1",
            "no_proxy": "localhost,127.0.0.1,::1",
        }

    def close(self) -> None:
        self._sock.close()


def _pipe(src: socket.socket, dst: socket.socket) -> None:
    try:
        while chunk := src.recv(65536):
            dst.sendall(chunk)
    except OSError:
        pass
    finally:
        try:
            dst.shutdown(socket.SHUT_WR)
        except OSError:
            pass


class Hub:
    """The demo's customer master hub, on a leased port."""

    def __init__(self) -> None:
        from tests.port_lease import lease_ports

        (self.port,) = lease_ports(1)
        self.project = f"customer360-demo-{os.getpid()}"

    @property
    def env(self) -> dict[str, str]:
        return {"CUSTOMER360_PG_PORT": str(self.port), **store_namespaces(_DEMO)}

    def _compose(self, *args: str, check: bool = True) -> subprocess.CompletedProcess:
        return subprocess.run(
            [
                "docker",
                "compose",
                "-p",
                self.project,
                "-f",
                str(_DEMO / "docker-compose.yml"),
                *args,
            ],
            env={**os.environ, **self.env},
            capture_output=True,
            text=True,
            check=check,
            timeout=900,
        )

    def up(self) -> None:
        try:
            self._compose("up", "-d", "--wait")
        except subprocess.CalledProcessError as exc:
            logs = self._compose("logs", "--no-color", check=False).stdout
            raise AssertionError(f"the hub did not come up: {exc.stderr}\n{logs[-4000:]}") from exc

    def down(self) -> None:
        self._compose("down", "-v", "--remove-orphans", check=False)


@pytest.fixture(scope="module")
def proxy():
    recording = RecordingProxy()
    try:
        yield recording
    finally:
        recording.close()


@pytest.fixture(scope="module")
def hub():
    built = Hub()
    built.up()
    try:
        yield built
    finally:
        built.down()


@pytest.fixture(scope="module")
def server(hub, proxy, tmp_path_factory, request):
    from tests.integration.isolated_server import IsolatedServer

    # DIAGNOSTIC (J915): pytest's fd capture holds fd 2; write to the fd it saved before that.
    capman = request.config.pluginmanager.getplugin("capturemanager")
    _OUT[0] = capman._global_capturing.err.targetfd_save

    home = tmp_path_factory.mktemp("customer360_demo")
    srv = IsolatedServer(
        "customer360_demo",
        engine="duckdb",
        config=str(_DEMO / "config.yaml"),
        control_plane="own",
        materialize_store_url=f"duckdb:///{home / 'store.duckdb'}",
        env={**hub.env, **proxy.env, "PROVISA_DATA_DIR": str(home / "provisa")},
    )
    stop = threading.Event()
    threading.Thread(target=_watch, args=(srv, stop), daemon=True).start()
    _say("[c360] server starting\n")
    srv.start(timeout=400)
    _say("[c360] server up\n")
    try:
        yield srv
    finally:
        stop.set()
        srv.stop_process()


_OUT = [2]


def _say(text: str) -> None:  # DIAGNOSTIC (J915)
    os.write(_OUT[0], text.encode("utf-8", "replace"))


def _watch(
    server, stop: threading.Event
) -> None:  # DIAGNOSTIC (J915): reverted once the runner kill is found
    import time

    start = time.monotonic()
    while not stop.wait(20):
        held = getattr(server, "_stderr_file", None)
        log = Path(held.name) if held is not None else Path("/nonexistent")
        mem = subprocess.run(["free", "-m"], capture_output=True, text=True).stdout
        top = subprocess.run(
            ["ps", "-eo", "rss,nlwp,etimes,args", "--sort=-rss"], capture_output=True, text=True
        ).stdout.splitlines()[:6]
        disk = subprocess.run(["df", "-h", "/", "/tmp"], capture_output=True, text=True).stdout
        proc = getattr(server, "_proc", None)
        rss_kb = 0
        if proc is not None and Path(f"/proc/{proc.pid}/status").exists():
            for row in Path(f"/proc/{proc.pid}/status").read_text().splitlines():
                if row.startswith("VmRSS:"):
                    rss_kb = int(row.split()[1])
        keep = 8
        if rss_kb > 4_000_000:
            # The server dumps every thread's stack to its stderr on SIGUSR1 (main.py).
            import signal

            os.kill(proc.pid, signal.SIGUSR1)
            time.sleep(2)
            keep = 250
        tail = log.read_text(errors="replace").splitlines()[-keep:] if log.exists() else []
        _say(
            f"\n[c360-watch {time.monotonic() - start:.0f}s]\n{mem}{disk}"
            + "\n".join(row[:240] for row in top)
            + "\n-- server stderr tail --\n"
            + "\n".join(row[:300] for row in tail)
            + "\n"
        )


@pytest.fixture(scope="module")
def set_up(server):
    _say("[c360] set_up starting\n")
    proc = subprocess.Popen(
        [
            sys.executable,
            str(_REPO / "scripts" / "run_demo_setup.py"),
            str(_DEMO),
            "--url",
            server.base_url,
        ],
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
    )
    out = []
    try:
        for line in proc.stdout:
            out.append(line)
            _say(f"[c360-setup] {line}")
        proc.wait(timeout=1800)
    finally:
        _say(f"[c360] set_up ended {proc.poll()}\n")
    assert proc.returncode == 0, "".join(out)
    return server


def _post(server, path: str, body: dict) -> httpx.Response:
    return httpx.post(
        f"{server.base_url}{path}", json=body, headers={"X-Provisa-Role": _ROLE}, timeout=900
    )


def _sql(server, sql: str) -> list[dict[str, Any]]:
    resp = _post(server, "/data/sql", {"sql": sql})
    assert resp.status_code == 200, f"{sql}: HTTP {resp.status_code}: {resp.text}"
    return resp.json()["data"]["sql"]


def test_each_system_lands_from_the_rows_the_demo_declares(server):
    for table, rel in _SAAS_TABLES.items():
        declared = len((_DEMO / "replica_rows" / rel).read_text().rstrip("\n").split("\n")) - 1
        (row,) = _sql(server, f'SELECT COUNT(*) AS n FROM "customer360"."{table}"')
        assert row["n"] == declared, table


def test_the_hub_is_read_live(server):
    (row,) = _sql(server, 'SELECT COUNT(*) AS n FROM "customer360"."xref"')
    assert row["n"] == (_DEMO / "db" / "init.sql").read_text().split("INSERT INTO mdm.xref")[
        1
    ].split("INSERT INTO")[0].count("\n  (")


@pytest.mark.parametrize(
    "step", [s for s in _TOUR if {"sql", "cypher"} & set(s)], ids=lambda s: s["key"]
)
def test_each_tour_statement_answers_rows(set_up, step):
    if "sql" in step:
        assert _sql(set_up, step["sql"]), step["key"]
        return
    resp = _post(set_up, "/data/cypher", {"query": step["cypher"]})
    assert resp.status_code == 200, resp.text
    assert resp.json()["rows"], step["key"]


def test_the_example_query_answers_through_the_hub(set_up):
    query = (_DEMO / "queries" / "1-renewal-risk.graphql").read_text()
    resp = _post(set_up, "/data/graphql", {"query": query})
    assert resp.status_code == 200 and "errors" not in resp.json(), resp.text
    rows = resp.json()["data"]["c__viewRenewalRiskHub"]
    assert len(rows) == 7 and "Larkspur Analytics" not in {r["account"] for r in rows}


def test_the_hub_answer_misses_larkspur_and_the_corrected_answer_has_it(set_up):
    hub = {
        r["account"]
        for r in _sql(set_up, 'SELECT account FROM "customer360"."view_renewal_risk_hub"')
    }
    checked = _sql(
        set_up,
        'SELECT account, overdue_invoices, missed_by_hub FROM "customer360"."view_renewal_risk_checked"',
    )
    assert "Larkspur Analytics" not in hub and len(hub) == 7
    assert {r["account"] for r in checked} == hub | {"Larkspur Analytics"}
    assert [(r["account"], r["overdue_invoices"]) for r in checked if r["missed_by_hub"]] == [
        ("Larkspur Analytics", 1)
    ]


def test_the_check_finds_the_wrong_merge_beside_the_other_findings(set_up):
    found = _sql(
        set_up,
        'SELECT finding, COUNT(*) AS n FROM "customer360"."view_hub_findings" GROUP BY finding',
    )
    assert {r["finding"]: r["n"] for r in found} == {
        "wrong merge": 2,
        "duplicate customer": 2,
        "coverage gap": 5,
        "stale link": 2,
    }
    (merge,) = _sql(
        set_up,
        'SELECT hub_customer, evidence_customer, linked_by FROM "customer360"."view_hub_findings" '
        "WHERE finding = 'wrong merge' AND evidence_customer = 'Larkspur Analytics'",
    )
    assert merge == {
        "hub_customer": "Larkspur Labs",
        "evidence_customer": "Larkspur Analytics",
        "linked_by": "bulk import",
    }


def test_no_request_leaves_for_a_live_account(set_up, proxy):
    # Read every system's tables again after the tour's statements: nothing is fetched.
    for table in _SAAS_TABLES:
        assert _sql(set_up, f'SELECT * FROM "customer360"."{table}" LIMIT 1'), table
    reached = [r for r in proxy.requests if any(host in r for host in _SAAS_HOSTS)]
    assert reached == [], reached
