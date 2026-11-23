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
def server(hub, proxy, tmp_path_factory):
    from tests.integration.isolated_server import IsolatedServer

    from tests.port_lease import lease_ports

    home = tmp_path_factory.mktemp("customer360_demo")
    # The entity-resolution code source reads its tables over a Flight listener minted for each
    # call (REQ-2226), on a block of one port: its calls read one at a time.
    (minted_port, flight_port) = lease_ports(2)
    srv = IsolatedServer(
        "customer360_demo",
        engine="duckdb",
        config=str(_DEMO / "config.yaml"),
        control_plane="own",
        materialize_store_url=f"duckdb:///{home / 'store.duckdb'}",
        env={
            **hub.env,
            **proxy.env,
            "PROVISA_DATA_DIR": str(home / "provisa"),
            "PROVISA_MINTED_PGWIRE_PORT": str(minted_port),
            "PROVISA_MINTED_FLIGHT_PORT": str(flight_port),
            "PROVISA_MINTED_FLIGHT_PORTS": "1",
            "PROVISA_RUNTIME_DEPS_CACHE": str(home / "runtime-deps"),
        },
    )
    srv.start(timeout=400)
    try:
        yield srv
    finally:
        srv.stop_process()


@pytest.fixture(scope="module")
def set_up(server):
    done = subprocess.run(
        [
            sys.executable,
            str(_REPO / "scripts" / "run_demo_setup.py"),
            str(_DEMO),
            "--url",
            server.base_url,
        ],
        capture_output=True,
        text=True,
        timeout=1800,
    )
    assert done.returncode == 0, f"{done.stdout}\n{done.stderr}"
    return server


def _post(server, path: str, body: dict, role: str = _ROLE) -> httpx.Response:
    return httpx.post(
        f"{server.base_url}{path}", json=body, headers={"X-Provisa-Role": role}, timeout=900
    )


def _sql(server, sql: str, role: str = _ROLE) -> list[dict[str, Any]]:
    resp = _post(server, "/data/sql", {"sql": sql}, role)
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
        # Five gaps in the hub, and the four records no rule gives a customer (REQ-2034,
        # amended 2026-11-14): Harbor Lane Bakery, Halcyon Mdia Grp, Tessa Rowe Design, TBD.
        "coverage gap": 9,
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


# --------------------------------------------------------------------------------------------- #
# Rerunning entity resolution from Provisa (REQ-2034, amended 2026-11-14). Last: they rewrite the  #
# bridges, which the tests above do not read.                                                   #
# --------------------------------------------------------------------------------------------- #

#: The role that reruns entity resolution: write, the bridges' delete grant, the commands.
_ENTITY_STEWARD = "entity_steward"
_BRIDGES = {"rerun_org_bridge": "entity_org_bridge", "rerun_person_bridge": "entity_person_bridge"}
_KEY = ("source_schema", "source_table", "source_column", "source_key")


def _job():
    """The demo's matching job, er_job, as the shell runs it."""
    code = str(_DEMO / "entity_resolution")
    if code not in sys.path:
        sys.path.insert(0, code)
    import er_job

    return er_job


def _hub_sql(hub: Hub, sql: str, params: tuple = ()) -> list[tuple]:
    """``sql`` on the demo's database itself, as its owner."""
    import psycopg

    with psycopg.connect(
        host="127.0.0.1",
        port=hub.port,
        user="postgres",
        password="postgres",
        dbname="customer360" + hub.env.get("CUSTOMER360_PG_NS", ""),
        autocommit=True,
    ) as conn:
        cur = conn.execute(sql, params)
        return cur.fetchall() if cur.description else []


def _bridge(hub: Hub, table: str, columns: list[str]) -> dict[tuple, dict]:
    """The bridge's rows as the database holds them, by key."""
    rows = _hub_sql(hub, f"SELECT {', '.join(columns)} FROM er.{table}")
    return {r[:4]: dict(zip(columns, r)) for r in rows}


def _expected(job) -> dict[str, dict[tuple, dict]]:
    """Each bridge's rows as the job matches the rows the demo declares (db/er.sql), by key."""
    rows, columns = job.read_replica_rows(_DEMO)
    bridges = job.run(rows, columns, job.SEED_RUN_ID)
    return {
        "entity_org_bridge": {tuple(r[k] for k in _KEY): r for r in bridges.org},
        "entity_person_bridge": {tuple(r[k] for k in _KEY): r for r in bridges.person},
    }


def _without_run(row: dict) -> dict:
    return {k: v for k, v in row.items() if k != "match_run_id"}


def _same(got: dict, want: dict) -> bool:
    """One bridge row equal to another apart from its run, its scores within a tolerance."""
    for column, value in _without_run(want).items():
        held = got[column]
        if isinstance(value, float) or isinstance(held, float):
            if value is None or held is None or abs(held - value) > 1e-14 * max(1.0, abs(value)):
                return False
        elif held != value:
            return False
    return True


def _rerun(server, command: str, role: str = _ENTITY_STEWARD) -> httpx.Response:
    return _post(server, "/data/sql", {"sql": f"SELECT * FROM {command}()"}, role)


def test_the_rerun_commands_replace_each_bridge_and_a_second_run_keeps_the_ids(set_up, hub):
    job = _job()
    expected = _expected(job)
    columns = {"entity_org_bridge": job.ORG_COLUMNS, "entity_person_bridge": job.PERSON_COLUMNS}
    for command, table in _BRIDGES.items():
        cols = columns[table]
        shipped = _bridge(hub, table, cols)
        assert set(shipped) == set(expected[table]), table  # the database holds db/er.sql
        changed, dropped = sorted(shipped)[:2]
        # Before the rerun: one row's match changed, one mention's row gone, and a row for a
        # mention no longer in any system.
        _hub_sql(
            hub,
            f"UPDATE er.{table} SET canonical_id = 'stale' WHERE "
            + " AND ".join(f"{k} = %s" for k in _KEY),
            changed,
        )
        _hub_sql(
            hub, f"DELETE FROM er.{table} WHERE " + " AND ".join(f"{k} = %s" for k in _KEY), dropped
        )
        gone = ("jira", "bug", shipped[changed]["source_column"], "a mention removed from Jira")
        stale = {**shipped[changed], **dict(zip(_KEY, gone)), "match_run_id": "er-removed"}
        _hub_sql(
            hub,
            f"INSERT INTO er.{table} ({', '.join(cols)}) VALUES ({', '.join(['%s'] * len(cols))})",
            tuple(stale[c] for c in cols),
        )

        resp = _rerun(set_up, command)
        assert resp.status_code == 200, f"{command}: HTTP {resp.status_code}: {resp.text}"
        first = _bridge(hub, table, cols)
        # Replaced: the removed mention deleted, the dropped one back, the changed one matched
        # again, every row the job's match of the rows the demo declares, of one new run.
        assert gone not in first
        assert set(first) == set(expected[table]), table
        wrong = [k for k, row in first.items() if not _same(row, expected[table][k])]
        assert wrong == [], (table, wrong[:3])
        runs = {row["match_run_id"] for row in first.values()}
        assert len(runs) == 1 and runs != {job.SEED_RUN_ID}, runs

        resp = _rerun(set_up, command)
        assert resp.status_code == 200, f"{command}: HTTP {resp.status_code}: {resp.text}"
        second = _bridge(hub, table, cols)
        assert {k: r["canonical_id"] for k, r in second.items()} == {
            k: r["canonical_id"] for k, r in first.items()
        }, table


def test_a_role_without_the_bridges_delete_grant_may_not_rerun(set_up, hub):
    # The org admin writes, is assigned the commands through data_steward and may insert and update
    # bridge rows, but holds no delete grant on them, which replacing a bridge needs.
    job = _job()
    for command, table in _BRIDGES.items():
        before = _bridge(hub, table, job.ORG_COLUMNS[:4])
        resp = _rerun(set_up, command, _ROLE)
        assert resp.status_code != 200, f"{command}: {resp.text}"
        assert "delete grant" in resp.text, resp.text
        assert _bridge(hub, table, job.ORG_COLUMNS[:4]) == before, table
