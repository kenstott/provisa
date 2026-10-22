# Copyright (c) 2026 Kenneth Stott
# Canary: e5ebd686-3501-41cd-8e4c-a9a3c0ba3fec
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""REQ-2006, REQ-2077, REQ-2076, REQ-2089: the named demo ``dataroom`` works as shipped.

Its own docker-compose.yml brings Northfield Supply's order and finance database up on a port of
this test's own (db/init.sql). A real Provisa server (DuckDB engine, its own control plane, password
sign-in) loads a copy of the demo's directory: its config.yaml as is, and the CRM's declared rows
(replica_rows/crm), landed as the Salesforce source's replica with no call to Salesforce. The
demo's setup runs the profiler.

The seller sets the room up as the tour shows it: two local accounts for bidder A's clean team,
each granted bidder_a until the deal date. Signed in, an analyst reads the aggregate views, the
profiles and the masked sample and the rest of the seller's records, and is refused every
customer identity -- the customers', the CRM's businesses' and their deals' names; naming bidder
B's role is a role probe, refused and written to the org's trail. The query audit holds what each bidder looked
at. A grant past its date is no longer held, and one action -- removing the grant -- ends an
analyst's access while the audit stays.
"""

# Requirements: REQ-2006, REQ-2077, REQ-2076, REQ-2089

from __future__ import annotations

import base64
import os
import shutil
import subprocess
import time
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

import httpx
import pytest

from provisa.core.demo_bundles import store_namespaces

pytestmark = [pytest.mark.integration]

_REPO = Path(__file__).resolve().parents[2]
_DEMO = _REPO / "demo" / "named" / "dataroom"
_ROLE = "org_admin"
_JWT_SECRET = "dataroom-demo-test-signing-secret-0123456789"
_SELLER = ("seller", "seller-password-0123")
#: Bidder A's clean team: two analysts, each a local account bound to bidder_a.
_ANALYSTS = {"ana.alvarez": "clean-team-pw-0001", "ben.brooks": "clean-team-pw-0002"}
#: The deal date: every bidder grant ends at it.
_DEAL_DATE = datetime(2027, 3, 31, 23, 59, 59, tzinfo=UTC)
_VIEWS = ("customer_counts", "cohorts", "revenue_by_segment", "pipeline", "data_quality")
#: The card's statement and the tour's: what bidder A is shown of revenue.
ANSWER_SQL = (
    "SELECT year, segment, orders, revenue, average_order "
    'FROM "dataroom"."view_revenue_by_segment" ORDER BY year, segment'
)


class DataroomStack:
    """The demo's compose stack, on a leased port."""

    def __init__(self) -> None:
        from tests.port_lease import lease_ports

        (self.pg_port,) = lease_ports(1)
        self.project = f"dataroom-demo-{os.getpid()}"

    @property
    def env(self) -> dict[str, str]:
        return {"DATAROOM_PG_PORT": str(self.pg_port), **store_namespaces(_DEMO)}

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
            timeout=600,
        )

    def up(self) -> None:
        try:
            self._compose("up", "-d", "--wait", "postgres")
        except subprocess.CalledProcessError as exc:
            logs = self._compose("logs", "--no-color", check=False).stdout
            raise AssertionError(
                f"the dataroom stack did not come up: {exc.stderr}\n{logs[-4000:]}"
            ) from exc

    def down(self) -> None:
        self._compose("down", "-v", "--remove-orphans", check=False)


@pytest.fixture(scope="module")
def stack():
    built = DataroomStack()
    built.up()
    try:
        yield built
    finally:
        built.down()


@pytest.fixture(scope="module")
def home(tmp_path_factory) -> Path:
    """A copy of the demo's directory: the server reads its config and declared rows there."""
    copy = tmp_path_factory.mktemp("dataroom_demo") / "dataroom"
    shutil.copytree(_DEMO, copy)
    return copy


def _basic(user: str, password: str) -> dict[str, str]:
    return {"Authorization": "Basic " + base64.b64encode(f"{user}:{password}".encode()).decode()}


_SELLER_HEADERS = {**_basic(*_SELLER), "X-Provisa-Role": _ROLE}


@pytest.fixture(scope="module")
def server(stack, home, tmp_path_factory):
    from tests.integration.isolated_server import IsolatedServer

    data = tmp_path_factory.mktemp("dataroom_demo_data")
    srv = IsolatedServer(
        "dataroom_demo",
        engine="duckdb",
        config=str(home / "config.yaml"),
        control_plane="own",
        materialize_store_url=f"duckdb:///{data / 'store.duckdb'}",
        auth={
            "provider": "basic",
            "jwt_secret": _JWT_SECRET,
            "assignments_source": "provisa",
            "default_assignments": [],
            "superuser": {"username": _SELLER[0], "password": _SELLER[1]},
        },
        env={**stack.env, "PROVISA_DATA_DIR": str(data / "provisa")},
    )
    srv.start(timeout=600)
    try:
        # The demo's setup, as its template build runs it: the profiler's first run (setup.yaml).
        resp = httpx.post(
            f"{srv.base_url}/admin/profilers/room-profiler/run",
            headers=_SELLER_HEADERS,
            timeout=900,
        )
        assert resp.status_code == 200, resp.text
        yield srv
    finally:
        srv.stop_process()


def _seller(server, method: str, path: str, body: dict | None = None) -> httpx.Response:
    return httpx.request(
        method, f"{server.base_url}{path}", json=body, headers=_SELLER_HEADERS, timeout=300
    )


@pytest.fixture(scope="module")
def analysts(server) -> dict[str, dict[str, Any]]:
    """Bidder A's two analysts, invited as the tour shows: a local account each, bound to bidder_a
    until the deal date. Each signed in: its id, its grant and its bearer token."""
    out: dict[str, dict[str, Any]] = {}
    for username, password in _ANALYSTS.items():
        made = _seller(
            server,
            "POST",
            "/admin/users/",
            {"username": username, "password": password, "email": f"{username}@bidder-a.example"},
        )
        assert made.status_code == 200, made.text
        user_id = made.json()["id"]
        granted = _seller(
            server,
            "POST",
            f"/admin/users/{user_id}/assignments",
            {"role_id": "bidder_a", "domain_id": "*", "expires_at": _DEAL_DATE.isoformat()},
        )
        assert granted.status_code == 200, granted.text
        signed_in = httpx.post(
            f"{server.base_url}/auth/login",
            json={"username": username, "password": password},
            timeout=60,
        )
        assert signed_in.status_code == 200, signed_in.text
        out[username] = {
            "id": user_id,
            "grant": granted.json(),
            "token": signed_in.json()["access_token"],
        }
    return out


def _as(server, token: str, role: str, sql: str) -> httpx.Response:
    return httpx.post(
        f"{server.base_url}/data/sql",
        json={"sql": sql},
        headers={"Authorization": f"Bearer {token}", "X-Provisa-Role": role},
        timeout=300,
    )


def _rows(resp: httpx.Response) -> list[dict[str, Any]]:
    assert resp.status_code == 200, resp.text
    return resp.json()["data"]["sql"]


def _ana(analysts) -> str:
    return analysts["ana.alvarez"]["token"]


def test_the_crm_is_its_declared_rows(server):
    [accounts] = _rows(_as_seller(server, 'SELECT COUNT(*) AS n FROM "dataroom"."accounts"'))
    [deals] = _rows(_as_seller(server, 'SELECT COUNT(*) AS n FROM "dataroom"."opportunities"'))
    assert (accounts["n"], deals["n"]) == (240, 720)


def _as_seller(server, sql: str) -> httpx.Response:
    return _seller(server, "POST", "/data/sql", {"sql": sql})


def test_each_analyst_is_bound_to_bidder_a_until_the_deal_date(server, analysts):
    for analyst in analysts.values():
        resp = _seller(server, "GET", f"/admin/users/{analyst['id']}/assignments")
        assert resp.status_code == 200, resp.text
        [grant] = resp.json()
        assert grant["role_id"] == "bidder_a"
        assert datetime.fromisoformat(grant["expires_at"]) == _DEAL_DATE


@pytest.mark.parametrize("view", _VIEWS)
def test_an_analyst_reads_each_aggregate_view(server, analysts, view):
    assert _rows(_as(server, _ana(analysts), "bidder_a", f'SELECT * FROM "dataroom"."view_{view}"'))


def test_the_aggregates_are_the_seller_s_own_numbers(server, analysts):
    quality = {
        (r["table_name"], r["data_check"]): r["failing_rows"]
        for r in _rows(
            _as(server, _ana(analysts), "bidder_a", 'SELECT * FROM "dataroom"."view_data_quality"')
        )
    }
    assert quality == {
        ("customers", "has an email"): 64,
        ("customers", "phone has ten digits"): 55,
        ("invoices", "paid"): 282,
    }
    counts = _rows(
        _as(
            server,
            _ana(analysts),
            "bidder_a",
            'SELECT SUM(customers) AS customers FROM "dataroom"."view_customer_counts"',
        )
    )
    assert counts == [{"customers": 2400}]
    assert _rows(_as(server, _ana(analysts), "bidder_a", ANSWER_SQL))


def test_an_analyst_reads_the_profiles(server, analysts):
    rows = _rows(
        _as(
            server,
            _ana(analysts),
            "bidder_a",
            'SELECT column_name, null_share FROM "dataroom"."invoice_profile"',
        )
    )
    paid = [r for r in rows if r["column_name"] == "paid_at"]
    assert paid and paid[0]["null_share"] > 0


def test_the_sample_withholds_every_identity(server, analysts):
    rows = _rows(
        _as(
            server,
            _ana(analysts),
            "bidder_a",
            'SELECT * FROM "dataroom"."view_customer_sample"',
        )
    )
    assert len(rows) == 48
    assert "name" not in rows[0]
    assert all(r["email"] is None or r["email"].startswith("***@") for r in rows)
    assert all(not any(ch.isdigit() for ch in r["phone"]) for r in rows)


@pytest.mark.parametrize(
    "sql",
    [
        'SELECT name FROM "dataroom"."customers"',
        'SELECT "Name" FROM "dataroom"."accounts"',
        'SELECT "Name" FROM "dataroom"."opportunities"',
    ],
    ids=["customer-names", "account-names", "deal-names"],
)
def test_an_analyst_is_refused_every_identity(server, analysts, sql):
    resp = _as(server, _ana(analysts), "bidder_a", sql)
    assert resp.status_code == 403, resp.text
    assert "V003" in resp.text, resp.text


def test_an_analyst_reads_the_rest_of_the_seller_s_records(server, analysts):
    rows = _rows(
        _as(
            server,
            _ana(analysts),
            "bidder_a",
            'SELECT o.ordered_at, o.amount, c.segment FROM "dataroom"."orders" AS o '
            'JOIN "dataroom"."customers" AS c ON c.id = o.customer_id LIMIT 5',
        )
    )
    assert len(rows) == 5


def _org_id(server) -> str:
    resp = _seller(server, "GET", "/auth/me")
    assert resp.status_code == 200, resp.text
    return resp.json()["active_org_id"]


def test_naming_bidder_b_s_role_is_a_role_probe(server, analysts):
    from sqlalchemy import text

    from provisa.core.environments import org_schema
    from provisa.security.role_probe import ACTION

    resp = _as(
        server, _ana(analysts), "bidder_b", 'SELECT * FROM "dataroom"."view_customer_counts"'
    )
    assert resp.status_code == 403, resp.text
    # The org's administrative trail, as the team page reads it.
    trail = f'"{org_schema(_org_id(server))}".admin_audit_log'
    deadline = time.monotonic() + 30
    while True:
        with server.plane.engine().connect() as conn:
            probes = conn.execute(
                text(f"SELECT subject_id, detail FROM {trail} WHERE action = :action"),
                {"action": ACTION},
            ).fetchall()
        probes = [p for p in probes if p.detail.get("requested_role") == "bidder_b"]
        if probes:
            break
        assert time.monotonic() < deadline, "no role probe was recorded"
        time.sleep(1)
    assert probes[0].subject_id == analysts["ana.alvarez"]["id"]


def _audit(server) -> list[dict[str, Any]]:
    return _rows(
        _as_seller(
            server,
            'SELECT user_id, role_id, table_names, status_code FROM "ops"."query_audit_log" '
            "WHERE role_id = 'bidder_a'",
        )
    )


def test_the_audit_holds_what_each_analyst_looked_at(server, analysts):
    _rows(_as(server, _ana(analysts), "bidder_a", ANSWER_SQL))
    _as(server, _ana(analysts), "bidder_a", 'SELECT name FROM "dataroom"."customers"')
    rows = _audit(server)
    ana = [r for r in rows if r["user_id"] == analysts["ana.alvarez"]["id"]]
    # A view is read through the tables it reads: the audit names those.
    assert any("finance.public.orders" in r["table_names"] for r in ana), ana
    assert any(r["status_code"] >= 400 for r in ana), "the refused statements are audited too"


def test_a_grant_past_its_date_is_no_longer_held(server, analysts):
    ben = analysts["ben.brooks"]
    assert _rows(
        _as(server, ben["token"], "bidder_a", 'SELECT * FROM "dataroom"."view_customer_counts"')
    )
    past = (datetime.now(tz=UTC) - timedelta(minutes=1)).isoformat()
    _seller(server, "DELETE", f"/admin/users/{ben['id']}/assignments/{ben['grant']['id']}")
    regranted = _seller(
        server,
        "POST",
        f"/admin/users/{ben['id']}/assignments",
        {"role_id": "bidder_a", "domain_id": "*", "expires_at": past},
    )
    assert regranted.status_code == 200, regranted.text
    ben["grant"] = regranted.json()
    refused = _as(
        server, ben["token"], "bidder_a", 'SELECT * FROM "dataroom"."view_customer_counts"'
    )
    assert refused.status_code == 403, refused.text


def test_removing_the_grant_ends_access_and_keeps_the_audit(server, analysts):
    ana = analysts["ana.alvarez"]
    before = [r for r in _audit(server) if r["user_id"] == ana["id"]]
    assert before
    removed = _seller(
        server, "DELETE", f"/admin/users/{ana['id']}/assignments/{ana['grant']['id']}"
    )
    assert removed.status_code in (200, 204), removed.text
    refused = _as(
        server, ana["token"], "bidder_a", 'SELECT * FROM "dataroom"."view_customer_counts"'
    )
    assert refused.status_code == 403, refused.text
    after = [r for r in _audit(server) if r["user_id"] == ana["id"]]
    assert len(after) >= len(before)
