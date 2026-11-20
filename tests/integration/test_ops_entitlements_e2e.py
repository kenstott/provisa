# Copyright (c) 2026 Kenneth Stott
# Canary: 8c3e1f74-2b9d-4a60-9e57-d4a1b6c8f203
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""ops.entitlements and ops.entitlement_expiries on a real server (REQ-2219).

* SQL over ops.entitlements as org_admin returns the rows the store holds, each with its valid
  period: a changed list's first row is closed at the second's valid_from.
* A role the ops domain grants nothing is refused, as for every ops table.
* A subscription to ops.entitlement_expiries through the real ``/data/subscribe`` handler receives
  a rule created as an insert, the row read back as the subscriber may read it.
"""

# Requirements: REQ-2219

from __future__ import annotations

import json
import queue
import threading
import time

import httpx
import pytest
import sqlalchemy as sa
import yaml

from tests.integration.isolated_server import IsolatedServer, drop_org_schema

pytestmark = [pytest.mark.integration, pytest.mark.local_platform]

_ORG = "ops_entitlements"
_ROLE = "org_admin"
_OTHER = "catalog_reader"


def _config() -> dict:
    return {
        "federation_engine": "duckdb",
        "auth": {
            "provider": "none",
            "assignments_source": "provisa",
            "default_assignments": [{"domain_id": "*", "role_id": _ROLE}],
        },
        "naming": {"domain_prefix": False, "convention": "snake", "rules": []},
        "cache": {"enabled": False},
        "domains": [{"id": "sales", "description": "Sales"}],
        "roles": [
            {
                "id": _OTHER,
                "description": "Reads the sales catalog only.",
                "capabilities": ["catalog_read"],
                "domain_access": ["sales"],
            }
        ],
        "sources": [],
        "tables": [],
    }


@pytest.fixture(scope="module")
def server(tmp_path_factory):
    import asyncio

    work = tmp_path_factory.mktemp("ops_entitlements")
    path = work / "config.yaml"
    path.write_text(yaml.safe_dump(_config()))
    srv = IsolatedServer(_ORG, engine="duckdb", config=str(path), control_plane="own")
    srv.start(timeout=240)
    try:
        yield srv
    finally:
        srv.stop_process()
        asyncio.run(drop_org_schema(_ORG))


def _plane(srv: IsolatedServer) -> sa.Engine:
    assert srv.plane is not None
    return sa.create_engine(srv.plane.url.replace("+asyncpg", ""))


def _put_list(srv: IsolatedServer, identifiers: list[str], valid_from: str) -> int:
    """A provisa row of alice's viewer tables list, written as the store writes one."""
    with _plane(srv).begin() as conn:
        return conn.execute(
            sa.text(
                f"INSERT INTO org_{_ORG}.entitlements "
                "(source, principal, role_id, kind, identifiers, reason, set_by, valid_from) "
                "VALUES ('provisa', 'alice', 'viewer', 'tables', CAST(:ids AS jsonb), 'audit', "
                "'u-ops', CAST(:vf AS timestamptz)) RETURNING id"
            ),
            {"ids": json.dumps(identifiers), "vf": valid_from},
        ).scalar_one()


def _put_rule(srv: IsolatedServer) -> int:
    with _plane(srv).begin() as conn:
        return conn.execute(
            sa.text(
                f"INSERT INTO org_{_ORG}.entitlement_expiries "
                "(rule_key, principal, role_id, ends_at, ends, list_kind, reason, set_by) "
                "VALUES ('r1', 'alice', 'viewer', now() + interval '30 days', "
                "CAST(:ends AS jsonb), 'tables', 'audit', 'u-ops') RETURNING id"
            ),
            {"ends": json.dumps({"read": ["sales.orders"]})},
        ).scalar_one()


def _sql(srv: IsolatedServer, sql: str, role: str) -> httpx.Response:
    return httpx.post(
        f"{srv.base_url}/data/sql",
        json={"sql": sql},
        headers={"X-Provisa-Role": role},
        timeout=srv.request_timeout + 60,
    )


def _rows(response: httpx.Response) -> list[dict]:
    body = response.json()
    rows = body.get("data") or body.get("rows") or []
    if isinstance(rows, dict):
        rows = next(iter(rows.values()))
    return rows


class _Subscription:
    """``/data/subscribe/<table>`` read on a thread: each data frame's event, in order."""

    def __init__(self, srv: IsolatedServer, table: str) -> None:
        self.events: queue.Queue[dict] = queue.Queue()
        self.connected = threading.Event()
        self._stop = threading.Event()
        self._srv = srv
        self._table = table
        self._thread = threading.Thread(target=self._read, daemon=True)
        self._thread.start()
        assert self.connected.wait(60), self._srv.dump_stderr_debug()[-4000:]

    def _read(self) -> None:
        with httpx.stream(
            "GET",
            f"{self._srv.base_url}/data/subscribe/{self._table}",
            headers={"X-Provisa-Role": _ROLE},
            timeout=httpx.Timeout(10, read=None),
        ) as response:
            assert response.status_code == 200, response.read()
            for line in response.iter_lines():
                if line.startswith(": connected"):
                    self.connected.set()
                elif line.startswith("data: "):
                    self.events.put(json.loads(line.removeprefix("data: ")))
                if self._stop.is_set():
                    return

    def next_for(self, rid: int, timeout: float = 30) -> dict:
        deadline = time.monotonic() + timeout
        while True:
            left = deadline - time.monotonic()
            assert left > 0, f"no event for row {rid}"
            event = self.events.get(timeout=left)
            if event.get("row", {}).get("id") == rid:
                return event

    def close(self) -> None:
        self._stop.set()


def test_org_admin_reads_the_store_with_valid_periods(server):
    first = _put_list(server, ["sales.orders"], "2026-01-01T00:00:00Z")
    second = _put_list(server, ["sales.*"], "2026-02-01T00:00:00Z")
    response = _sql(
        server,
        "SELECT id, source, principal, role_id, kind, identifiers, valid_from, valid_to, "
        "recorded_at FROM ops.entitlements ORDER BY id",
        _ROLE,
    )
    assert response.status_code == 200, f"{response.text}\n{server.dump_stderr_debug()[-6000:]}"
    rows = {r["id"]: r for r in _rows(response)}
    assert set(rows) == {first, second}
    assert rows[first]["valid_to"] == rows[second]["valid_from"]
    assert rows[second]["valid_to"] is None
    assert rows[first]["source"] == "provisa"
    assert rows[first]["principal"] == "alice"
    assert rows[first]["recorded_at"] is not None


def test_a_role_the_ops_domain_grants_nothing_is_refused(server):
    for table in ("entitlements", "entitlement_expiries"):
        response = _sql(server, f"SELECT id FROM ops.{table}", _OTHER)
        assert response.status_code == 403, response.text
        assert response.json()["code"] == "data.governance_refused", response.text


def test_a_subscriber_sees_an_expiry_rule_created(server):
    sub = _Subscription(server, "entitlement_expiries")
    try:
        rid = _put_rule(server)
        created = sub.next_for(rid)
        assert created["op"] == "INSERT"
        row = created["row"]
        assert row["rule_key"] == "r1"
        assert row["principal"] == "alice"
        assert row["list_kind"] == "tables"
        assert row["valid_to"] is None
    finally:
        sub.close()
