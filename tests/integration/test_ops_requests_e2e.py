# Copyright (c) 2026 Kenneth Stott
# Canary: 5d9b3e71-8f2a-4c06-b4e1-a7c3f9d2e518
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""ops.requests on a real server (REQ-2216): a workflow tool subscribes to it, and an enterprise's
own table relates to it.

* A subscription to ops.requests through the real ``/data/subscribe`` handler receives a request
  raised as an insert and its decision as an update, each row read back as the subscriber may
  read it -- the event's schema is the table's columns.
* A decision carrying an Idempotency-Key applies once: the repeat is answered with the first
  result.
* A registered user table (the enterprise's process extension) is related to ops.requests by
  request id, and the request and its process data are read in one GraphQL query.
"""

# Requirements: REQ-2215, REQ-2216, REQ-2217

from __future__ import annotations

import json
import queue
import threading
import time

import httpx
import pytest
import sqlalchemy as sa
import yaml
from sqlalchemy.engine import make_url

from tests.helpers import registered_id, release_field
from tests.integration.isolated_server import IsolatedServer, drop_org_schema

pytestmark = [pytest.mark.integration, pytest.mark.local_platform]

_ORG = "ops_requests"
_ROLE = "org_admin"
_EXT_SCHEMA = "rq_ext"


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
        "domains": [{"id": "process", "description": "The enterprise's process data"}],
        "roles": [],
        "sources": [],
        "tables": [],
    }


@pytest.fixture(scope="module")
def server(tmp_path_factory):
    import asyncio

    work = tmp_path_factory.mktemp("ops_requests")
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


def _admin(srv: IsolatedServer, mutation: str) -> dict:
    response = httpx.post(
        f"{srv.base_url}/admin/graphql",
        json={"query": "mutation { " + mutation + " { success message } }"},
        headers={"X-Provisa-Role": _ROLE},
        timeout=srv.request_timeout + 120,
    )
    assert response.status_code == 200, response.text
    assert "errors" not in response.json(), response.text
    (result,) = response.json()["data"].values()
    assert result["success"], result["message"]
    return result


def _graphql(srv: IsolatedServer, query: str) -> dict:
    response = httpx.post(
        f"{srv.base_url}/data/graphql",
        json={"query": query},
        headers={"X-Provisa-Role": _ROLE},
        timeout=srv.request_timeout + 60,
    )
    assert response.status_code == 200, response.text
    body = response.json()
    assert "errors" not in body, body
    return body["data"]


def _raise(srv: IsolatedServer, environment: str = "prod") -> int:
    """A pending request, written as the queue writes one."""
    with _plane(srv).begin() as conn:
        return conn.execute(
            sa.text(
                f"INSERT INTO org_{_ORG}.creation_requests "
                "(request_type, capability, payload, requested_by, environment, addressed_roles) "
                "VALUES ('view', 'create_view', CAST(:payload AS jsonb), 'u-ann', :env, "
                "CAST(:roles AS jsonb)) RETURNING id"
            ),
            {
                "payload": json.dumps({"name": "v_sales"}),
                "env": environment,
                "roles": '["org_admin"]',
            },
        ).scalar_one()


class _Subscription:
    """``/data/subscribe/requests`` read on a thread: each data frame's event, in order."""

    def __init__(self, srv: IsolatedServer) -> None:
        self.events: queue.Queue[dict] = queue.Queue()
        self.connected = threading.Event()
        self._stop = threading.Event()
        self._srv = srv
        self._thread = threading.Thread(target=self._read, daemon=True)
        self._thread.start()
        assert self.connected.wait(60), self._srv.dump_stderr_debug()[-4000:]

    def _read(self) -> None:
        with httpx.stream(
            "GET",
            f"{self._srv.base_url}/data/subscribe/requests",
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
            assert left > 0, f"no event for request {rid}"
            event = self.events.get(timeout=left)
            if event.get("row", {}).get("id") == rid:
                return event

    def close(self) -> None:
        self._stop.set()


def test_a_subscriber_receives_a_request_raised_and_decided(server):
    sub = _Subscription(server)
    try:
        rid = _raise(server, environment="dev")
        raised = sub.next_for(rid)
        assert raised["op"] == "INSERT"
        row = raised["row"]
        assert row["state"] == "pending"
        assert row["environment"] == "dev"
        assert row["request_type"] == "view"
        assert row["requested_by"] == "u-ann"
        assert row["decision"] is None

        reject = httpx.post(
            f"{server.base_url}/admin/creation-requests/{rid}/reject",
            json={"reason": "duplicate"},
            headers={"X-Provisa-Role": _ROLE, "Idempotency-Key": f"decide-{rid}"},
            timeout=server.request_timeout,
        )
        assert reject.status_code == 200, reject.text
        decided = sub.next_for(rid)
        assert decided["op"] == "UPDATE"
        assert decided["row"]["state"] == "rejected"
        assert decided["row"]["decision"] == "rejected"
        assert decided["row"]["decision_reason"] == "duplicate"
        assert decided["row"]["decided_at"] is not None

        # The workflow tool retries the same decision: answered with the first result, no change.
        again = httpx.post(
            f"{server.base_url}/admin/creation-requests/{rid}/reject",
            json={"reason": "duplicate"},
            headers={"X-Provisa-Role": _ROLE, "Idempotency-Key": f"decide-{rid}"},
            timeout=server.request_timeout,
        )
        assert again.status_code == 200, again.text
        assert again.json() == reject.json()
        # Without the key the same call is the conflict it always was.
        bare = httpx.post(
            f"{server.base_url}/admin/creation-requests/{rid}/reject",
            json={"reason": "duplicate"},
            headers={"X-Provisa-Role": _ROLE},
            timeout=server.request_timeout,
        )
        assert bare.status_code == 409, bare.text
    finally:
        sub.close()


def test_a_user_table_relates_to_ops_requests_by_request_id(server):
    rid = _raise(server)
    url = make_url(server.plane.url)  # type: ignore[union-attr]
    with _plane(server).begin() as conn:
        conn.execute(sa.text(f"DROP SCHEMA IF EXISTS {_EXT_SCHEMA} CASCADE"))
        conn.execute(sa.text(f"CREATE SCHEMA {_EXT_SCHEMA}"))
        conn.execute(
            sa.text(
                f"CREATE TABLE {_EXT_SCHEMA}.request_trail "
                "(id integer PRIMARY KEY, request_id integer NOT NULL, step text NOT NULL)"
            )
        )
        conn.execute(
            sa.text(f"INSERT INTO {_EXT_SCHEMA}.request_trail VALUES (1, :rid, 'manager_signed')"),
            {"rid": rid},
        )
    try:
        _admin(
            server,
            f'createSource(input: {{id: "process-pg", type: "postgresql", host: "{url.host}", '
            f'port: {url.port}, database: "{url.database}", username: "{url.username}", '
            f'password: "{url.password}"}})',
        )
        registered = _admin(
            server,
            'registerTable(input: {sourceId: "process-pg", domainId: "process", '
            f'schemaName: "{_EXT_SCHEMA}", tableName: "request_trail", columns: ['
            f'{{name: "id", visibleTo: ["{_ROLE}"], dataType: "integer", isPrimaryKey: true}}, '
            f'{{name: "request_id", visibleTo: ["{_ROLE}"], dataType: "integer"}}, '
            f'{{name: "step", visibleTo: ["{_ROLE}"], dataType: "text"}}]}})',
        )
        _admin(server, release_field(registered_id(registered["message"])))
        _admin(
            server,
            'createRelationship(input: {id: "trail-request", sourceTableId: "request_trail", '
            'targetTableId: "requests", sourceColumn: "request_id", targetColumn: "id", '
            'cardinality: "many-to-one", graphqlAlias: "request"})',
        )
        data = _graphql(
            server, "{ request_trail { id step request { id state environment requested_by } } }"
        )
        assert data["request_trail"] == [
            {
                "id": 1,
                "step": "manager_signed",
                "request": {
                    "id": rid,
                    "state": "pending",
                    "environment": "prod",
                    "requested_by": "u-ann",
                },
            }
        ]
    finally:
        with _plane(server).begin() as conn:
            conn.execute(sa.text(f"DROP SCHEMA IF EXISTS {_EXT_SCHEMA} CASCADE"))
