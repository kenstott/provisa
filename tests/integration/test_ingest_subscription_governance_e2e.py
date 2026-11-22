# Copyright (c) 2026 Kenneth Stott
# Canary: ce6e0b64-1f97-4987-8769-2601a554c038
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""An ingest table's SSE subscription is governed per subscriber -- on a real server (REQ-336).

One server with an ingest source ``pushed`` and its table ``events(ext_id, region, email)``.
``east_reader`` reads only the east rows (a row rule) and every reader but ``org_admin`` reads
``email`` as ``***`` (a mask). Both subscribe to ``GET /data/subscribe/events``; rows are then
pushed through ``POST /data/ingest/pushed/events``. The ingest provider polls the table's
``_updated_at`` watermark and each new row is read back as the subscriber: ``east_reader`` is
delivered the east row only, masked; ``org_admin`` is delivered both, clear.

Lands on the TEST instance only: one real server over a database the harness creates."""

from __future__ import annotations

import json
import os
import threading
import time

import httpx
import pytest

from tests.integration.worker_boot_harness import WorkerBoot, _config

pytestmark = [pytest.mark.integration]

_PG_HOST = os.environ.get("PG_HOST", "localhost")
_PG_PORT = int(os.environ.get("PG_PORT", "5432"))
_ADMIN, _EAST = "org_admin", "east_reader"
_ROLES = [_ADMIN, "analyst", _EAST]
_MASK = "***"
_PUSHED = [
    {"ext_id": "e1", "region": "east", "email": "east@x"},
    {"ext_id": "w1", "region": "west", "email": "west@x"},
]
# The provider polls every 5 s; a pushed row reaches a subscriber well within this.
_DELIVERY_S = 60.0


@pytest.fixture(scope="module")
def server():
    base = _config(_PG_HOST, _PG_PORT, "unused")
    boot = WorkerBoot(
        1,
        pg_host=_PG_HOST,
        pg_port=_PG_PORT,
        extra_config={
            "sources": base["sources"] + [{"id": "pushed", "type": "ingest"}],
            "tables": [
                {
                    "source_id": "pushed",
                    "domain_id": "sales",
                    # The org's control-plane schema, where ingest writes (REQ-1771).
                    "schema": "org_default",
                    "table": "events",
                    "columns": [
                        {
                            "name": "ext_id",
                            "data_type": "text",
                            "visible_to": _ROLES,
                            "is_primary_key": True,
                        },
                        {"name": "region", "data_type": "text", "visible_to": _ROLES},
                        {
                            "name": "email",
                            "data_type": "text",
                            "visible_to": _ROLES,
                            "masks": [
                                {"to": [_ADMIN], "mask": None},
                                {"mask": {"type": "constant", "value": _MASK}},
                            ],
                        },
                    ],
                },
            ],
            "roles": [
                {
                    "id": _EAST,
                    "description": "Test role.",
                    "capabilities": ["query_development", "full_results"],
                    "domain_access": ["*"],
                },
            ],
            "rls_rules": [
                {
                    "description": "Test rule.",
                    "table_id": "events",
                    "role_id": _EAST,
                    "filter": "region = 'east'",
                }
            ],
        },
        env={"PROVISA_REDIRECT_ENABLED": "false"},
    )
    boot.create_database()
    try:
        boot.start()
        boot.wait_all_ready(timeout=300)
        yield boot
    finally:
        boot.cleanup()


class _Subscriber(threading.Thread):
    """One subscription to ``events`` as ``role``, collecting each delivered change until it has
    ``want`` of them or the delivery window ends."""

    def __init__(self, boot, role: str, want: int) -> None:
        super().__init__(daemon=True)
        self.url = f"http://127.0.0.1:{boot.ports['http']}/data/subscribe/events"
        self.role = role
        self.want = want
        self.connected = threading.Event()
        self.status: int | None = None
        self.events: list[dict] = []
        self.error: BaseException | None = None

    def run(self) -> None:
        deadline = time.monotonic() + _DELIVERY_S
        try:
            with httpx.Client(timeout=httpx.Timeout(30.0, read=30.0)) as client:
                with client.stream(
                    "GET",
                    self.url,
                    headers={"X-Provisa-Role": self.role, "Accept": "text/event-stream"},
                ) as resp:
                    self.status = resp.status_code
                    if resp.status_code != 200:
                        self.connected.set()
                        return
                    for line in resp.iter_lines():
                        if line == ": connected":
                            self.connected.set()
                        elif line.startswith("data: "):
                            self.events.append(json.loads(line[len("data: ") :]))
                        if len(self.events) >= self.want or time.monotonic() > deadline:
                            return
        except BaseException as exc:  # reported by the test that reads it
            self.error = exc
            self.connected.set()


def _push(boot, rows: list[dict]) -> None:
    resp = httpx.post(
        f"http://127.0.0.1:{boot.ports['http']}/data/ingest/pushed/events",
        json=rows,
        headers={"X-Provisa-Role": _ADMIN},
        timeout=120,
    )
    assert resp.status_code == 202, resp.text


def test_each_subscriber_is_delivered_the_pushed_rows_as_its_role_reads_them(server):
    admin = _Subscriber(server, _ADMIN, want=2)
    east = _Subscriber(server, _EAST, want=1)
    for sub in (admin, east):
        sub.start()
        assert sub.connected.wait(60), f"{sub.role}'s subscription never opened"
        assert sub.error is None, sub.error
        assert sub.status == 200, sub.status

    # The west row first, and only once the admin has been delivered it the east row: were the
    # west row delivered to east_reader it would be the first change east_reader sees.
    west, east_row = _PUSHED[1], _PUSHED[0]
    _push(server, [west])
    deadline = time.monotonic() + _DELIVERY_S
    while not admin.events:
        assert admin.is_alive() and time.monotonic() < deadline, (admin.error, admin.events)
        time.sleep(0.5)
    _push(server, [east_row])

    for sub in (admin, east):
        sub.join(_DELIVERY_S + 30)
        assert sub.error is None, sub.error

    assert [(e["op"], e["row"]["ext_id"], e["row"]["email"]) for e in admin.events] == [
        ("INSERT", "w1", "west@x"),
        ("INSERT", "e1", "east@x"),
    ], admin.events

    # The row rule keeps the west row from east_reader; the mask is applied to what it reads.
    assert [(e["op"], e["row"]["ext_id"], e["row"]["email"]) for e in east.events] == [
        ("INSERT", "e1", _MASK)
    ], east.events
