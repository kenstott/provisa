# Copyright (c) 2026 Kenneth Stott
# Canary: 6d1e8b37-4f92-4a05-b7c3-9e2a5c8d0f41
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The post-trial license notice never fails a request (REQ-1137), on a real server.

A server whose trial is pinned past expiry answers SQL over HTTP, GraphQL, REST and the admin API
with 200, each response carrying the notice header; a server pinned before expiry carries none."""

from __future__ import annotations

import datetime
import json
import os
import urllib.request

import pytest

from tests.integration.worker_boot_harness import WorkerBoot

pytestmark = [pytest.mark.integration]

_PG_HOST = os.environ.get("PG_HOST", "localhost")
_PG_PORT = int(os.environ.get("PG_PORT", "5432"))


def _boot(first_seen: datetime.date):
    boot = WorkerBoot(
        1,
        pg_host=_PG_HOST,
        pg_port=_PG_PORT,
        env={"PROVISA_LICENSING_FIRST_SEEN": first_seen.isoformat()},
    )
    boot.create_database()
    boot.start()
    boot.wait_all_ready(timeout=300)
    return boot


@pytest.fixture(scope="module")
def expired():
    boot = _boot(datetime.date.today() - datetime.timedelta(days=60))
    try:
        yield boot
    finally:
        boot.cleanup()


@pytest.fixture(scope="module")
def in_trial():
    boot = _boot(datetime.date.today())
    try:
        yield boot
    finally:
        boot.cleanup()


def _call(boot, method: str, path: str, body: dict | None = None):
    req = urllib.request.Request(
        f"http://127.0.0.1:{boot.ports['http']}{path}",
        data=None if body is None else json.dumps(body).encode(),
        headers={"Content-Type": "application/json", "x-provisa-role": "org_admin"},
        method=method,
    )
    with urllib.request.urlopen(req, timeout=120) as resp:
        return resp.status, resp.headers.get("x-provisa-license-notice"), resp.read().decode()


_REQUESTS = {
    "sql": ("POST", "/data/sql", {"sql": "SELECT id FROM sales.orders ORDER BY id"}),
    "graphql": ("POST", "/data/graphql", {"query": "{ s__orders { id } }"}),
    "rest": ("GET", "/data/rest/sales/orders", None),
    "admin": ("POST", "/admin/graphql", {"query": "{ __typename }"}),
}


@pytest.mark.parametrize("surface", sorted(_REQUESTS))
def test_past_the_trial_every_request_answers_200_with_the_notice(expired, surface):
    status, notice, body = _call(expired, *_REQUESTS[surface])
    assert status == 200, body
    assert notice is not None and "trial period has elapsed" in notice, notice
    assert "error" not in json.loads(body), body


@pytest.mark.parametrize("surface", ["sql", "graphql"])
def test_within_the_trial_no_notice_is_sent(in_trial, surface):
    status, notice, body = _call(in_trial, *_REQUESTS[surface])
    assert status == 200, body
    assert notice is None
