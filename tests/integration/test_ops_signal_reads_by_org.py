# Copyright (c) 2026 Kenneth Stott
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Each org reads its own ops signals, on Trino.

The Trino engine's ``otel`` catalog is one store every org's spans land in. Two servers, one org
each, share it (as one deployment's orgs do); a span row of each org, and one of no org, are written
to ``otel.signals.traces`` the way the compaction writes them. Each org's ``"ops"."traces"`` answers
its own row only, and the row of no org is answered to neither. The platform administrator,
control-plane only, reads none of them."""

# Requirements: REQ-302, REQ-1349

from __future__ import annotations

import os
import uuid

import httpx
import pytest
import trino

from tests.integration.isolated_server import IsolatedServer

pytestmark = [pytest.mark.integration, pytest.mark.requires_trino]

_TRINO_HOST = os.environ.get("TRINO_HOST", "localhost")
_TRINO_PORT = int(os.environ.get("TRINO_PORT", "8080"))
_RUN = uuid.uuid4().hex[:10]
_ORGS = (f"opsa{_RUN[:6]}", f"opsb{_RUN[:6]}")
_TRACES = {_ORGS[0]: f"a-{_RUN}", _ORGS[1]: f"b-{_RUN}", None: f"n-{_RUN}"}


def _trino(sql: str) -> None:
    cur = trino.dbapi.connect(host=_TRINO_HOST, port=_TRINO_PORT, user="itest").cursor()
    cur.execute(sql)
    cur.fetchall()


@pytest.fixture(scope="module")
def servers():
    started = [IsolatedServer(org, engine="trino") for org in _ORGS]
    try:
        for srv in started:
            srv.start(timeout=600)  # its boot creates otel.signals.* (ops_trino)
        yield dict(zip(_ORGS, started))
    finally:
        for srv in started:
            srv.stop_process()


@pytest.fixture(scope="module")
def spans(servers):
    rows = ", ".join(
        f"('{trace}', 'work', '{{}}', {'NULL' if org is None else repr(org)}, CURRENT_DATE)"
        for org, trace in _TRACES.items()
    )
    _trino(
        "INSERT INTO otel.signals.traces (trace_id, span_name, span_attributes, tenant_id, _date) "
        f"VALUES {rows}"
    )
    try:
        yield
    finally:
        listed = ", ".join(f"'{t}'" for t in _TRACES.values())
        _trino(f"DELETE FROM otel.signals.traces WHERE trace_id IN ({listed})")


def _traces_read_by(server: IsolatedServer) -> set[str]:
    listed = ", ".join(f"'{t}'" for t in _TRACES.values())
    resp = httpx.post(
        f"{server.base_url}/data/sql",
        json={"sql": f'SELECT trace_id FROM "ops"."traces" WHERE trace_id IN ({listed})'},
        headers={"X-Provisa-Role": "org_admin"},
        timeout=300,
    )
    assert resp.status_code == 200, resp.text
    return {r["trace_id"] for r in resp.json()["data"]["sql"]}


@pytest.mark.parametrize("org", _ORGS)
def test_each_org_reads_only_its_own_spans(servers, spans, org):
    assert _traces_read_by(servers[org]) == {_TRACES[org]}


@pytest.mark.parametrize("org", _ORGS)
def test_the_platform_administrator_reads_no_orgs_spans(servers, spans, org):
    # platform_admin is control-plane only: it holds no data surface in any org (REQ-1327), so
    # its read of the signals is refused and names none of them.
    listed = ", ".join(f"'{t}'" for t in _TRACES.values())
    resp = httpx.post(
        f"{servers[org].base_url}/data/sql",
        json={"sql": f'SELECT trace_id FROM "ops"."traces" WHERE trace_id IN ({listed})'},
        headers={"X-Provisa-Role": "platform_admin"},
        timeout=300,
    )
    assert resp.status_code >= 400, resp.text
    assert not any(t in resp.text for t in _TRACES.values()), resp.text
