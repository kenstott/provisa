# Copyright (c) 2026 Kenneth Stott
# Canary: 3d1f6a82-95c4-4e07-b2a8-7c0e4f1d9b35
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""REQ-2133: service calls from a preflight are metered and audited as for a code source. The real
:func:`provisa.codesource.services.answer` runs under the grant the preflight binds; only the
org's database and the dq read are stood in for."""

# Requirements: REQ-2133

from __future__ import annotations

from contextlib import asynccontextmanager, contextmanager
from types import SimpleNamespace

import pytest

from provisa.codesource import service_ops, services as services_mod
from provisa.core.request_context import reset_current_org, set_current_org
from provisa.events.processor import NodeContext
from provisa.mv import preflight_services
from provisa.mv.preflight import Decision, run_preflight

pytestmark = pytest.mark.unit

_SUBJECT = "view orders_daily"
_CHECK = (
    "import provisa_services as ps\n"
    "def preflight(streams, ctx):\n"
    "    ps.audit('preflight.checked', rows=3)\n"
    "    ps.metric('preflight.runs', 1)\n"
    "    ps.dq.dataset(7)\n"
    "    return ctx.ok()\n"
)


def _ctx() -> NodeContext:
    return NodeContext(node="view-orders_daily", kind="mv", claimed=[], prior_hash=None)


@pytest.fixture
def world(monkeypatch):
    rows: list[dict] = []
    spans: list[dict] = []
    points: list[dict] = []
    seen_grants: list = []

    class Conn:
        async def execute_core(self, stmt):
            rows.append(dict(stmt.compile().params))

    class Db:
        model = None

        @asynccontextmanager
        async def acquire(self):
            yield Conn()

    @asynccontextmanager
    async def bound(grant):
        yield Db(), {}

    monkeypatch.setattr("provisa.codesource.llm_service.org_bound", bound)

    @contextmanager
    def recording_stage(_tracer, name, **_kw):
        span = SimpleNamespace(attrs={}, name=name)
        span.set_attribute = lambda k, v: span.attrs.__setitem__(k, v)
        spans.append({"name": name, "attrs": span.attrs})
        yield span

    monkeypatch.setattr(services_mod, "stage", recording_stage)

    class Instrument:
        def add(self, value, point):
            points.append({"value": value, **point})

    monkeypatch.setattr(service_ops, "_instrument", lambda kind, name: Instrument())

    async def dq_answer(grant, role, method, sub, span):
        seen_grants.append(grant)
        return b"{}"

    monkeypatch.setattr("provisa.dq.service.answer", dq_answer)
    monkeypatch.setattr(
        "provisa.api.app.state",
        SimpleNamespace(admin_db=object(), multitenancy=False),
        raising=False,
    )
    token = set_current_org("acme")
    yield SimpleNamespace(rows=rows, spans=spans, points=points, grants=seen_grants)
    reset_current_org(token)


@pytest.mark.asyncio
async def test_every_service_call_is_a_metered_stage_naming_the_view(world):
    check = preflight_services.compile_check(_CHECK, subject=_SUBJECT)
    verdict = await run_preflight(check, {}, _ctx())
    assert verdict.decision is Decision.CONTINUE
    served = {s["name"]: s["attrs"] for s in world.spans}
    assert set(served) == {
        "codesource.service.audit",
        "codesource.service.metric",
        "codesource.service.dq",
    }
    assert all(a["codesource.source"] == _SUBJECT for a in served.values())


@pytest.mark.asyncio
async def test_a_metric_point_carries_the_view_and_the_org(world):
    check = preflight_services.compile_check(_CHECK, subject=_SUBJECT)
    await run_preflight(check, {}, _ctx())
    (point,) = world.points
    assert point["value"] == 1
    assert point["provisa.codesource.source"] == _SUBJECT


@pytest.mark.asyncio
async def test_an_audit_event_is_a_row_in_the_orgs_trail_as_the_view(world):
    check = preflight_services.compile_check(_CHECK, subject=_SUBJECT)
    await run_preflight(check, {}, _ctx())
    (row,) = world.rows
    assert row["action"] == service_ops.CODE_AUDIT
    assert row["org_id"] == "acme"
    assert row["actor_id"] == f"source:{_SUBJECT}" and row["subject_id"] == "preflight.checked"
    assert row["detail"]["source"] == _SUBJECT and row["detail"]["fields"] == {"rows": 3}


@pytest.mark.asyncio
async def test_calls_run_under_the_views_org_grant_which_is_revoked_after(world):
    check = preflight_services.compile_check(_CHECK, subject=_SUBJECT, product_ids=("p1",))
    await run_preflight(check, {}, _ctx())
    (grant,) = world.grants
    assert grant.org_id == "acme" and grant.source_id == _SUBJECT
    assert grant.product_ids == ("p1",)
    assert services_mod._grants == {} or grant not in services_mod._grants.values()


@pytest.mark.asyncio
async def test_a_call_after_the_check_returned_is_refused_and_unrecorded(world):
    check = preflight_services.compile_check(_CHECK, subject=_SUBJECT)
    await run_preflight(check, {}, _ctx())
    before = len(world.rows)
    status, _kind, _body = services_mod.answer("POST", "/gone-token/audit", b"{}")
    assert status == 401 and len(world.rows) == before
