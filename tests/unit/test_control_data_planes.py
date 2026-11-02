# Copyright (c) 2026 Kenneth Stott
# Canary: 66218c7d-7ce0-4e62-89aa-52e4f2698b4e
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Control plane and data plane modes, and the plan facade between them (REQ-1916).

The facade's two sides are run for real: a control node's plan listener (an Arrow Flight server on
a loopback port) and a data node's call to it. The plan half of the pipeline is the in-process
planner with its governance replaced by a recorder, so what is checked is what crosses: the same
plan whichever side made it, made inside the identity the data node's surface bound, and refused
by name when the deployment key did not seal it or it is too old.
"""

# Requirements: REQ-1916

from __future__ import annotations

import base64
import dataclasses
import pickle  # nosec B403 - the test builds its own payloads
import time
from typing import Any, cast

import pytest

from provisa.audit.pipeline import PendingAudit
from provisa.core import process_mode, process_region
from provisa.core.statement_warnings import ServerWarning, collecting, warn
from provisa.pgwire import _pipeline
from provisa.pgwire._pipeline import _Plan, _mint_stamp, stamp_is_valid
from provisa.planes import facade, portable, signing
from provisa.transpiler.router import Route

_KEY = base64.b64encode(bytes(range(32))).decode()


@pytest.fixture(autouse=True)
def deployment_key(monkeypatch):
    monkeypatch.setenv("PROVISA_ENCRYPTION_KEY", _KEY)
    signing._key.cache_clear()
    yield
    signing._key.cache_clear()


@pytest.fixture
def mode():
    was = process_mode.mode()
    yield process_mode.set_mode
    process_mode.set_mode(was)


# --- the modes ---------------------------------------------------------------------------------


def test_the_three_modes_and_what_each_does(mode):
    assert process_mode.MODES == ("every", "control", "data")
    expected = {"every": (True, False), "control": (True, False), "data": (False, True)}
    for name, (background, remote) in expected.items():
        mode(name)
        assert (process_mode.runs_background_work(), process_mode.plans_remotely()) == (
            background,
            remote,
        ), name


def test_a_control_node_refuses_data_requests_and_a_data_node_serves_them(mode):
    mode(process_mode.CONTROL)
    with pytest.raises(process_mode.ControlServesNoData, match="control mode"):
        process_mode.refuse_data_request("pgwire")
    mode(process_mode.DATA)
    process_mode.refuse_data_request("pgwire")


def test_a_data_node_is_launched_with_its_control_node(monkeypatch, mode):
    monkeypatch.setenv("PROVISA_MODE", "data")
    monkeypatch.delenv("PROVISA_CONTROL", raising=False)
    with pytest.raises(process_region.LaunchRefused, match="--control <host:port>"):
        process_region.bind_mode_from_environment()
    monkeypatch.setenv("PROVISA_CONTROL", "ctl:8817")
    process_region.bind_mode_from_environment()
    assert process_mode.mode() == process_mode.DATA


def test_a_control_node_is_launched_with_its_plan_port(monkeypatch, mode):
    monkeypatch.setenv("PROVISA_MODE", "control")
    monkeypatch.delenv("PROVISA_PLANE_PORT", raising=False)
    with pytest.raises(process_region.LaunchRefused, match="--plane-port <port>"):
        process_region.bind_mode_from_environment()


@pytest.mark.parametrize("old", ["query", "coordinator"])
def test_the_old_modes_are_gone(monkeypatch, mode, old):
    monkeypatch.setenv("PROVISA_MODE", old)
    with pytest.raises(process_region.LaunchRefused, match=f"unknown process mode '{old}'"):
        process_region.bind_mode_from_environment()


# --- the seal ----------------------------------------------------------------------------------


def test_a_sealed_message_opens_to_its_payload():
    assert signing.unseal("plan", signing.seal("plan", b"governed")) == b"governed"


def test_an_altered_message_is_refused_as_forged():
    sealed = bytearray(signing.seal("plan", b"governed"))
    sealed[-1] ^= 1
    with pytest.raises(signing.PlanForged, match="plane plan refused as forged"):
        signing.unseal("plan", bytes(sealed))


def test_a_message_sealed_under_another_key_is_refused_as_forged():
    other = signing.seal("plan", b"governed", key=b"k" * 32)
    with pytest.raises(signing.PlanForged, match="does not verify under the deployment key"):
        signing.unseal("plan", other)


def test_a_message_of_one_kind_is_not_accepted_as_another():
    with pytest.raises(signing.PlanForged, match="it is a plan request, not a plan"):
        signing.unseal("plan", signing.seal("plan request", b"x"))


def test_an_old_message_is_refused_as_stale():
    sealed = signing.seal("plan", b"governed", now=time.time() - signing.MAX_AGE_S - 5)
    with pytest.raises(signing.PlanStale, match="plane plan refused as stale"):
        signing.unseal("plan", sealed)


def test_the_key_is_derived_from_the_deployment_master_key(monkeypatch):
    first = signing._key()
    monkeypatch.setenv("PROVISA_ENCRYPTION_KEY", base64.b64encode(b"z" * 32).decode())
    signing._key.cache_clear()
    assert signing._key() != first and len(first) == 32  # noqa: PLR2004 - SHA-256


# --- the plan crossing -------------------------------------------------------------------------


def _governed_plan(sql: str = "SELECT 1", role_id: str = "analyst") -> _Plan:
    return _Plan(
        route=Route.ENGINE,
        sql=f"/* governed for {role_id} */ {sql}",
        source_id=None,
        dialect="duckdb",
        physical_sql=f"{sql} LIMIT 1001",
        sources=frozenset({"sales"}),
        route_reason="cross-source",
        role_id=role_id,
        table_ids=(7,),
        stamp=_mint_stamp(),
        audit=PendingAudit(
            user_id="ada",
            surface="pgwire",
            role_id=role_id,
            query_text=sql,
            table_ids=lambda: (7,),
            started=time.monotonic(),
            model_stamp=42,
            enforced=lambda: {"rls": ["region = 'eu'"]},
        ),
        warnings=[ServerWarning("api.cut", "cut at max_pages")],
    )


async def test_a_plan_crosses_whole_and_is_admitted_with_this_nodes_stamp():
    plan = _governed_plan()
    arrived = portable.import_plan(await portable.export_plan(plan))
    assert stamp_is_valid(arrived.stamp) and arrived.stamp != plan.stamp
    assert (arrived.sql, arrived.physical_sql, arrived.route) == (
        plan.sql,
        plan.physical_sql,
        plan.route,
    )
    assert arrived.audit.model_stamp == 42  # noqa: PLR2004 - REQ-1914: the stamp it was governed under
    assert arrived.audit.table_ids == (7,) and arrived.audit.enforced == {"rls": ["region = 'eu'"]}


async def test_a_plan_not_sealed_by_the_deployment_key_is_never_run():
    sealed = await portable.export_plan(_governed_plan())
    payload = pickle.loads(signing.unseal(portable.PLAN, sealed))  # nosec B301
    payload.plan.sql = "SELECT * FROM salaries"
    forged = signing.seal(portable.PLAN, pickle.dumps(payload), key=b"x" * 32)
    with pytest.raises(signing.PlanForged):
        portable.import_plan(forged)


async def test_a_stale_plan_is_refused_by_name(monkeypatch):
    sealed = await portable.export_plan(_governed_plan())
    real = time.time
    monkeypatch.setattr(time, "time", lambda: real() + signing.MAX_AGE_S + 5)
    with pytest.raises(signing.PlanStale):
        portable.import_plan(sealed)


async def test_an_ungoverned_plan_is_never_exported():
    plan = dataclasses.replace(_governed_plan(), stamp=None)
    with pytest.raises(PermissionError, match="ungoverned plan rejected"):
        await portable.export_plan(plan)


async def test_a_plan_this_slice_does_not_carry_is_refused_by_name():
    plan = dataclasses.replace(_governed_plan(), temp=object())
    with pytest.raises(portable.PlanNotPortable, match="write a temporary table"):
        await portable.export_plan(plan)


# --- the facade, local and remote --------------------------------------------------------------


@pytest.fixture
def planner(monkeypatch):
    """The in-process planner with governance replaced by a recorder of what it was asked, and
    in what identity."""
    from provisa.api import app as app_module
    from provisa.audit.context import current_audit_identity
    from provisa.core.request_context import current_org, current_session_vars

    seen: list[dict] = []

    async def _planned(sql, role_id, **how):
        identity = current_audit_identity()
        seen.append(
            {
                "org": current_org.get(),
                "session_vars": dict(current_session_vars.get()),
                "user": identity.user_id if identity else None,
                "serve_cached": how.get("serve_cached"),
            }
        )
        warn(ServerWarning("api.cut", "cut at max_pages"))
        return dataclasses.replace(_governed_plan(sql, role_id), warnings=[])

    async def _nothing(*_a, **_k):
        return None

    async def _same(plan, _state):
        return plan

    monkeypatch.setattr(_pipeline, "_govern_and_route_planned", _planned)
    monkeypatch.setattr(_pipeline, "_wake_before_governing", _nothing)
    monkeypatch.setattr(_pipeline, "_attach_tier_caps", _same)
    monkeypatch.setattr(_pipeline, "_attach_live_caps", _same)
    monkeypatch.setattr(app_module, "ensure_serving_runtime", _nothing)
    return seen


@pytest.fixture
def control_node(monkeypatch):
    """A control node's plan listener on a loopback port, and this process asking it."""
    from provisa.planes.control_server import start

    server = start(0)
    monkeypatch.setenv(facade.CONTROL_ENV, f"127.0.0.1:{server.port}")
    monkeypatch.setattr(facade, "_client", None)
    yield server
    server.shutdown()
    facade._client = None


def _comparable(plan: _Plan) -> dict:
    fields = dataclasses.asdict(dataclasses.replace(plan, stamp=None, audit=None))
    fields.pop("warnings")
    return fields


async def test_local_and_remote_give_the_same_plan(planner, control_node, mode):
    from provisa.audit.context import audit_identity_scope
    from provisa.core.request_context import (
        reset_current_org,
        reset_session_vars,
        set_current_org,
        set_session_vars,
    )

    org, vars_ = set_current_org("acme"), set_session_vars({"x-provisa-region": "eu"})
    try:
        with audit_identity_scope("ada", "pgwire"):
            mode(process_mode.EVERY)
            with collecting() as local_warnings:
                local = await _pipeline._govern_and_route("SELECT 1", "analyst", serve_cached=True)
            mode(process_mode.DATA)
            with collecting() as remote_warnings:
                remote = await _pipeline._govern_and_route("SELECT 1", "analyst", serve_cached=True)
    finally:
        reset_session_vars(vars_)
        reset_current_org(org)
    assert _comparable(remote) == _comparable(local)
    assert stamp_is_valid(remote.stamp)
    assert [w.code for w in remote_warnings] == [w.code for w in local_warnings] == ["api.cut"]
    local_seen, remote_seen = planner
    # The control node governed in the identity the data node's surface bound ...
    assert {k: remote_seen[k] for k in ("org", "session_vars", "user")} == {
        k: local_seen[k] for k in ("org", "session_vars", "user")
    }
    assert remote_seen["user"] == "ada" and remote_seen["org"] == "acme"
    # ... and, for a data node, never read the response cache: a hit would be rows.
    assert (local_seen["serve_cached"], remote_seen["serve_cached"]) == (True, False)


async def test_a_forged_request_is_refused_by_name_by_the_control_node(control_node):
    from pyarrow import flight

    client = flight.FlightClient(f"grpc://127.0.0.1:{control_node.port}")
    request = signing.seal(portable.REQUEST, pickle.dumps(("ping", {}, (), {})), key=b"q" * 32)
    (result,) = client.do_action(flight.Action("ping", request))
    ok, error = pickle.loads(signing.unseal(facade.ANSWER, result.body.to_pybytes()))  # nosec
    assert not ok and isinstance(error, signing.PlanForged)


async def test_a_data_node_without_its_control_node_fails_loudly(monkeypatch, mode):
    monkeypatch.setenv(facade.CONTROL_ENV, "127.0.0.1:1")
    monkeypatch.setattr(facade, "_client", None)
    mode(process_mode.DATA)
    try:
        with pytest.raises(facade.ControlUnreachable, match="cannot reach its control node"):
            await _pipeline._govern_and_route("SELECT 1", "analyst")
    finally:
        facade._client = None


async def test_the_launch_check_refuses_a_data_node_whose_control_node_is_down(monkeypatch):
    monkeypatch.setenv(facade.CONTROL_ENV, "127.0.0.1:1")
    monkeypatch.setattr(facade, "_client", None)
    try:
        with pytest.raises(process_region.LaunchRefused, match="cannot reach its control node"):
            facade.require_control()
    finally:
        facade._client = None


def test_the_launch_check_passes_against_a_live_control_node(control_node):
    facade.require_control()


# --- counters back -----------------------------------------------------------------------------


def test_a_data_node_reports_facts_never_rows(monkeypatch):
    from datetime import datetime, timezone

    from provisa.audit.writer import AuditRecord
    from provisa.planes import audit_report

    sent: list[list[dict]] = []
    monkeypatch.setattr(facade, "report", sent.append)
    writer = audit_report.RemoteAuditWriter().start()
    record = AuditRecord(
        record_db=object(),
        tenant_id="acme",
        user_id="ada",
        role_id="analyst",
        query_text="SELECT 1",
        table_ids=lambda: (7,),
        source="pgwire",
        status_code=200,
        duration_ms=3,
        logged_at=datetime.now(timezone.utc),
        trace_id=None,
        encryption=cast(Any, object()),
        route="virtual",
        row_count=1000,
        model_stamp=42,
        model_env="prod",
        enforced=lambda: {"row_limit": "cut"},
        route_reason="cross-source",
        sources=("sales",),
        data_age=None,
        region="default",
        time_travel=None,
        tables=[],
        used_roles=lambda: ("analyst",),
    )
    writer.enqueue(record)
    assert writer.flush(5.0)
    writer.close(1.0)
    ((facts,),) = sent
    assert facts["row_count"] == 1000 and facts["model_stamp"] == 42  # noqa: PLR2004
    assert facts["table_ids"] == (7,) and facts["enforced"] == {"row_limit": "cut"}
    assert facts["used_roles"] == ("analyst",)
    assert not {"record_db", "encryption", "tables", "meter_pool", "hot_counts"} & set(facts)


def test_the_key_is_required_to_seal(monkeypatch):
    monkeypatch.delenv("PROVISA_ENCRYPTION_KEY")
    monkeypatch.setattr("provisa.encryption.providers._load_from_keychain", lambda _key_id: None)
    signing._key.cache_clear()
    with pytest.raises(RuntimeError, match="no deployment master key"):
        signing.seal("plan", b"x")


@pytest.mark.parametrize(
    "error",
    [
        signing.PlanForged("plan", "its seal does not verify"),
        signing.PlanStale("plan", 75.0),
        portable.PlanNotPortable("embed texts after the read"),
        facade.ControlUnreachable("ctl:8817", "refused"),
        facade.RemoteRefusal("ApprovalDenied", "steward said no"),
        process_mode.ControlServesNoData("pgwire"),
    ],
)
def test_a_refusal_crosses_the_socket_as_itself(error):
    ok, crossed = pickle.loads(  # nosec B301
        signing.unseal(facade.ANSWER, facade.seal_answer(False, error))
    )
    assert not ok and type(crossed) is type(error) and str(crossed) == str(error)
