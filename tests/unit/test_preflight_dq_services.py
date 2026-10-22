# Copyright (c) 2026 Kenneth Stott
# Canary: 4f1a8d63-0c2e-4b97-a5d1-9e7c3b2f6a80
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A preflight calls Provisa's services, dq among them, in-process; a view may hold on failing checks
(REQ-2133, REQ-1165 and REQ-957/964 amended 2026-10-21)."""

# Requirements: REQ-2133, REQ-1165, REQ-957, REQ-964

from __future__ import annotations

import json
from types import SimpleNamespace

import pytest

from provisa.codesource import services as services_mod
from provisa.codesource.services import ServiceRefused
from provisa.dq import service as dq
from provisa.events.processor import NodeContext
from provisa.mv import hold_checks, preflight_eval, preflight_services
from provisa.mv.preflight import Decision, run_preflight
from provisa.mv.preflight_sql import translate
from provisa.mv.preprocess import (
    check_preprocess_purity,
    compile_preprocess,
    uses_services,
)

_CHECK = (
    "import provisa_services as ps\n"
    "def preflight(streams, ctx):\n"
    "    found = ps.dq.dataset(7)\n"
    "    if found['checker_tables']:\n"
    "        return ctx.abort(found['dataset'])\n"
    "    return ctx.ok()\n"
)


def _ctx(**over) -> NodeContext:
    return NodeContext(node="view-orders_daily", kind="mv", claimed=[], prior_hash=None, **over)


# -- the one import -------------------------------------------------------------------------------


@pytest.mark.parametrize(
    "source",
    [
        _CHECK,
        "def preflight(streams, ctx):\n    import provisa_services\n    return ctx.ok()",
        "import provisa_services\ndef preflight(streams, ctx):\n    return ctx.ok()",
    ],
)
def test_importing_provisa_services_alone_is_admitted(source):
    ok, reason = check_preprocess_purity(source)
    assert ok, reason
    assert uses_services(source)


@pytest.mark.parametrize(
    "source",
    [
        "import provisa_services, os\ndef preflight(streams, ctx):\n    return ctx.ok()",
        "from provisa_services import dq\ndef preflight(streams, ctx):\n    return ctx.ok()",
        "def preflight(streams, ctx):\n    import json\n    return ctx.ok()",
    ],
)
def test_every_other_import_is_still_refused(source):
    ok, _reason = check_preprocess_purity(source)
    assert not ok


def test_a_check_that_calls_services_is_never_an_sql_probe():
    shaped = (
        "import provisa_services\n"
        "def preflight(streams, ctx):\n"
        "    if any(r['amount'] < 0 for r in streams['orders']):\n"
        "        return ctx.abort('negative')\n"
        "    return ctx.ok()\n"
    )
    assert translate(shaped) is None
    assert not uses_services(shaped.split("\n", 1)[1])


# -- the services, bound in-process ---------------------------------------------------------------


@pytest.fixture
def answered(monkeypatch):
    """Provisa's services as the check reaches them: each call recorded with its grant, answered by
    ``reply`` (dq's answer for a path)."""
    from provisa.core.request_context import reset_current_org, set_current_org

    seen: list[dict] = []
    replies: dict[str, dict] = {}

    def answer(method, path, body, headers):
        token, _, rest = path.lstrip("/").partition("/")
        grant = services_mod._grant(token)
        seen.append({"method": method, "path": rest, "role": grant.role, "grant": grant})
        return 200, "application/json", json.dumps(replies[rest]).encode()

    monkeypatch.setattr(services_mod, "answer", answer)
    monkeypatch.setattr("provisa.api.app.state", SimpleNamespace(admin_db=object()), raising=False)
    token = set_current_org("acme")
    yield SimpleNamespace(seen=seen, replies=replies)
    reset_current_org(token)


@pytest.mark.asyncio
async def test_a_check_reaches_dq_as_the_builds_role_under_a_grant_revoked_after(answered):
    from provisa.core import region_admin

    answered.replies["dq/dataset/7"] = {"dataset": "sales.orders", "checker_tables": [{}]}
    check = preflight_services.compile_check(_CHECK, subject="view orders_daily", product_id="p1")
    verdict = await run_preflight(check, {}, _ctx())
    assert verdict.decision is Decision.ABORT and verdict.reason == "sales.orders"
    (call,) = answered.seen
    assert call["method"] == "GET" and call["role"] == region_admin.ROLE
    assert call["grant"].org_id == "acme" and call["grant"].product_id == "p1"
    assert call["grant"].source_id == "view orders_daily"
    assert services_mod._grants == {} or call["grant"] not in services_mod._grants.values()
    assert preflight_services.services_module()._transport.get() is None


def test_a_check_that_calls_no_service_is_compiled_as_before():
    plain = "def preflight(streams, ctx):\n    return ctx.ok()"
    check = preflight_services.compile_check(plain, subject="view v")
    assert check is not None and check.__name__ == "preflight"


def test_outside_provisa_the_client_still_needs_its_address(monkeypatch):
    client = preflight_services.services_module()
    monkeypatch.delenv("PROVISA_SERVICES", raising=False)
    with pytest.raises(client.ServiceError, match="PROVISA_SERVICES"):
        client.dq.dataset("sales.orders")


# -- dq's answers ---------------------------------------------------------------------------------


@pytest.fixture
def meta(monkeypatch):
    """The meta domain as a role reads it: each governed read answered by the first entry whose
    text is in its SQL."""
    reads: list[tuple[str, str]] = []
    tables: list[tuple[str, list[dict]]] = []

    async def read(sql, role):
        reads.append((sql, role))
        for needle, rows in tables:
            if needle in sql:
                return rows
        return []

    monkeypatch.setattr(dq, "_read", read)
    return SimpleNamespace(reads=reads, tables=tables)


def _checker(id_, name, *, ran=True, product=None):
    return {
        "id": id_,
        "domain_id": "dq",
        "schema_name": "dq",
        "table_name": name,
        "alias": name,
        "description": f"{name} checks",
        "dq_contract": "checks for orders: []",
        "product_id": product,
        "latest_outcome": "fail" if ran else None,
        "latest_run_at": "2026-10-21 10:00:00" if ran else None,
    }


@pytest.mark.asyncio
async def test_a_dataset_answers_its_checkers_their_latest_runs_and_its_profiles(meta):
    meta.tables += [
        ("WHERE domain_id = 'sales'", [
            {"id": 7, "domain_id": "sales", "schema_name": "sales", "table_name": "orders", "alias": "orders"}
        ]),
        ("FROM meta.product_checks", [{"checker_table_id": 11, "state": "expectation"}]),
        ("checked_table_id = 7", [_checker(11, "orders_soda"), _checker(12, "orders_gx", ran=False)]),
        ("dq.orders_soda WHERE outcome <> 'pass'", [{"check_name": "row_count > 0", "outcome": "fail"}]),
        ("profiled_table_id = 7", [
            {"id": 21, "domain_id": "p", "schema_name": "p", "table_name": "orders_7_profile_columns", "alias": None}
        ]),
        ("p.orders_7_profile_columns WHERE run_time", [{"run_id": "r9", "column": "amount", "null_share": 0.0}]),
    ]  # fmt: skip
    found = await dq.dataset("sales.orders", "analyst", view_product="p1")
    assert found["dataset"] == "sales.orders" and found["dataset_id"] == 7
    soda, gx = found["checker_tables"]
    assert soda["latest_run"]["outcome"] == "fail"
    assert soda["latest_run"]["failing_checks"] == [
        {"check_name": "row_count > 0", "outcome": "fail"}
    ]
    assert soda["relation_to_view_product"] == "expectation"
    assert gx["latest_run"] is None and gx["relation_to_view_product"] is None
    (profile,) = found["profiles"]
    assert profile["profile_table"] == "p.orders_7_profile_columns"
    assert profile["latest_run"][0]["run_id"] == "r9"
    assert {role for _sql, role in meta.reads} == {"analyst"}


@pytest.mark.asyncio
async def test_a_dataset_the_role_cannot_see_is_none(meta):
    assert await dq.dataset("sales.secret", "analyst") is None


@pytest.mark.asyncio
async def test_a_product_answers_each_check_by_how_it_shows_it(meta):
    meta.tables += [
        ("FROM meta.data_products", [{"id": "p1"}]),
        ("FROM meta.product_checks", [
            {"checker_table_id": 11, "state": "promise"},
            {"checker_table_id": 12, "state": "orphan"},
        ]),
        ("WHERE id IN (11, 12)", [_checker(11, "out_soda"), _checker(12, "stale_soda", ran=False)]),
    ]  # fmt: skip
    found = await dq.product("p1", "steward")
    assert [c["checker_table"] for c in found["promise"]] == ["dq.out_soda"]
    assert [c["checker_table"] for c in found["orphan"]] == ["dq.stale_soda"]
    assert found["expectation"] == [] and found["partner_dq"] == []


@pytest.mark.asyncio
async def test_dq_is_read_only_answers_a_role_and_names_what_it_cannot_find(meta):
    from provisa.core.request_context import reset_current_org, set_current_org

    grant = SimpleNamespace(org_id="acme", product_id=None)
    span = SimpleNamespace(set_attribute=lambda *a: None)
    token = set_current_org("acme")
    try:
        with pytest.raises(ServiceRefused) as no_role:
            await dq.answer(grant, None, "GET", "dataset/sales.orders", span)
        assert no_role.value.status == 403
        with pytest.raises(ServiceRefused) as wrong_method:
            await dq.answer(grant, "analyst", "POST", "dataset/sales.orders", span)
        assert wrong_method.value.status == 405
        with pytest.raises(ServiceRefused) as missing:
            await dq.answer(grant, "analyst", "GET", "dataset/sales.orders", span)
        assert missing.value.status == 404 and "sales.orders" in missing.value.reason
    finally:
        reset_current_org(token)


# -- Hold on failing checks -----------------------------------------------------------------------


def _dataset(name, *checks):
    return {
        "dataset": name,
        "checker_tables": [
            {"checker_table": c, "latest_run": None if o is None else {"outcome": o}}
            for c, o in checks
        ],
    }


@pytest.mark.asyncio
async def test_hold_aborts_naming_each_failing_checker_and_its_input(answered):
    answered.replies["dq/dataset/1"] = _dataset("sales.orders", ("dq.orders_soda", "fail"))
    answered.replies["dq/dataset/2"] = _dataset(
        "crm.customers", ("dq.customers_gx", "pass"), ("dq.customers_new", None)
    )
    check = preflight_services.compile_check(hold_checks.SOURCE, subject="view v")
    verdict = await run_preflight(check, {}, _ctx(nearest_inputs=(1, 2)))
    assert verdict.decision is Decision.ABORT
    assert verdict.reason == "a check on its inputs failed: dq.orders_soda failed on sales.orders"


@pytest.mark.asyncio
async def test_hold_continues_when_every_check_passed_or_never_ran(answered):
    answered.replies["dq/dataset/2"] = _dataset(
        "crm.customers", ("dq.customers_gx", "pass"), ("dq.customers_new", None)
    )
    check = preflight_services.compile_check(hold_checks.SOURCE, subject="view v")
    verdict = await run_preflight(check, {}, _ctx(nearest_inputs=(2,)))
    assert verdict.decision is Decision.CONTINUE


@pytest.mark.asyncio
async def test_hold_and_the_views_own_check_both_must_continue(answered, monkeypatch):
    async def inputs(table_id):
        return (1,)

    monkeypatch.setattr(hold_checks, "nearest_inputs", inputs)
    own_aborts = "def preflight(streams, ctx):\n    return ctx.abort('own check')"

    answered.replies["dq/dataset/1"] = _dataset("sales.orders", ("dq.orders_soda", "fail"))
    held = await preflight_eval.evaluate_streams(
        None, own_aborts, [], _ctx(), hold=True, table_id=5
    )
    assert held is not None and "dq.orders_soda failed on sales.orders" in (held.reason or "")

    answered.replies["dq/dataset/1"] = _dataset("sales.orders", ("dq.orders_soda", "pass"))
    alone = await preflight_eval.evaluate_streams(None, None, [], _ctx(), hold=True, table_id=5)
    assert alone is not None and alone.decision is Decision.CONTINUE


@pytest.mark.asyncio
async def test_hold_on_a_view_that_is_no_table_is_refused_by_name():
    with pytest.raises(ValueError, match="no registered view table"):
        await preflight_eval.evaluate_streams(None, None, [], _ctx(), hold=True, table_id=None)


def test_the_option_shows_code_a_steward_can_copy():
    ok, reason = check_preprocess_purity(hold_checks.SOURCE)
    assert ok, reason
    assert compile_preprocess(hold_checks.SOURCE) is not None
