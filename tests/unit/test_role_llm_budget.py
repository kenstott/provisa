# Copyright (c) 2026 Kenneth Stott
# Canary: 9632894c-decb-4401-86ca-71011ff742ca
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A role's LLM budget (REQ-1174, amended 2026-10-27): a per-call cap, and rolling windows of an
hour, a day, a week and a month counted per org and role in the control plane -- per-minute,
per-hour and per-day buckets, each window a sum over them -- held to the most generous of the roles
a call acts as, a refusal naming the window and when its oldest spend leaves it."""

# Requirements: REQ-1174

from __future__ import annotations

import asyncio
from datetime import UTC, datetime, timedelta

import pytest

from tests.unit.test_mcp_chat import _a_configured_vendor_key  # noqa: F401  (a fixture)
from provisa.security.llm_budget import (
    Budget,
    BudgetRefused,
    Limit,
    Spend,
    WindowUse,
    admit,
    budget_of,
    debit,
    over_cap,
    refusal,
    usage,
    window_use,
)

pytestmark = pytest.mark.unit

NOW = datetime(2026, 10, 27, 12, 30, 15, tzinfo=UTC)


def _role(**limits) -> dict:
    return {"rate_limit": limits}


def test_a_roles_budget_is_its_call_cap_and_the_windows_it_sets():
    role = _role(
        llm_call_cap={"amount": 5000, "unit": "tokens"},
        llm_windows={"day": {"amount": 2, "unit": "usd"}, "hour": None},
    )
    assert budget_of(role) == Budget(Limit(5000, "tokens"), {"day": Limit(2, "usd")})
    assert budget_of(_role(requests_per_second=5)) is None


def test_each_window_sums_the_buckets_of_its_grain_within_it():
    at = lambda **d: NOW - timedelta(**d)  # noqa: E731
    buckets = [
        ("minute", at(minutes=10), 100, 0.1),
        ("minute", at(minutes=59), 50, 0.05),
        ("minute", at(minutes=61), 999, 9.0),  # left the hour
        ("hour", at(hours=2), 300, 0.3),
        ("hour", at(hours=23), 200, 0.2),
        ("hour", at(hours=25), 999, 9.0),  # left the day, still in the week
        ("day", at(days=29), 400, 0.4),
        ("day", at(days=31), 999, 9.0),  # left the month
    ]
    budget = Budget(
        None,
        {
            "hour": Limit(1000, "tokens"),
            "day": Limit(1, "usd"),
            "week": Limit(5000, "tokens"),
            "month": Limit(10, "usd"),
        },
    )
    uses = {u.window: u for u in window_use(buckets, budget, NOW)}
    assert uses["hour"].used == 150
    assert uses["hour"].frees_at == at(minutes=59) + timedelta(hours=1)
    assert uses["day"].used == pytest.approx(0.5)
    assert uses["day"].frees_at == at(hours=23) + timedelta(days=1)
    assert uses["week"].used == 300 + 200 + 999
    assert uses["month"].used == pytest.approx(0.4)


def test_a_window_with_nothing_spent_frees_at_no_time():
    (use,) = window_use([], Budget(None, {"hour": Limit(10, "tokens")}), NOW)
    assert (use.used, use.frees_at, use.full) == (0, None, False)


def test_the_per_call_cap_is_held_against_the_calls_own_use():
    budget = Budget(Limit(1000, "tokens"))
    assert over_cap("analyst", budget, Spend(tokens=1000)) is None
    assert over_cap("analyst", budget, Spend(tokens=1001)) == (
        "role 'analyst' caps one LLM call at 1,000 tokens; this one used 1,001 tokens"
    )
    assert over_cap("analyst", Budget(Limit(0.5, "usd")), Spend(usd=0.75)) == (
        "role 'analyst' caps one LLM call at $0.5000; this one used $0.7500"
    )
    assert over_cap("analyst", None, Spend(tokens=10**9)) is None


def test_a_refusal_names_the_window_and_when_its_oldest_spend_leaves_it():
    use = WindowUse("day", Limit(2, "usd"), 2.25, datetime(2026, 10, 28, 9, 0, tzinfo=UTC))
    assert refusal("analyst", use) == (
        "role 'analyst' has spent its LLM budget for the day: $2.2500 of $2.0000; "
        "its oldest spend leaves it at 2026-10-28 09:00 UTC"
    )
    month = WindowUse("month", Limit(100, "tokens"), 100, None)
    assert refusal("analyst", month) == (
        "role 'analyst' has spent its LLM budget for the month (30 days): 100 tokens of 100 tokens"
    )


# -- counted in the control plane ----------------------------------------------------------------


@pytest.fixture
def model_db(control_plane):
    return control_plane.org()


def test_spend_counted_for_two_users_on_one_role_is_one_counter(model_db):
    """No per-user metering: whoever acts as the role spends its budget."""
    budget = Budget(None, {"hour": Limit(1000, "tokens"), "day": Limit(1, "usd")})

    async def run():
        await debit(model_db, "analyst", Spend(300, 0.25), NOW - timedelta(minutes=5))  # one user
        await debit(model_db, "analyst", Spend(200, 0.25), NOW - timedelta(minutes=5))  # another
        await debit(model_db, "steward", Spend(999, 9.0), NOW)
        return {u.window: u.used for u in await usage(model_db, "analyst", budget, NOW)}

    assert asyncio.run(run()) == {"hour": 500, "day": pytest.approx(0.5)}


def test_spend_leaves_each_window_as_it_ages(model_db):
    budget = Budget(None, {"hour": Limit(1000, "tokens"), "week": Limit(10_000, "tokens")})

    async def run():
        await debit(model_db, "analyst", Spend(400, 0), NOW - timedelta(minutes=50))
        later = NOW + timedelta(minutes=15)
        return {u.window: u.used for u in await usage(model_db, "analyst", budget, later)}

    assert asyncio.run(run()) == {"hour": 0, "week": 400}


def test_buckets_no_window_reads_are_dropped(model_db):
    from sqlalchemy import select

    from provisa.core.schema_org import role_llm_usage as t

    async def run():
        await debit(model_db, "analyst", Spend(1, 0), NOW - timedelta(days=40))
        await debit(model_db, "analyst", Spend(1, 0), NOW)
        async with model_db.acquire() as conn:
            rows = (await conn.execute_core(select(t.c.grain, t.c.bucket))).fetchall()
        return sorted(g for g, bucket in rows)

    # The 40-day-old spend is past every grain's keeping; today's is in all three.
    assert asyncio.run(run()) == ["day", "hour", "minute"]


ROLES = {
    "free": _role(),
    "tight": _role(llm_windows={"hour": {"amount": 100, "unit": "tokens"}}),
    "roomy": _role(llm_windows={"hour": {"amount": 10_000, "unit": "tokens"}}),
    "dollars": _role(llm_windows={"day": {"amount": 1, "unit": "usd"}}),
}


def test_a_call_acting_as_several_roles_is_held_to_the_most_generous(model_db):
    async def run():
        await debit(model_db, "tight", Spend(100, 0), NOW - timedelta(minutes=1))
        await debit(model_db, "roomy", Spend(5000, 0), NOW - timedelta(minutes=1))
        return [
            (await admit(model_db, ROLES, acting, priced=True, now=NOW)).role_id
            for acting in (["tight", "roomy"], ["roomy", "free"], ["dollars", "roomy"])
        ]

    # tight is full; free has no budget; dollars has all its room, roomy half of its.
    assert asyncio.run(run()) == ["roomy", "free", "dollars"]


def test_a_call_no_role_has_room_for_is_refused_naming_the_window_that_frees_first(model_db):
    async def run():
        await debit(model_db, "tight", Spend(100, 0), NOW - timedelta(minutes=20))
        await admit(model_db, ROLES, ["tight"], priced=True, now=NOW)

    with pytest.raises(BudgetRefused) as refused:
        asyncio.run(run())
    assert str(refused.value) == (
        "role 'tight' has spent its LLM budget for the hour: 100 tokens of 100 tokens; "
        "its oldest spend leaves it at 2026-10-27 13:10 UTC"
    )


def test_a_role_with_a_usd_limit_uses_only_priced_models(model_db):
    with pytest.raises(BudgetRefused, match="uses only models with a polly.model_prices entry"):
        asyncio.run(admit(model_db, ROLES, ["dollars"], priced=False, now=NOW))
    assert asyncio.run(admit(model_db, ROLES, ["tight"], priced=False, now=NOW)).role_id == "tight"


# -- a Polly turn ---------------------------------------------------------------------------------


def _turn(monkeypatch, model_db, rate_limit, responses):
    """A Polly turn as ``analyst``, whose role sets ``rate_limit``; the model answers
    ``responses``. Its events."""
    from provisa.api.mcp import chat as chat_mod
    from tests.unit.test_mcp_chat import _install_fake_anthropic, _state

    monkeypatch.setenv("PROVISA_MCP_CHAT_MODEL", "claude-haiku-5-5")
    _install_fake_anthropic(monkeypatch, responses)
    state = _state()
    state.model_db = model_db
    state.roles["analyst"] = {"id": "analyst", "rate_limit": rate_limit}

    async def run():
        convo = [{"role": "user", "content": "how many orders?"}]
        return [ev async for ev in chat_mod.run_chat(state, "analyst", convo)]

    return asyncio.run(run())


def _answer(text, fresh, out):
    from tests.unit.test_mcp_chat import _block

    from types import SimpleNamespace

    return SimpleNamespace(
        content=[_block(type="text", text=text)],
        stop_reason="end_turn",
        usage=SimpleNamespace(
            input_tokens=fresh,
            output_tokens=out,
            cache_read_input_tokens=0,
            cache_creation_input_tokens=0,
        ),
    )


def _tool_round(fresh, out):
    from types import SimpleNamespace

    from tests.unit.test_mcp_chat import _block

    return SimpleNamespace(
        content=[_block(type="tool_use", name="search_catalog", input={"query": "x"}, id="t1")],
        stop_reason="tool_use",
        usage=SimpleNamespace(
            input_tokens=fresh,
            output_tokens=out,
            cache_read_input_tokens=0,
            cache_creation_input_tokens=0,
        ),
    )


@pytest.fixture(autouse=False)
def _no_search(monkeypatch):
    from provisa.api.mcp import tools as mcp_tools

    async def _search(_state, _role, _query, k=5):
        return []

    monkeypatch.setattr(mcp_tools, "search_catalog", _search)


@pytest.mark.usefixtures("_a_configured_vendor_key")
def test_a_turn_is_counted_against_its_roles_windows(monkeypatch, model_db):
    limits = {"llm_windows": {"day": {"amount": 100_000, "unit": "tokens"}}}
    events = _turn(monkeypatch, model_db, limits, [_answer("40 orders", 300, 20)])
    assert [e["type"] for e in events][-2:] == ["usage", "done"]
    budget = budget_of({"rate_limit": limits})
    (day,) = asyncio.run(usage(model_db, "analyst", budget, datetime.now(UTC)))
    assert day.used == 320


@pytest.mark.usefixtures("_a_configured_vendor_key")
def test_a_turn_whose_role_has_spent_its_window_is_refused_before_the_model_is_asked(
    monkeypatch, model_db
):
    limits = {"llm_windows": {"hour": {"amount": 100, "unit": "tokens"}}}
    asyncio.run(debit(model_db, "analyst", Spend(150, 0)))
    events = _turn(monkeypatch, model_db, limits, [])
    assert [e["type"] for e in events] == ["error", "done"]
    assert events[0]["error"].startswith(
        "role 'analyst' has spent its LLM budget for the hour: 150 tokens of 100 tokens; "
        "its oldest spend leaves it at "
    )


@pytest.mark.usefixtures("_a_configured_vendor_key", "_no_search")
def test_a_turn_ends_at_its_roles_per_call_cap(monkeypatch, model_db):
    limits = {"llm_call_cap": {"amount": 500, "unit": "tokens"}}
    events = _turn(
        monkeypatch,
        model_db,
        limits,
        [_tool_round(400, 50), _tool_round(400, 50), _answer("x", 1, 1)],
    )
    assert [e["type"] for e in events][-3:] == ["error", "usage", "done"]
    error = next(e for e in events if e["type"] == "error")["error"]
    assert error == "role 'analyst' caps one LLM call at 500 tokens; this one used 900 tokens"


# -- set on the role's form, shown on its page --------------------------------------------------------

_LIMITS = {
    "requests_per_second": 5,
    "llm_call_cap": {"amount": 5000.0, "unit": "tokens"},
    "llm_windows": {
        "day": {"amount": 2.0, "unit": "usd"},
        "month": {"amount": 40.0, "unit": "usd"},
    },
}


def test_the_admin_api_reads_a_roles_llm_budget():
    from provisa.api.admin._row_mappers import _role_from_row

    row = {
        "id": "analyst",
        "description": "reads",
        "capabilities": [],
        "domain_access": [],
        "demonstrated": [],
        "rate_limit": _LIMITS,
        "parents": [],
        "residency_values": [],
        "session_vars": {},
        "inactivity_days": None,
    }
    limits = _role_from_row(row, detail=True, effective=[]).rate_limit
    assert (limits.llm_call_cap.amount, limits.llm_call_cap.unit) == (5000.0, "tokens")
    windows = limits.llm_windows
    assert (windows.hour, windows.week) == (None, None)
    assert (windows.day.amount, windows.day.unit, windows.month.amount) == (2.0, "usd", 40.0)


def test_the_role_form_saves_its_llm_budget_as_the_model_holds_it():
    from pydantic import ValidationError

    from provisa.api.admin.schema_mutation import _llm_limit
    from provisa.api.admin.types import LlmLimitInput
    from provisa.core.models import LlmLimit

    assert _llm_limit(LlmLimitInput(amount=2.5, unit="usd")) == LlmLimit(amount=2.5, unit="usd")
    assert _llm_limit(None) is None
    with pytest.raises(ValidationError, match="unit"):
        _llm_limit(LlmLimitInput(amount=2.5, unit="euros"))
    with pytest.raises(ValidationError, match="amount"):
        _llm_limit(LlmLimitInput(amount=0, unit="tokens"))


def test_the_roles_page_reads_each_windows_use_and_when_room_frees(model_db, monkeypatch):
    import json
    from types import SimpleNamespace

    from sqlalchemy import insert

    from provisa.api.admin import roles_router
    from provisa.core.schema_org import roles

    async def run():
        async with model_db.acquire() as conn:
            await conn.execute_core(
                insert(roles).values(
                    id="budgeted", description="d", capabilities=[], rate_limit=_LIMITS
                )
            )
        await debit(model_db, "budgeted", Spend(100, 0.5), datetime.now(UTC) - timedelta(hours=2))
        return await roles_router.llm_usage("budgeted", SimpleNamespace(state=SimpleNamespace()))

    monkeypatch.setattr(roles_router, "_active_org", lambda request: "default")
    monkeypatch.setattr(roles_router, "_pool", lambda request: model_db)
    monkeypatch.setattr(
        roles_router, "role_definitions_visible", lambda request, held: lambda r: True
    )
    answered = json.loads(json.dumps(asyncio.run(run())))
    day, month = answered["windows"]
    assert (day["window"], day["unit"], day["limit"], day["used"]) == ("day", "usd", 2.0, 0.5)
    assert day["frees_at"] is not None and month["used"] == 0.5
