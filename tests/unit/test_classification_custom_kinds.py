# Copyright (c) 2026 Kenneth Stott
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""REQ-2147 (amended 2026-11-25): an org admin's custom classification kinds and its changes to
the shipped ones.

A custom kind is a name, a regular expression with an optional shipped check, the share of values
that must pass, whether it is sensitive, its mask and its glossary term; it is added, edited and
deleted. A shipped kind keeps its coded test, but its share, sensitivity and mask may change. Both
are the org's ``classification`` setting (part of the model, so they travel with it), and are
tested and suggested exactly as the shipped kinds are.
"""

# Requirements: REQ-2147

from __future__ import annotations

import types
from typing import Any, cast

import pytest
from sqlalchemy import insert, select

from provisa.api.admin import profiler_classifications_router as router_mod
from provisa.api.errors import ApiError
from provisa.core.env_classes import carries_setting
from provisa.core.org_settings import read_org_overrides
from provisa.core.schema_org import profiler_classifications
from provisa.profiler import classifications, run
from provisa.profiler.classify import catalogue, custom_kind, judge, kind_of, org_catalogue
from provisa.profiler.statement import ColumnSpec, Sample

EMPLOYEE_ID = {
    "label": "Employee ID",
    "pattern": r"EMP-\d{6}",
    "check": None,
    "ignore": None,
    "threshold": 0.8,
    "sensitive": False,
    "mask_preset": None,
    "term": "employee id",
}


def _custom(**over: Any) -> dict:
    return {"id": "custom.employee_id", **EMPLOYEE_ID, **over}


def _request() -> Any:
    return types.SimpleNamespace(
        state=types.SimpleNamespace(
            identity=types.SimpleNamespace(user_id="sam", roles=[], acting_roles=[]),
            active_org_id="acme",
        )
    )


def _body(**over: Any) -> router_mod.CustomKind:
    fields = {k: v for k, v in EMPLOYEE_ID.items() if k != "mask_preset"}
    return router_mod.CustomKind(**{**fields, "maskPreset": EMPLOYEE_ID["mask_preset"], **over})


@pytest.fixture
def plane(control_plane, monkeypatch):
    """The router bound to the test's own plane, the caller holding sensitive_data."""
    db = control_plane.org()
    monkeypatch.setattr(router_mod, "_db", lambda: db)
    monkeypatch.setattr(router_mod, "require_capability_request", lambda request, cap: None)
    return db


# -- the kinds ---------------------------------------------------------------------------------


def test_a_custom_kind_is_tested_and_suggested_as_a_shipped_one():
    kind = custom_kind(_custom())
    assert kind.custom and kind.families == ("text", "numeric")
    values = [f"EMP-{n:06d}" for n in range(9)] + ["EMP-12", None, "nobody"]
    judged = judge("text", values, (kind,))
    (match,) = judged.matches
    assert (match.tested, match.passed) == (11, 9)
    assert match.evidence("staff", "badge") == (
        "staff.badge looks like Employee ID: 82% of sampled values pass its test"
    )
    assert judge("text", values[:9], (kind,)).too_few  # under ten values: too few, as any kind


def test_a_custom_kind_takes_a_shipped_check_and_ignored_characters():
    card = custom_kind(
        _custom(
            id="custom.card",
            label="Card",
            pattern=r"\d{16}",
            check="payment_card",
            ignore=r"[\s-]",
            sensitive=True,
            mask_preset="card_last4",
        )
    )
    values = ["4111 1111 1111 1111"] * 9 + ["4111-1111-1111-1112"]
    (match,) = judge("text", values, (card,)).matches
    assert match.passed == 9  # the last fails the Luhn check


def test_a_value_its_check_cannot_read_fails_the_check_and_stops_nothing():
    # The pattern lets letters through to a check that reads digits.
    odd = custom_kind(_custom(pattern=r"[A-Z0-9]{9}", check="us_bank_routing_number"))
    judged = judge("text", ["ABCDEFGHI"] * 5 + ["011000015"] * 5, (odd,))
    assert judged.values == 10 and judged.matches == ()  # half pass: under the 80% it needs


@pytest.mark.parametrize(
    ("over", "said"),
    [
        ({"pattern": "EMP-(\\d"}, "does not compile"),
        ({"check": "horoscope"}, "no check 'horoscope'"),
        ({"mask_preset": "ssn_last4"}, "a mask preset is a sensitive kind's"),
        ({"sensitive": True}, "a mask preset is a sensitive kind's"),
        ({"threshold": 1.5}, "threshold must be in"),
    ],
)
def test_a_custom_kind_that_would_not_load_is_refused_by_name(over, said):
    with pytest.raises(ValueError, match=said):
        custom_kind(_custom(**over))


def test_a_shipped_kind_keeps_its_test_and_takes_the_orgs_share_sensitivity_and_mask():
    kinds = org_catalogue(
        {"city": {"threshold": 0.5, "sensitive": True, "mask_preset": "full_redact"}}, ()
    )
    city = next(k for k in kinds if k.id == "city")
    assert (city.threshold, city.sensitive, city.mask_preset) == (0.5, True, "full_redact")
    assert city.test == kind_of("city").test and not city.custom
    # A kind made sensitive comes before every kind that is not.
    assert kinds.index(city) < min(i for i, k in enumerate(kinds) if not k.sensitive)
    with pytest.raises(ValueError, match="only"):
        org_catalogue({"us_ssn": {"pattern": r"\d+"}}, ())


def test_the_setting_holds_only_what_the_org_changed_and_travels_with_the_model():
    settings = classifications.with_shipped_change(
        classifications.SHIPPED, "city", {"threshold": 0.5}
    )
    assert classifications.setting_of(settings) == {"changed": {"city": {"threshold": 0.5}}}
    back = classifications.with_shipped_change(settings, "city", {"threshold": 0.9})
    assert classifications.setting_of(back) is None  # every shipped value again: no change
    assert carries_setting(classifications.SETTING_KEY)


# -- the routes: stored in the org's model -------------------------------------------------------


@pytest.mark.asyncio
async def test_an_org_admin_adds_edits_and_deletes_a_custom_kind(plane):
    request = cast(Any, _request())
    made = await router_mod.create_kind(request, _body())
    assert made["id"] == "custom.employee_id"
    doc = next(k for k in made["kinds"] if k["id"] == made["id"])
    assert doc["custom"] and doc["pattern"] == r"EMP-\d{6}" and doc["enabled"]
    stored = (await read_org_overrides(plane))["classification"]
    assert stored == {"custom": [{"id": "custom.employee_id", **EMPLOYEE_ID}]}
    assert made["id"] in {k.id for k in await classifications.enabled_kinds(plane)}

    changed = await router_mod.change_kind(
        request,
        made["id"],
        router_mod.KindChange(label="Staff badge", threshold=0.95, enabled=False),
    )
    doc = next(k for k in changed["kinds"] if k["id"] == made["id"])
    # The id stays when the name changes: decisions are recorded by it.
    assert (doc["label"], doc["threshold"], doc["enabled"]) == ("Staff badge", 0.95, False)
    assert made["id"] not in {k.id for k in await classifications.enabled_kinds(plane)}

    left = await router_mod.delete_kind(request, made["id"])
    assert made["id"] not in {k["id"] for k in left["kinds"]}
    assert "classification" not in await read_org_overrides(plane)


@pytest.mark.asyncio
async def test_a_custom_kind_is_refused_by_name_where_it_would_not_load(plane):
    request = cast(Any, _request())
    await router_mod.create_kind(request, _body())
    for body, said in (
        (_body(), "exists"),
        (_body(label="Bad", pattern="("), "does not compile"),
        (_body(label="Masked", maskPreset="no_such_mask", sensitive=True), "no_such_mask"),
    ):
        with pytest.raises(ApiError) as refused:
            await router_mod.create_kind(request, body)
        assert refused.value.code == "profile.classification_kind_invalid"
        assert said in str(refused.value.detail)
    with pytest.raises(ApiError) as bad_edit:
        await router_mod.change_kind(
            request, "custom.employee_id", router_mod.KindChange(pattern="(")
        )
    assert "does not compile" in str(bad_edit.value.detail)
    # Nothing refused was stored.
    assert [k.id for k in await classifications.org_kinds(plane) if k.custom] == [
        "custom.employee_id"
    ]


@pytest.mark.asyncio
async def test_a_shipped_kind_changes_its_settings_never_its_test_and_is_never_deleted(plane):
    request = cast(Any, _request())
    changed = await router_mod.change_kind(request, "city", router_mod.KindChange(threshold=0.6))
    assert next(k for k in changed["kinds"] if k["id"] == "city")["threshold"] == 0.6
    assert (await read_org_overrides(plane))["classification"] == {
        "changed": {"city": {"threshold": 0.6}}
    }
    with pytest.raises(ApiError) as test:
        await router_mod.change_kind(request, "us_ssn", router_mod.KindChange(pattern=r"\d+"))
    assert "copy it into a custom kind" in str(test.value.detail)
    with pytest.raises(ApiError) as deleted:
        await router_mod.delete_kind(request, "us_ssn")
    assert "disabled, not deleted" in str(deleted.value.detail)
    with pytest.raises(ApiError) as unknown:
        await router_mod.change_kind(request, "horoscope", router_mod.KindChange(threshold=0.5))
    assert unknown.value.code == "profile.classification_kind_unknown"


@pytest.mark.asyncio
async def test_deleting_a_custom_kind_forgets_the_decisions_on_it(control_plane, plane):
    from provisa.core.models import Column, Table
    from provisa.core.repositories import table as table_repo
    from provisa.core.schema_org import domains, registered_tables, sources

    request = cast(Any, _request())
    made = await router_mod.create_kind(request, _body())
    with control_plane.engine().begin() as c:
        c.exec_driver_sql('SET LOCAL search_path TO "org_default"')
        c.execute(insert(domains).values(id="hr", description=""))
        c.execute(insert(sources).values(id="__derived__", type="derived"))
    staff = Table(
        source_id="__derived__",
        domain_id="hr",
        schema_name="s",
        table_name="staff",
        columns=[Column(name="badge", data_type="text", visible_to=[])],
        view_sql="SELECT 1",
    )
    async with plane.acquire() as conn:
        await table_repo.upsert(conn, staff)
        table_id = (
            await conn.execute_core(
                select(registered_tables.c.id).where(registered_tables.c.table_name == "staff")
            )
        ).scalar_one()
    kind = next(k for k in await classifications.org_kinds(plane) if k.id == made["id"])
    async with plane.acquire() as conn:
        await classifications.record(
            conn, table_id, "badge", kind, status="rejected", evidence="e", run_id="r", by="sam"
        )
    await router_mod.delete_kind(request, made["id"])
    async with plane.acquire() as conn:
        left = (await conn.execute_core(select(profiler_classifications))).fetchall()
    assert left == []


# -- the run: a custom kind is suggested ----------------------------------------------------------


@pytest.mark.asyncio
async def test_a_profile_run_suggests_a_custom_kind(monkeypatch):
    async def _further(sql, sample, where, table_name):
        return ["badge", "__sample_rows"], [(f"EMP-{n:06d}", 12) for n in range(12)]

    monkeypatch.setattr(run, "_run_further", _further)
    target = types.SimpleNamespace(
        pgwire_name="hr.staff",
        table_name="staff",
        columns=[ColumnSpec("badge", "varchar", "text", "badge")],
    )
    read = types.SimpleNamespace(sample=Sample("whole"), where="whole table")
    kinds = (*catalogue(), custom_kind(_custom()))
    judged, _ = await run.read_classifications(cast(Any, target), cast(Any, read), kinds, 500)
    (suggestion,) = run.suggestions_of(judged)
    assert suggestion.match.kind.id == "custom.employee_id" and suggestion.match.share == 1.0
    results = {
        "plausible_type": [
            {"column_name": "badge", "plausible_type": "identifier", "confidence": 0.5}
        ]
    }
    run.classified_plausible_types(results, [suggestion], "staff", kinds)
    assert results["plausible_type"][0]["plausible_type"] == "custom.employee_id"


# -- Polly: the same routes, as the caller ------------------------------------------------------


@pytest.mark.asyncio
async def test_polly_lists_creates_changes_and_deletes_kinds(plane):
    from provisa.api.mcp import model_tool_specs

    state = types.SimpleNamespace(contexts={"steward": object()})
    request = _request()

    async def call(name: str, **args: Any) -> dict:
        return await model_tool_specs.dispatch(state, "steward", name, args, request)

    listed = await call("list_classification_kinds")
    assert {k["id"] for k in listed["kinds"]} == {k.id for k in catalogue()}
    made = await call(
        "create_classification_kind",
        **{**{k: v for k, v in EMPLOYEE_ID.items() if k != "mask_preset"}, "maskPreset": None},
    )
    assert made["id"] == "custom.employee_id"
    changed = await call("change_classification_kind", kind_id="city", enabled=False)
    assert next(k for k in changed["kinds"] if k["id"] == "city")["enabled"] is False
    # Only the fields sent change: city keeps its shipped share.
    assert next(k for k in changed["kinds"] if k["id"] == "city")["threshold"] == 0.9
    left = await call("delete_classification_kind", kind_id="custom.employee_id")
    assert "custom.employee_id" not in {k["id"] for k in left["kinds"]}
