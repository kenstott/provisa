# Copyright (c) 2026 Kenneth Stott
# Canary: 610a1be1-d37b-4339-96bf-60eb8a0f1f12
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""REQ-2147: a steward's decision on a classification suggestion, and what accepting does.

Accepting links the column to the kind's Property term and, for a sensitive kind, marks it pii and
names the kind's mask preset; on a key column it marks pii and offers the mask but leaves the
column on its Class term, saying why. A rejected suggestion is recorded as rejected on every later
run's row for that column and kind. A decision is refused to a caller without sensitive_data in
the table's domain. An org admin's disabled kinds are not tested.
"""

# Requirements: REQ-2147

from __future__ import annotations

import types
from contextlib import asynccontextmanager
from datetime import UTC, datetime
from typing import Any, cast

import pytest
from sqlalchemy import insert, select

from provisa.api.admin import profiler_classifications_router as router_mod
from provisa.api.errors import ApiError
from provisa.core.models import Column, Table
from provisa.core.repositories import glossary as glossary_repo
from provisa.core.repositories import table as table_repo
from provisa.core.schema_org import domains, registered_tables, sources, tag_assignments
from provisa.profiler import classifications, run
from provisa.profiler.classify import Match, catalogue, kind_of
from provisa.profiler.statement import ColumnSpec, Sample, classification_sql

SCHEMA = "org_default"
EVIDENCE = "customers.tax_id looks like a US social security number: 97% of sampled values pass"


def _request() -> Any:
    return types.SimpleNamespace(
        state=types.SimpleNamespace(
            identity=types.SimpleNamespace(user_id="sam", roles=[], acting_roles=[]),
            active_org_id="acme",
        )
    )


def _customers() -> Table:
    return Table(
        source_id="__derived__",
        domain_id="sales",
        schema_name="s",
        table_name="customers",
        columns=[
            Column(name="id", data_type="integer", visible_to=[], is_primary_key=True),
            Column(name="tax_id", data_type="text", visible_to=[]),
            Column(name="city", data_type="text", visible_to=[]),
        ],
        view_sql="SELECT 1",
    )


@asynccontextmanager
async def _model(control_plane, monkeypatch, *, allowed: frozenset[str] | None = None):
    """customers (keyed by id) in the sales domain; the router bound to the test's own plane,
    with the caller holding sensitive_data in ``allowed`` (None: every domain)."""
    with control_plane.engine().begin() as c:
        c.exec_driver_sql(f'SET LOCAL search_path TO "{SCHEMA}"')
        c.execute(insert(domains).values(id="sales", description=""))
        c.execute(insert(sources).values(id="__derived__", type="derived"))
    db = control_plane.org()

    async def _member(conn, table_id, request):
        # Every column served: which ones a caller may read is test_profiler_read_rights.
        return {"served": None}

    monkeypatch.setattr(router_mod, "_db", lambda: db)
    monkeypatch.setattr(router_mod, "_member", _member)
    monkeypatch.setattr(router_mod, "_physical", lambda table_id, column, served: column)
    monkeypatch.setattr(router_mod, "require_capability_request", lambda request, cap: None)
    monkeypatch.setattr(
        router_mod, "allowed_domains_for_capability_request", lambda request, cap: allowed
    )
    async with db.acquire() as conn:
        await table_repo.upsert(conn, _customers())
        table_id = (
            await conn.execute_core(
                select(registered_tables.c.id).where(registered_tables.c.table_name == "customers")
            )
        ).scalar_one()
    yield db, table_id


def _decision(column: str, kind: str, status: str = "accepted") -> Any:
    return router_mod.ClassificationDecision(
        column=column, kind=kind, status=status, evidence=EVIDENCE, runId="run-1"
    )


async def _term_name(db, table_id: int, column: str) -> str:
    async with db.acquire() as conn:
        term = await glossary_repo.get_term_by_ref(conn, table_id, column)
    assert term is not None
    return term["name"]


async def _pii(db, table_id: int, column: str) -> list[dict]:
    async with db.acquire() as conn:
        rows = await conn.execute_core(
            select(tag_assignments).where(
                (tag_assignments.c.base_tag_id == "pii")
                & (tag_assignments.c.table_id == table_id)
                & (tag_assignments.c.column_name == column)
            )
        )
        return [dict(r._mapping) for r in rows.fetchall()]


@pytest.mark.asyncio
async def test_accepting_a_sensitive_kind_links_the_term_marks_pii_and_offers_the_mask(
    control_plane, monkeypatch
):
    async with _model(control_plane, monkeypatch) as (db, table_id):
        answer = await router_mod.decide_classification(
            cast(Any, _request()), table_id, _decision("tax_id", "us_ssn")
        )
        assert answer["term"] == "us social security number"
        assert answer["termNote"] is None
        assert answer["pii"] is True
        assert answer["maskPreset"] == "ssn_last4"
        assert await _term_name(db, table_id, "tax_id") == "us social security number"
        (tag,) = await _pii(db, table_id, "tax_id")
        assert tag["reason"] == EVIDENCE
        async with db.acquire() as conn:
            decided = await classifications.decisions(conn, table_id)
        assert decided[("tax_id", "us_ssn")]["status"] == "accepted"
        assert decided[("tax_id", "us_ssn")]["decided_by"] == "sam"


@pytest.mark.asyncio
async def test_accepting_on_a_key_column_marks_pii_and_keeps_its_class_term(
    control_plane, monkeypatch
):
    async with _model(control_plane, monkeypatch) as (db, table_id):
        before = await _term_name(db, table_id, "id")
        answer = await router_mod.decide_classification(
            cast(Any, _request()), table_id, _decision("id", "us_ssn")
        )
        assert answer["term"] is None
        assert answer["termNote"] == "a key column's meaning is its Class term"
        assert answer["pii"] is True
        assert answer["maskPreset"] == "ssn_last4"
        assert await _term_name(db, table_id, "id") == before
        assert len(await _pii(db, table_id, "id")) == 1


@pytest.mark.asyncio
async def test_accepting_a_reference_kind_links_the_term_and_marks_nothing_pii(
    control_plane, monkeypatch
):
    async with _model(control_plane, monkeypatch) as (db, table_id):
        answer = await router_mod.decide_classification(
            cast(Any, _request()), table_id, _decision("city", "city")
        )
        assert (answer["term"], answer["pii"], answer["maskPreset"]) == ("city", False, None)
        assert await _term_name(db, table_id, "city") == "city"
        assert await _pii(db, table_id, "city") == []


@pytest.mark.asyncio
async def test_a_rejection_changes_nothing_and_is_recorded_on_later_runs_until_withdrawn(
    control_plane, monkeypatch
):
    async with _model(control_plane, monkeypatch) as (db, table_id):
        before = await _term_name(db, table_id, "tax_id")
        answer = await router_mod.decide_classification(
            cast(Any, _request()), table_id, _decision("tax_id", "us_ssn", "rejected")
        )
        assert answer == {"status": "rejected"}
        assert await _term_name(db, table_id, "tax_id") == before
        assert await _pii(db, table_id, "tax_id") == []
        ssn, card = kind_of("us_ssn"), kind_of("payment_card")
        suggestions = [
            classifications.Suggestion("tax_id", "tax_id", Match(ssn, 100, 97)),
            classifications.Suggestion("tax_id", "tax_id", Match(card, 100, 95)),
        ]
        async with db.acquire() as conn:
            decided = await classifications.decisions(conn, table_id)
        rows = classifications.result_rows("customers", suggestions, decided)
        assert [(r["kind"], r["status"]) for r in rows] == [
            ("us_ssn", "rejected"),
            ("payment_card", "suggested"),
        ]
        await router_mod.forget_classification(
            cast(Any, _request()),
            table_id,
            router_mod.ForgetClassification(physicalColumn="tax_id", kind="us_ssn"),
        )
        async with db.acquire() as conn:
            assert await classifications.decisions(conn, table_id) == {}
        with pytest.raises(ApiError) as missing:
            await router_mod.forget_classification(
                cast(Any, _request()),
                table_id,
                router_mod.ForgetClassification(physicalColumn="tax_id", kind="us_ssn"),
            )
        assert missing.value.status_code == 404


@pytest.mark.asyncio
async def test_a_decision_is_refused_without_sensitive_data_in_the_tables_domain(
    control_plane, monkeypatch
):
    async with _model(control_plane, monkeypatch, allowed=frozenset({"hr"})) as (db, table_id):
        with pytest.raises(ApiError) as refused:
            await router_mod.decide_classification(
                cast(Any, _request()), table_id, _decision("tax_id", "us_ssn")
            )
        assert refused.value.status_code == 403
        assert refused.value.code == "auth.domain_denied"
        async with db.acquire() as conn:
            assert await classifications.decisions(conn, table_id) == {}


@pytest.mark.asyncio
async def test_an_unknown_kind_or_status_is_refused_by_name(control_plane, monkeypatch):
    async with _model(control_plane, monkeypatch) as (_db, table_id):
        with pytest.raises(ApiError) as kind:
            await router_mod.decide_classification(
                cast(Any, _request()), table_id, _decision("tax_id", "horoscope_sign")
            )
        assert kind.value.code == "profile.classification_kind_unknown"
        with pytest.raises(ApiError) as status:
            await router_mod.decide_classification(
                cast(Any, _request()), table_id, _decision("tax_id", "us_ssn", "maybe")
            )
        assert status.value.code == "profile.classification_status_invalid"


@pytest.mark.asyncio
async def test_a_disabled_kind_is_not_tested(control_plane):
    from provisa.core.org_settings import write_org_overrides

    db = control_plane.org()
    assert {k.id for k in await classifications.enabled_kinds(db)} == {k.id for k in catalogue()}
    await write_org_overrides(
        db, {"classification": classifications.setting_for({"city", "us_passport"})}, updated_by="a"
    )
    enabled = {k.id for k in await classifications.enabled_kinds(db)}
    assert enabled == {k.id for k in catalogue()} - {"city", "us_passport"}
    await write_org_overrides(
        db, {"classification": classifications.setting_for(set())}, updated_by="a"
    )
    assert len(await classifications.enabled_kinds(db)) == len(catalogue())


def test_an_unknown_disabled_kind_is_refused_by_name():
    with pytest.raises(ValueError, match="horoscope_sign"):
        classifications.setting_for({"horoscope_sign"})
    with pytest.raises(ValueError, match="horoscope_sign"):
        classifications.disabled_of({"classification": {"disabled": ["horoscope_sign"]}})


# -- the run's side --------------------------------------------------------------------------------


def test_the_value_sample_is_drawn_at_random_from_the_runs_own_sample():
    cols = [ColumnSpec("tax_id", "varchar", "text", "tax_id")]
    sql = classification_sql("sales.customers", cols, Sample("block", 0.1), 500)
    assert "TABLESAMPLE SYSTEM (10.0)" in sql
    assert sql.endswith("ORDER BY RANDOM() LIMIT 500")
    # The rows the sample held, counted before the limit: what the statement read of the source.
    assert 'COUNT(*) OVER () AS "__sample_rows"' in sql
    assert 'CAST(s."tax_id" AS VARCHAR)' in sql


def test_the_first_passing_kind_becomes_the_columns_plausible_type():
    key = {"run_id": "r", "run_time": datetime.now(UTC)}
    results = {
        "plausible_type": [
            {
                **key,
                "column_name": "tax_id",
                "plausible_type": "identifier",
                "confidence": 0.8,
                "evidence": "x",
            },
            {
                **key,
                "column_name": "note",
                "plausible_type": "free_text",
                "confidence": 0.7,
                "evidence": "y",
            },
        ]
    }
    suggestions = [
        classifications.Suggestion(
            "tax_id", "tax_id", Match(kind_of("ca_social_insurance_number"), 50, 48)
        ),
        classifications.Suggestion("tax_id", "tax_id", Match(kind_of("us_ssn"), 50, 49)),
    ]
    run.classified_plausible_types(results, suggestions, "customers")
    tax, note = results["plausible_type"]
    assert tax["plausible_type"] == "us_ssn"  # first in the catalogue, not the highest share
    assert tax["confidence"] == 49 / 50
    assert tax["evidence"].startswith("customers.tax_id looks like a US social security number")
    assert note["plausible_type"] == "free_text"


@pytest.mark.asyncio
async def test_the_run_tests_only_the_columns_of_the_enabled_kinds_families(monkeypatch):
    seen: list[str] = []

    async def _further(sql, sample, where, table_name):
        seen.append(sql)
        # Twelve rows of a sample that held forty: the statement's limit, and what the source read.
        return ["city", "__sample_rows"], [("Paris", 40)] * 12

    monkeypatch.setattr(run, "_run_further", _further)
    target = types.SimpleNamespace(
        pgwire_name="sales.stores",
        table_name="stores",
        columns=[
            ColumnSpec("city", "varchar", "text", "city"),
            ColumnSpec("opened", "date", "temporal", "opened"),
        ],
    )
    read = types.SimpleNamespace(sample=Sample("whole"), where="whole table")
    (suggestion,), attempts = await run.read_classifications(
        cast(Any, target), cast(Any, read), (kind_of("city"),), 500
    )
    assert suggestion.match.kind.id == "city" and suggestion.match.share == 1.0
    # Its read is recorded as the dependence statements' are: the rows the sample held.
    assert attempts == [{"statement": "classification", "percent": 100.0, "rows": 40}]
    assert '"opened"' not in seen[0]
    assert await run.read_classifications(cast(Any, target), cast(Any, read), (), 500) == (
        [],
        [],
    )
    assert len(seen) == 1  # no kinds, no statement
