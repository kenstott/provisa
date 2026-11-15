# Copyright (c) 2026 Kenneth Stott
# Canary: 8017e474-fdc2-4520-abf8-f3b5bb63f541
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""REQ-2147 (KINDS ARE PATTERN RULES; CONTAINED VALUES ARE HINTS): a kind found inside free text is
a hint -- the share of sampled values that held it, never the text -- and never a classification.

A text column with SSNs inside sentences gets a us_ssn hint with the right share, and what is kept
holds no matched text; a column whose values ARE SSNs classifies and is not hinted as well; a
kind's check refuses a lookalike inside text as it refuses one whole; and the sensitive coverage
report lists a hinted column nothing restricts, and not one a mask restricts.
"""

# Requirements: REQ-2147

from __future__ import annotations

from collections.abc import Sequence

import pytest

from provisa.profiler import classify, hints
from provisa.profiler.classify import MIN_TESTED, catalogue, kind_of

SSNS = ["123-45-6789", "078-05-1120", "219-09-9999"]
NOTES = [
    "Customer called about a late delivery.",
    "Asked to update the shipping address.",
    "Refund issued for order 88213.",
    "Prefers email contact after 5pm.",
    "Left a voicemail, no answer.",
    "Account closed at the customer's request.",
    "Payment received, thank you note sent.",
]
# Three of ten notes hold an SSN inside a sentence.
WITH_SSNS = [
    f"Verified identity with SSN {SSNS[0]} on the phone.",
    f"Caller gave {SSNS[1]}, matches file.",
    f"SSN on the form: {SSNS[2]}",
    *NOTES,
]
COLUMNS = [hints.TextColumn("notes", "notes")]


def _hints(values: Sequence[str | None]) -> dict[str, classify.Hint]:
    return {h.kind.id: h for h in classify.contained_hints(values, catalogue())}


def test_ssns_inside_sentences_give_a_us_ssn_hint_with_their_share():
    found = _hints(WITH_SSNS)
    ssn = found["us_ssn"]
    assert (ssn.tested, ssn.matched) == (10, 3)
    assert ssn.share == pytest.approx(0.3, rel=1e-14)
    assert ssn.evidence() == "matches a US social security number in 30% of sampled values"
    # A hint is not a classification: the whole-value test classifies none of these values.
    assert classify.classify("text", list[str | None](WITH_SSNS), catalogue()) == []


def test_a_hint_keeps_no_matched_text():
    rows = hints.hint_rows(7, "scan-1", COLUMNS, ["notes"], [(v,) for v in WITH_SSNS], catalogue())
    (row,) = [r for r in rows if r["kind"] == "us_ssn"]
    assert row == {
        "table_id": 7,
        "physical_column": "notes",
        "kind": "us_ssn",
        "scan_id": "scan-1",
        "values_tested": 10,
        "values_matched": 3,
        "share": pytest.approx(0.3, rel=1e-14),
        "evidence": "matches a US social security number in 30% of sampled values",
    }
    kept = " ".join(str(v) for r in rows for v in r.values())
    for ssn in SSNS:
        assert ssn not in kept
        assert ssn.replace("-", "") not in kept
    for value in WITH_SSNS:
        assert value not in kept


def test_a_whole_value_ssn_column_classifies_and_is_not_hinted_too():
    values: list[str | None] = [
        *SSNS,
        "123-45-6780",
        "123-45-6781",
        "123-45-6782",
        "123-45-6783",
        "123-45-6784",
    ]
    values += ["123-45-6785", "123-45-6786"]
    assert len(values) >= MIN_TESTED
    (match,) = [m for m in classify.classify("text", values, catalogue()) if m.kind.id == "us_ssn"]
    assert match.share == pytest.approx(1.0, rel=1e-14)
    assert "us_ssn" not in _hints(values)


@pytest.mark.parametrize(
    "lookalike",
    [
        "000-12-3456",  # no area 000
        "666-12-3456",  # no area 666
        "912-12-3456",  # no area 900-999
        "123-00-4567",  # no group 00
        "123-45-0000",  # no serial 0000
    ],
)
def test_the_check_refuses_a_lookalike_inside_text(lookalike):
    assert classify.contains(kind_of("us_ssn"), f"Ref {lookalike} on file.") is False
    values = [f"Ref {lookalike} on file.", *NOTES, *NOTES]
    assert "us_ssn" not in _hints(values)


def test_a_kind_inside_a_longer_run_of_characters_is_not_the_kind():
    ssn = kind_of("us_ssn")
    assert classify.contains(ssn, "Verified SSN 123-45-6789.") is True
    assert classify.contains(ssn, "token x123-45-6789y") is False
    assert classify.contains(ssn, "id 9123-45-67891") is False
    card = kind_of("payment_card")
    assert classify.contains(card, "paid with 4111111111111111 today") is True
    assert classify.contains(card, "paid with 4111111111111112 today") is False  # Luhn fails


def test_too_few_values_hint_nothing():
    assert _hints(WITH_SSNS[: MIN_TESTED - 1]) == {}


def test_only_text_columns_are_scanned():
    table = {
        "columns": [
            {"column_name": "notes", "data_type": "varchar"},
            {"column_name": "Tax Id", "alias": "tax_ref", "data_type": "text"},
            {"column_name": "amount", "data_type": "bigint"},
            {"column_name": "seen_at", "data_type": "timestamp"},
        ]
    }
    assert hints.text_columns(table) == [
        hints.TextColumn("notes", "notes"),
        hints.TextColumn("tax_ref", "Tax Id"),
    ]


# --- the coverage report ----------------------------------------------------------------------------


async def test_the_coverage_report_lists_a_hinted_unrestricted_column_and_not_a_restricted_one(
    control_plane,
):
    from sqlalchemy import insert, text

    from provisa.api._meta_views import _META_TABLE_VIEWS, _OPS_REPORT_VIEWS
    from provisa.api.startup_seed import _adapt_view_ddl
    from provisa.core.schema_org import (
        domains,
        profiler_sensitive_hints,
        registered_tables,
        sources,
        table_columns,
        tag_assignments,
    )

    db = control_plane.org()
    try:
        async with db.acquire() as conn:
            await conn.execute_core(insert(domains).values(id="crm", description="crm"))
            await conn.execute_core(insert(sources).values(id="s1", type="postgres"))
            await conn.execute_core(
                insert(registered_tables).values(
                    id=1, source_id="s1", domain_id="crm", schema_name="public", table_name="calls"
                )
            )
            mask = [{"roles": ["analyst"], "kind": "constant", "value": "***"}]
            for name, masks in (("notes", []), ("memo", mask), ("summary", []), ("ssn", [])):
                await conn.execute_core(
                    insert(table_columns).values(
                        table_id=1,
                        domain_id="crm",
                        column_name=name,
                        data_type="varchar",
                        masks=masks,
                    )
                )
            # ssn is pii-tagged: restricted, and listed as a sensitive column, not as a hint.
            await conn.execute_core(
                insert(tag_assignments).values(
                    tag_id="pii",
                    base_tag_id="pii",
                    object_type="column",
                    table_id=1,
                    column_name="ssn",
                    object_key="column:1:ssn",
                )
            )
            rows = hints.hint_rows(
                1,
                "scan-1",
                [hints.TextColumn(c, c) for c in ("notes", "memo", "ssn")],
                ["notes", "memo", "ssn"],
                [(v, v, v) for v in WITH_SSNS],
                catalogue(),
            )
            assert {r["physical_column"] for r in rows} == {"notes", "memo", "ssn"}
            await conn.execute_core_many(insert(profiler_sensitive_hints), rows)
            await conn.execute(_adapt_view_ddl(_META_TABLE_VIEWS["sensitive_coverage"]))
            await conn.execute(_adapt_view_ddl(_OPS_REPORT_VIEWS["sensitive_column_coverage"]))
            found = await conn.execute_core(
                text(
                    "SELECT id, column_name, tags, status, reason FROM sensitive_column_coverage "
                    "ORDER BY id"
                )
            )
            seen = {r[1]: tuple(r) for r in found.fetchall()}
    finally:
        await db.close()
    # notes: hinted, nothing restricts it. memo: masked. summary: no hint. ssn: tagged pii.
    assert seen["notes"] == (
        "1:notes",
        "notes",
        None,
        "hinted",
        "matches a US social security number in 30% of sampled values",
    )
    assert "memo" not in seen and "summary" not in seen
    assert seen["ssn"][3] == "no_fake"


# --- the scan, end to end -------------------------------------------------------------------------


async def _registered(conn) -> None:
    from sqlalchemy import insert

    from provisa.core.schema_org import domains, registered_tables, sources

    await conn.execute_core(insert(domains).values(id="crm", description="crm"))
    await conn.execute_core(insert(sources).values(id="s1", type="postgres"))
    await conn.execute_core(
        insert(registered_tables).values(
            id=1, source_id="s1", domain_id="crm", schema_name="public", table_name="calls"
        )
    )


async def test_a_scan_runs_to_its_end_and_stores_only_counts(control_plane, monkeypatch):
    import types

    from sqlalchemy import select, text

    from provisa.api.admin import table_read
    from provisa.core.schema_org import profiler_sensitive_hints, profiler_sensitive_scans

    table = {
        "id": 1,
        "columns": [
            {"column_name": "notes", "data_type": "varchar"},
            {"column_name": "amount", "data_type": "bigint"},
        ],
    }
    read = []

    async def read_as_caller(state, table_id, role_id, *, limit=None, request=None):
        read.append((table_id, role_id, limit))
        return types.SimpleNamespace(
            column_names=["notes", "amount"], rows=[(v, 10) for v in WITH_SSNS]
        )

    monkeypatch.setattr(table_read, "_in_service", lambda state, tid: table if tid == 1 else None)
    monkeypatch.setattr(table_read, "read_as_caller", read_as_caller)
    db = control_plane.org()
    try:
        async with db.acquire() as conn:
            await _registered(conn)
            scan_id = await hints.start(conn, 1, "steward@example.com")
            assert (await hints.latest_scan(conn, 1))["status"] == "running"
        await hints.run(types.SimpleNamespace(model_db=db), 1, scan_id)
        async with db.acquire() as conn:
            scan = await hints.latest_scan(conn, 1)
            found = await hints.hints_of(conn, 1)
            stored = [
                str(v)
                for t in (profiler_sensitive_scans, profiler_sensitive_hints)
                for r in (await conn.execute_core(select(t))).fetchall()
                for v in r
            ]
            scanned = (
                await conn.execute_core(text("SELECT count(*) FROM profiler_sensitive_hints"))
            ).scalar()
    finally:
        await db.close()
    assert read == [(1, "org_admin", hints.SCAN_ROWS)]
    assert scan["status"] == "succeeded" and scan["error"] is None
    assert (scan["sampled_rows"], scan["scanned_columns"]) == (10, 1)
    assert [(h["physical_column"], h["kind"], h["values_matched"]) for h in found] == [
        ("notes", "us_ssn", 3)
    ]
    assert scanned == 1
    for value in [*SSNS, *WITH_SSNS]:
        assert not any(value in s for s in stored)


async def test_a_scan_that_cannot_read_fails_saying_why(control_plane, monkeypatch):
    import types

    from provisa.api.admin import table_read

    monkeypatch.setattr(table_read, "_in_service", lambda state, tid: None)
    monkeypatch.setattr(table_read, "draft_table", lambda state, tid: None)
    db = control_plane.org()
    try:
        async with db.acquire() as conn:
            await _registered(conn)
            scan_id = await hints.start(conn, 1, None)
        await hints.run(types.SimpleNamespace(model_db=db), 1, scan_id)
        async with db.acquire() as conn:
            scan = await hints.latest_scan(conn, 1)
    finally:
        await db.close()
    assert scan["status"] == "failed"
    assert scan["error"] == "table 1 is no longer registered"


async def test_a_scan_left_running_past_its_time_reads_as_failed(control_plane):
    import datetime as dt

    from sqlalchemy import update

    from provisa.core.schema_org import profiler_sensitive_scans as ss

    db = control_plane.org()
    try:
        async with db.acquire() as conn:
            await _registered(conn)
            scan_id = await hints.start(conn, 1, None)
            long_ago = dt.datetime.now(dt.UTC) - hints.SCAN_TIMEOUT - dt.timedelta(minutes=1)
            await conn.execute_core(
                update(ss).where(ss.c.scan_id == scan_id).values(started_at=long_ago)
            )
            scan = await hints.latest_scan(conn, 1)
            again = await hints.latest_scan(conn, 1)
    finally:
        await db.close()
    assert (scan["status"], scan["error"]) == ("failed", hints.TIMED_OUT)
    assert again["status"] == "failed" and again["finished_at"] is not None


# -- a kind's own pattern for searching inside text (contained_pattern) ----------------------------

# Per kind that declares a contained_pattern: a positive (a real number as it is written in text)
# and a lookalike negative (a number that passes the kind's check but is written as the plain
# digit run an order or reference number is, or breaks the kind's stricter shape).
CONTAINED_CASES = {
    "us_ssn": ("SSN 123-45-6789 on file", "Order 123456789 shipped"),
    "payment_card": ("card 4111 1111 1111 1111", "ref 4111 1111 1111 1112"),
    "us_itin": ("ITIN 912-70-1234 filed", "Order 912701234 shipped"),
    "us_bank_routing_number": ("routing 021000021 for wires", "Order 400000008 shipped"),
    "de_steuer_id": ("Steuer-ID 65 929 970 413", "Order 65929970413 shipped"),
    "au_tfn": ("TFN 123 456 009 lodged", "Order 123456009 shipped"),
    "uk_nhs_number": ("NHS 943 476 5005 recorded", "Order 9434765005 shipped"),
    "in_aadhaar": ("Aadhaar 2341 2341 2305 linked", "Order 234123412305 shipped"),
    "nl_bsn": ("BSN 111 222 308 recorded", "Order 111222308 shipped"),
    "ca_social_insurance_number": ("SIN 123 456 709 on file", "Order 123456709 shipped"),
    "br_cpf": ("CPF 529.982.247-25 on file", "Order 52998224725 shipped"),
}


def test_every_kind_with_a_contained_pattern_has_a_case_and_no_case_lacks_one():
    declared = {k.id for k in catalogue() if k.contained_pattern is not None}
    assert declared == set(CONTAINED_CASES)


@pytest.mark.parametrize("kind_id", sorted(CONTAINED_CASES))
def test_a_contained_pattern_finds_the_written_form_and_refuses_the_lookalike(kind_id):
    kind = kind_of(kind_id)
    positive, negative = CONTAINED_CASES[kind_id]
    assert classify.contains(kind, positive) is True
    assert classify.contains(kind, negative) is False


# The plain digit run a contained_pattern no longer finds inside text is still the kind as a whole
# value: the whole-value test uses the kind's pattern, not its contained_pattern.
WHOLE_VALUES = {
    "us_ssn": "123456789",
    "us_itin": "912701234",
    "us_bank_routing_number": "400000008",
    "de_steuer_id": "65929970413",
    "au_tfn": "123456009",
    "uk_nhs_number": "9434765005",
    "in_aadhaar": "234123412305",
    "nl_bsn": "111222308",
    "ca_social_insurance_number": "123456709",
    "br_cpf": "52998224725",
}


@pytest.mark.parametrize("kind_id", sorted(WHOLE_VALUES))
def test_a_contained_pattern_leaves_the_whole_value_test_alone(kind_id):
    assert classify.matches_whole_value(kind_of(kind_id), WHOLE_VALUES[kind_id]) is True


def test_an_order_number_is_no_ssn_hint_and_a_dashed_ssn_is():
    values = ["Order 123456789 shipped", *NOTES, *NOTES]
    assert "us_ssn" not in _hints(values)
    values = ["SSN 123-45-6789 on file", *NOTES, *NOTES]
    assert _hints(values)["us_ssn"].matched == 1


@pytest.mark.parametrize(
    "text",
    ["card 4111 1111 1111 1111", "card 4111-1111-1111-1111", "card 4111111111111111"],
)
def test_a_card_written_with_spaces_hyphens_or_none_is_a_payment_card_hint(text):
    assert classify.contains(kind_of("payment_card"), text) is True
    assert "payment_card" in _hints([text, *NOTES, *NOTES])


def test_a_grouped_card_still_needs_the_luhn_check():
    assert classify.contains(kind_of("payment_card"), "card 4111-1111-1111-1112") is False
