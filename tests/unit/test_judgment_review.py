# Copyright (c) 2026 Kenneth Stott
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Judgments under review (REQ-2094): every judgment carries a why, stamped by the server with
who decided it, when and on what evidence; the review checks flag what dated or wrong it; the
queue is written through one function; and a finding is reaffirmed, dismissed or cleared."""

# Requirements: REQ-2094

from __future__ import annotations

import datetime as dt
from types import SimpleNamespace

import pytest
from sqlalchemy import insert, select, text

from provisa.core import model_change
from provisa.judgments import findings as fq
from provisa.judgments import staleness, why
from provisa.judgments.inventory import Judgment

AT = dt.datetime(2026, 10, 19, 12, tzinfo=dt.timezone.utc)
TODAY = AT.date()


# --- the why ------------------------------------------------------------------------------------


def test_a_why_is_checked_by_field():
    assert why.parse(
        {"reason": "pii by law", "bases": ["gdpr", "gdpr"], "review_on": "2027-01-01"}, "x"
    ) == {"reason": "pii by law", "bases": ["gdpr"], "review_on": "2027-01-01"}
    with pytest.raises(why.WhyRefused, match="no field 'because'"):
        why.parse({"because": "x"}, "x")
    with pytest.raises(why.WhyRefused, match="ISO date"):
        why.parse({"review_on": "next spring"}, "x")
    # The server's fields are recomputed, never taken as given.
    assert why.parse({"decided_by": "eve", "evidence": {"run_id": "r"}}, "x") == {}


def test_a_save_that_changes_nothing_keeps_the_decider_and_time():
    previous = {
        "reason": "r",
        "decided_by": "ann",
        "decided_at": "2026-01-01T00:00:00+00:00",
        "evidence": {"run_id": "r1"},
    }
    kept = why.stamp(previous, {"reason": "r"}, changed=False, evidence={"run_id": "r2"})
    assert kept["decided_by"] == "ann" and kept["evidence"] == {"run_id": "r1"}
    moved = why.stamp(previous, {"reason": "other"}, changed=False, evidence={"run_id": "r2"})
    assert moved["evidence"] == {"run_id": "r2"} and moved["decided_at"] != previous["decided_at"]


# --- the checks -----------------------------------------------------------------------------------


def _judgment(**why_fields) -> Judgment:
    return Judgment(
        fq.TAG,
        "tag_assignment",
        "pii@column:1:email",
        {"decided_at": "2026-01-01T00:00:00+00:00", **why_fields},
        table_id=1,
        column="email",
        extra={"object_kind": "column"},
    )


def _now(**kw) -> staleness.TableNow:
    base = dict(columns={"email": "varchar", "id": "integer"}, run_id="r2", facts={}, drifting={})
    base.update(kw)
    return staleness.TableNow(**base)


def _kinds(found) -> set[str]:
    return {f.finding for f in found}


def test_each_change_dates_a_judgment():
    evidence = {
        "run_id": "r1",
        "columns": ["email", "id"],
        "column": {"data_type": "varchar", "family": "text", "plausible_type": "email"},
    }
    j = _judgment(evidence=evidence, decided_by="ann", bases=["gdpr"], review_on="2026-10-01")
    found = staleness.check(
        j,
        _now(
            columns={"email": "text", "id": "integer"},
            facts={"email": {"data_type": "varchar", "family": "text", "plausible_type": "city"}},
            drifting={"r1": {"email"}},
        ),
        today=TODAY,
        members={"bob"},
        bases={"gdpr": dt.datetime(2026, 6, 1, tzinfo=dt.timezone.utc)},
    )
    assert _kinds(found) == {
        staleness.SCHEMA_CHANGED,  # retyped at registration
        staleness.TYPE_CHANGED,  # plausible type
        staleness.PROFILE_DRIFT,
        staleness.DECIDER_LEFT,
        staleness.BASIS_CHANGED,
        staleness.REVIEW_DUE,
    }
    by = {f.finding: f.category for f in found}
    assert by[staleness.BASIS_CHANGED] == by[staleness.REVIEW_DUE] == fq.DATED_BY_CONTEXT
    assert by[staleness.PROFILE_DRIFT] == fq.DATED_BY_DATA


def test_a_judgment_with_nothing_changed_has_no_finding():
    evidence = {"run_id": "r1", "columns": ["email", "id"], "column": {"data_type": "varchar"}}
    j = _judgment(evidence=evidence, decided_by="ann", bases=["gdpr"], review_on="2027-01-01")
    found = staleness.check(
        j,
        _now(),
        today=TODAY,
        members={"ann"},
        bases={"gdpr": dt.datetime(2025, 1, 1, tzinfo=dt.timezone.utc)},
    )
    assert found == []


def test_a_removed_column_and_an_added_one():
    gone = staleness.check(
        _judgment(evidence={}),
        _now(columns={"id": "integer"}),
        today=TODAY,
        members=set(),
        bases={},
    )
    assert _kinds(gone) == {staleness.SCHEMA_CHANGED}
    rule = Judgment(fq.ROW_RULE, "row_rule", "7", {"evidence": {"columns": ["id"]}}, table_id=1)
    added = staleness.check(rule, _now(), today=TODAY, members=set(), bases={})
    assert added[0].finding == staleness.COLUMN_ADDED and added[0].observed == {
        "columns": ["email"]
    }


def test_a_column_added_to_a_table_with_a_fake_is_undecided():
    fake = Judgment(
        fq.FAKE,
        "column",
        "1:email",
        {"evidence": {"columns": ["email"]}},
        table_id=1,
        column="email",
    )
    assert staleness.undecided_columns([fake], _now()) == [("id", {"column": "id"})]
    plain = Judgment(fq.ROW_RULE, "row_rule", "7", {"evidence": {"columns": ["email"]}}, table_id=1)
    assert staleness.undecided_columns([plain], _now()) == []


def test_an_expired_tag_assignment_is_listed():
    j = Judgment(
        fq.TAG,
        "tag_assignment",
        "deprecated@table:1",
        {},
        table_id=1,
        expires_on="2026-09-01",
        extra={"object_kind": "table"},
    )
    assert _kinds(staleness.check(j, None, today=TODAY, members=set(), bases={})) == {
        staleness.EXPIRED
    }


def test_resolving_needs_the_right_that_edits_the_decision():
    from provisa.judgments.resolution import read_rights, resolve_rights

    assert resolve_rights(fq.MASK, sensitive=True) == ("sensitive_data",)
    assert resolve_rights(fq.MASK, sensitive=False) == ("masking_config",)
    assert resolve_rights(fq.POSTURE, sensitive=False) == ("access_config",)
    assert "table_registration" in resolve_rights(fq.FAKE, sensitive=False)
    assert read_rights(True) == ("sensitive_data",) and read_rights(False) == ("view_governance",)


def test_a_credential_reference_is_not_plain_text():
    from provisa.judgments.posture import is_reference

    assert is_reference("${secret:warehouse_pw}") and is_reference("${env:PG_PASSWORD}")
    assert not is_reference("hunter2") and not is_reference("x ${secret:a}")


# --- the store ------------------------------------------------------------------------------------


async def _seed(conn) -> int:
    from provisa.core.schema_org import domains, registered_tables, roles, sources, table_columns

    await conn.execute_core(insert(domains).values(id="crm", description="crm"))
    await conn.execute_core(
        insert(sources).values(id="s1", type="postgres", password_ref="hunter2")
    )
    await conn.execute_core(
        insert(roles).values(id="reach_all", capabilities=[], domain_access=["*"])
    )
    tid = (
        await conn.execute_core(
            insert(registered_tables)
            .values(source_id="s1", domain_id="crm", schema_name="public", table_name="customers")
            .returning(registered_tables.c.id)
        )
    ).scalar_one()
    for name in ("email", "ssn"):
        await conn.execute_core(
            insert(table_columns).values(
                table_id=tid, domain_id="crm", column_name=name, data_type="varchar"
            )
        )
    return tid


async def test_the_queue_is_written_once_per_key_and_cleared_when_unseen(control_plane):
    db = control_plane.org()
    try:
        async with db.acquire() as conn:
            later = AT + dt.timedelta(hours=1)
            kw = dict(
                object_type="column",
                object_key="1:email",
                judgment=fq.FAKE,
                category=fq.WRONG,
                finding="leak_rate",
                observed={"measure": 1.0},
            )
            key = await fq.record_finding(conn, at=AT, **kw)
            await fq.record_finding(conn, at=later, **kw)
            (row,) = (await conn.execute_core(select(fq.jf))).fetchall()
            assert row.first_seen == AT and row.last_seen == later
            assert (
                await fq.clear_unseen(
                    conn, [key], judgments=(fq.FAKE,), findings=("leak_rate",), at=later
                )
                == 0
            )
            assert (
                await fq.clear_unseen(
                    conn, [], judgments=(fq.FAKE,), findings=("leak_rate",), at=later
                )
                == 1
            )
            assert await fq.open_findings(conn) == []
            # Seen again after it cleared: open again, from now.
            reopened = AT + dt.timedelta(hours=2)
            await fq.record_finding(conn, at=reopened, **kw)
            (found,) = await fq.open_findings(conn)
            assert found["first_seen"] == reopened and found["resolution"] is None
    finally:
        await db.close()


async def test_a_judgment_saved_is_stamped_and_reaffirmed(control_plane):
    from provisa.core.models import TagAssignment
    from provisa.core.repositories import tag as tag_repo
    from provisa.core.schema_org import tag_assignments
    from provisa.judgments.bases import put_basis
    from provisa.judgments.resolution import reaffirm

    db = control_plane.org()
    try:
        async with model_change.scope("why", actor=lambda: "ann"), db.acquire() as conn:
            tid = await _seed(conn)
            await put_basis(conn, "gdpr", name="GDPR", description=None, owner="legal")
            tag = TagAssignment(
                tag_id="pii",
                object_type="column",
                table_id=tid,
                column_name="email",
                reason="holds addresses",
                why={"purpose": "keep addresses out of test data", "bases": ["gdpr"]},
            )
            await tag_repo.assign(conn, tag)
            row = (await conn.execute_core(select(tag_assignments))).fetchone()._mapping
            assert row["decided_by"] == "ann" and row["decided_at"] is not None
            assert row["purpose"] == "keep addresses out of test data" and row["bases"] == ["gdpr"]
            first = row["decided_at"]
            # A save that does not restate the why keeps it, and its decider and time.
            await tag_repo.assign(conn, tag.model_copy(update={"why": None}))
            row = (await conn.execute_core(select(tag_assignments))).fetchone()._mapping
            assert row["decided_at"] == first and row["bases"] == ["gdpr"]
            # An unknown basis is refused by name.
            with pytest.raises(why.WhyRefused, match="no basis 'ccpa'"):
                await tag_repo.assign(conn, tag.model_copy(update={"why": {"bases": ["ccpa"]}}))
            key = await fq.record_finding(
                conn,
                object_type="tag_assignment",
                object_key=f"pii@column:{tid}:email",
                judgment=fq.TAG,
                category=fq.DATED_BY_CONTEXT,
                finding="review_due",
                observed={},
                at=AT,
            )
            (finding,) = await fq.open_findings(conn)
            assert finding["object_key"] == key.object_key
            assert await reaffirm(conn, finding, by="bob", at=AT) == 1
            row = (await conn.execute_core(select(tag_assignments))).fetchone()._mapping
            assert row["decided_by"] == "bob" and row["decided_at"] == AT
            assert await fq.open_findings(conn) == []
    finally:
        await db.close()


async def test_a_basis_declared_changed_dates_its_judgments(control_plane):
    from provisa.core.models import TagAssignment
    from provisa.core.repositories import tag as tag_repo
    from provisa.judgments.bases import declare_changed, put_basis
    from provisa.judgments.review import review_org

    db = control_plane.org()
    try:
        async with model_change.scope("why", actor=lambda: "ann"), db.acquire() as conn:
            tid = await _seed(conn)
            await put_basis(conn, "gdpr", name="GDPR", description=None, owner=None)
            await tag_repo.assign(
                conn,
                TagAssignment(
                    tag_id="pii",
                    object_type="column",
                    table_id=tid,
                    column_name="ssn",
                    why={"bases": ["gdpr"]},
                ),
            )
            changed = dt.datetime.now(dt.timezone.utc) + dt.timedelta(seconds=1)
            await declare_changed(conn, "gdpr", note="new guidance", by="lee", at=changed)
        state = SimpleNamespace(model_db=db, admin_db=control_plane.platform, auth_config={})
        async with model_change.scope("review"):
            counts = await review_org(state, "default", basis_id="gdpr")
        assert counts["recorded"] >= 1
        async with db.acquire() as conn:
            found = {(f["judgment"], f["finding"]) for f in await fq.open_findings(conn)}
        assert (fq.TAG, staleness.BASIS_CHANGED) in found
        assert (fq.TAG, staleness.DECIDER_LEFT) in found  # ann is no member of the org
    finally:
        await db.close()


async def test_the_whole_review_lists_undecided_columns_and_posture(control_plane):
    from provisa.core.models import TagAssignment
    from provisa.core.repositories import tag as tag_repo
    from provisa.judgments import posture
    from provisa.judgments.review import NO_FAKE, review_org

    db = control_plane.org()
    try:
        async with model_change.scope("why"), db.acquire() as conn:
            tid = await _seed(conn)
            await tag_repo.assign(
                conn,
                TagAssignment(tag_id="pii", object_type="column", table_id=tid, column_name="ssn"),
            )
        state = SimpleNamespace(model_db=db, admin_db=control_plane.platform, auth_config=None)
        async with model_change.scope("review"):
            await review_org(state, "default")
        async with db.acquire() as conn:
            found = {
                (f["category"], f["finding"], f["object_key"]) for f in await fq.open_findings(conn)
            }
        assert (fq.UNDECIDED, NO_FAKE, f"{tid}:ssn") in found
        assert (fq.WORTH_A_LOOK, posture.PLAIN_TEXT_CREDENTIAL, "source:s1") in found
        assert (fq.WORTH_A_LOOK, posture.UNBOUNDED_DOMAIN_ACCESS, "role:reach_all") in found
        assert (fq.WORTH_A_LOOK, posture.UNSECURED_AUTH, "deployment:auth") in found
        async with db.acquire() as conn:
            rows = (
                await conn.execute_core(
                    text("SELECT 1 FROM judgment_findings WHERE resolved_at IS NULL")
                )
            ).fetchall()
        assert rows
    finally:
        await db.close()


async def test_a_posture_finding_is_dismissed_with_a_note(control_plane):
    from provisa.judgments.resolution import ResolveRefused, dismiss, reaffirm

    db = control_plane.org()
    try:
        async with db.acquire() as conn:
            await fq.record_finding(
                conn,
                object_type="posture",
                object_key="deployment:auth",
                judgment=fq.POSTURE,
                category=fq.WORTH_A_LOOK,
                finding="unsecured_auth",
                observed={},
                at=AT,
            )
            (finding,) = await fq.open_findings(conn)
            with pytest.raises(ResolveRefused, match="note"):
                await dismiss(conn, finding, note="  ", by="ann", at=AT)
            with pytest.raises(ResolveRefused, match="dismissed with a note"):
                await reaffirm(conn, finding, by="ann", at=AT)
            await dismiss(conn, finding, note="a demo laptop", by="ann", at=AT)
            assert await fq.open_findings(conn) == []
    finally:
        await db.close()


async def test_a_fake_and_a_mask_entry_carry_their_why(control_plane):
    from provisa.core.models import Column, MaskEntry, Table
    from provisa.core.repositories import table as table_repo
    from provisa.core.schema_org import domains, sources, table_columns

    with control_plane.engine().begin() as c:
        c.exec_driver_sql('SET LOCAL search_path TO "org_default"')
        c.execute(insert(domains).values(id="d", description=""))
        c.execute(insert(sources).values(id="__derived__", type="postgresql"))

    def table(fake_why, mask_why) -> Table:
        return Table(
            source_id="__derived__",
            domain_id="d",
            schema_name="s",
            table_name="people",
            view_sql="SELECT 1",
            columns=[
                Column(
                    name="email",
                    data_type="text",
                    visible_to=[],
                    fake="email()",
                    fake_why=fake_why,
                    masks=[MaskEntry(to=["*"], mask=None, why=mask_why)],
                )
            ],
        )

    async with (
        model_change.scope("why", actor=lambda: "ann"),
        control_plane.org().acquire() as conn,
    ):
        await table_repo.upsert(conn, table({"reason": "an address"}, {"purpose": "readable"}))
        fake_why, masks = (
            await conn.execute_core(select(table_columns.c.fake_why, table_columns.c.masks))
        ).fetchone()
        assert fake_why["reason"] == "an address" and fake_why["decided_by"] == "ann"
        assert masks[0]["why"]["purpose"] == "readable" and masks[0]["why"]["decided_by"] == "ann"
        # Saved again without the why (an editor that does not send it): the why stands.
        await table_repo.upsert(conn, table(None, None))
        again, masks_again = (
            await conn.execute_core(select(table_columns.c.fake_why, table_columns.c.masks))
        ).fetchone()
        assert again == fake_why and masks_again[0]["why"] == masks[0]["why"]


async def test_the_ops_view_joins_each_finding_to_its_why(control_plane):
    from provisa.api._meta_views import _OPS_REPORT_VIEWS
    from provisa.api.startup_seed import _adapt_view_ddl
    from provisa.core.models import TagAssignment
    from provisa.core.repositories import tag as tag_repo

    db = control_plane.org()
    try:
        async with model_change.scope("why", actor=lambda: "ann"), db.acquire() as conn:
            tid = await _seed(conn)
            await tag_repo.assign(
                conn,
                TagAssignment(
                    tag_id="pii",
                    object_type="column",
                    table_id=tid,
                    column_name="email",
                    reason="addresses",
                    why={"purpose": "out of test data"},
                ),
            )
            await fq.record_finding(
                conn,
                object_type="tag_assignment",
                object_key=f"pii@column:{tid}:email",
                judgment=fq.TAG,
                category=fq.DATED_BY_CONTEXT,
                finding="review_due",
                observed={"review_on": "2026-10-01"},
                at=AT,
            )
            await conn.execute(_adapt_view_ddl(_OPS_REPORT_VIEWS["judgment_review"]))
            (row,) = (
                await conn.execute_core(
                    text(
                        "SELECT finding, reason, purpose, decided_by, domain_id FROM judgment_review"
                    )
                )
            ).fetchall()
        assert tuple(row) == ("review_due", "addresses", "out of test data", "ann", "crm")
    finally:
        await db.close()


async def test_a_failed_adequacy_check_is_a_wrong_finding(control_plane):
    from provisa.fakes import adequacy

    db = control_plane.org()
    try:
        async with db.acquire() as conn:
            row = {
                "table_id": 1,
                "column_name": "email",
                "check_name": adequacy.KIND_MISMATCH,
                "status": adequacy.FAILED,
                "measure": None,
                "detail": "the column plausibly holds email values, and its fake makes city",
            }
            await adequacy._record_findings(conn, [row], AT)
            (found,) = await fq.open_findings(conn)
            assert (found["category"], found["finding"]) == (fq.WRONG, adequacy.KIND_MISMATCH)
            # The check passes on the next run: the finding clears.
            await adequacy._record_findings(conn, [{**row, "status": adequacy.PASSED}], AT)
            assert await fq.open_findings(conn) == []
    finally:
        await db.close()
