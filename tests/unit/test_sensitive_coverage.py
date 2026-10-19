# Copyright (c) 2026 Kenneth Stott
# Canary: f5ec31a5-c0dc-40d0-ad0e-92488ce920f5
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Sensitive-column coverage (REQ-2091): the sensitive_coverage meta view lists every sensitive
column with its fake and synthetic rule and whether it is covered, has no fake, or failed an
adequacy check; and the three adequacy checks flag a bad fake or rule and pass a good one, without
any real value in what they return."""

# Requirements: REQ-2091

from __future__ import annotations

import dataclasses
import datetime as dt

import pytest

from provisa.fakes import adequacy
from provisa.fakes.kinds import parse

# --- the checks, on crafted rules ------------------------------------------------------------------

REAL_EMAILS = [f"person{i}@realcorp.example" for i in range(40)]


@pytest.fixture
def platform_key():
    from provisa.fakes import digest

    held = digest._key
    digest.set_key(b"adequacy-test-key-0123456789abcdef")
    yield
    digest._key = held


def _leak(declaration: str, data_type: str, values: list) -> adequacy.Result:
    kind = parse(declaration)
    pairs = adequacy.fake_pairs("c", data_type, kind, False, None, values)
    return adequacy.leak_rate(kind, pairs, threshold=0.001)


def _says_no_value(result: adequacy.Result, values: list) -> None:
    shown = " ".join(str(v) for v in dataclasses.astuple(result))
    for value in values:
        assert str(value) not in shown


def test_a_fake_that_replaces_every_value_passes_the_leak_rate(platform_key):
    result = _leak("email()", "varchar", REAL_EMAILS)
    assert result.status == adequacy.PASSED and result.measure == 0.0
    _says_no_value(result, REAL_EMAILS)


def test_a_fake_that_leaves_values_unchanged_fails_the_leak_rate(platform_key):
    # A prefix as long as the values keeps each one whole.
    codes = [f"ac{i:03d}" for i in range(40)]
    result = _leak("prefix(10)", "varchar", codes)
    assert result.status == adequacy.FAILED and result.measure == 1.0
    assert "above the threshold" in result.detail and "keep the part prefix keeps" in result.detail
    _says_no_value(result, codes)


def test_a_truncation_reports_the_share_left_whole(platform_key):
    days = [dt.date(2024, 1, 1), dt.date(2024, 2, 1), dt.date(2024, 3, 17), dt.date(2024, 4, 9)]
    result = _leak("truncate('month')", "date", days)
    assert result.status == adequacy.FAILED and result.measure == pytest.approx(0.5)
    _says_no_value(result, days)


def test_a_kind_against_what_the_column_plausibly_holds():
    bad = adequacy.kind_mismatch("city()", "email", 0.95)
    assert bad.status == adequacy.FAILED and "email" in bad.detail and "city" in bad.detail
    assert adequacy.kind_mismatch("email()", "email", 0.95).status == adequacy.PASSED
    # Not an identifying kind, or an unconfident profile: nothing to compare.
    assert adequacy.kind_mismatch("hash()", "email", 0.95).status == adequacy.PASSED
    assert adequacy.kind_mismatch("city()", "email", 0.2).status == adequacy.PASSED
    assert adequacy.kind_mismatch("city()", None, None).status == adequacy.NOT_RUN


def test_a_rule_that_reads_a_sensitive_column_real_is_flagged():
    unfaked = frozenset({"ssn"})
    copied = adequacy.copies_real("sql(ssn || '-x')", None, unfaked)
    assert copied.status == adequacy.FAILED and "ssn" in copied.detail
    rule = adequacy.copies_real(None, "sql(ssn)", unfaked)
    assert rule.status == adequacy.FAILED and "synthetic rule" in rule.detail
    # Naming a column that is faked, or no column, reads no real value.
    assert adequacy.copies_real("sql(ssn)", None, frozenset()).status == adequacy.PASSED
    assert adequacy.copies_real("email()", "categories()", unfaked).status == adequacy.PASSED


# --- the view ---------------------------------------------------------------------------------------


async def _seed(conn) -> None:
    from sqlalchemy import insert

    from provisa.core.schema_org import (
        domains,
        registered_tables,
        sources,
        table_columns,
        tag_assignments,
        tags,
    )

    await conn.execute_core(insert(domains).values(id="crm", description="crm"))
    await conn.execute_core(insert(sources).values(id="s1", type="postgres"))
    await conn.execute_core(
        insert(registered_tables).values(
            id=1, source_id="s1", domain_id="crm", schema_name="public", table_name="customers"
        )
    )
    columns = {
        "email": "email()",  # pii, faked
        "ssn": None,  # pii, no fake
        "deal": None,  # mnpi (an org tag with the sensitive option), no fake
        "region": None,  # tagged, but not sensitive
        "name": None,  # not tagged
    }
    for name, fake in columns.items():
        await conn.execute_core(
            insert(table_columns).values(
                table_id=1, domain_id="crm", column_name=name, data_type="varchar", fake=fake
            )
        )
    await conn.execute_core(insert(tags).values(id="mnpi", description="mnpi", sensitive=True))
    await conn.execute_core(insert(tags).values(id="geo", description="geo", sensitive=False))
    for column, tag in (("email", "pii"), ("ssn", "pii"), ("deal", "mnpi"), ("region", "geo")):
        await conn.execute_core(
            insert(tag_assignments).values(
                tag_id=tag,
                base_tag_id=tag,
                object_type="column",
                table_id=1,
                column_name=column,
                object_key=f"column:1:{column}",
            )
        )


async def test_the_view_lists_every_sensitive_column_and_its_coverage(control_plane):
    from sqlalchemy import insert, text, update

    from provisa.api._meta_views import _META_TABLE_VIEWS
    from provisa.api.startup_seed import _adapt_view_ddl
    from provisa.core.env_data import uncovered_sensitive
    from provisa.core.environments import org_schema
    from provisa.core.schema_org import fake_adequacy_checks, table_columns

    from tests.pg_control_plane import ORG

    db = control_plane.org()
    try:
        async with db.acquire() as conn:
            await _seed(conn)
            await conn.execute(_adapt_view_ddl(_META_TABLE_VIEWS["sensitive_coverage"]))

            async def coverage() -> dict[str, tuple]:
                rows = await conn.execute_core(
                    text(
                        "SELECT column_name, tags, status, has_fake, fake_kind, "
                        "fake_derives_from_real, reason FROM sensitive_coverage"
                    )
                )
                return {r[0]: tuple(r[1:]) for r in rows.fetchall()}

            seen = await coverage()
            assert set(seen) == {"email", "ssn", "deal"}
            assert seen["email"][:4] == ("pii", "covered", True, "email")
            assert seen["ssn"][1:3] == ("no_fake", False)
            assert seen["deal"][:2] == ("mnpi", "no_fake")

            # The view and the environment's Test (fake) choice read the same columns.
            no_fake = sorted(f"customers.{c}" for c, v in seen.items() if v[1] == "no_fake")
            assert await uncovered_sensitive(conn, org_schema(ORG)) == no_fake

            # Declaring a fake covers the column.
            await conn.execute_core(
                update(table_columns)
                .where(table_columns.c.table_id == 1, table_columns.c.column_name == "ssn")
                .values(fake="hash()")
            )
            seen = await coverage()
            assert seen["ssn"][1] == "covered" and seen["ssn"][4] is True

            # A failed check -- its latest run -- makes the column inadequately covered.
            earlier = dt.datetime(2026, 10, 1, tzinfo=dt.timezone.utc)
            later = dt.datetime(2026, 10, 2, tzinfo=dt.timezone.utc)
            for at, status in ((earlier, "passed"), (later, "failed")):
                await conn.execute_core(
                    insert(fake_adequacy_checks).values(
                        table_id=1,
                        column_name="email",
                        check_name="kind_mismatch",
                        checked_at=at,
                        status=status,
                        detail="the column plausibly holds email values, and its fake makes city",
                    )
                )
            seen = await coverage()
            assert seen["email"][1] == "inadequate"
            assert seen["email"][5].startswith("kind_mismatch: the column plausibly holds email")
    finally:
        await db.close()
