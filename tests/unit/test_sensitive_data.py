# Copyright (c) 2026 Kenneth Stott
# Canary: 5e0d98a3-1eb3-4619-a0fd-2fa663cbc64c
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Sensitive data (REQ-1943): a column carrying a tag with the Sensitive data option -- pii, or an
organisation's own such as mnpi -- is sensitive, and only a holder of the sensitive_data right
changes how it is hidden: its column grants, role masks, fake or synthetic rule."""

# Requirements: REQ-1943

from __future__ import annotations

from types import SimpleNamespace

import pytest
from sqlalchemy import insert

from provisa.core.database import Database, create_engine_from_url
from provisa.core.schema_org import (
    domains,
    metadata,
    registered_tables,
    sources,
    table_columns,
    tag_assignments,
    tags,
)
from provisa.api.admin._hiding_guard import hiding_refusal
from provisa.security.sensitive import hiding_changes, system_sensitive_tag_ids

_STORED = {
    "visible_to": ["analyst"],
    "unmasked_to": [],
    "mask_type": None,
    "mask_pattern": None,
    "mask_replace": None,
    "mask_value": None,
    "mask_precision": None,
    "fake": None,
    "fake_stable": False,
    "synthetic_rule": None,
}


def _col(name: str, **changed) -> SimpleNamespace:
    return SimpleNamespace(name=name, **{**_STORED, **changed})


def test_the_built_in_pii_tag_is_sensitive():
    assert system_sensitive_tag_ids() == frozenset({"pii"})


def test_a_change_to_how_a_sensitive_column_is_hidden_is_named():
    stored = {
        "email": {"column_name": "email", **_STORED},
        "amount": {"column_name": "amount", **_STORED},
    }
    columns = [
        _col("email", fake="email()", visible_to=["analyst", "developer"]),
        _col("amount", fake="uniform(min=1, max=9)"),
    ]
    assert hiding_changes(stored, columns, frozenset({"email"})) == [
        "email (visible_to)",
        "email (fake)",
    ]
    # Unchanged, or reordered grants: nothing to name.
    same = [_col("email", visible_to=["analyst"])]
    assert hiding_changes(stored, same, frozenset({"email"})) == []


@pytest.fixture
async def model(tmp_path) -> Database:
    engine = create_engine_from_url(f"sqlite+pysqlite:///{tmp_path / 'org.db'}")
    with engine.begin() as conn:
        metadata.create_all(conn)
    db = Database(engine, name="org")
    async with db.acquire() as conn:
        await conn.execute_core(insert(sources).values(id="pg", type="postgresql"))
        await conn.execute_core(insert(domains).values(id="sales"))
        await conn.execute_core(
            insert(registered_tables).values(
                id=1, source_id="pg", domain_id="sales", schema_name="public", table_name="people"
            )
        )
        for name in ("email", "deal", "amount"):
            await conn.execute_core(
                insert(table_columns).values(table_id=1, column_name=name, visible_to=["analyst"])
            )
        # REQ-1943: an organisation's own sensitive tag.
        await conn.execute_core(
            insert(tags).values(id="mnpi", applies_to=["column"], sensitive=True)
        )
        for tag, column in (("pii", "email"), ("mnpi", "deal")):
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
    return db


def _table(*columns) -> SimpleNamespace:
    return SimpleNamespace(
        source_id="pg",
        schema_name="public",
        table_name="people",
        domain_id="sales",
        columns=list(columns),
    )


# REQ-1943, REQ-1944: who may change how a sensitive column is hidden is decided by the table save's
# hiding guard -- the right a role carries, paired with its domains.
_ROLES = {
    "editor": {"id": "editor", "capabilities": ["table_registration"], "domain_access": ["*"]},
    "steward": {"id": "steward", "capabilities": ["sensitive_data"], "domain_access": ["sales"]},
}
_STATE = SimpleNamespace(roles=_ROLES)


def _as(role_id: str) -> SimpleNamespace:
    return SimpleNamespace(user_id="u1", roles=[role_id])


async def _refusal(model: Database, role_id: str, table: SimpleNamespace) -> str | None:
    async with model.acquire() as conn:
        return await hiding_refusal(conn, table, identity=_as(role_id), state=_STATE, editor=True)


async def test_a_caller_without_the_right_cannot_change_a_sensitive_columns_hiding(model):
    refused = await _refusal(
        model, "editor", _table(_col("email", fake="email()"), _col("deal", mask_type="constant"))
    )
    assert refused is not None and "sensitive_data" in refused
    assert "email (fake)" in refused and "deal (mask_type)" in refused


async def test_a_column_that_is_not_sensitive_is_open_to_a_table_editor(model):
    assert (
        await _refusal(model, "editor", _table(_col("amount", fake="uniform(min=1, max=9)")))
        is None
    )


async def test_a_holder_of_the_right_may_change_it(model):
    assert await _refusal(model, "steward", _table(_col("email", fake="email()"))) is None


def test_in_test_synthetic_a_sensitive_columns_rule_may_not_copy_real_values():
    from provisa.fakes.kinds import parse
    from provisa.synthetic.plan import DatasetRefused, refuse_copied_sensitive

    def table(rule: str) -> SimpleNamespace:
        return SimpleNamespace(
            name="people", pii=frozenset({"email"}), fakes={"email": parse(rule, rule=True)}
        )

    with pytest.raises(DatasetRefused, match=r"people\.email.*sensitive_data"):
        refuse_copied_sensitive([table("categories()")])  # drawn from the profile: real values
    with pytest.raises(DatasetRefused, match=r"people\.email"):
        refuse_copied_sensitive([table("profile()")])
    refuse_copied_sensitive([table("categories((a, b))")])  # named values: none of them real
    refuse_copied_sensitive([table("email()")])
    # One declaring neither a fake nor a rule copies nothing: check_pii is what names it.
    refuse_copied_sensitive([SimpleNamespace(name="people", pii=frozenset({"email"}), fakes={})])
