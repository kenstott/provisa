# Copyright (c) 2026 Kenneth Stott
# Canary: 3b7d1e94-6a02-4c8f-95e1-d04f2a7c6b18
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""REQ-2158 (amended 2026-10-29, A CONTRACT KEEPS ITS VERSIONS AND NAMES ITS BREAKING CHANGES), as
REQ-2180 (amended 2026-11-11, CONTRACTS ARE DERIVED FROM THE OFFERS) keeps it: every version of a
contract an offer gives on a dataset is kept, with when and by whom it was made; each records its
differences from the one before it, and one that can break a consumer is marked breaking.

A contract is derived, never saved by hand: a new version is made when a build derives terms that
differ from the stored ones (provisa.api.offer_derivation), so what a version differs by is what
its dataset, its offer, or the masks, row rules and promises its role reads under became."""

# Requirements: REQ-2158, REQ-2180

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import Any

import pytest
from pydantic import ValidationError
from sqlalchemy import insert, select, update

from provisa.api.offer_derivation import rederive_all
from provisa.compiler.supertype import holds
from provisa.core import contract_versions, model_change
from provisa.core import offer_contracts as oc
from provisa.core.database import Database
from provisa.core.models import (
    Column,
    ContractCheck,
    DataProduct,
    DataProductOffer,
    OfferTerms,
    Table,
    VersionColumn,
)
from provisa.core.repositories import data_product as data_product_repo
from provisa.core.repositories import data_product_offer as offers
from provisa.core.repositories import table as table_repo
from provisa.core.schema_org import (
    checker_targets,
    data_product_tables,
    domains,
    registered_tables,
    sources,
    table_columns,
)
from tests.unit.offer_build import Built, installed

_AT = datetime(2026, 11, 11, tzinfo=UTC)


@dataclass
class _Version:
    """What two versions of a contract are compared by: the fields it states and the checks the
    product promised. ``label`` names the version, which is no difference itself."""

    label: str
    columns: list[VersionColumn]
    checks: list[ContractCheck] = field(default_factory=list)


def _contract(version: str = "1.0.0", *columns: VersionColumn) -> _Version:
    return _Version(version, list(columns))


def _diff(before: _Version, after: _Version):
    return contract_versions.differences(before.columns, after.columns, before.checks, after.checks)


def _col(name: str, type_: str = "integer", **kw) -> VersionColumn:
    return VersionColumn(name=name, type=type_, **kw)


def _check(name: str = "row_count", definition: str = "", **kw) -> ContractCheck:
    return ContractCheck(name=name, definition=definition, **kw)


def _kinds(before, after, checks_before=(), checks_after=()) -> dict[str, bool]:
    before.checks, after.checks = list(checks_before), list(checks_after)
    return {c.kind: c.breaking for c in _diff(before, after)}


# -- the differences ------------------------------------------------------------------------------


def test_a_field_added_is_recorded_and_breaks_nothing():
    before, after = _contract("1.0.0", _col("id")), _contract("1.1.0", _col("id"), _col("note"))
    (change,) = _diff(before, after)
    assert (change.kind, change.subject, change.after, change.breaking) == (
        "field_added",
        "note",
        "integer",
        False,
    )


def test_a_field_removed_is_breaking():
    assert _kinds(
        _contract("1.0.0", _col("id"), _col("note", "text")), _contract("2.0.0", _col("id"))
    ) == {"field_removed": True}


def test_a_field_renamed_is_one_breaking_difference():
    (change,) = _diff(
        _contract("1.0.0", _col("id"), _col("note", "text")),
        _contract("2.0.0", _col("id"), _col("remark", "text")),
    )
    assert (change.kind, change.subject, change.after, change.breaking) == (
        "field_renamed",
        "note",
        "remark",
        True,
    )


def test_two_candidates_for_a_rename_are_a_removal_and_two_adds():
    # Which of the two the old field became cannot be told, so neither is called its new name.
    assert _kinds(
        _contract("1.0.0", _col("note", "text")),
        _contract("2.0.0", _col("a", "text"), _col("b", "text")),
    ) == {"field_removed": True, "field_added": False}


def test_a_type_changed_incompatibly_is_breaking():
    assert _kinds(
        _contract("1.0.0", _col("id", "bigint")), _contract("2.0.0", _col("id", "integer"))
    ) == {"field_type_changed": True}
    assert _kinds(
        _contract("1.0.0", _col("id", "integer")), _contract("2.0.0", _col("id", "text"))
    ) == {"field_type_changed": True}


def test_a_version_column_refuses_a_null_type():
    # REQ-1426 guarantees a registered column a type: a snapshot never holds one without.
    with pytest.raises(ValidationError):
        VersionColumn(name="note", type=None)


def test_a_type_widened_is_recorded_and_breaks_nothing():
    assert _kinds(
        _contract("1.0.0", _col("id", "integer")), _contract("1.1.0", _col("id", "bigint"))
    ) == {"field_type_changed": False}


def test_a_type_only_respelled_is_no_difference():
    assert (
        _kinds(_contract("1.0.0", _col("id", "INTEGER")), _contract("1.0.1", _col("id", "integer")))
        == {}
    )


@pytest.mark.parametrize(
    ("before", "after", "compatible"),
    [
        ("integer", "bigint", True),
        ("bigint", "integer", False),
        ("real", "double", True),
        ("double", "real", False),
        ("varchar(10)", "varchar(20)", True),
        ("varchar(20)", "varchar(10)", False),
        ("varchar(10)", "text", True),
        ("text", "varchar(10)", False),
        ("numeric(10, 2)", "numeric(12, 2)", True),
        ("numeric(10, 2)", "numeric(10, 3)", False),
        ("integer", "numeric(12, 0)", True),
        ("integer", "numeric(5, 0)", False),
        ("integer", "double", False),
        ("date", "timestamp", False),
        ("timestamp", "timestamp(3)", True),
        ("timestamp(3)", "timestamp", False),
        ("timestamp(3)", "timestamp(6)", True),
        ("uuid", "text", False),
        ("uuid", "UUID", True),
    ],
)
def test_a_type_holds_another_only_when_every_value_still_reads(before, after, compatible):
    assert holds(before, after) is compatible


def test_a_field_made_required_is_breaking_and_the_reverse_is_not():
    optional, required = _col("id", required=False), _col("id", required=True)
    assert _kinds(_contract("1.0.0", optional), _contract("2.0.0", required)) == {
        "field_required": True
    }
    assert _kinds(_contract("1.0.0", required), _contract("2.0.0", optional)) == {
        "field_optional": False
    }


def test_a_field_description_changed_breaks_nothing():
    assert _kinds(
        _contract("1.0.0", _col("id")), _contract("1.0.1", _col("id", description="The key"))
    ) == {"field_described": False}


# -- what an offer promises: weakened, it can break a consumer -----------------------------------
#
# REQ-2158's declared service levels are gone: what a contract promises beyond its fields and
# checks is its offer's terms (the freshness all of a product's contracts share is written once on
# the product). A promise weakened is breaking; strengthened, it is not.


def _offer_terms(**kw: Any) -> OfferTerms:
    offer = DataProductOffer(name="o", role_id="viewer", **kw)
    return oc.terms(
        offer,
        [oc.SeenColumn("id", "integer")],
        row_rules=[],
        sample_views=["orders_sample"] if offer.data_level == "sample" else [],
        checks=[],
        profiles=["orders_1_profile_columns"],
    )


@pytest.mark.parametrize(
    ("before", "after", "kind", "weakened"),
    [
        ({"duration_days": 90}, {"duration_days": 30}, "duration_changed", True),
        ({"duration_days": 30}, {"duration_days": 90}, "duration_changed", False),
        ({"duration_days": 30}, {}, "duration_changed", False),
        ({}, {"duration_days": 30}, "duration_changed", True),
        ({}, {"dq_results": False}, "dq_results_changed", True),
        ({"dq_results": False}, {}, "dq_results_changed", False),
        ({"profiles": True}, {}, "profiles_changed", True),
        ({}, {"profiles": True}, "profiles_changed", False),
        ({}, {"data_level": "sample", "sample_views": [3]}, "data_level_changed", True),
        ({"data_level": "synthetic"}, {}, "data_level_changed", False),
    ],
)
def test_a_promise_weakened_is_breaking(before, after, kind, weakened):
    changes = oc.differences(_offer_terms(**before), _offer_terms(**after))
    assert {c.kind: c.breaking for c in changes} == {kind: weakened}


def test_an_unchanged_promise_is_no_difference():
    assert oc.differences(_offer_terms(duration_days=30), _offer_terms(duration_days=30)) == []


def test_a_promised_check_removed_is_breaking():
    assert _kinds(_contract(), _contract("2.0.0"), [_check()], []) == {"check_removed": True}


def test_a_promised_check_added_breaks_nothing():
    assert _kinds(_contract(), _contract("1.1.0"), [], [_check()]) == {"check_added": False}


def test_a_check_is_known_by_its_column_as_well_as_its_name():
    assert _kinds(
        _contract(),
        _contract("1.1.0"),
        [_check("missing", column="id")],
        [_check("missing", column="id"), _check("missing", column="name")],
    ) == {"check_added": False}


@pytest.mark.parametrize(
    ("before", "after", "weakened"),
    [
        # Soda thresholds.
        ("threshold:\n  must_be_less_than: 5", "threshold:\n  must_be_less_than: 10", True),
        ("threshold:\n  must_be_less_than: 10", "threshold:\n  must_be_less_than: 5", False),
        ("threshold:\n  must_be_greater_than: 10", "threshold:\n  must_be_greater_than: 1", True),
        ("threshold:\n  must_be_greater_than: 1", "threshold:\n  must_be_greater_than: 10", False),
        # Great Expectations kwargs.
        ('{"mostly": 0.99}', '{"mostly": 0.9}', True),
        ('{"mostly": 0.9}', '{"mostly": 0.99}', False),
        ('{"max_value": 100, "min_value": 1}', '{"max_value": 200, "min_value": 1}', True),
        ('{"max_value": 100, "min_value": 5}', '{"max_value": 100, "min_value": 1}', True),
        # A bound dropped holds to nothing; a change that cannot be read as stronger can break.
        ("threshold:\n  must_be_less_than: 5", "", True),
        ("query: select 1", "query: select 2", True),
    ],
)
def test_a_promised_check_weakened_is_breaking(before, after, weakened):
    assert _kinds(
        _contract(), _contract("2.0.0"), [_check(definition=before)], [_check(definition=after)]
    ) == {"check_changed": weakened}


def test_a_check_lowered_from_fail_to_warn_is_breaking():
    assert _kinds(
        _contract(),
        _contract("2.0.0"),
        [_check(severity="fail")],
        [_check(severity="warn")],
    ) == {"check_changed": True}


def test_a_check_raised_from_warn_to_fail_breaks_nothing():
    assert _kinds(
        _contract(),
        _contract("2.0.0"),
        [_check(severity="warn")],
        [_check(severity="fail")],
    ) == {"check_changed": False}


def test_what_all_of_a_products_contracts_share_is_no_part_of_a_contracts_versions():
    # REQ-2180: the description, freshness and support are written once on the product, so
    # changing them makes no contract version: they are no term of one.
    assert not {"description", "freshness", "support"} & set(OfferTerms.model_fields)


def test_an_offers_terms_of_use_are_terms_of_its_contracts():
    # REQ-2180 (amended 2026-11-11, THE PRODUCT PAGE IS SUMMARY ROWS): each offer carries its own
    # terms of use -- purpose, usage and limitations -- shown in its contract, so changing them
    # versions the offer's contracts.
    assert {"purpose", "usage", "limitations"} <= set(OfferTerms.model_fields)


def test_a_version_label_alone_is_no_difference():
    assert _kinds(_contract("1.0.0", _col("id")), _contract("1.0.1", _col("id"))) == {}


# -- a derived contract records the version ------------------------------------------------------


def _terms_of(*columns: str, checks=()) -> OfferTerms:
    return oc.terms(
        DataProductOffer(name="o", role_id="viewer"),
        [oc.SeenColumn(c, "integer") for c in columns],
        row_rules=[],
        sample_views=[],
        checks=list(checks),
        profiles=[],
    )


def test_the_first_derivation_records_a_version_with_no_differences():
    contract = oc.recorded(None, _terms_of("id"), at=_AT)
    (first,) = contract.versions
    assert first.version == oc.FIRST_OFFER_CONTRACT_VERSION
    assert first.changes == []
    assert first.saved_at == _AT


def test_a_new_version_keeps_the_ones_before_it_and_says_how_it_differs():
    one = oc.recorded(None, _terms_of("id"), at=_AT)
    two = oc.recorded(one, _terms_of("id", "note"), at=_AT)
    assert [v.version for v in two.versions] == ["1.0.0", "1.1.0"]
    assert [c.subject for c in two.versions[-1].changes] == ["note"]


def test_deriving_what_is_stored_records_nothing():
    one = oc.recorded(None, _terms_of("id"), at=_AT)
    assert oc.recorded(one, _terms_of("id"), at=_AT).versions == one.versions


def test_a_version_records_the_promised_checks_it_was_derived_with():
    check = _check("missing", column="id")
    contract = oc.recorded(None, _terms_of("id", checks=[check]), at=_AT)
    assert contract.versions[0].terms.checks == [check]
    dropped = oc.recorded(contract, _terms_of("id"), at=_AT)
    assert [c.kind for c in dropped.versions[-1].changes] == ["check_removed"]


async def test_a_version_is_made_by_the_changes_actor():
    async with model_change.scope("save", actor=lambda: "ken@example.com"):
        contract = oc.recorded(None, _terms_of("id"), at=_AT)
    assert contract.versions[0].saved_by == "ken@example.com"


def test_a_change_no_one_made_is_the_systems():
    assert oc.recorded(None, _terms_of("id"), at=_AT).versions[0].saved_by is None


# -- the model store ------------------------------------------------------------------------------


def _table_model(*columns: str, name: str = "orders", data_type: str | None = "integer") -> Table:
    return Table(
        source_id="pg",
        domain_id="sales",
        schema_name="public",
        table_name=name,
        columns=[Column(name=c, visible_to=["viewer"], data_type=data_type) for c in columns],
    )


@pytest.fixture
async def plane(control_plane) -> Database:
    """Product p, its default offer (viewer), its member orders (id, amount)."""
    db = control_plane.org()
    async with db.acquire() as conn:
        await conn.execute_core(insert(sources).values(id="pg", type="postgresql"))
        await conn.execute_core(insert(domains).values(id="sales"))
        await data_product_repo.upsert(conn, DataProduct(id="p", domain_id="sales", name="P"))
        await table_repo.upsert(conn, _table_model("id", "amount"))
        await data_product_repo.set_members(
            conn, "p", table_ids=[await _table_id(conn)], commands=[]
        )
    return db


@pytest.fixture
def built(monkeypatch) -> Built:
    return installed(monkeypatch)


async def _table_id(conn, name: str = "orders") -> int:
    return (
        await conn.execute_core(
            select(registered_tables.c.id).where(registered_tables.c.table_name == name)
        )
    ).scalar_one()


async def _derived(conn, built: Built, name: str = "orders"):
    """A build's derivation of every offer, then the default offer's contract on ``name``."""
    await rederive_all(conn, await built.state(conn))
    (offer,) = await offers.list_offers(conn, "p")
    return (await offers.contracts(conn, offer["id"]))[await _table_id(conn, name)]


async def _member(conn, name: str) -> None:
    ids = [await _table_id(conn), await _table_id(conn, name)]
    await data_product_repo.set_members(conn, "p", table_ids=ids, commands=[])


async def test_every_version_is_kept_in_the_model_store(plane, built):
    async with plane.acquire() as conn:
        await _derived(conn, built)
        # The member loses a column; the next version states the member as it is then.
        await table_repo.upsert(conn, _table_model("id"))
        async with model_change.scope("save", actor=lambda: "ken@example.com"):
            stored = await _derived(conn, built)
    assert [v.version for v in stored.versions] == ["1.0.0", "2.0.0"]
    latest = stored.versions[-1]
    assert latest.saved_by == "ken@example.com"
    assert [(c.kind, c.subject, c.breaking) for c in latest.changes] == [
        ("field_removed", "amount", True)
    ]


async def test_a_version_states_the_members_columns(plane, built):
    async with plane.acquire() as conn:
        stored = await _derived(conn, built)
    assert [(f.name, f.type) for f in stored.versions[-1].terms.fields] == [
        ("id", "integer"),
        ("amount", "integer"),
    ]


async def test_a_table_save_that_changes_nothing_makes_no_version(plane, built):
    async with plane.acquire() as conn:
        first = await _derived(conn, built)
        await table_repo.upsert(conn, _table_model("id", "amount"))
        again = await _derived(conn, built)
    assert again.versions == first.versions


async def test_a_resave_that_omits_a_stored_columns_type_keeps_the_stored_type(plane, built):
    """REQ-1426: a table save need not restate a column's type; the contract states the type the
    save writes, the stored one, so no version is made."""
    async with plane.acquire() as conn:
        first = await _derived(conn, built)
        await table_repo.upsert(conn, _table_model("id", "amount", data_type=None))
        again = await _derived(conn, built)
    assert again.versions == first.versions


@pytest.mark.parametrize(
    "first, second",
    [
        ("numeric(10,2)", "decimal(10, 2)"),
        ("numeric(10,2)", "NUMERIC(10,2)"),
        ("money", "numeric(21,4)"),
        ("inet", "text"),
    ],
)
async def test_a_column_respelled_or_subtyped_is_no_difference_between_versions(
    plane, built, first, second
):
    """REQ-2171: a table's save stores one spelling of a type, so the same column spelled two
    ways is not a changed type."""
    async with plane.acquire() as conn:
        await table_repo.upsert(conn, _table_model("amount", name="ledger", data_type=first))
        await _member(conn, "ledger")
        before = await _derived(conn, built, "ledger")
        await table_repo.upsert(conn, _table_model("amount", name="ledger", data_type=second))
        after = await _derived(conn, built, "ledger")
    assert after.versions == before.versions


async def test_a_real_type_change_is_breaking_between_versions(plane, built):
    async with plane.acquire() as conn:
        await table_repo.upsert(
            conn, _table_model("amount", name="ledger", data_type="numeric(10,2)")
        )
        await _member(conn, "ledger")
        await _derived(conn, built, "ledger")
        await table_repo.upsert(conn, _table_model("amount", name="ledger", data_type="text"))
        stored = await _derived(conn, built, "ledger")
    assert [(c.kind, c.breaking) for c in stored.versions[-1].changes] == [
        ("field_type_changed", True)
    ]


async def test_a_malformed_numeric_is_refused_before_a_version_is_made(plane, built):
    async with plane.acquire() as conn:
        first = await _derived(conn, built)
        with pytest.raises(table_repo.NumericSizeRefused):
            await table_repo.upsert(conn, _table_model("id", "amount", data_type="numeric(0,2)"))
        assert (await _derived(conn, built)).versions == first.versions


def _held_to(monkeypatch, predicate: str) -> None:
    """The acting role's U rule on data_products is ``predicate``."""
    from provisa.core import model_row_rules

    monkeypatch.setattr(
        model_row_rules,
        "_provider",
        lambda role, table, right: (
            model_row_rules.ModelRule(predicate=predicate)
            if (table, right) == ("data_products", "U")
            else None
        ),
    )


async def _save_offer(conn) -> dict:
    return await offers.save(
        conn,
        "p",
        DataProductOffer(name="analysts", role_id="viewer", profiles=True),
        offer_id=None,
        by="pets_team",
    )


async def test_an_offer_whose_contracts_are_derived_is_held_by_its_products_u_rule(
    plane, monkeypatch
):  # REQ-2188
    from provisa.core.model_row_rules import ModelRowRefused
    from provisa.core.request_context import current_acting_role

    _held_to(monkeypatch, "id = 'someone_elses'")
    token = current_acting_role.set("pets_team")
    try:
        async with plane.acquire() as conn:
            with pytest.raises(ModelRowRefused, match="data_products"):
                await _save_offer(conn)
    finally:
        current_acting_role.reset(token)
    async with plane.acquire() as conn:
        assert [o["name"] for o in await offers.list_offers(conn, "p")] == ["default"]


async def test_an_offer_of_a_product_the_rule_admits_saves(plane, monkeypatch):  # REQ-2188
    from provisa.core.request_context import current_acting_role

    _held_to(monkeypatch, "id = 'p'")
    token = current_acting_role.set("pets_team")
    try:
        async with plane.acquire() as conn:
            await _save_offer(conn)
            names = [o["name"] for o in await offers.list_offers(conn, "p")]
    finally:
        current_acting_role.reset(token)
    assert names == ["default", "analysts"]


async def _a_check_on(conn, table_id: int, *, owner: str | None) -> None:
    await conn.execute_core(insert(sources).values(id="soda", type="soda"))
    checker = (
        await conn.execute_core(
            insert(registered_tables)
            .values(
                source_id="soda",
                domain_id="sales",
                schema_name="dq",
                table_name="orders_quality",
                dq_contract="dataset: a/b/c",
                dq_rules=[
                    {
                        "column_name": "amount",
                        "check_type": "missing",
                        "definition": "threshold:\n  level: warn",
                        "extra": "",
                    }
                ],
            )
            .returning(registered_tables.c.id)
        )
    ).scalar()
    await conn.execute_core(
        insert(checker_targets).values(table_id=checker, checked_table_id=table_id)
    )
    if owner is not None:  # REQ-1634: the checker table is a member of its owning product
        await conn.execute_core(
            insert(data_product_tables).values(product_id=owner, table_id=checker)
        )


async def test_a_version_holds_the_products_promised_checks_on_the_member(plane, built):
    async with plane.acquire() as conn:
        await _a_check_on(conn, await _table_id(conn), owner="p")
        stored = await _derived(conn, built)
    assert stored.versions[-1].terms.checks == [
        ContractCheck(
            name="missing",
            column="amount",
            severity="warn",
            definition="threshold:\n  level: warn",
        )
    ]


async def test_a_check_no_product_owns_is_no_promise(plane, built):
    async with plane.acquire() as conn:
        await _a_check_on(conn, await _table_id(conn), owner=None)
        stored = await _derived(conn, built)
    assert stored.versions[-1].terms.checks == []


async def test_a_member_column_with_no_data_type_fails_the_derivation_and_makes_no_version(
    plane, built
):
    async with plane.acquire() as conn:
        first = await _derived(conn, built)
        table_id = await _table_id(conn)
        # table_repo.upsert refuses an untyped column (REQ-1426), so a row holding none is a
        # corrupt store: the derivation fails loudly rather than state a type nobody declared.
        await conn.execute_core(
            update(table_columns)
            .where(table_columns.c.table_id == table_id, table_columns.c.column_name == "amount")
            .values(data_type=None)
        )
        with pytest.raises(ValidationError):
            await _derived(conn, built)
        (offer,) = await offers.list_offers(conn, "p")
        assert (await offers.contracts(conn, offer["id"]))[table_id] == first
