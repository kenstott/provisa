# Copyright (c) 2026 Kenneth Stott
# Canary: 3b7d1e94-6a02-4c8f-95e1-d04f2a7c6b18
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""REQ-2158 (amended 2026-10-29, A CONTRACT KEEPS ITS VERSIONS AND NAMES ITS BREAKING CHANGES):
every saved version of a contract is kept, with when and by whom it was saved; each records its
differences from the one before it, and one that can break a consumer is marked breaking."""

# Requirements: REQ-2158

from __future__ import annotations

import pytest
from sqlalchemy import insert, select

from provisa.compiler.supertype import holds
from provisa.core import contract_versions, data_contracts, model_change
from provisa.core.contract_versions import differences
from provisa.core.database import Database
from provisa.core.models import (
    Column,
    ContractCheck,
    ContractColumn,
    ContractSlaProperty,
    ContractTerms,
    DataContract,
    DataProduct,
    Table,
)
from provisa.core.repositories import data_product as data_product_repo
from provisa.core.repositories import table as table_repo
from provisa.core.schema_org import checker_targets, domains, registered_tables, sources
from provisa.core.store_config import _table


def _contract(version: str = "1.0.0", *columns: ContractColumn, **kw) -> DataContract:
    return DataContract(version=version, columns=list(columns), **kw)


def _col(name: str, type_: str = "integer", **kw) -> ContractColumn:
    return ContractColumn(name=name, type=type_, **kw)


def _sla(prop: str, value, unit: str | None = None, **kw) -> ContractSlaProperty:
    return ContractSlaProperty(property=prop, value=value, unit=unit, **kw)


def _check(name: str = "row_count", definition: str = "", **kw) -> ContractCheck:
    return ContractCheck(name=name, definition=definition, **kw)


def _kinds(before, after, checks_before=(), checks_after=()) -> dict[str, bool]:
    return {
        c.kind: c.breaking
        for c in differences(before, list(checks_before), after, list(checks_after))
    }


# -- the differences ------------------------------------------------------------------------------


def test_a_field_added_is_recorded_and_breaks_nothing():
    before, after = _contract("1.0.0", _col("id")), _contract("1.1.0", _col("id"), _col("note"))
    (change,) = differences(before, [], after, [])
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
    (change,) = differences(
        _contract("1.0.0", _col("id"), _col("note", "text")),
        [],
        _contract("2.0.0", _col("id"), _col("remark", "text")),
        [],
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


def test_a_promised_sla_removed_is_breaking():
    assert _kinds(_contract(sla=[_sla("availability", 99.9, "%")]), _contract("2.0.0")) == {
        "sla_removed": True
    }


def test_a_promised_sla_added_breaks_nothing():
    assert _kinds(_contract(), _contract("1.1.0", sla=[_sla("availability", 99.9, "%")])) == {
        "sla_added": False
    }


@pytest.mark.parametrize(
    ("prop", "before", "after", "weakened"),
    [
        ("availability", 99.9, 99.5, True),
        ("availability", 99.5, 99.9, False),
        ("retention", 90, 30, True),
        ("retention", 30, 90, False),
        ("throughput", 100, 50, True),
        ("latency", 60, 120, True),
        ("latency", 120, 60, False),
        ("errorRate", 1, 5, True),
        ("frequency", 1, 24, True),
        # A numeral as text compares as its number.
        ("availability", "99.9", "99.5", True),
        # Nothing says which way a property Provisa does not know runs, so it can break.
        ("escalationHours", 4, 2, True),
        ("availability", "high", "99.5", True),
    ],
)
def test_a_promised_sla_weakened_is_breaking(prop, before, after, weakened):
    old = _contract(sla=[_sla(prop, before, "%")])
    new = _contract("2.0.0", sla=[_sla(prop, after, "%")])
    assert _kinds(old, new) == {"sla_changed": weakened}


def test_an_sla_in_another_unit_can_break():
    # 60 s and 2 m are not compared: which is the stronger is not read from the unit.
    assert _kinds(
        _contract(sla=[_sla("latency", 60, "s")]), _contract("2.0.0", sla=[_sla("latency", 2, "m")])
    ) == {"sla_changed": True}


def test_an_sla_only_redescribed_breaks_nothing():
    assert _kinds(
        _contract(sla=[_sla("latency", 60, "s")]),
        _contract("1.0.1", sla=[_sla("latency", 60, "s", description="Measured at the reader")]),
    ) == {"sla_changed": False}


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


def test_the_contract_description_and_terms_are_differences_that_break_nothing():
    assert _kinds(_contract(), _contract("1.0.1", description="Every pet")) == {
        "description_changed": False
    }
    assert _kinds(
        _contract(terms=ContractTerms(usage="Join on id")),
        _contract("1.0.1", terms=ContractTerms(usage="Join on pet_id")),
    ) == {"terms_changed": False}


def test_a_version_or_status_alone_is_no_difference():
    assert (
        _kinds(_contract("1.0.0", _col("id")), _contract("1.0.1", _col("id"), status="active"))
        == {}
    )


# -- a save records the version -------------------------------------------------------------------


def test_the_first_save_records_a_version_with_no_differences():
    contract = contract_versions.recorded(None, _contract("0.1.0", _col("id")), [])
    (first,) = contract.versions
    assert first.contract.version == "0.1.0"
    assert first.changes == []
    assert first.saved_at is not None


def test_a_new_version_keeps_the_ones_before_it_and_says_how_it_differs():
    one = contract_versions.recorded(None, _contract("1.0.0", _col("id")), [])
    two = contract_versions.recorded(one, _contract("2.0.0", _col("id"), _col("note")), [])
    assert [v.contract.version for v in two.versions] == ["1.0.0", "2.0.0"]
    assert [c.subject for c in two.versions[-1].changes] == ["note"]


def test_saving_what_is_stored_records_nothing():
    one = contract_versions.recorded(None, _contract("1.0.0", _col("id")), [])
    again = contract_versions.recorded(one, _contract("1.0.0", _col("id")), [])
    assert again.versions == one.versions


def test_saving_the_same_version_replaces_it_and_measures_from_the_one_before():
    one = contract_versions.recorded(None, _contract("1.0.0", _col("id")), [])
    two = contract_versions.recorded(one, _contract("2.0.0", _col("id"), _col("a")), [])
    edited = contract_versions.recorded(two, _contract("2.0.0", _col("id"), _col("b")), [])
    assert [v.contract.version for v in edited.versions] == ["1.0.0", "2.0.0"]
    assert [c.subject for c in edited.versions[-1].changes] == ["b"]


def test_a_version_records_the_promised_checks_it_was_saved_with():
    check = _check("missing", column="id")
    contract = contract_versions.recorded(None, _contract("1.0.0"), [check])
    assert contract.versions[0].checks == [check]
    dropped = contract_versions.recorded(contract, _contract("2.0.0"), [])
    assert [c.kind for c in dropped.versions[-1].changes] == ["check_removed"]


async def test_a_version_is_saved_by_the_changes_actor():
    async with model_change.scope("save", actor=lambda: "ken@example.com"):
        contract = contract_versions.recorded(None, _contract("1.0.0"), [])
    assert contract.versions[0].saved_by == "ken@example.com"


def test_a_change_no_one_made_is_the_systems():
    assert contract_versions.recorded(None, _contract("1.0.0"), []).versions[0].saved_by is None


def _for_save(**kw) -> DataContract | None:
    args = {
        "product_id": "p",
        "is_checker": False,
        "schema_name": "s",
        "table_name": "t",
        "declared": None,
        "stored": None,
        "stored_product_id": None,
        "column_types": {"id": "integer"},
    }
    return data_contracts.for_save(**{**args, **kw})


def test_a_new_members_draft_is_its_first_version():
    contract = _for_save()
    assert [v.contract.version for v in contract.versions] == ["0.1.0"]


def test_a_reload_keeps_the_versions_of_a_contract_it_does_not_declare():
    first = _for_save(declared=_contract("1.0.0", _col("id", "integer")))
    stored = first.model_dump(mode="json")
    assert _for_save(stored=stored, stored_product_id="p").versions == first.versions


def test_a_declared_contract_cannot_supply_its_own_history():
    first = _for_save(declared=_contract("1.0.0", _col("id", "integer")))
    forged = _contract("2.0.0", _col("id", "integer")).model_copy(update={"versions": []})
    saved = _for_save(stored=first.model_dump(mode="json"), stored_product_id="p", declared=forged)
    assert [v.contract.version for v in saved.versions] == ["1.0.0", "2.0.0"]


def test_a_configuration_export_leaves_the_stores_record_of_versions_out():
    # REQ-1919: as created_at: a configuration declares a contract, the store records its history.
    row = {
        "source_id": "pg",
        "domain_id": "sales",
        "schema_name": "public",
        "table_name": "orders",
        "columns": [],
        "contract": _for_save().model_dump(mode="json"),
    }
    assert "versions" not in _table(row, None)["contract"]


# -- the model store ------------------------------------------------------------------------------


def _table_model(*columns: str, contract: DataContract | None = None) -> Table:
    return Table(
        source_id="pg",
        domain_id="sales",
        schema_name="public",
        table_name="orders",
        product_id="p",
        contract=contract,
        columns=[Column(name=c, visible_to=["analyst"], data_type="integer") for c in columns],
    )


@pytest.fixture
async def plane(control_plane) -> Database:
    db = control_plane.org()
    async with db.acquire() as conn:
        await conn.execute_core(insert(sources).values(id="pg", type="postgresql"))
        await conn.execute_core(insert(domains).values(id="sales"))
        await data_product_repo.upsert(conn, DataProduct(id="p", domain_id="sales", name="P"))
        await table_repo.upsert(conn, _table_model("id", "amount"))
    return db


async def _orders_id(conn) -> int:
    row = (
        await conn.execute_core(
            select(registered_tables.c.id).where(registered_tables.c.table_name == "orders")
        )
    ).fetchone()
    return row.id


async def _stored(conn) -> DataContract:
    row = (
        await conn.execute_core(
            select(registered_tables.c.contract).where(registered_tables.c.table_name == "orders")
        )
    ).fetchone()
    return DataContract.model_validate(row.contract)


async def test_save_contract_keeps_every_version_in_the_model_store(plane):
    async with plane.acquire() as conn:
        table_id = await _orders_id(conn)
        await data_product_repo.save_contract(
            conn, table_id, _contract("1.0.0", _col("id"), _col("amount"))
        )
        async with model_change.scope("save", actor=lambda: "ken@example.com"):
            await data_product_repo.save_contract(conn, table_id, _contract("2.0.0", _col("id")))
        stored = await _stored(conn)
    assert [v.contract.version for v in stored.versions] == ["0.1.0", "1.0.0", "2.0.0"]
    latest = stored.versions[-1]
    assert latest.saved_by == "ken@example.com"
    assert [(c.kind, c.subject, c.breaking) for c in latest.changes] == [
        ("field_removed", "amount", True)
    ]


async def test_a_table_save_keeps_the_versions_save_contract_made(plane):
    async with plane.acquire() as conn:
        table_id = await _orders_id(conn)
        await data_product_repo.save_contract(conn, table_id, _contract("1.0.0", _col("id")))
        await table_repo.upsert(conn, _table_model("id", "amount"))
        stored = await _stored(conn)
    assert [v.contract.version for v in stored.versions] == ["0.1.0", "1.0.0"]


async def test_a_refused_save_records_no_version(plane):
    async with plane.acquire() as conn:
        table_id = await _orders_id(conn)
        with pytest.raises(data_contracts.ContractRefused):
            await data_product_repo.save_contract(conn, table_id, _contract("1.0.0", _col("nope")))
        stored = await _stored(conn)
    assert [v.contract.version for v in stored.versions] == ["0.1.0"]


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


async def test_a_contract_is_held_by_its_products_u_rule(plane, monkeypatch):  # REQ-2188
    from provisa.core.model_row_rules import ModelRowRefused
    from provisa.core.request_context import current_acting_role

    _held_to(monkeypatch, "id = 'someone_elses'")
    token = current_acting_role.set("pets_team")
    try:
        async with plane.acquire() as conn:
            table_id = await _orders_id(conn)
            with pytest.raises(ModelRowRefused, match="data_products"):
                await data_product_repo.save_contract(
                    conn, table_id, _contract("1.0.0", _col("id"))
                )
    finally:
        current_acting_role.reset(token)
    async with plane.acquire() as conn:
        stored = await _stored(conn)
    assert [v.contract.version for v in stored.versions] == ["0.1.0"]


async def test_a_contract_of_a_product_the_rule_admits_saves(plane, monkeypatch):  # REQ-2188
    from provisa.core.request_context import current_acting_role

    _held_to(monkeypatch, "id = 'p'")
    token = current_acting_role.set("pets_team")
    try:
        async with plane.acquire() as conn:
            table_id = await _orders_id(conn)
            await data_product_repo.save_contract(conn, table_id, _contract("1.0.0", _col("id")))
            stored = await _stored(conn)
    finally:
        current_acting_role.reset(token)
    assert [v.contract.version for v in stored.versions] == ["0.1.0", "1.0.0"]


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
                product_id=owner,
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


async def test_a_version_holds_the_products_promised_checks_on_the_member(plane):
    async with plane.acquire() as conn:
        table_id = await _orders_id(conn)
        await _a_check_on(conn, table_id, owner="p")
        await data_product_repo.save_contract(
            conn, table_id, _contract("1.0.0", _col("id"), _col("amount"))
        )
        stored = await _stored(conn)
    assert stored.versions[-1].checks == [
        ContractCheck(
            name="missing",
            column="amount",
            severity="warn",
            definition="threshold:\n  level: warn",
        )
    ]


async def test_a_check_no_product_owns_is_no_promise(plane):
    async with plane.acquire() as conn:
        table_id = await _orders_id(conn)
        await _a_check_on(conn, table_id, owner=None)
        await data_product_repo.save_contract(
            conn, table_id, _contract("1.0.0", _col("id"), _col("amount"))
        )
        stored = await _stored(conn)
    assert stored.versions[-1].checks == []
