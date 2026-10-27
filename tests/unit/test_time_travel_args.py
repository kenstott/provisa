# Copyright (c) 2026 Kenneth Stott
# Canary: 7d2e4b19-3c5a-4f8e-a6b1-9e0f2c7d4a58
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""REQ-1163 (amended 2026-10-25): time travel is an argument of the relation that has versions.
A statement names when it reads each bitemporal table or view where it names it, so two
references read two times; an Iceberg or Delta table travels by snapshot (as_recorded); any other
relation refuses the arguments by name."""

from __future__ import annotations

import pytest

from provisa.mv.time_travel import Travel, lift

_KINDS = {
    "view_eod_totals": "bitemporal",
    "transactions": "bitemporal",
    "events": "snapshot",
    "accounts": "plain",
}


def _kind(table):
    return _KINDS.get(table.name)


def test_two_references_read_one_view_at_two_times():
    sql, travel = lift(
        "SELECT a.total, b.total FROM banking.view_eod_totals(as_recorded => '2026-10-01') a "
        "JOIN banking.view_eod_totals(as_recorded => '2026-10-05') b ON a.day = b.day",
        _kind,
    )
    assert "=>" not in sql
    assert "banking.view_eod_totals AS a" in sql and "banking.view_eod_totals AS b" in sql
    assert travel.at("a") == "@TIMESTAMP '2026-10-01 00:00:00.000000'"
    assert travel.at("b") == "@TIMESTAMP '2026-10-05 00:00:00.000000'"


def test_both_arguments_are_one_time_and_an_unaliased_reference_is_read_by_its_name():
    sql, travel = lift(
        "SELECT * FROM banking.transactions(as_of => '2026-09-30', as_recorded => '2026-10-01')",
        _kind,
    )
    assert sql == "SELECT * FROM banking.transactions AS transactions"
    assert travel.at("transactions") == (
        "TIMESTAMP '2026-09-30 00:00:00.000000'@TIMESTAMP '2026-10-01 00:00:00.000000'"
    )


def test_a_reference_without_arguments_reads_at_the_requests_default():
    sql, travel = lift(
        "SELECT * FROM banking.transactions(as_of => '2026-09-30') t JOIN banking.transactions u "
        "ON t.id = u.id",
        _kind,
        "TIMESTAMP '2026-01-01 00:00:00.000000'",
    )
    assert travel.at("t") == "TIMESTAMP '2026-09-30 00:00:00.000000'"
    assert travel.at("u") == "TIMESTAMP '2026-01-01 00:00:00.000000'"


def test_a_statement_without_arguments_is_unchanged():
    sql = "SELECT * FROM banking.transactions"
    assert lift(sql, _kind) == (sql, Travel())


def test_a_snapshot_travels_by_as_recorded_and_refuses_as_of():
    _, travel = lift("SELECT * FROM lake.events(as_recorded => '2026-10-01') e", _kind)
    assert travel.at("e") == "@TIMESTAMP '2026-10-01 00:00:00.000000'"
    with pytest.raises(ValueError, match="lake.events maps no valid time"):
        lift("SELECT * FROM lake.events(as_of => '2026-10-01') e", _kind)


def test_a_relation_that_keeps_no_versions_refuses_the_arguments_by_name():
    with pytest.raises(ValueError, match="banking.accounts keeps no versions"):
        lift("SELECT * FROM banking.accounts(as_of => '2026-10-01')", _kind)


def test_a_malformed_time_is_refused_naming_the_relation_and_argument():
    with pytest.raises(ValueError, match="banking.transactions: invalid as_recorded"):
        lift("SELECT * FROM banking.transactions(as_recorded => 'last tuesday')", _kind)


def test_another_argument_is_refused_by_name():
    with pytest.raises(ValueError, match="takes no argument at_least"):
        lift("SELECT * FROM banking.transactions(as_of => '2026-10-01', at_least => '1')", _kind)


def test_two_travelling_references_under_one_alias_are_refused():
    with pytest.raises(ValueError, match="share the alias t"):
        lift(
            "SELECT * FROM banking.transactions(as_of => '2026-10-01') t, "
            "(SELECT * FROM banking.transactions(as_of => '2026-10-02') t) x",
            _kind,
        )


def test_a_commands_call_is_left_to_the_command():
    sql = "SELECT * FROM active_users(as_of => '2026-10-01')"
    assert lift(sql, _kind, commands=frozenset({"active_users"})) == (sql, Travel())


def test_a_bound_time_is_written_in_and_the_other_values_renumbered():
    from provisa.mv.time_travel import bind_travel_params

    sql, params = bind_travel_params(
        "SELECT * FROM reproduce.view_exposure(as_recorded => $2) e WHERE e.desk = $1 "
        "AND e.exposure > $3",
        ["fx", "2026-10-01T17:00:00", 5],
    )
    assert params == ["fx", 5]
    assert "as_recorded => '2026-10-01T17:00:00'" in sql
    assert "e.desk = $1" in sql and "e.exposure > $2" in sql
    _, travel = lift(sql, lambda _t: "bitemporal")
    assert travel.at("e") == "@TIMESTAMP '2026-10-01 17:00:00.000000'"


def test_a_bound_time_keeps_the_relations_name_as_written():
    """The relation's call form is a function to the parser; its quoted name is kept as written,
    or a lower-case view is asked for by an upper-case name no catalog has."""
    from provisa.mv.time_travel import bind_travel_params

    sql, _ = bind_travel_params(
        'SELECT e.desk FROM "reproduce"."view_exposure"(as_of => \'2026-09-30T12:00:00\', '
        "as_recorded => $1) AS e",
        ["2026-10-01T17:00:00"],
    )
    assert '"reproduce"."view_exposure"(' in sql


def test_a_bound_time_that_is_not_text_is_refused():
    from provisa.mv.time_travel import bind_travel_params

    with pytest.raises(ValueError, match="as_of takes an ISO 8601 time"):
        bind_travel_params("SELECT * FROM t(as_of => $1) x", [5])


def test_the_audit_records_the_one_time_a_statement_reads_at():
    one = "@TIMESTAMP '2026-10-01 00:00:00.000000'"
    other = "@TIMESTAMP '2026-10-05 00:00:00.000000'"
    assert Travel(default=one).single() == one
    assert Travel(by_alias={"a": one, "b": one}).single() == one
    assert Travel(default=one, by_alias={"a": one}).single() == one
    assert Travel(by_alias={"a": one, "b": other}).single() is None
    assert Travel(default=other, by_alias={"a": one}).single() is None
