# Copyright (c) 2026 Kenneth Stott
# Canary: 5e9b3d07-6a14-4c82-b7f5-2d0c8e4a1f93
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""One admission for every data write (provisa/compiler/write_admission.py).

A data write passes the governance stage every statement passes, and is admitted there by the
role's ``write`` right and its write grant for the operation (REQ-2074); a grant that states no
rows or check takes the role's row filter — on the rows it touches and on the rows it leaves
behind. Each governed write is run on embedded PostgreSQL (tests/governed_pg.py) and judged by
the rows it changes, not by the text governance gave it. The real-server proof, with rows read
at the source on every surface, is tests/integration/test_write_admission_every_surface.py."""

from __future__ import annotations

import pytest

from provisa.compiler.stage2 import GovernanceContext, apply_governance
from provisa.compiler.write_admission import WriteNotAdmitted
from provisa.security.masking import MaskingRule, MaskType
from provisa.security.mutation_authz import ColumnNotWritable
from tests.governed_pg import Refused, governed_database
from tests.write_governance import grants_over

_EAST = {1: "region = 'east'"}
_ORDERS = {
    "sales.orders": [
        {"id": 1, "region": "east"},
        {"id": 2, "region": "west"},
        {"id": 3, "region": "east"},
    ]
}


@pytest.fixture(scope="module")
def pg(embedded_postgres):
    return governed_database(embedded_postgres, "test_write_admission")


def _after(pg, governed: str, tables=None, **run) -> list[tuple]:
    """The orders left once ``governed`` runs over :data:`_ORDERS`."""
    return pg.written(governed, tables or _ORDERS, **run).after["sales.orders"]


def _gov(
    *,
    can_write: bool = True,
    writable: tuple[str, ...] = ("id", "region"),
    rls: dict[int, str] | None = None,
) -> GovernanceContext:
    gov = GovernanceContext()
    gov.role_id = "writer"
    gov.can_write = can_write
    gov.table_map = {"sales.orders": 1, "public.orders": 1, "orders": 1}
    gov.all_columns = {1: [("id", "integer"), ("region", "varchar")]}
    gov.write_grants = {1: grants_over("writer", writable)}
    gov.rls_rules = rls or {}
    gov.write_ops = {1: frozenset({"insert", "update", "delete"})}  # a writable source
    gov.write_returns_rows = {1: True}  # ... that returns the rows it writes
    gov.write_refused_forms = {1: frozenset()}  # ... and carries every statement form
    return gov


# --- the right ------------------------------------------------------------------------------------


@pytest.mark.parametrize(
    "sql",
    [
        "INSERT INTO sales.orders (id, region) VALUES (1, 'east')",
        "UPDATE sales.orders SET region = 'x' WHERE id = 1",
        "DELETE FROM sales.orders WHERE id = 1",
        "MERGE INTO sales.orders USING s ON sales.orders.id = s.id WHEN MATCHED THEN DELETE",
    ],
)
def test_a_role_without_the_write_right_is_refused_naming_it(sql):
    with pytest.raises(WriteNotAdmitted, match="does not hold the 'write' right"):
        apply_governance(sql, _gov(can_write=False))


def test_a_write_to_a_table_that_is_not_registered_is_refused():
    with pytest.raises(WriteNotAdmitted, match="not a registered table"):
        apply_governance("DELETE FROM sales.nope WHERE id = 1", _gov())


def test_a_read_needs_no_write_right(pg):
    governed = apply_governance("SELECT id FROM sales.orders", _gov(can_write=False, rls=_EAST))
    assert sorted(pg.answer(governed, _ORDERS)) == [(1,), (3,)]  # the read filter, as before


# --- the columns ----------------------------------------------------------------------------------


def test_an_update_needs_the_columns_it_sets(pg):
    only_region = _gov(writable=("region",))
    governed = apply_governance("UPDATE sales.orders SET region = 'x' WHERE id = 1", only_region)
    assert _after(pg, governed) == [(1, "x"), (2, "west"), (3, "east")]
    with pytest.raises(ColumnNotWritable, match="column 'id'"):
        apply_governance("UPDATE sales.orders SET id = 2 WHERE id = 1", only_region)


def test_an_insert_needs_the_columns_it_lists_or_all_when_it_lists_none(pg):
    only_region = _gov(writable=("region",))
    governed = apply_governance("INSERT INTO sales.orders (region) VALUES ('x')", only_region)
    assert (None, "x") in _after(pg, governed)
    with pytest.raises(ColumnNotWritable, match="column 'id'"):
        apply_governance("INSERT INTO sales.orders (id, region) VALUES (1, 'x')", only_region)
    with pytest.raises(ColumnNotWritable, match="column 'id'"):
        apply_governance("INSERT INTO sales.orders VALUES (1, 'x')", only_region)


def test_a_delete_needs_a_delete_grant():  # REQ-2074: not a grant on every column
    gov = _gov(writable=("region",))
    del gov.write_grants[1]["delete"]
    with pytest.raises(WriteNotAdmitted, match="holds no delete grant"):
        apply_governance("DELETE FROM sales.orders WHERE id = 1", gov)


def test_a_column_that_declares_no_writable_by_is_writable_by_nobody():
    with pytest.raises(ColumnNotWritable):
        apply_governance("UPDATE sales.orders SET region = 'x' WHERE id = 1", _gov(writable=()))


# --- the rows it may touch ------------------------------------------------------------------------


def test_the_row_filter_is_anded_into_an_update_and_a_delete(pg):
    gov = _gov(rls=_EAST)
    updated = apply_governance("UPDATE sales.orders SET id = 5 WHERE id = 1 OR id = 2", gov)
    # id 2 is west: outside the filter, so the OR does not reach it
    assert _after(pg, updated) == [(2, "west"), (3, "east"), (5, "east")]
    deleted = apply_governance("DELETE FROM sales.orders", gov)
    assert _after(pg, deleted) == [(2, "west")]


def test_the_filter_follows_the_statements_alias(pg):
    governed = apply_governance("DELETE FROM sales.orders AS o WHERE o.id < 3", _gov(rls=_EAST))
    assert _after(pg, governed) == [(2, "west"), (3, "east")]


def test_a_role_with_no_filter_writes_the_statement_as_given(pg):
    sql = "UPDATE sales.orders SET region = 'x' WHERE id < 3"
    assert _after(pg, apply_governance(sql, _gov())) == _after(pg, sql)
    assert _after(pg, sql) == [(1, "x"), (2, "x"), (3, "east")]


# --- the rows it leaves behind --------------------------------------------------------------------


def test_an_insert_outside_the_filter_is_refused_and_inside_is_admitted(pg):
    gov = _gov(rls=_EAST)
    inside = "INSERT INTO sales.orders (id, region) VALUES (4, 'east')"
    assert _after(pg, apply_governance(inside, gov)) == _after(pg, inside)
    assert (4, "east") in _after(pg, inside)
    with pytest.raises(WriteNotAdmitted, match="outside role 'writer'"):
        apply_governance("INSERT INTO sales.orders (id, region) VALUES (1, 'west')", gov)
    # every row is checked: one outside refuses the statement
    with pytest.raises(WriteNotAdmitted, match="outside role 'writer'"):
        apply_governance(
            "INSERT INTO sales.orders (id, region) VALUES (1, 'east'), (2, 'west')", gov
        )


def test_bound_values_are_checked_like_literals():
    gov = _gov(rls=_EAST)
    sql = 'INSERT INTO "public"."orders" ("id", "region") VALUES ($1, $2)'
    assert apply_governance(sql, gov, {}, [1, "east"])
    with pytest.raises(WriteNotAdmitted, match="outside role 'writer'"):
        apply_governance(sql, gov, {}, [1, "west"])
    with pytest.raises(WriteNotAdmitted, match="neither a literal nor a bound parameter"):
        apply_governance(sql, gov, {}, None)


def test_what_cannot_be_decided_before_the_write_is_refused_for_an_insert():
    gov = _gov(rls=_EAST)
    with pytest.raises(WriteNotAdmitted, match="INSERT … SELECT"):
        apply_governance(
            "INSERT INTO sales.orders (id, region) SELECT id, region FROM sales.orders", gov
        )
    with pytest.raises(WriteNotAdmitted, match="cannot be decided"):
        apply_governance("INSERT INTO sales.orders (id) VALUES (1)", gov)  # region not supplied
    with pytest.raises(WriteNotAdmitted, match="neither a literal"):
        apply_governance("INSERT INTO sales.orders (id, region) VALUES (1, lower('EAST'))", gov)


def test_a_session_variable_in_the_filter_is_the_requests():
    gov = _gov(rls={1: "region = current_setting('provisa.user_region')"})
    sql = "INSERT INTO sales.orders (id, region) VALUES (1, 'east')"
    assert apply_governance(sql, gov, {"user_region": "east"})
    with pytest.raises(WriteNotAdmitted, match="outside role"):
        apply_governance(sql, gov, {"user_region": "west"})
    with pytest.raises(WriteNotAdmitted, match="outside role"):
        apply_governance(sql, gov, {})  # bound nowhere: NULL, which admits nothing


def test_an_update_may_not_move_a_row_out_of_the_filter():
    gov = _gov(rls=_EAST)
    with pytest.raises(WriteNotAdmitted, match="the row is outside role 'writer'"):
        apply_governance("UPDATE sales.orders SET region = 'west' WHERE id = 1", gov)
    # staying inside, or changing a column the filter does not read, is admitted
    assert apply_governance("UPDATE sales.orders SET region = 'east' WHERE id = 1", gov)
    assert apply_governance("UPDATE sales.orders SET id = 9 WHERE id = 1", gov)


def test_a_new_value_computed_from_the_row_is_refused_not_applied_to_some_rows(pg):
    gov = _gov(rls=_EAST)
    with pytest.raises(WriteNotAdmitted, match="cannot be\\s+decided before writing"):
        apply_governance("UPDATE sales.orders SET region = lower(region) WHERE id = 1", gov)
    # a computed value for a column the filter does not read is the statement's own business,
    # on the rows the filter admits
    governed = apply_governance("UPDATE sales.orders SET id = id + 10 WHERE id < 3", gov)
    assert _after(pg, governed) == [(11, "east"), (2, "west"), (3, "east")]


def test_an_update_the_filter_cannot_be_decided_for_is_refused():
    # the filter reads two columns; the UPDATE sets one, so the row's own value of the other
    # would decide — not known before writing
    gov = _gov(rls={1: "region = 'east' AND id < 100"})
    with pytest.raises(WriteNotAdmitted, match="cannot be decided before writing"):
        apply_governance("UPDATE sales.orders SET region = 'east' WHERE id = 1", gov)
    assert apply_governance("UPDATE sales.orders SET region = 'east', id = 5 WHERE id = 1", gov)
    with pytest.raises(WriteNotAdmitted, match="outside role"):
        apply_governance("UPDATE sales.orders SET region = 'east', id = 500 WHERE id = 1", gov)


# --- never admitted on doubt ----------------------------------------------------------------------


def test_a_case_insensitive_match_is_decided_before_the_write():
    """sqlglot's in-process evaluator decides ILIKE (since 30.23): the supplied row is admitted when
    it matches and refused as outside the role when it does not -- never refused on doubt."""
    admitted = _gov(rls={1: "region ILIKE 'EAST'"})
    apply_governance("INSERT INTO sales.orders (id, region) VALUES (1, 'east')", admitted)
    refused = _gov(rls={1: "region ILIKE 'WEST'"})
    with pytest.raises(WriteNotAdmitted, match="outside role"):
        apply_governance("INSERT INTO sales.orders (id, region) VALUES (1, 'east')", refused)


@pytest.mark.parametrize(
    "row_filter",
    [
        "soundex(region) = 'E230'",  # a function the in-process evaluator does not have
        "region SIMILAR TO 'e%'",  # an operator it does not have
        "region ~ '^e'",
        "region = ANY(ARRAY['east', 'west'])",
        "CAST(region AS INTEGER) = 1",  # a cast that fails for the supplied value
        "region > 5",  # a comparison between the supplied text and a number
        "region IN (SELECT region FROM sales.allowed)",  # reads another table
    ],
)
def test_a_filter_that_cannot_be_evaluated_before_the_write_refuses_it(pg, row_filter):
    gov = _gov(rls={1: row_filter})
    with pytest.raises(WriteNotAdmitted, match="cannot be decided before writing"):
        apply_governance("INSERT INTO sales.orders (id, region) VALUES (1, 'east')", gov)
    with pytest.raises(WriteNotAdmitted, match="cannot be decided before writing"):
        apply_governance("UPDATE sales.orders SET region = 'east' WHERE id = 1", gov)
    # the rows it may touch are still the rows it can read: a DELETE carries the filter, and
    # deletes what a DELETE filtered by it deletes (or is refused where that one is)
    tables = {**_ORDERS, "sales.allowed": [{"region": "east"}]}
    governed = apply_governance("DELETE FROM sales.orders", gov)
    expected = f"DELETE FROM sales.orders WHERE {row_filter}"
    try:
        want = _after(pg, expected, tables)
    except Refused:
        with pytest.raises(Refused):
            _after(pg, governed, tables)
        return
    assert _after(pg, governed, tables) == want
    assert want != _ORDERS["sales.orders"]  # the filter keeps some rows from the DELETE
    # and a read of the table answers the rows the filter keeps, its own subquery its own
    read = apply_governance("SELECT id FROM sales.orders", gov)
    expected_read = f"SELECT id FROM sales.orders WHERE {row_filter}"
    assert sorted(pg.answer(read, tables)) == sorted(pg.answer(expected_read, tables))


def test_an_evaluator_failure_of_any_kind_refuses(monkeypatch):
    import sqlglot.executor

    def _broken(*_a, **_k):
        raise RuntimeError("not an executor error")

    monkeypatch.setattr(sqlglot.executor, "execute", _broken)
    with pytest.raises(WriteNotAdmitted, match="cannot be decided before writing"):
        apply_governance(
            "INSERT INTO sales.orders (id, region) VALUES (1, 'east')", _gov(rls=_EAST)
        )


def test_the_in_process_answer_decides_where_a_source_would_compare_differently():
    """A source with a case-insensitive collation holds 'EAST' = 'east'; the evaluation here is
    by exact text. The answer here decides admission and the source is not consulted, so the
    row is refused on every source alike."""
    gov = _gov(rls=_EAST)
    with pytest.raises(WriteNotAdmitted, match="outside role"):
        apply_governance("INSERT INTO sales.orders (id, region) VALUES (1, 'EAST')", gov)
    with pytest.raises(WriteNotAdmitted, match="outside role"):
        apply_governance("UPDATE sales.orders SET region = 'East' WHERE id = 1", gov)
    assert apply_governance("INSERT INTO sales.orders (id, region) VALUES (1, 'east')", gov)


def test_a_merge_by_a_filtered_role_is_refused():
    with pytest.raises(WriteNotAdmitted, match="MERGE"):
        apply_governance(
            "MERGE INTO sales.orders USING s ON sales.orders.id = s.id WHEN MATCHED THEN DELETE",
            _gov(rls=_EAST),
        )


def test_a_write_is_not_bounded_by_the_roles_row_ceiling(pg):
    """The ceiling bounds the rows a read returns. Applied to a write it failed with an error
    about having no SELECT to bound — which was, by accident, the only thing stopping a write by
    a role without ``full_results``."""
    gov = _gov()
    gov.limit_ceiling = 1
    written = pg.written(apply_governance("UPDATE sales.orders SET region = 'x'", gov), _ORDERS)
    assert written.count == 3
    assert written.after["sales.orders"] == [(1, "x"), (2, "x"), (3, "x")]
    assert len(pg.answer(apply_governance("SELECT id FROM sales.orders", gov), _ORDERS)) == 1


# --- TRUNCATE (REQ-1942) --------------------------------------------------------------------------


def test_truncate_runs_as_itself_for_a_writer_with_no_row_filter(pg):
    governed = apply_governance("TRUNCATE TABLE sales.orders", _gov())
    assert governed.upper().startswith("TRUNCATE")  # REQ-1942: never rewritten to a DELETE
    assert _after(pg, governed) == []


def test_truncate_needs_the_write_right():
    with pytest.raises(WriteNotAdmitted, match="does not hold the 'write' right"):
        apply_governance("TRUNCATE TABLE sales.orders", _gov(can_write=False))


def test_truncate_is_refused_under_a_row_filter_naming_it_and_delete():
    with pytest.raises(WriteNotAdmitted, match=r"row filter.*region = 'east'.*Use DELETE"):
        apply_governance("TRUNCATE TABLE sales.orders", _gov(rls=_EAST))


def test_truncate_empties_one_table_at_a_time():
    with pytest.raises(WriteNotAdmitted, match="one table at a time"):
        apply_governance("TRUNCATE TABLE sales.orders, sales.orders", _gov())


# --- a write reaches only readable rows (REQ-039, amended 2026-11-04) -----------------------------

_NOTED = {
    "sales.orders": [
        {"id": 1, "region": "east", "note": "a"},
        {"id": 2, "region": "west", "note": "b"},
        {"id": 3, "region": "east", "note": "c"},
    ]
}
_UNCHANGED = sorted((r["id"], r["region"], r["note"]) for r in _NOTED["sales.orders"])


def _granted_beyond_reads(*, two_steps: bool = False) -> GovernanceContext:
    """A role reading the east orders whose grants admit every row: the read rule alone keeps a
    write off the west order. ``two_steps``: its source cannot run a write led by WITH."""
    gov = _gov(writable=("id", "region", "note"), rls=_EAST)
    gov.all_columns = {1: [("id", "integer"), ("region", "varchar"), ("note", "varchar")]}
    for grant in gov.write_grants[1].values():
        grant["rows"] = "TRUE"
        grant["check"] = "TRUE"
    gov.pk_columns = {1: ["id"]}
    gov.write_refused_forms = {1: frozenset({"with_write"} if two_steps else ())}
    return gov


def _note_of(after: list[tuple], key: int) -> str | None:
    return next((note for id_, _region, note in after if id_ == key), None)


@pytest.mark.parametrize(
    "sql",
    [
        "UPDATE sales.orders SET note = 'x' WHERE id = {id}",  # governed in place
        "UPDATE sales.orders SET note = 'x' WHERE id IN (SELECT {id})",  # keyed
        "UPDATE sales.orders SET note = (SELECT 'x') WHERE id = {id}",  # keyed by its SET
    ],
)
@pytest.mark.parametrize("two_steps", [False, True])
def test_an_update_never_changes_a_row_outside_the_read_rule(pg, sql, two_steps):
    gov = _granted_beyond_reads(two_steps=two_steps)
    outside = pg.written(apply_governance(sql.format(id=2), gov), _NOTED, two_steps=two_steps)
    assert outside.after["sales.orders"] == _UNCHANGED
    inside = pg.written(apply_governance(sql.format(id=1), gov), _NOTED, two_steps=two_steps)
    assert _note_of(inside.after["sales.orders"], 1) == "x"
    assert _note_of(inside.after["sales.orders"], 2) == "b"
    # the count says of the west order what it says of no order at all
    missing = pg.written(apply_governance(sql.format(id=99), gov), _NOTED, two_steps=two_steps)
    assert (inside.count, outside.count, missing.count) == (1, 0, 0)


@pytest.mark.parametrize(
    "sql",
    [
        "DELETE FROM sales.orders WHERE id = {id}",  # governed in place
        "DELETE FROM sales.orders WHERE id IN (SELECT {id})",  # keyed
        "DELETE FROM sales.orders WHERE EXISTS (SELECT 1 WHERE {id} = sales.orders.id)",  # keyed
    ],
)
@pytest.mark.parametrize("two_steps", [False, True])
def test_a_delete_never_removes_a_row_outside_the_read_rule(pg, sql, two_steps):
    gov = _granted_beyond_reads(two_steps=two_steps)
    outside = pg.written(apply_governance(sql.format(id=2), gov), _NOTED, two_steps=two_steps)
    assert outside.after["sales.orders"] == _UNCHANGED
    inside = pg.written(apply_governance(sql.format(id=1), gov), _NOTED, two_steps=two_steps)
    assert [r[0] for r in inside.after["sales.orders"]] == [2, 3]
    missing = pg.written(apply_governance(sql.format(id=99), gov), _NOTED, two_steps=two_steps)
    assert (inside.count, outside.count, missing.count) == (1, 0, 0)


def test_a_write_reaching_every_row_reaches_only_the_readable_ones(pg):
    gov = _granted_beyond_reads()
    updated = pg.written(apply_governance("UPDATE sales.orders SET note = 'x'", gov), _NOTED)
    assert updated.count == 2
    assert updated.after["sales.orders"] == [(1, "east", "x"), (2, "west", "b"), (3, "east", "x")]
    deleted = pg.written(apply_governance("DELETE FROM sales.orders", gov), _NOTED)
    assert deleted.count == 2
    assert deleted.after["sales.orders"] == [(2, "west", "b")]


@pytest.mark.parametrize(
    "sql, refused",
    [
        ("TRUNCATE TABLE sales.orders", "TRUNCATE"),
        (
            "MERGE INTO sales.orders USING s ON sales.orders.id = s.id WHEN MATCHED THEN DELETE",
            "MERGE",
        ),
        (
            "INSERT INTO sales.orders (id, region, note) VALUES (2, 'east', 'x') "
            "ON CONFLICT (id) DO UPDATE SET note = 'x'",
            "ON CONFLICT",
        ),
    ],
)
def test_a_write_that_cannot_carry_the_read_rule_is_refused_to_a_role_whose_reads_it_narrows(
    sql, refused
):
    """A TRUNCATE empties, a MERGE and an ON CONFLICT update change, rows the role may not read,
    and none carries a predicate: refused, however wide the role's grants are."""
    with pytest.raises(WriteNotAdmitted, match=refused):
        apply_governance(sql, _granted_beyond_reads())
    unread = _granted_beyond_reads()
    unread.rls_rules = {}
    apply_governance(sql, unread)  # admitted to a role whose reads are not narrowed


# --- in two steps, where the source cannot run a write led by WITH (REQ-039) ----------------------


@pytest.mark.parametrize(
    "sql",
    [
        "UPDATE sales.orders SET note = note || '!' WHERE id IN (SELECT id FROM sales.orders)",
        "UPDATE sales.orders SET note = (SELECT max(note) FROM sales.orders) WHERE id > 0",
        "UPDATE sales.orders SET note = 'x', region = 'east' WHERE id IN (SELECT 1 UNION SELECT 2)",
        "WITH k AS (SELECT 3 AS id) DELETE FROM sales.orders WHERE id IN (SELECT id FROM k)",
        "DELETE FROM sales.orders WHERE note IN (SELECT note FROM sales.orders WHERE id <> 1)",
    ],
)
def test_two_steps_write_what_the_single_statement_writes(pg, sql):
    single = pg.written(apply_governance(sql, _granted_beyond_reads()), _NOTED)
    governed = apply_governance(sql, _granted_beyond_reads(two_steps=True))
    two = pg.written(governed, _NOTED, two_steps=True)
    assert two.after == single.after
    assert two.count == single.count


def test_two_steps_return_the_served_masked_columns_of_the_rows_written(pg):
    gov = _granted_beyond_reads(two_steps=True)
    gov.masking_rules = {
        (1, "note"): (MaskingRule(mask_type=MaskType.constant, value="***"), "varchar")
    }
    governed = apply_governance(
        "UPDATE sales.orders SET region = 'east' WHERE id IN (SELECT id FROM sales.orders) "
        "RETURNING id",
        gov,
    )
    written = pg.written(governed, _NOTED, two_steps=True)
    assert sorted(written.returned) == [(1, "east", "***"), (3, "east", "***")]
    assert written.count == 2


def test_two_steps_never_change_the_key_they_write_by():
    import sqlglot

    from provisa.pgwire.keyed_writes import split

    governed = apply_governance(
        "UPDATE sales.orders SET id = id + 1 WHERE id IN (SELECT 1)",
        _granted_beyond_reads(two_steps=True),
    )
    with pytest.raises(PermissionError, match="may not change its key"):
        split(sqlglot.parse_one(governed, read="postgres"))
