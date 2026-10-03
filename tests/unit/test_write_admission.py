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
role's ``write`` right, the ``writable_by`` of each column it writes, and the role's row filter —
on the rows it touches and on the rows it leaves behind. The real-server proof, with rows read
at the source on every surface, is tests/integration/test_write_admission_every_surface.py."""

from __future__ import annotations

import pytest

from provisa.compiler.stage2 import GovernanceContext, apply_governance
from provisa.compiler.write_admission import WriteNotAdmitted
from provisa.security.mutation_authz import ColumnNotWritable

_EAST = {1: "region = 'east'"}


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
    gov.writable_columns = {1: frozenset(writable)}
    gov.rls_rules = rls or {}
    gov.write_ops = {1: frozenset({"insert", "update", "delete"})}  # a writable source
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


def test_a_read_needs_no_write_right():
    governed = apply_governance("SELECT id FROM sales.orders", _gov(can_write=False, rls=_EAST))
    assert "region" in governed and "east" in governed  # the read filter, as before


# --- the columns ----------------------------------------------------------------------------------


def test_an_update_needs_the_columns_it_sets():
    only_region = _gov(writable=("region",))
    assert "SET region" in apply_governance(
        "UPDATE sales.orders SET region = 'x' WHERE id = 1", only_region
    )
    with pytest.raises(ColumnNotWritable, match="column 'id'"):
        apply_governance("UPDATE sales.orders SET id = 2 WHERE id = 1", only_region)


def test_an_insert_needs_the_columns_it_lists_or_all_when_it_lists_none():
    only_region = _gov(writable=("region",))
    assert apply_governance("INSERT INTO sales.orders (region) VALUES ('x')", only_region)
    with pytest.raises(ColumnNotWritable, match="column 'id'"):
        apply_governance("INSERT INTO sales.orders (id, region) VALUES (1, 'x')", only_region)
    with pytest.raises(ColumnNotWritable, match="column 'id'"):
        apply_governance("INSERT INTO sales.orders VALUES (1, 'x')", only_region)


def test_a_delete_removes_whole_rows_and_needs_every_column():
    with pytest.raises(ColumnNotWritable, match="column 'id'"):
        apply_governance("DELETE FROM sales.orders WHERE id = 1", _gov(writable=("region",)))


def test_a_column_that_declares_no_writable_by_is_writable_by_nobody():
    with pytest.raises(ColumnNotWritable):
        apply_governance("UPDATE sales.orders SET region = 'x' WHERE id = 1", _gov(writable=()))


# --- the rows it may touch ------------------------------------------------------------------------


def test_the_row_filter_is_anded_into_an_update_and_a_delete():
    gov = _gov(rls=_EAST)
    updated = apply_governance("UPDATE sales.orders SET id = 5 WHERE id = 1 OR id = 2", gov)
    assert updated == (
        'UPDATE sales.orders SET id = 5 WHERE (id = 1 OR id = 2) AND ("orders"."region" = \'east\')'
    )
    assert apply_governance("DELETE FROM sales.orders", gov) == (
        'DELETE FROM sales.orders WHERE ("orders"."region" = \'east\')'
    )


def test_the_filter_follows_the_statements_alias():
    governed = apply_governance("DELETE FROM sales.orders AS o WHERE o.id = 1", _gov(rls=_EAST))
    assert '("o"."region" = \'east\')' in governed


def test_a_role_with_no_filter_writes_the_statement_as_given():
    sql = "UPDATE sales.orders SET region = 'x' WHERE id = 1"
    assert apply_governance(sql, _gov()) == sql


# --- the rows it leaves behind --------------------------------------------------------------------


def test_an_insert_outside_the_filter_is_refused_and_inside_is_admitted():
    gov = _gov(rls=_EAST)
    inside = "INSERT INTO sales.orders (id, region) VALUES (1, 'east')"
    assert apply_governance(inside, gov) == inside
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
    with pytest.raises(WriteNotAdmitted, match="new values put the row outside"):
        apply_governance("UPDATE sales.orders SET region = 'west' WHERE id = 1", gov)
    # staying inside, or changing a column the filter does not read, is admitted
    assert apply_governance("UPDATE sales.orders SET region = 'east' WHERE id = 1", gov)
    assert apply_governance("UPDATE sales.orders SET id = 9 WHERE id = 1", gov)


def test_a_new_value_computed_from_the_row_is_refused_not_applied_to_some_rows():
    gov = _gov(rls=_EAST)
    with pytest.raises(WriteNotAdmitted, match="cannot be\\s+decided before writing"):
        apply_governance("UPDATE sales.orders SET region = lower(region) WHERE id = 1", gov)
    # a computed value for a column the filter does not read is the statement's own business
    governed = apply_governance("UPDATE sales.orders SET id = id + 1 WHERE id = 1", gov)
    assert governed == (
        'UPDATE sales.orders SET id = id + 1 WHERE id = 1 AND ("orders"."region" = \'east\')'
    )


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


@pytest.mark.parametrize(
    "row_filter",
    [
        "soundex(region) = 'E230'",  # a function the in-process evaluator does not have
        "region ILIKE 'EAST'",  # an operator it does not have
        "region ~ '^e'",
        "region = ANY(ARRAY['east', 'west'])",
        "CAST(region AS INTEGER) = 1",  # a cast that fails for the supplied value
        "region > 5",  # a comparison between the supplied text and a number
        "region IN (SELECT region FROM sales.allowed)",  # reads another table
    ],
)
def test_a_filter_that_cannot_be_evaluated_before_the_write_refuses_it(row_filter):
    gov = _gov(rls={1: row_filter})
    with pytest.raises(WriteNotAdmitted, match="cannot be decided before writing"):
        apply_governance("INSERT INTO sales.orders (id, region) VALUES (1, 'east')", gov)
    with pytest.raises(WriteNotAdmitted, match="cannot be decided before writing"):
        apply_governance("UPDATE sales.orders SET region = 'east' WHERE id = 1", gov)
    # the rows it may touch are still the rows it can read: a DELETE carries the filter
    assert "WHERE" in apply_governance("DELETE FROM sales.orders", gov)


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


def test_a_write_is_not_bounded_by_the_roles_row_ceiling():
    """The ceiling bounds the rows a read returns. Applied to a write it failed with an error
    about having no SELECT to bound — which was, by accident, the only thing stopping a write by
    a role without ``full_results``."""
    gov = _gov()
    gov.limit_ceiling = 100
    sql = "UPDATE sales.orders SET region = 'x' WHERE id = 1"
    assert apply_governance(sql, gov) == sql
    assert apply_governance("SELECT id FROM sales.orders", gov).endswith("LIMIT 100")
