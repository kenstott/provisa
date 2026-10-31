# Copyright (c) 2026 Kenneth Stott
# Canary: 6a3df0c8-27b1-4e96-a05c-9c14e7d283bf
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""REQ-2190: a statement is told it is about some tables only where its own conditions prove every
catalog row it can use belongs to them. Anything not proved reads the model whole: being wrong here
is a missing row, so the cases that must NOT pin matter more than those that must."""

# Requirements: REQ-2190, REQ-127

from __future__ import annotations

import pytest

from provisa.pgwire.catalog_route import CatalogRequest, address
from provisa.transpiler.metadata_facts import catalog_facts

_REQUEST = CatalogRequest("reader", "provisa", catalog_facts("reader", {}), {})
_COLUMNS = (
    "SELECT a.attname FROM pg_catalog.pg_class c JOIN pg_catalog.pg_attribute a "
    "ON a.attrelid = c.oid WHERE {}"
)


def _asked(sql: str):
    return address(sql, _REQUEST)[1]


@pytest.mark.parametrize(
    ("sql", "names"),
    (
        ("SELECT relname FROM pg_class WHERE relname = 'orders'", {"orders"}),
        ("SELECT c.oid FROM pg_class c WHERE c.relname IN ('a', 'b')", {"a", "b"}),
        (_COLUMNS.format("c.relname = 'orders'"), {"orders"}),
        (
            "SELECT i.indkey, x.relname FROM pg_class c JOIN pg_index i ON i.indrelid = c.oid "
            "JOIN pg_class x ON x.oid = i.indexrelid WHERE c.relname = 'orders'",
            {"orders"},
        ),
        (
            "SELECT pg_get_constraintdef(k.oid) FROM pg_constraint k "
            "JOIN pg_class c ON c.oid = k.conrelid WHERE c.relname = 'orders'",
            {"orders"},
        ),
        (
            "SELECT 1 FROM pg_class c LEFT JOIN pg_description d ON d.objoid = c.oid "
            "WHERE c.relname = 'orders'",
            {"orders"},
        ),
        (
            "SELECT 1 FROM (SELECT oid AS o FROM pg_class WHERE relname = 'orders') x "
            "JOIN pg_attribute a ON a.attrelid = x.o",
            {"orders"},
        ),
        (
            "SELECT c.relname, EXISTS (SELECT 1 FROM pg_attribute a WHERE a.attrelid = c.oid) "
            "FROM pg_class c WHERE c.relname = 'orders'",
            {"orders"},
        ),
    ),
)
def test_a_statement_its_conditions_bind_is_pinned(sql, names):
    assert _asked(sql) == names


@pytest.mark.parametrize(
    "sql",
    (
        "SELECT relname FROM pg_class",
        # A disjunct binds no row.
        "SELECT relname FROM pg_class WHERE relname = 'a' OR relnatts = 2",
        "SELECT relname FROM pg_class c WHERE NOT c.relname = 'a'",
        "SELECT relname FROM pg_class WHERE relname <> 'a'",
        # Another read is free of the condition.
        "SELECT 1 FROM pg_class c, pg_attribute a WHERE c.relname = 'orders'",
        "SELECT 1 FROM pg_class c WHERE c.relname = 'a' UNION ALL SELECT 1 FROM pg_class c",
        "SELECT (SELECT count(*) FROM pg_attribute) FROM pg_class c WHERE c.relname = 'a'",
        # An outer join's condition binds the side it joins, never the other.
        "SELECT 1 FROM pg_class c LEFT JOIN pg_attribute a ON a.attrelid = c.oid "
        "AND c.relname = 'a'",
        "SELECT 1 FROM pg_attribute a LEFT JOIN pg_class c ON c.oid = a.attrelid "
        "AND c.relname = 'a'",
        "SELECT 1 FROM pg_class c RIGHT JOIN pg_attribute a ON a.attrelid = c.oid "
        "WHERE c.relname = 'a'",
        "SELECT 1 FROM pg_class c FULL JOIN pg_attribute a ON a.attrelid = c.oid "
        "WHERE c.relname = 'a'",
        # The other table of a foreign key is not the table asked about.
        "SELECT f.relname FROM pg_constraint k JOIN pg_class c ON c.oid = k.conrelid "
        "JOIN pg_class f ON f.oid = k.confrelid WHERE c.relname = 'orders'",
        "SELECT k.conname FROM pg_constraint k JOIN pg_class f ON f.oid = k.confrelid "
        "WHERE f.relname = 'orders'",
        # An index named goes by its table only the way the index is reached from it.
        "SELECT c.relname FROM pg_class x JOIN pg_index i ON i.indexrelid = x.oid "
        "JOIN pg_class c ON c.oid = i.indrelid WHERE x.relname = 'pk_orders'",
        # A relation that lists every table whatever it is asked.
        "SELECT 1 FROM pg_class c JOIN information_schema.columns k ON k.table_name = c.relname "
        "WHERE c.relname = 'orders'",
        # A function of a catalog asked about what no pinned read holds.
        "SELECT pg_get_constraintdef(12345) FROM pg_class c WHERE c.relname = 'a'",
        "SELECT obj_description(k.confrelid) FROM pg_constraint k "
        "JOIN pg_class c ON c.oid = k.conrelid WHERE c.relname = 'a'",
        "SELECT pg_table_is_visible(16385) FROM pg_class c WHERE c.relname = 'a'",
        # A name read through a cast is resolved by a read of the catalog.
        "SELECT a.attname FROM pg_attribute a WHERE a.attrelid = 'sales.orders'::regclass",
        # Not a literal.
        "SELECT 1 FROM pg_class c WHERE c.relname = current_user",
        "SELECT 1 FROM pg_class c WHERE c.relname LIKE 'ord%'",
        # Another relation's relname-like column.
        "SELECT 1 FROM pg_class c JOIN pg_stat_user_tables s ON s.relid = c.oid "
        "WHERE s.relname = 'orders'",
    ),
)
def test_what_is_not_proved_reads_the_model_whole(sql):
    assert _asked(sql) is None


def test_a_read_of_what_every_table_shares_needs_no_pin():
    sql = (
        "SELECT n.nspname, c.relname FROM pg_class c JOIN pg_namespace n "
        "ON n.oid = c.relnamespace WHERE c.relname = 'orders'"
    )
    assert _asked(sql) == {"orders"}


@pytest.mark.parametrize(
    ("sql", "condition"),
    (
        (_COLUMNS.format("TRUE"), "relkind <> 'i'"),
        (
            "SELECT 1 FROM pg_index i JOIN pg_class x ON x.oid = i.indexrelid",
            "relkind = 'i'",
        ),
    ),
)
def test_a_join_says_which_kind_of_pg_class_row_it_reads(sql, condition):
    assert condition in address(sql, _REQUEST)[0].replace('"', "")


@pytest.mark.parametrize(
    "sql",
    (
        "SELECT 1 FROM pg_class c LEFT JOIN pg_attribute a ON a.attrelid = c.oid",
        "SELECT 1 FROM pg_attribute a LEFT JOIN pg_class c ON c.oid = a.attrelid",
        "SELECT 1 FROM pg_class c JOIN pg_constraint k ON k.confrelid = c.oid",
        "SELECT 1 FROM pg_class c WHERE c.oid = 1 OR c.relnatts = 2",
    ),
)
def test_a_join_that_binds_no_pg_class_row_says_nothing_of_its_kind(sql):
    assert "relkind" not in address(sql, _REQUEST)[0]
