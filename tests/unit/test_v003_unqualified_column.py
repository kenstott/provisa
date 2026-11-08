# Copyright (c) 2026 Kenneth Stott
# Canary: 8c3f5e21-7a4d-4b9e-b06c-1d2e9f7a3c58
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""An unqualified column is judged at the relations in scope that have it (REQ-039, REQ-2190).

SQLAlchemy's psycopg2 dialect, at its default use_native_hstore=True, sends
``SELECT t.oid, typarray FROM pg_type t JOIN pg_namespace ns ...`` on every new connection.
``typarray`` is pg_type's and served; pg_namespace has no such column. It was refused (V003)
because an unqualified name was required of every relation in scope, so no SQLAlchemy psycopg2
client could connect (UI e2e run 42233953096). A column one relation hides is still refused
wherever it is named, and a name no relation in scope has is still refused.
"""

# Requirements: REQ-039, REQ-2190

from __future__ import annotations

import pytest
import sqlglot

from provisa.compiler.sql_validator import _check_column_visibility
from provisa.compiler.stage2 import GovernanceContext

# Meta relations with the columns they register, all served; a governed table with one hidden.
_SERVED = {
    "pg_type": ["oid", "typname", "typnamespace", "typarray"],
    "pg_namespace": ["oid", "nspname"],
}
_CUSTOMER = 100
_CUSTOMER_COLUMNS = ["id", "name", "ssn", "nspname"]
_CUSTOMER_VISIBLE = frozenset({"id", "name"})


def _gov() -> GovernanceContext:
    table_map = {"bank.customer": _CUSTOMER, "customer": _CUSTOMER}
    all_columns = {_CUSTOMER: [(c, "varchar") for c in _CUSTOMER_COLUMNS]}
    visible: dict[int, frozenset[str] | None] = {_CUSTOMER: _CUSTOMER_VISIBLE}
    for tid, (name, cols) in enumerate(_SERVED.items(), start=1):
        table_map[f"pg_catalog.{name}"] = tid
        table_map[name] = tid
        all_columns[tid] = [(c, "text") for c in cols]
        visible[tid] = frozenset(cols)
    return GovernanceContext(visible_columns=visible, table_map=table_map, all_columns=all_columns)


def _v003(sql: str) -> list[str]:
    tree = sqlglot.parse_one(sql, read="postgres")
    return [v.message for v in _check_column_visibility(tree, _gov()) if v.code == "V003"]


# psycopg2.extras.HstoreAdapter.get_oids, as SQLAlchemy's psycopg2 dialect sends it.
HSTORE_OIDS = (
    "SELECT t.oid, typarray\n"
    "FROM pg_type t JOIN pg_namespace ns\n"
    "    ON typnamespace = ns.oid\n"
    "WHERE typname = 'hstore';\n"
)


@pytest.mark.parametrize(
    "sql",
    [
        HSTORE_OIDS,
        "SELECT typarray, nspname FROM pg_catalog.pg_type t, pg_catalog.pg_namespace n "
        "WHERE n.oid = t.typnamespace",
        "SELECT name FROM bank.customer c JOIN pg_catalog.pg_type t ON t.oid = c.id",
    ],
)
def test_an_unqualified_column_is_judged_at_the_relation_that_has_it(sql):
    assert _v003(sql) == [], sql


@pytest.mark.parametrize(
    "sql",
    [
        # hidden where it resolves, beside a relation without it
        "SELECT ssn FROM bank.customer c JOIN pg_catalog.pg_type t ON t.oid = c.id",
        # two relations have the name: served by one, hidden by the other
        "SELECT nspname FROM pg_catalog.pg_namespace n, bank.customer c",
        # no relation in scope has the name
        "SELECT unregistered FROM pg_catalog.pg_type t JOIN pg_catalog.pg_namespace n "
        "ON n.oid = t.typnamespace",
    ],
)
def test_an_unqualified_column_hidden_or_unheld_is_refused(sql):
    assert _v003(sql), sql
