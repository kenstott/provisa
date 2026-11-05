# Copyright (c) 2026 Kenneth Stott
# Canary: 5f222ee3-26d5-4257-a917-8191317e51b2
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The client table function a catalog statement reads; a hidden column refused wherever it
resolves (REQ-039, amended 2026-11-04; REQ-2204).

pg_catalog.unnest is the client table function it is where the client table functions are
admitted (V006), and refused where none is. A governed column hidden from the role is refused
wherever it resolves: through a subquery, a join, an alias.

The psql 17 statement is the one sent in the BI replay (J595 triage, cause 8). The statements of
"a column is judged where it resolves" (A2) moved to fix/v003-a2 in the 2026-11-05 split.
"""

# Requirements: REQ-039, REQ-2204

from __future__ import annotations

import pytest
import sqlglot

from provisa.compiler.sql_types import CompilationContext
from provisa.compiler.sql_validator import (
    _check_column_visibility,
    _check_registered_relations,
)
from provisa.compiler.stage2 import GovernanceContext
from provisa.pgwire.catalog_route import CLIENT_TABLE_FUNCTIONS

# The catalog relations the statements read, each with the columns it registers; a meta table's
# visible set is its registered columns.
_CATALOG = {
    "pg_class": [
        "oid",
        "relname",
        "relnamespace",
        "relkind",
        "relchecks",
        "relhasindex",
        "relhasrules",
        "relhastriggers",
        "relrowsecurity",
        "relforcerowsecurity",
        "relispartition",
        "reloptions",
        "reltablespace",
        "reloftype",
        "relpersistence",
        "relreplident",
        "relam",
        "reltoastrelid",
        "relpartbound",
    ],
    "pg_am": ["oid", "amname"],
    "pg_policy": [
        "polname",
        "polpermissive",
        "polroles",
        "polqual",
        "polwithcheck",
        "polrelid",
        "polcmd",
    ],
    "pg_roles": ["oid", "rolname"],
    "pg_publication": ["oid", "pubname", "puballtables"],
    "pg_publication_namespace": ["pnpubid", "pnnspid"],
    "pg_publication_rel": ["prpubid", "prrelid", "prqual", "prattrs"],
    "pg_attribute": ["attrelid", "attname", "attnum"],
    "pg_inherits": ["inhrelid", "inhparent", "inhdetachpending"],
    "pg_index": [
        "indexrelid",
        "indrelid",
        "indisprimary",
        "indisunique",
        "indisclustered",
        "indisvalid",
        "indisreplident",
    ],
    "pg_constraint": ["oid", "conrelid", "conindid", "contype", "condeferrable", "condeferred"],
    "pg_type": ["oid", "typname", "typnamespace"],
    "pg_namespace": ["oid", "nspname"],
    "pg_enum": ["enumtypid"],
}
# A governed table: ``ssn`` is registered and hidden from the role.
_CUSTOMER = 100
_CUSTOMER_COLUMNS = ["id", "name", "ssn"]
_CUSTOMER_VISIBLE = frozenset({"id", "name"})


def _gov() -> GovernanceContext:
    table_map: dict[str, int] = {"bank.customer": _CUSTOMER, "customer": _CUSTOMER}
    all_columns = {_CUSTOMER: [(c, "varchar") for c in _CUSTOMER_COLUMNS]}
    visible: dict[int, frozenset[str] | None] = {_CUSTOMER: _CUSTOMER_VISIBLE}
    for tid, (name, cols) in enumerate(_CATALOG.items(), start=1):
        table_map[f"pg_catalog.{name}"] = tid
        table_map[name] = tid
        all_columns[tid] = [(c, "text") for c in cols]
        visible[tid] = frozenset(cols)
    return GovernanceContext(visible_columns=visible, table_map=table_map, all_columns=all_columns)


def _v003(sql: str) -> list[str]:
    tree = sqlglot.parse_one(sql, read="postgres")
    return [v.message for v in _check_column_visibility(tree, _gov()) if v.code == "V003"]


def _v006(sql: str) -> list[str]:
    tree = sqlglot.parse_one(sql, read="postgres")
    return [
        v.message
        for v in _check_registered_relations(
            tree, _gov(), CompilationContext(), table_functions=CLIENT_TABLE_FUNCTIONS
        )
    ]


_OID = "'16654'"

# psql 17 \d+ (statements 10/18/30): reloptions of the TOAST table through pg_catalog.unnest.
PSQL_TABLE = (
    "SELECT c.relchecks, c.relkind, c.relhasindex, c.relhasrules, c.relhastriggers, "
    "c.relrowsecurity, c.relforcerowsecurity, false AS relhasoids, c.relispartition, "
    "pg_catalog.array_to_string(c.reloptions || array(select 'toast.' || x from "
    "pg_catalog.unnest(tc.reloptions) x), ', ')\n"
    ", c.reltablespace, CASE WHEN c.reloftype = 0 THEN '' ELSE "
    "c.reloftype::pg_catalog.regtype::pg_catalog.text END, c.relpersistence, c.relreplident, "
    "am.amname\nFROM pg_catalog.pg_class c\n LEFT JOIN pg_catalog.pg_class tc ON "
    "(c.reltoastrelid = tc.oid)\nLEFT JOIN pg_catalog.pg_am am ON (c.relam = am.oid)\n"
    f"WHERE c.oid = {_OID};"
)

# -- the tools' statements: the client table function they read --------------------------------


@pytest.mark.parametrize("sql", [PSQL_TABLE], ids=["psql table (10/18/30)"])
def test_a_schema_qualified_unnest_is_the_client_table_function_it_is(sql):
    assert _v006(sql) == []


@pytest.mark.parametrize(
    "fn",
    [
        "pg_catalog.unnest(c.reloptions) x",
        "pg_catalog.unnest(c.reloptions, c.reloptions) x",
        "pg_catalog.unnest(c.reloptions) WITH ORDINALITY AS x(v, n)",
    ],
)
def test_pg_catalog_unnest_is_admitted_with_the_client_table_functions(fn):
    assert _v006(f"SELECT 1 FROM pg_catalog.pg_class c, {fn}") == []


def test_pg_catalog_unnest_is_refused_where_no_client_table_function_is_admitted():
    tree = sqlglot.parse_one("SELECT x FROM pg_catalog.unnest(ARRAY[1]) x", read="postgres")
    violations = _check_registered_relations(tree, _gov(), CompilationContext())
    assert [v.code for v in violations] == ["V006"]


def test_client_table_function_names_the_function_a_table_reads():
    from provisa.compiler.sql_validator import client_table_function

    def target(sql: str) -> sqlglot.exp.Table:
        table = sqlglot.parse_one(sql, read="postgres").find(sqlglot.exp.Table)
        assert table is not None
        return table

    assert client_table_function(target("SELECT 1 FROM pg_catalog.unnest(a) x"), {"unnest"})
    assert client_table_function(target("SELECT 1 FROM pg_catalog.json_each(a) x"), {"json_each"})
    assert not client_table_function(target("SELECT 1 FROM pg_catalog.unnest(a) x"), set())
    assert not client_table_function(target("SELECT 1 FROM read_csv('/etc/passwd') x"), {"unnest"})
    assert not client_table_function(target("SELECT 1 FROM pg_catalog.pg_class"), {"pg_class"})


# -- a governed column hidden from the role is refused wherever it resolves ------------------------


@pytest.mark.parametrize(
    "sql",
    [
        # plain, unqualified and qualified
        "SELECT ssn FROM bank.customer",
        "SELECT c.ssn FROM bank.customer c",
        # through an alias of the column
        "SELECT ssn AS tax_id FROM bank.customer",
        "SELECT c.ssn AS tax_id FROM bank.customer c",
        # through a join: unqualified, where the other relation does not have it
        "SELECT ssn FROM bank.customer c JOIN pg_catalog.pg_class k ON k.oid = c.id",
        "SELECT name, ssn FROM pg_catalog.pg_class k, bank.customer c",
        # through a table function beside it
        "SELECT ssn FROM bank.customer c, unnest(ARRAY[1]) x",
        "SELECT ssn FROM bank.customer c, generate_series(1, 3) s",
        # through a subquery: the subquery's own SELECT, and an outer reference into it
        "SELECT (SELECT ssn FROM bank.customer) FROM pg_catalog.pg_class",
        "SELECT ARRAY(SELECT ssn FROM bank.customer WHERE id = 1) FROM pg_catalog.pg_class",
        "SELECT (SELECT c.ssn) FROM bank.customer c",
        "SELECT (SELECT ssn) FROM bank.customer c",
        "SELECT (SELECT ssn FROM pg_catalog.pg_am) FROM bank.customer c",
        "SELECT (SELECT relname FROM pg_catalog.pg_class k WHERE k.relname = c.ssn) "
        "FROM bank.customer c",
        "SELECT (SELECT x FROM unnest(ARRAY[ssn]) x) FROM bank.customer c",
        "SELECT * FROM (SELECT ssn FROM bank.customer) d",
        "SELECT d.tax_id FROM (SELECT ssn AS tax_id FROM bank.customer) d",
        "WITH d AS (SELECT ssn FROM bank.customer) SELECT ssn FROM d",
        "SELECT pubname FROM pg_catalog.pg_publication UNION SELECT ssn FROM bank.customer",
    ],
)
def test_a_hidden_governed_column_is_refused_wherever_it_resolves(sql):
    assert _v003(sql), sql


@pytest.mark.parametrize(
    "sql",
    [
        # a name no relation registers, beside registered relations only: a physical column the
        # model does not serve, or none -- never passed through
        "SELECT unregistered FROM bank.customer",
        "SELECT unregistered FROM bank.customer c JOIN pg_catalog.pg_class k ON k.oid = c.id",
        "SELECT (SELECT unregistered) FROM bank.customer c",
        "SELECT unregistered FROM bank.customer c, unnest(ARRAY[1]) x",
    ],
)
def test_a_name_only_a_governed_relation_could_hold_is_refused(sql):
    assert _v003(sql), sql
