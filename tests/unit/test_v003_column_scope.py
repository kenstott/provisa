# Copyright (c) 2026 Kenneth Stott
# Canary: 5f222ee3-26d5-4257-a917-8191317e51b2
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A column is judged where it resolves (REQ-039, amended 2026-11-04; REQ-2204).

The column check (V003) resolves each unqualified column within its own SELECT scope to the
in-scope registered relation that has it, and refuses it only when that relation does not serve
the column to the role. A nested subquery's columns are judged in its own scope; an outer
reference where it resolves. A name no registered relation in scope has -- a table function's
alias or column, a derived table's column -- is passed to the engine. A governed column hidden
from the role is still refused wherever it resolves: through a subquery, a join, an alias.

The statements are the ones psql 17 and Metabase sent in the BI replay (J595 triage, causes 1
and 8), each answered by the catalog's registered relations.
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
# psql 17 \d+ (statements 12/23/35): policies, the role names in a nested ARRAY(SELECT ...).
PSQL_POLICIES = (
    "SELECT pol.polname, pol.polpermissive,\n  CASE WHEN pol.polroles = '{0}' THEN NULL ELSE "
    "pg_catalog.array_to_string(array(select rolname from pg_catalog.pg_roles where oid = any "
    "(pol.polroles) order by 1),',') END,\n  pg_catalog.pg_get_expr(pol.polqual, pol.polrelid),\n"
    "  pg_catalog.pg_get_expr(pol.polwithcheck, pol.polrelid),\n  CASE pol.polcmd\n"
    "    WHEN 'r' THEN 'SELECT'\n    WHEN 'a' THEN 'INSERT'\n    WHEN 'w' THEN 'UPDATE'\n"
    "    WHEN 'd' THEN 'DELETE'\n    END AS cmd\nFROM pg_catalog.pg_policy pol\n"
    f"WHERE pol.polrelid = {_OID} ORDER BY 1;"
)
# psql 17 \d+ (statements 14/25/37): publications, unqualified pubname over joins, generate_series.
PSQL_PUBLICATIONS = (
    "SELECT pubname\n     , NULL\n     , NULL\nFROM pg_catalog.pg_publication p\n"
    "     JOIN pg_catalog.pg_publication_namespace pn ON p.oid = pn.pnpubid\n"
    "     JOIN pg_catalog.pg_class pc ON pc.relnamespace = pn.pnnspid\n"
    f"WHERE pc.oid ={_OID} and pg_catalog.pg_relation_is_publishable({_OID})\nUNION\n"
    "SELECT pubname\n     , pg_get_expr(pr.prqual, c.oid)\n"
    "     , (CASE WHEN pr.prattrs IS NOT NULL THEN\n         (SELECT string_agg(attname, ', ')\n"
    "           FROM pg_catalog.generate_series(0, "
    "pg_catalog.array_upper(pr.prattrs::pg_catalog.int2[], 1)) s,\n"
    "                pg_catalog.pg_attribute\n"
    "          WHERE attrelid = pr.prrelid AND attnum = prattrs[s])\n"
    "        ELSE NULL END) FROM pg_catalog.pg_publication p\n"
    "     JOIN pg_catalog.pg_publication_rel pr ON p.oid = pr.prpubid\n"
    "     JOIN pg_catalog.pg_class c ON c.oid = pr.prrelid\n"
    f"WHERE pr.prrelid = {_OID}\nUNION\nSELECT pubname\n     , NULL\n     , NULL\n"
    "FROM pg_catalog.pg_publication p\n"
    f"WHERE p.puballtables AND pg_catalog.pg_relation_is_publishable({_OID})\nORDER BY 1;"
)
# psql 17 \d+ (statements 16/28/40): partitions, unqualified inhdetachpending over two tables.
PSQL_PARTITIONS = (
    "SELECT c.oid::pg_catalog.regclass, c.relkind, inhdetachpending, "
    "pg_catalog.pg_get_expr(c.relpartbound, c.oid)\n"
    "FROM pg_catalog.pg_class c, pg_catalog.pg_inherits i\n"
    f"WHERE c.oid = i.inhrelid AND i.inhparent = {_OID}\n"
    "ORDER BY pg_catalog.pg_get_expr(c.relpartbound, c.oid) = 'DEFAULT', "
    "c.oid::pg_catalog.regclass::pg_catalog.text;"
)
# psql 17 \d+ (statements 20/32): indexes, unqualified contype etc. from the LEFT JOINed constraint.
PSQL_INDEXES = (
    "SELECT c2.relname, i.indisprimary, i.indisunique, i.indisclustered, i.indisvalid, "
    "pg_catalog.pg_get_indexdef(i.indexrelid, 0, true),\n"
    "  pg_catalog.pg_get_constraintdef(con.oid, true), contype, condeferrable, condeferred, "
    "i.indisreplident, c2.reltablespace\n"
    "FROM pg_catalog.pg_class c, pg_catalog.pg_class c2, pg_catalog.pg_index i\n"
    "  LEFT JOIN pg_catalog.pg_constraint con ON (conrelid = i.indrelid AND conindid = "
    "i.indexrelid AND contype IN ('p','u','x'))\n"
    f"WHERE c.oid = {_OID} AND c.oid = i.indrelid AND i.indexrelid = c2.oid\n"
    "ORDER BY i.indisprimary DESC, c2.relname;"
)
# Metabase (statement 12): enum types, unqualified columns over a join and in a subquery.
METABASE_ENUMS = (
    "SELECT nspname, typname FROM pg_type t JOIN pg_namespace n ON n.oid = t.typnamespace "
    "WHERE t.oid IN (SELECT DISTINCT enumtypid FROM pg_enum e)"
)

_TOOL_STATEMENTS = {
    "psql table (10/18/30)": PSQL_TABLE,
    "psql policies (12/23/35)": PSQL_POLICIES,
    "psql publications (14/25/37)": PSQL_PUBLICATIONS,
    "psql partitions (16/28/40)": PSQL_PARTITIONS,
    "psql indexes (20/32)": PSQL_INDEXES,
    "metabase enums (12)": METABASE_ENUMS,
}


# -- the tools' statements: each column judged where it resolves ---------------------------------


@pytest.mark.parametrize("sql", list(_TOOL_STATEMENTS.values()), ids=list(_TOOL_STATEMENTS))
def test_a_tool_statement_is_not_refused_a_column_its_relation_serves(sql):
    assert _v003(sql) == []


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


# -- names no registered relation has are the engine's -------------------------------------------


@pytest.mark.parametrize(
    "sql",
    [
        "SELECT x FROM bank.customer c, unnest(ARRAY[1]) x",
        "SELECT x FROM bank.customer c, pg_catalog.unnest(ARRAY[1]) x",
        "SELECT s FROM bank.customer c, generate_series(1, 3) s",
        "SELECT relid, depth FROM bank.customer c, "
        "pg_catalog.pg_partition_tree('a') WITH ORDINALITY AS a(relid, depth)",
        "SELECT d.total FROM bank.customer c JOIN (SELECT id, 1 AS total FROM bank.customer) d "
        "ON d.id = c.id",
        "SELECT total FROM bank.customer c JOIN (SELECT id AS cid, 1 AS total FROM bank.customer) d "
        "ON d.cid = c.id",
        "SELECT (SELECT y FROM unnest(ARRAY[c.id]) y) FROM bank.customer c",
    ],
)
def test_a_name_no_registered_relation_has_is_passed_to_the_engine(sql):
    assert _v003(sql) == []


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
