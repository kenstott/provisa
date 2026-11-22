# Copyright (c) 2026 Kenneth Stott
# Canary: 439d906c-5040-4756-a26f-08cbd8b157c9
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A column is judged where it resolves (REQ-039, amended 2026-11-04; REQ-2204) -- A2.

RED until A2 is implemented on governance-by-construction. Moved from fix/v003-scope
(tests/unit/test_v003_column_scope.py and test_v003_review_findings.py) by the maintainer's split
of 2026-11-05: a name no registered relation in scope has -- a table function's alias or column, a
derived table's column, VALUES' column1..N, json_each's key and value -- is passed to the engine,
and a column a relation serves is not refused where it resolves. A hidden column is still refused
in every clause. The statements are the ones psql 17 and Metabase sent in the BI replay (J595
triage, causes 1 and 8).
"""

# Requirements: REQ-039, REQ-2204

from __future__ import annotations

import pytest
import sqlglot

from provisa.compiler.sql_types import CompilationContext
from provisa.compiler.sql_validator import _check_column_visibility, validate_sql
from provisa.compiler.stage2 import GovernanceContext
from provisa.pgwire.catalog_route import CLIENT_TABLE_FUNCTIONS

pytestmark = pytest.mark.unit

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


def _codes(sql: str, gov=None) -> list[str]:
    """The codes validate_sql refuses ``sql`` for, on a catalog statement (the client table
    functions admitted) for a role of every domain, the relationship guard aside."""
    return sorted(
        {
            v.code
            for v in validate_sql(
                sql,
                CompilationContext(),
                gov or _gov(),
                {"domain_access": ["*"]},
                [],
                bypass_relationship_guard=True,
                table_functions=CLIENT_TABLE_FUNCTIONS,
            )
        }
    )


# -- the tools' statements: each column judged where it resolves ---------------------------------


@pytest.mark.parametrize("sql", list(_TOOL_STATEMENTS.values()), ids=list(_TOOL_STATEMENTS))
def test_a_tool_statement_is_not_refused_a_column_its_relation_serves(sql):
    assert _v003(sql) == []


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


# -- a served column, or a name a source no registry governs is known to provide -----------------


@pytest.mark.parametrize(
    "sql",
    [
        # the served columns, in every clause
        "SELECT id FROM bank.customer WHERE name LIKE 'a%' ORDER BY name",
        "SELECT name, count(*) FROM bank.customer GROUP BY name HAVING max(id) > 1",
        "SELECT rank() OVER (PARTITION BY name ORDER BY id) FROM bank.customer",
        "SELECT c.id FROM bank.customer c JOIN pg_catalog.pg_am a ON a.amname = c.name",
        "SELECT * FROM bank.customer JOIN (VALUES (1)) v(id) USING (id)",
        # an output column named in ORDER BY or GROUP BY: the output, which the list judged
        "SELECT name AS n FROM bank.customer ORDER BY n",
        "SELECT name AS ssn FROM bank.customer ORDER BY ssn",
        "SELECT name AS n, count(*) FROM bank.customer GROUP BY n",
        "SELECT id FROM bank.customer UNION SELECT id FROM bank.customer ORDER BY id",
        # a name a source no registry governs is known to provide
        "SELECT column1 FROM bank.customer c, (VALUES (1)) v",
        "SELECT a, column2 FROM bank.customer c, (VALUES (1, 2)) v(a)",
        "SELECT key, value FROM bank.customer c, json_each('{}') j",
        "SELECT k, value FROM bank.customer c, jsonb_each('{}') AS j(k)",
        "SELECT x, ordinality FROM bank.customer c, unnest(ARRAY[c.id]) WITH ORDINALITY x",
        "SELECT a FROM bank.customer c, json_to_record('{}') AS r(a int)",
        "SELECT (SELECT column1 FROM (VALUES (c.id)) v) FROM bank.customer c",
    ],
)
def test_a_served_or_known_name_is_not_refused(sql):
    assert _codes(sql) == [], sql


# -- every clause --------------------------------------------------------------------------------


@pytest.mark.parametrize(
    "sql",
    [
        # WHERE, JOIN ON, GROUP BY, HAVING, ORDER BY, windows
        "SELECT id FROM bank.customer WHERE ssn IS NOT NULL",
        "SELECT c.id FROM bank.customer c JOIN pg_catalog.pg_am a ON a.amname = c.ssn",
        "SELECT name FROM bank.customer GROUP BY name, ssn",
        "SELECT name FROM bank.customer GROUP BY name HAVING max(ssn) > '5'",
        "SELECT id FROM bank.customer ORDER BY length(ssn)",
        "SELECT name AS ssn FROM bank.customer GROUP BY ssn",
        "SELECT rank() OVER w FROM bank.customer WINDOW w AS (PARTITION BY ssn)",
        "SELECT count(*) FILTER (WHERE ssn > '5') FROM bank.customer",
        "SELECT DISTINCT ON (ssn) id FROM bank.customer",
        "SELECT string_agg(name, ',' ORDER BY ssn) FROM bank.customer",
        # USING and a NATURAL join's shared column, with or without a star
        "SELECT id FROM bank.customer JOIN (VALUES ('x')) v(ssn) USING (ssn)",
        "SELECT id FROM bank.customer NATURAL JOIN (VALUES ('x')) v(ssn)",
        "SELECT id FROM bank.customer NATURAL JOIN json_populate_record(NULL::bank.customer, '{}') r",
        # FROM-function arguments and LATERAL items
        "SELECT s FROM bank.customer c, generate_series(1, length(c.ssn)) s",
        "SELECT z.n FROM bank.customer c, LATERAL (SELECT 1 AS n WHERE c.ssn > '5') z",
        "SELECT k.key FROM bank.customer c, json_each(json_build_object('a', ssn)) k",
        # inside a subquery, a CTE, a set operation
        "SELECT id FROM bank.customer WHERE id IN (SELECT id FROM bank.customer WHERE ssn > '5')",
        "WITH d AS (SELECT id FROM bank.customer WHERE ssn > '5') SELECT id FROM d",
        "SELECT id FROM bank.customer UNION SELECT id FROM bank.customer ORDER BY 1 LIMIT "
        "(SELECT count(*) FROM bank.customer WHERE ssn > '5')",
        "SELECT id FROM bank.customer EXCEPT SELECT id FROM bank.customer WHERE ssn > '5'",
    ],
)
def test_a_hidden_column_is_refused_in_every_clause(sql):
    assert "V003" in _codes(sql), sql
