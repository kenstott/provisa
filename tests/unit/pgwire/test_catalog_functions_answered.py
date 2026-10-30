# Copyright (c) 2026 Kenneth Stott
# Canary: 6dfe1f26-f872-4e85-a481-7afe99a2c6b2
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""PGWIRE'S CATALOG FUNCTIONS (REQ-2167, amended 2026-10-27), part 2b: the catalog functions psql,
pgAdmin and DBeaver call that the catalog had no answer for, each answered as PostgreSQL answers it
for what the catalog holds -- in the clients' own statements, and called on their own -- or refused
by name."""

from __future__ import annotations

import pytest
from unittest.mock import MagicMock

from provisa.pgwire.catalog import answer, classify


def _state():
    ctx = MagicMock()
    ctx.tables = {}
    state = MagicMock()
    state.multitenancy = False
    state.contexts = {"testrole": ctx}
    state.schema_build_cache = {"column_types": {}}
    return state


def _rows(sql: str) -> list[tuple]:
    assert classify(sql) == "INTERCEPT", sql
    return answer(sql, "testrole", _state()).rows


def _table_oid() -> int:
    ((oid,),) = _rows(
        "SELECT oid FROM pg_catalog.pg_class WHERE relkind = 'r' ORDER BY oid LIMIT 1"
    )
    return oid


def test_the_encoding_is_utf8_and_its_number_six():
    # psql \dO: the collations usable in the database's encoding.
    assert _rows(
        "SELECT getdatabaseencoding(), pg_char_to_encoding('utf-8'), "
        "pg_char_to_encoding('LATIN1'), pg_char_to_encoding(NULL)"
    ) == [("UTF8", 6, -1, None)]
    assert _rows(
        "SELECT c.collname FROM pg_catalog.pg_collation c WHERE c.collencoding IN "
        "(-1, pg_catalog.pg_char_to_encoding(pg_catalog.getdatabaseencoding()))"
    ) == _rows("SELECT collname FROM pg_catalog.pg_collation WHERE collencoding IN (-1, 6)")


def test_a_size_is_unknown_and_the_clients_size_queries_run():
    # Provisa reports no storage: every size is NULL, never a made-up 0.
    assert _rows(
        "SELECT pg_relation_size(1), pg_total_relation_size(1), pg_indexes_size(1), "
        "pg_table_size(1), pg_database_size(1), pg_tablespace_size(1), pg_column_size(1)"
    ) == [(None,) * 7]
    # psql \l+ and \dP+, DBeaver's database sizes, pgAdmin's tablespace statistics.
    assert _rows(
        "SELECT d.datname, CASE WHEN pg_catalog.has_database_privilege(d.datname, 'CONNECT') "
        "THEN pg_catalog.pg_size_pretty(pg_catalog.pg_database_size(d.datname)) "
        "ELSE 'No Access' END AS \"Size\" FROM pg_catalog.pg_database d"
    ) == [("provisa", None)]
    assert _rows(
        "SELECT pg_catalog.pg_size_pretty(sum(pg_catalog.pg_table_size(c.oid))) "
        "FROM pg_catalog.pg_class c"
    ) == [(None,)]
    assert _rows("SELECT db.datname, pg_database_size(db.oid) FROM pg_catalog.pg_database db") == [
        ("provisa", None)
    ]
    assert _rows('SELECT pg_catalog.pg_tablespace_size(1663::OID) AS "Size"') == [(None,)]


def test_a_literal_is_quoted_as_postgresql_quotes_it():
    assert _rows(
        "SELECT pg_catalog.quote_literal('it''s'), quote_literal('a\\b'), quote_literal(5), "
        "quote_literal(NULL::text)"
    ) == [("'it''s'", "E'a\\\\b'", "'5'", None)]


def test_options_and_acls_have_no_items():
    # psql \d's FDW options, DBeaver's role privileges.
    rows = _rows(
        "SELECT c.relname, pg_catalog.array_to_string(ARRAY(SELECT "
        "pg_catalog.quote_ident(option_name) || ' ' || pg_catalog.quote_literal(option_value) "
        "FROM pg_catalog.pg_options_to_table(c.reloptions)), ', ') FROM pg_catalog.pg_class c"
    )
    assert rows and {options for _, options in rows} == {""}
    assert (
        _rows(
            "SELECT acl.grantee FROM pg_catalog.pg_class c "
            "CROSS JOIN LATERAL pg_catalog.aclexplode(c.relacl) AS acl"
        )
        == []
    )
    assert _rows("SELECT (aclexplode(relacl)).grantee FROM pg_catalog.pg_class") == []
    assert _rows("SELECT acldefault('r'::\"char\", 10)") == [(None,)]


def test_a_relation_is_its_own_partition_tree_and_ancestor():
    oid = _table_oid()
    assert _rows(f"SELECT * FROM pg_catalog.pg_partition_tree({oid})") == [(oid, None, True, 0)]
    # psql \d+ of a table: its partitions, counted.
    assert _rows(
        "SELECT (SELECT count(*) FROM pg_catalog.pg_partition_tree(c.oid) ppt) "
        f"FROM pg_catalog.pg_class c WHERE c.oid = {oid}"
    ) == [(1,)]
    # psql \d of a table: the foreign keys referencing it or an ancestor.
    assert _rows(f"SELECT * FROM pg_catalog.pg_partition_ancestors('{oid}')") == [(oid,)]
    # In a select list it is a regclass: the oid and name a regclass result column is sent by.
    ((ancestor,),) = _rows(f"SELECT pg_catalog.pg_partition_ancestors({oid})")
    assert ancestor["oid"] == oid
    assert (
        _rows(
            "SELECT conname FROM pg_catalog.pg_constraint WHERE confrelid IN "
            f"(SELECT pg_catalog.pg_partition_ancestors('{oid}'))"
        )
        == []
    )
    ((view,),) = _rows("SELECT oid FROM pg_catalog.pg_class WHERE relkind = 'v' LIMIT 1")
    assert _rows(f"SELECT * FROM pg_catalog.pg_partition_tree({view})") == []


def test_the_settings_are_pg_settings():
    assert _rows(
        "SELECT name, setting FROM pg_catalog.pg_show_all_settings() WHERE name = 'server_version'"
    ) == _rows("SELECT name, setting FROM pg_catalog.pg_settings WHERE name = 'server_version'")
    # pgAdmin's table properties.
    assert _rows(
        "SELECT (SELECT st.setting FROM pg_catalog.pg_show_all_settings() st "
        "WHERE st.name = 'default_table_access_method') AS default_amname"
    ) == _rows(
        "SELECT (SELECT setting FROM pg_catalog.pg_settings "
        "WHERE name = 'default_table_access_method')"
    )


def test_an_object_is_described_and_identified_from_the_catalog():
    oid = _table_oid()
    ((relname, attname),) = _rows(
        "SELECT c.relname, a.attname FROM pg_catalog.pg_class c JOIN pg_catalog.pg_attribute a "
        f"ON a.attrelid = c.oid WHERE c.oid = {oid} AND a.attnum = 1"
    )
    # The caller's own alias (c) names its relation, not the macro's.
    assert _rows(
        "SELECT pg_catalog.pg_describe_object(1259, c.oid, 0), "
        f"pg_catalog.pg_describe_object(1259, c.oid, 1) FROM pg_catalog.pg_class c WHERE c.oid = {oid}"
    ) == [(f"table {relname}", f"column {attname} of table {relname}")]
    assert _rows(
        "SELECT pg_describe_object(1247, 23, 0), pg_describe_object(2615, 2200, 0), "
        "pg_describe_object(1213, 1663, 0)"
    ) == [("type integer", "schema public", None)]
    # DBeaver: an aggregate's function, by 'pg_proc'::regclass.
    assert (
        _rows(
            "SELECT (pg_identify_object('pg_proc'::regclass, aggfnoid, 0)).identity "
            "FROM pg_catalog.pg_aggregate"
        )
        == []
    )
    assert _rows(
        f"SELECT (pg_identify_object('pg_class'::regclass, {oid}, 0)).identity, "
        f"(pg_identify_object(1259, {oid}, 1)).identity, "
        "(pg_identify_object(1247, 25, 0)).identity, (pg_identify_object(1247, 23, 0)).identity"
    ) == [
        (
            f"pg_catalog.{relname}",
            f"pg_catalog.{relname}.{attname}",
            "pg_catalog.text",
            "integer",
        )
    ]


def test_a_vector_of_types_is_their_names():
    assert _rows("SELECT oidvectortypes([23, 25, 16])") == [("integer, text, boolean",)]
    assert _rows("SELECT oidvectortypes(CAST([] AS INTEGER[]))") == [("",)]
    assert _rows("SELECT oidvectortypes(NULL)") == [(None,)]


def test_nothing_is_a_cursor_or_a_sequence():
    assert _rows("SELECT * FROM pg_cursor()") == []
    assert _rows("SELECT pg_sequence_last_value(1)") == [(None,)]
    assert _rows("SELECT pg_statistics_obj_is_visible(1)") == [(True,)]


@pytest.mark.parametrize("call", ["plpgsql_validator(1)", "pg_catalog.fmgr_c_validator(1)"])
def test_a_function_provisa_has_nothing_to_run_on_is_refused_by_name(call):
    from provisa.pgwire.server import _sqlstate_of

    name = call.split("(")[0].removeprefix("pg_catalog.")
    with pytest.raises(NotImplementedError, match=name) as refused:
        _rows(f"SELECT {call}")
    assert _sqlstate_of(refused.value) == "0A000"
