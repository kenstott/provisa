# Copyright (c) 2026 Kenneth Stott
# Canary: 6f1d2c84-93ab-4e57-b0c6-2a7e5d18c349
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Every served catalog relation has PostgreSQL 17's columns, in its order, with its type OIDs and
typmods (REQ-2167), the columns it adds holding what PostgreSQL shows for this server, and the
queries psql, pgAdmin and DBeaver send to them keep working."""

# Requirements: REQ-2167

from __future__ import annotations

import io
import struct
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest

from provisa.pgwire.catalog import answer
from provisa.pgwire.catalog_pg17 import RELATIONS
from provisa.pgwire.catalog_populate import _build_catalog_db, _populate_pg_description
from provisa.pgwire.server import ProvisaQueryResult

PG_CLASS, PG_NAMESPACE = 1259, 2615

_ROLE = "testrole"


def _state():
    ctx = MagicMock()
    ctx.tables = {}
    state = MagicMock()
    state.multitenancy = False
    state.contexts = {_ROLE: ctx}
    state.schema_build_cache = {"column_types": {}}
    return state


def _fields(sql: str) -> list[tuple[str, int, int]]:
    """Each column's name, type OID and typmod, as the RowDescription gives them to a client."""
    from buenavista.postgres import BuenaVistaHandler

    result = ProvisaQueryResult(answer(sql, _ROLE, _state()), sql)
    handler = BuenaVistaHandler.__new__(BuenaVistaHandler)
    handler.wfile = io.BytesIO()
    handler.send_row_description(result)
    out = handler.wfile.getvalue()
    (count,) = struct.unpack("!h", out[5:7])
    fields, at = [], 7
    for _ in range(count):
        end = out.index(b"\x00", at)
        _table, _attnum, type_oid, _len, typmod, _fmt = struct.unpack(
            "!ihihih", out[end + 1 : end + 19]
        )
        fields.append((out[at:end].decode(), type_oid, typmod))
        at = end + 19
    return fields


def _rows(sql: str, state=None) -> list[tuple]:
    return [tuple(r) for r in answer(sql, _ROLE, state or _state()).rows]


@pytest.mark.parametrize("schema,relation", sorted(RELATIONS))
def test_a_served_relation_has_postgresqls_columns_order_types_and_typmods(schema, relation):
    assert _fields(f"SELECT * FROM {schema}.{relation} LIMIT 0") == [
        (column, oid, -1) for column, _, oid in RELATIONS[(schema, relation)]
    ]


_PG_ATTRIBUTE_OF_A_TABLE = (
    "SELECT attcompression, attmissingval, attstattarget, atthasmissing "
    "FROM pg_catalog.pg_attribute WHERE attrelid = {oid} ORDER BY attnum LIMIT 1"
)


def test_pg_attribute_of_a_catalog_relation_is_its_postgresql_17_columns():
    # psql's \d pg_catalog.pg_type, and any client listing a catalog relation's columns.
    (relid,) = _rows(
        "SELECT c.oid FROM pg_catalog.pg_class c JOIN pg_catalog.pg_namespace n "
        "ON n.oid = c.relnamespace WHERE c.relname = 'pg_type' AND n.nspname = 'pg_catalog'"
    )[0]
    listed = _rows(
        "SELECT attname, atttypid, attlen, attbyval FROM pg_catalog.pg_attribute "
        f"WHERE attrelid = {relid} AND attnum > 0 ORDER BY attnum"
    )
    assert [(name, oid) for name, oid, _, _ in listed] == [
        (column, oid) for column, _, oid in RELATIONS[("pg_catalog", "pg_type")]
    ]
    by_name = {name: (length, byval) for name, _, length, byval in listed}
    assert by_name["typname"] == (64, False)
    assert by_name["oid"] == (4, True)


def test_pg_attribute_a_columns_added_columns_are_postgresqls_for_a_plain_column():
    (relid,) = _rows("SELECT oid FROM pg_catalog.pg_class WHERE relname = 'pg_type'")[0]
    # No compression method, no stored default, statistics target unset (NULL in PostgreSQL 17).
    assert _rows(_PG_ATTRIBUTE_OF_A_TABLE.format(oid=relid)) == [("", None, None, False)]


def test_pg_class_counts_a_catalog_relations_columns():
    assert _rows("SELECT relnatts FROM pg_catalog.pg_class WHERE relname = 'pg_type'") == [
        (len(RELATIONS[("pg_catalog", "pg_type")]),)
    ]


def test_pg_database_shows_a_libc_database_of_no_login_event_trigger():
    assert _rows(
        "SELECT datlocprovider, dathasloginevt, datlocale, daticurules, datcollversion, datconnlimit "
        "FROM pg_catalog.pg_database"
    ) == [("c", False, None, None, None, -1)]


def test_pg_type_names_the_subscript_handler_postgresql_has_for_the_type():
    handlers = dict(
        _rows(
            "SELECT typname, typsubscript FROM pg_catalog.pg_type "
            "WHERE typname IN ('int4', '_text', 'int2vector', 'jsonb', 'name', 'point', "
            "'sql_identifier', 'cardinal_number')"
        )
    )
    assert handlers == {
        "int4": 0,
        "_text": 6179,
        "int2vector": 6179,
        "jsonb": 6098,
        "name": 6180,
        "point": 6180,
        # A domain subscripts as its base type does.
        "sql_identifier": 6180,
        "cardinal_number": 0,
    }


def test_a_description_names_its_catalog_by_oid():
    import duckdb

    db = duckdb.connect()
    db.execute(
        "CREATE TABLE _pg_description (objoid INTEGER, classoid INTEGER, objsubid INTEGER, "
        "description VARCHAR)"
    )
    idx = SimpleNamespace(
        ns_map={"sales": 17000},
        tables=[("sales", "sales", "orders", 2, 9002)],
        col_attnum={},
        metric_oids={},
    )
    raw = [{"id": 2, "description": "Orders", "columns": []}]
    _populate_pg_description(db, idx, raw, [{"id": "sales", "description": "Sales"}])
    rows = db.execute("SELECT objoid, classoid FROM _pg_description ORDER BY objoid").fetchall()
    db.close()
    assert rows == [(9002, PG_CLASS), (17000, PG_NAMESPACE)]


@pytest.mark.parametrize(
    "predicate",
    [
        "d.classoid = 'pg_class'::regclass",
        "d.classoid = 'pg_catalog.pg_class'::regclass",
        "d.classoid = 'pg_catalog.pg_class'::pg_catalog.regclass",
        "d.classoid = 'pg_class'::regclass::oid",
        f"d.classoid = {PG_CLASS}",
    ],
)
def test_datagrip_and_pgjdbc_filter_comments_by_their_catalog(predicate):
    db = _build_catalog_db(_ROLE, _state())
    db.execute(f"INSERT INTO _pg_description VALUES (16400, {PG_CLASS}, 0, 'a table')")
    db.execute(f"INSERT INTO _pg_description VALUES (16400, {PG_NAMESPACE}, 0, 'a schema')")
    from provisa.pgwire.catalog_rewrite import _rewrite_for_duckdb

    rewritten = _rewrite_for_duckdb(
        f"SELECT d.description FROM pg_catalog.pg_description d WHERE {predicate}", _ROLE, "provisa"
    )
    assert db.execute(rewritten).fetchall() == [("a table",)]
    db.close()


@pytest.mark.parametrize(
    "relation,column",
    [("pg_catalog.pg_type", "typname"), ("pg_catalog.pg_stat_activity", "pid")],
)
def test_a_regclass_literal_compares_with_a_column_of_oids(relation, column):
    # pgAdmin and SQLAlchemy reflect a relation with attrelid = '<relation>'::regclass.
    assert _rows(
        "SELECT count(*) FROM pg_catalog.pg_attribute "
        f"WHERE attrelid = '{relation}'::regclass AND attname = '{column}'"
    ) == [(1,)]


def test_a_catalog_relation_is_listed_under_postgresqls_oid_for_it():
    assert _rows(
        "SELECT relname, oid FROM pg_catalog.pg_class "
        "WHERE relname IN ('pg_class', 'pg_namespace') ORDER BY oid"
    ) == [("pg_class", PG_CLASS), ("pg_namespace", PG_NAMESPACE)]


def test_pgjdbcs_column_comment_join_finds_the_comment():
    # pgjdbc PgDatabaseMetaData.getColumns (DBeaver, DataGrip): classoid joined to pg_class.
    db = _build_catalog_db(_ROLE, _state())
    db.execute(f"INSERT INTO _pg_description VALUES (16400, {PG_CLASS}, 1, 'a column')")
    from provisa.pgwire.catalog_rewrite import _rewrite_for_duckdb

    rewritten = _rewrite_for_duckdb(
        "SELECT dsc.description, dn.nspname FROM pg_catalog.pg_description dsc "
        "LEFT JOIN pg_catalog.pg_class dc ON (dc.oid = dsc.classoid AND dc.relname = 'pg_class') "
        "LEFT JOIN pg_catalog.pg_namespace dn ON (dc.relnamespace = dn.oid AND "
        "dn.nspname = 'pg_catalog')",
        _ROLE,
        "provisa",
    )
    assert db.execute(rewritten).fetchall() == [("a column", "pg_catalog")]
    db.close()


def test_psqls_describe_index_runs():
    # psql 17's \d (describe.c, describeOneTableDetails): a table's indexes.
    sql = (
        "SELECT c2.relname, i.indisprimary, i.indisunique, i.indisclustered, i.indisvalid, "
        "i.indisreplident, c2.reltablespace, i.indnullsnotdistinct "
        "FROM pg_catalog.pg_class c, pg_catalog.pg_class c2, pg_catalog.pg_index i "
        "WHERE c.oid = 0 AND c.oid = i.indrelid AND i.indexrelid = c2.oid "
        "ORDER BY i.indisprimary DESC, c2.relname"
    )
    assert _rows(sql) == []


def test_psqls_list_databases_runs():
    # psql 17's \l (describe.c, listAllDbs).
    sql = (
        'SELECT d.datname as "Name", pg_catalog.pg_get_userbyid(d.datdba) as "Owner", '
        'd.datcollate as "Collate", d.datctype as "Ctype", d.datlocale as "Locale", '
        'd.daticurules as "ICU Rules", '
        "CASE d.datlocprovider WHEN 'b' THEN 'builtin' WHEN 'c' THEN 'libc' WHEN 'i' THEN 'icu' "
        'END AS "Locale Provider" FROM pg_catalog.pg_database d ORDER BY 1'
    )
    ((_name, _owner, collate, ctype, locale, rules, provider),) = _rows(sql)
    assert (collate, ctype, locale, rules, provider) == (
        "en_US.UTF-8",
        "en_US.UTF-8",
        None,
        None,
        "libc",
    )


def test_psqls_list_roles_runs():
    # psql 17's \du (describe.c, describeRoles): pg_roles read by name.
    state = _state()
    state.roles = {"analyst": {"id": "analyst"}}
    sql = (
        "SELECT r.rolname, r.rolsuper, r.rolinherit, r.rolcreaterole, r.rolcreatedb, "
        "r.rolcanlogin, r.rolconnlimit, r.rolvaliduntil, r.rolreplication, r.rolbypassrls, r.oid "
        "FROM pg_catalog.pg_roles r WHERE r.rolname = 'analyst' ORDER BY 1"
    )
    ((name, *_, oid),) = _rows(sql, state)
    assert name == "analyst" and isinstance(oid, int)


def test_psqls_role_membership_query_runs():
    # psql 17's \drg (describe.c, describeRoleGrants).
    sql = (
        "SELECT m.roleid, pg_catalog.pg_get_userbyid(m.member), m.admin_option, "
        "m.inherit_option, m.set_option, pg_catalog.pg_get_userbyid(m.grantor) "
        "FROM pg_catalog.pg_auth_members m ORDER BY 1"
    )
    assert _rows(sql) == []


def test_psqls_collation_list_runs():
    # psql 17's \dO (describe.c, listCollations).
    sql = (
        "SELECT c.collname, c.collcollate, c.collctype, c.colllocale, c.collicurules, "
        "c.collprovider FROM pg_catalog.pg_collation c ORDER BY 1"
    )
    assert ("C", "C", "C", None, None, "c") in _rows(sql)


def test_psqls_subscription_list_runs():
    # psql 17's \dRs+ (describe.c, describeSubscriptions).
    sql = (
        "SELECT subname, subenabled, subbinary, substream, subtwophasestate, subdisableonerr, "
        "suborigin, subpasswordrequired, subrunasowner, subfailover, subsynccommit, subconninfo, "
        "subskiplsn FROM pg_catalog.pg_subscription ORDER BY 1"
    )
    assert _rows(sql) == []


def test_pgadmins_activity_and_statistics_queries_run():
    # pgAdmin dashboard/sql/default/activity.sql and tables/sql/16_plus/stats.sql.
    activity = (
        "SELECT pid, datname, usename, application_name, client_addr, backend_start, xact_start, "
        "state, wait_event_type, wait_event, query, backend_type, leader_pid, query_id "
        "FROM pg_catalog.pg_stat_activity sa WHERE sa.datname = 'provisa' ORDER BY pid"
    )
    tables = (
        "SELECT seq_scan, last_seq_scan, seq_tup_read, idx_scan, last_idx_scan, idx_tup_fetch, "
        "n_tup_newpage_upd FROM pg_catalog.pg_stat_user_tables WHERE relname = 'orders'"
    )
    indexes = (
        "SELECT indexrelname, idx_scan, last_idx_scan, idx_tup_read, idx_tup_fetch "
        "FROM pg_catalog.pg_stat_user_indexes"
    )
    assert _rows(activity) == [] and _rows(tables) == [] and _rows(indexes) == []


def test_pg_statio_user_tables_is_its_own_relation_not_the_scan_statistics():
    # DBeaver's and pgAdmin's table I/O statistics.
    sql = (
        "SELECT relid, heap_blks_read, heap_blks_hit, idx_blks_read, idx_blks_hit, "
        "toast_blks_read, toast_blks_hit, tidx_blks_read, tidx_blks_hit "
        "FROM pg_catalog.pg_statio_user_tables"
    )
    assert _rows(sql) == []
