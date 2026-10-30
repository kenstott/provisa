# Copyright (c) 2026 Kenneth Stott
# Canary: 91c4e7d2-5b0a-4f63-a18e-3d7f06bc2e15
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""PGWIRE'S CATALOG FUNCTIONS (REQ-2167): the families of pg_catalog functions served through the
engine's equivalent or from the catalog, each statement run as the conformance probe runs it, the
type PostgreSQL 17's pg_proc says and the value its documentation or source gives."""

# Requirements: REQ-2167
# ruff: noqa: F811 -- the fixtures are conn and pgwire of the module they are imported from

from __future__ import annotations

from typing import Any

import pytest
from psycopg import pq

from provisa.federation import duckdb_pg_scalars as scalars
from provisa.pgwire.catalog_pg17 import TYPES
from provisa.pgwire.catalog_pg17_overloads import FUNCTION_OVERLOADS
from tests.unit.pgwire.pg17_answers import RECORDED
from tests.unit.pgwire.pg17_signatures import SIGNATURES
from tests.unit.pgwire.test_type_conversions import conn, pgwire  # noqa: F401

BOOL, BYTEA, NAME, INT8, INT4, TEXT, OID = 16, 17, 19, 20, 23, 25, 26
FLOAT8, NUMERIC, TEXT_ARRAY, TIMESTAMPTZ, TIMESTAMP = 701, 1700, 1009, 1184, 1114
TIMETZ = 1266
PG_LSN, XID8, RECORD, VOID, MONEY, ANYELEMENT = 3220, 5069, 2249, 2278, 790, 2283


def _run(conn: Any, sql: str) -> Any:
    return conn.conn.pgconn.exec_params(sql.encode(), [], result_format=0)


def _rows(res: Any) -> list[Any]:
    return [None if (v := res.get_value(i, 0)) is None else v.decode() for i in range(res.ntuples)]


#: statement -> (type OID, the rows' text).
ANSWERS: dict[str, tuple[int, list[Any]]] = {
    # Text: the operators' functions.
    "SELECT textcat('a', 'b') AS v": (TEXT, ["ab"]),
    "SELECT textlike('abc', 'a%') AS v": (BOOL, ["t"]),
    "SELECT textnlike('abc', 'a%') AS v": (BOOL, ["f"]),
    "SELECT texticlike('ABC', 'a%') AS v": (BOOL, ["t"]),
    "SELECT texticnlike('ABC', 'a%') AS v": (BOOL, ["f"]),
    "SELECT textregexeq('abc', '^a') AS v": (BOOL, ["t"]),
    "SELECT textregexne('abc', '^a') AS v": (BOOL, ["f"]),
    "SELECT texticregexeq('ABC', '^a') AS v": (BOOL, ["t"]),
    "SELECT texticregexne('ABC', '^a') AS v": (BOOL, ["f"]),
    "SELECT namelike('abc'::name, 'a_c') AS v": (BOOL, ["t"]),
    "SELECT nameregexeq('abc'::name, 'b') AS v": (BOOL, ["t"]),
    "SELECT bpcharlike('abc'::character(3), 'a%') AS v": (BOOL, ["t"]),
    "SELECT pg_catalog.\"like\"('abc', 'a%') AS v": (BOOL, ["t"]),
    "SELECT notlike('abc', 'a%') AS v": (BOOL, ["f"]),
    "SELECT textlike('a%', 'a\\%') AS v": (BOOL, ["t"]),
    'SELECT pg_catalog."textcat"(NULL::text, NULL::text) AS v': (TEXT, [None]),
    "SELECT nameconcatoid('a'::name, 5::oid) AS v": (NAME, ["a_5"]),
    "SELECT quote_nullable(NULL::text) AS v": (TEXT, ["NULL"]),
    "SELECT quote_nullable('o''k') AS v": (TEXT, ["'o''k'"]),
    # Numbers.
    "SELECT dcbrt(8) AS v": (FLOAT8, ["2"]),
    "SELECT dround(2.5) AS v": (FLOAT8, ["2"]),
    "SELECT dtrunc(2.7) AS v": (FLOAT8, ["2"]),
    "SELECT sind(30) AS v": (FLOAT8, ["0.5"]),
    "SELECT sind(90) AS v": (FLOAT8, ["1"]),
    "SELECT sind(-30) AS v": (FLOAT8, ["-0.5"]),
    "SELECT tand(45) AS v": (FLOAT8, ["1"]),
    "SELECT tand(135) AS v": (FLOAT8, ["-1"]),
    "SELECT scale(8.41) AS v": (INT4, ["2"]),
    "SELECT scale(8::numeric) AS v": (INT4, ["0"]),
    "SELECT to_bin(5) AS v": (TEXT, ["101"]),
    "SELECT to_oct(8) AS v": (TEXT, ["10"]),
    "SELECT to_bin(-1) AS v": (TEXT, ["1" * 32]),
    "SELECT to_bin(-1::bigint) AS v": (TEXT, ["1" * 64]),
    "SELECT factorial(5::bigint) AS v": (NUMERIC, ["120"]),
    "SELECT factorial(30::bigint) AS v": (NUMERIC, ["265252859812191058636308480000000"]),
    "SELECT gcd(12.0, 18.0) AS v": (NUMERIC, ["6.0"]),
    "SELECT lcm(2.5, 1.5) AS v": (NUMERIC, ["7.5"]),
    "SELECT generate_series(1.5, 3.5) AS v": (NUMERIC, ["1.5", "2.5", "3.5"]),
    "SELECT generate_series(1.0, 2.0, 0.5) AS v": (NUMERIC, ["1.0", "1.5", "2.0"]),
    "SELECT every(true) AS v": (BOOL, ["t"]),
    # Functions named for a type.
    'SELECT pg_catalog."numeric"(1::integer) AS v': (NUMERIC, ["1"]),
    'SELECT pg_catalog."numeric"(NULL::integer) AS v': (NUMERIC, [None]),
    "SELECT text(true) AS v": (TEXT, ["true"]),
    'SELECT pg_catalog."text"(NULL::boolean) AS v': (TEXT, [None]),
    "SELECT name('a'::text) AS v": (NAME, ["a"]),
    "SELECT pg_catalog.\"varchar\"('a'::name) AS v": (1043, ["a"]),
    "SELECT bool(1) AS v": (BOOL, ["t"]),
    "SELECT oid(5::bigint) AS v": (OID, ["5"]),
    "SELECT pg_catalog.\"char\"('ab'::text) AS v": (18, ["a"]),
    # Bytes.
    "SELECT byteacat('\\x01'::bytea, '\\x02'::bytea) AS v": (BYTEA, ["\\x0102"]),
    "SELECT bytealike('\\x0102'::bytea, '\\x0125'::bytea) AS v": (BOOL, ["t"]),
    "SELECT byteanlike('\\x0102'::bytea, '\\x0125'::bytea) AS v": (BOOL, ["f"]),
    "SELECT get_byte('\\x0102'::bytea, 1) AS v": (INT4, ["2"]),
    "SELECT set_byte('\\x0102'::bytea, 1, 3) AS v": (BYTEA, ["\\x0103"]),
    "SELECT get_bit('\\x01'::bytea, 0) AS v": (INT4, ["1"]),
    "SELECT set_bit('\\x00'::bytea, 1, 1) AS v": (BYTEA, ["\\x02"]),
    "SELECT bit_count('\\xff01'::bytea) AS v": (INT8, ["9"]),
    "SELECT substring('\\x010203'::bytea, 2, 1) AS v": (BYTEA, ["\\x02"]),
    "SELECT substring('\\x010203'::bytea, 2) AS v": (BYTEA, ["\\x0203"]),
    "SELECT substr('\\x010203'::bytea, 2, 1) AS v": (BYTEA, ["\\x02"]),
    "SELECT overlay('\\x010203'::bytea PLACING '\\xff'::bytea FROM 2) AS v": (
        BYTEA,
        ["\\x01ff03"],
    ),
    "SELECT convert_to('a', 'UTF8') AS v": (BYTEA, ["\\x61"]),
    "SELECT convert_from('\\x61'::bytea, 'UTF8') AS v": (TEXT, ["a"]),
    "SELECT convert('\\xc3a9'::bytea, 'UTF8', 'LATIN1') AS v": (BYTEA, ["\\xe9"]),
    "SELECT length('\\xc3a9'::bytea, 'UTF8') AS v": (INT4, ["1"]),
    # Regular expressions.
    "SELECT regexp_match('abc', '(b)(c)') AS v": (TEXT_ARRAY, ["{b,c}"]),
    "SELECT regexp_match('abc', 'B', 'i') AS v": (TEXT_ARRAY, ["{b}"]),
    "SELECT regexp_match('abc', 'x') AS v": (TEXT_ARRAY, [None]),
    "SELECT regexp_substr('abcabc', 'b.', 1, 2) AS v": (TEXT, ["bc"]),
    "SELECT regexp_substr('abc', '(b)(c)', 1, 1, '', 2) AS v": (TEXT, ["c"]),
    "SELECT regexp_split_to_array('a1b2c', '[0-9]', 'i') AS v": (TEXT_ARRAY, ["{a,b,c}"]),
    "SELECT regexp_split_to_table('aXb', 'x', 'i') AS v": (TEXT, ["a", "b"]),
    "SELECT string_to_table('a,b', ',') AS v": (TEXT, ["a", "b"]),
    "SELECT string_to_table('a,b', ',', 'b') AS v": (TEXT, ["a", None]),
    "SELECT string_to_table(NULL::text, ',') AS v": (TEXT, []),
    "SELECT similar_to_escape('a%') AS v": (TEXT, ["^(?:a.*)$"]),
    "SELECT similar_escape('a_', NULL) AS v": (TEXT, ["^(?:a.)$"]),
    "SELECT like_escape('a#%', '#') AS v": (TEXT, ["a\\%"]),
    # Unicode and identifiers.
    "SELECT is_normalized('a', 'NFC') AS v": (BOOL, ["t"]),
    "SELECT unistr('\\0041') AS v": (TEXT, ["A"]),
    "SELECT unicode_assigned('a') AS v": (BOOL, ["t"]),
    "SELECT parse_ident('\"A\".b') AS v": (TEXT_ARRAY, ["{A,b}"]),
    "SELECT abbrev('10.1.0.0/16'::cidr) AS v": (TEXT, ["10.1/16"]),
    "SELECT abbrev('10.1.0.1/32'::inet) AS v": (TEXT, ["10.1.0.1"]),
    "SELECT jsonb_typeof('1'::jsonb) AS v": (TEXT, ["number"]),
    "SELECT jsonb_typeof('{}'::jsonb) AS v": (TEXT, ["object"]),
    "SELECT pg_size_bytes('1 kB') AS v": (INT8, ["1024"]),
    "SELECT pg_size_bytes('1.5 GB') AS v": (INT8, ["1610612736"]),
    # Time.
    "SELECT date_add('2024-01-01 00:00:00+00'::timestamptz, '1 day'::interval) AS v": (
        TIMESTAMPTZ,
        ["2024-01-02 00:00:00+00"],
    ),
    "SELECT isfinite('1 day'::interval) AS v": (BOOL, ["t"]),
    "SELECT timezone('1 hour'::interval, '2024-01-01 00:00:00+00'::timestamptz) AS v": (
        TIMESTAMP,
        ["2024-01-01 01:00:00"],
    ),
    "SELECT timezone('1 hour'::interval, '12:00:00+00'::timetz) AS v": (TIMETZ, ["13:00:00+01"]),
    "SELECT timezone('-2 hours'::interval, '12:00:00+00'::timetz) AS v": (TIMETZ, ["10:00:00-02"]),
    # Xact ids and snapshots.
    "SELECT pg_snapshot_xmin('10:20:12,15'::pg_snapshot) AS v": (XID8, ["10"]),
    "SELECT pg_snapshot_xmax('10:20:12,15'::pg_snapshot) AS v": (XID8, ["20"]),
    "SELECT pg_snapshot_xip('10:20:12,15'::pg_snapshot) AS v": (XID8, ["12", "15"]),
    "SELECT pg_visible_in_snapshot('5'::xid8, '10:20:12'::pg_snapshot) AS v": (BOOL, ["t"]),
    "SELECT pg_visible_in_snapshot('12'::xid8, '10:20:12'::pg_snapshot) AS v": (BOOL, ["f"]),
    "SELECT pg_visible_in_snapshot('13'::xid8, '10:20:12'::pg_snapshot) AS v": (BOOL, ["t"]),
    "SELECT pg_visible_in_snapshot('25'::xid8, '10:20:12'::pg_snapshot) AS v": (BOOL, ["f"]),
    "SELECT txid_snapshot_xmin('10:20:'::txid_snapshot) AS v": (INT8, ["10"]),
    "SELECT txid_visible_in_snapshot(5, '10:20:'::txid_snapshot) AS v": (BOOL, ["t"]),
    "SELECT pg_current_xact_id_if_assigned() AS v": (XID8, [None]),
    "SELECT txid_current_if_assigned() AS v": (INT8, [None]),
    "SELECT xid8eq('1'::xid8, '1'::xid8) AS v": (BOOL, ["t"]),
    "SELECT xid8cmp('1'::xid8, '2'::xid8) AS v": (INT4, ["-1"]),
    "SELECT xid8_larger('1'::xid8, '2'::xid8) AS v": (XID8, ["2"]),
    "SELECT xid8send('1'::xid8) AS v": (BYTEA, ["\\x0000000000000001"]),
    "SELECT mxid_age('0'::xid) AS v": (INT4, ["2147483647"]),
    "SELECT regtypesend(23::regtype) AS v": (BYTEA, ["\\x00000017"]),
    # The WAL of a server that has written none.
    "SELECT pg_current_wal_lsn() AS v": (PG_LSN, ["0/0"]),
    "SELECT pg_last_wal_receive_lsn() AS v": (PG_LSN, [None]),
    "SELECT pg_last_xact_replay_timestamp() AS v": (TIMESTAMPTZ, [None]),
    "SELECT pg_wal_lsn_diff('0/10'::pg_lsn, '0/8'::pg_lsn) AS v": (NUMERIC, ["8"]),
    "SELECT pg_walfile_name('0/1000000'::pg_lsn) AS v": (TEXT, ["000000010000000000000001"]),
    "SELECT pg_lsn(23::numeric) AS v": (PG_LSN, ["0/17"]),
    "SELECT pg_walfile_name_offset('0/1000010'::pg_lsn) AS v": (
        RECORD,
        ["(000000010000000000000001,16)"],
    ),
    "SELECT pg_split_walfile_name('000000010000000000000001') AS v": (RECORD, ["(1,1)"]),
    "SELECT pg_rotate_logfile() AS v": (BOOL, ["f"]),
    # Object identifiers by name: NULL for no such object.
    "SELECT to_regclass('pg_class') AS v": (2205, ["pg_class"]),
    "SELECT to_regclass('pg_catalog.pg_class') AS v": (2205, ["pg_class"]),
    "SELECT to_regclass('no_such_relation') AS v": (2205, [None]),
    "SELECT to_regnamespace('pg_catalog') AS v": (4089, ["pg_catalog"]),
    "SELECT to_regnamespace('no_such_schema') AS v": (4089, [None]),
    "SELECT to_regtype('integer') AS v": (2206, ["integer"]),
    "SELECT to_regtype('no_such_type') AS v": (2206, [None]),
    "SELECT to_regrole('no_such_role') AS v": (4096, [None]),
    "SELECT to_regproc('no_such_function') AS v": (24, [None]),
    "SELECT to_regtypemod('varchar(32)') AS v": (INT4, ["36"]),
    "SELECT to_regtypemod('numeric(10,2)') AS v": (INT4, ["655366"]),
    "SELECT to_regtypemod('integer') AS v": (INT4, ["-1"]),
    "SELECT to_regtypemod('no_such_type') AS v": (INT4, [None]),
    "SELECT regclass('pg_catalog.pg_class') AS v": (2205, ["pg_class"]),
    "SELECT pg_partition_root(2147483000) AS v": (2205, [None]),
    "SELECT pg_get_object_address('schema', '{pg_catalog}', '{}') AS v": (
        RECORD,
        ["(2615,11,0)"],
    ),
    "SELECT pg_identify_object_as_address(1247, 23, 0) AS v": (RECORD, ["(type,{integer},{})"]),
    # Input functions.
    "SELECT pg_input_is_valid('42', 'integer') AS v": (BOOL, ["t"]),
    "SELECT pg_input_is_valid('x', 'integer') AS v": (BOOL, ["f"]),
    "SELECT pg_input_is_valid(NULL::text, 'integer') AS v": (BOOL, [None]),
    "SELECT pg_input_error_info('42', 'integer') AS v": (RECORD, ["(,,,)"]),
    "SELECT pg_input_error_info('x', 'integer') AS v": (
        RECORD,
        ['("invalid input syntax for type integer: ""x""",,,22P02)'],
    ),
    # Encodings, and who is asking.
    "SELECT pg_encoding_max_length(6) AS v": (INT4, ["4"]),
    "SELECT pg_encoding_max_length(0) AS v": (INT4, ["1"]),
    "SELECT pg_encoding_max_length(35) AS v": (INT4, ["2"]),
    "SELECT pg_encoding_max_length(99) AS v": (INT4, [None]),
    "SELECT getpgusername() AS v": (NAME, ["alice"]),
    "SELECT pg_catalog.system_user() AS v": (TEXT, [None]),
    # Money, bits, ranges.
    "SELECT money(1::integer) AS v": (MONEY, ["$1.00"]),
    "SELECT flt8_mul_cash(2.5::float8, 2::money) AS v": (MONEY, ["$5.00"]),
    "SELECT bitcat(B'10'::bit varying, B'01'::bit varying) AS v": (1562, ["1001"]),
    "SELECT bitshiftleft(B'0011'::bit(4), 1) AS v": (1560, ["0110"]),
    "SELECT bitshiftright(B'1100'::bit(4), 1) AS v": (1560, ["0110"]),
    "SELECT numrange(1.5, 2.5) AS v": (3906, ["[1.5,2.5)"]),
    "SELECT numrange(1, 1) AS v": (3906, ["empty"]),
    "SELECT json_object('{a,1,b,2}'::text[]) AS v": (114, ['{"a" : "1", "b" : "2"}']),
    "SELECT json_object('{a,b}'::text[], '{1,2}'::text[]) AS v": (
        114,
        ['{"a" : "1", "b" : "2"}'],
    ),
    "SELECT json_each('{\"a\": 1}'::json) AS v": (RECORD, ["(a,1)"]),
    "SELECT trunc('08:00:2b:01:02:03'::macaddr) AS v": (829, ["08:00:2b:00:00:00"]),
    # Geometry.
    "SELECT area('(2,2),(0,0)'::box) AS v": (FLOAT8, ["4"]),
    "SELECT area('((0,0),(1,0),(1,1),(0,1))'::path) AS v": (FLOAT8, ["1"]),
    "SELECT area('[(0,0),(1,0)]'::path) AS v": (FLOAT8, [None]),
    "SELECT center('(2,2),(0,0)'::box) AS v": (600, ["(1,1)"]),
    "SELECT center('<(1,2),3>'::circle) AS v": (600, ["(1,2)"]),
    "SELECT diagonal('(2,2),(0,0)'::box) AS v": (601, ["[(2,2),(0,0)]"]),
    "SELECT height('(2,3),(0,0)'::box) AS v": (FLOAT8, ["3"]),
    "SELECT width('(2,3),(0,0)'::box) AS v": (FLOAT8, ["2"]),
    "SELECT radius('<(1,2),3>'::circle) AS v": (FLOAT8, ["3"]),
    "SELECT isclosed('((0,0),(1,1))'::path) AS v": (BOOL, ["t"]),
    "SELECT isopen('((0,0),(1,1))'::path) AS v": (BOOL, ["f"]),
    "SELECT ishorizontal('[(0,1),(2,1)]'::lseg) AS v": (BOOL, ["t"]),
    "SELECT isvertical('[(1,0),(1,2)]'::lseg) AS v": (BOOL, ["t"]),
    "SELECT isparallel('[(0,0),(1,1)]'::lseg, '[(0,1),(1,2)]'::lseg) AS v": (BOOL, ["t"]),
    "SELECT isperp('[(0,0),(1,0)]'::lseg, '[(0,0),(0,1)]'::lseg) AS v": (BOOL, ["t"]),
    "SELECT pt_contained_circle('(1,1)'::point, '<(0,0),2>'::circle) AS v": (BOOL, ["t"]),
    "SELECT pt_contained_poly('(1,1)'::point, '((0,0),(2,0),(2,2),(0,2))'::polygon) AS v": (
        BOOL,
        ["t"],
    ),
    "SELECT pt_contained_poly('(3,3)'::point, '((0,0),(2,0),(2,2),(0,2))'::polygon) AS v": (
        BOOL,
        ["f"],
    ),
    "SELECT length('[(0,0),(3,4)]'::lseg) AS v": (FLOAT8, ["5"]),
    "SELECT length('[(0,0),(3,4)]'::path) AS v": (FLOAT8, ["5"]),
    # Text search values, in text form.
    "SELECT strip('a:1 b:2'::tsvector) AS v": (3614, ["'a' 'b'"]),
    "SELECT length('a:1 b:2'::tsvector) AS v": (INT4, ["2"]),
    "SELECT unnest('a:1 b:2'::tsvector) AS v": (RECORD, ["(a,{1},{D})", "(b,{2},{D})"]),
    # Statistics Provisa does not keep.
    "SELECT pg_stat_reset() AS v": (VOID, [""]),
    "SELECT pg_stat_reset_shared('wal') AS v": (VOID, [""]),
    "SELECT pg_stat_reset_shared(NULL::text) AS v": (VOID, [""]),
    "SELECT pg_stat_reset_replication_slot(NULL::text) AS v": (VOID, [""]),
    "SELECT pg_stat_have_stats('wal', 0, 0) AS v": (BOOL, ["t"]),
    "SELECT pg_stat_have_stats('relation', 1, 2) AS v": (BOOL, ["f"]),
    # What needs no object.
    "SELECT pg_client_encoding() AS v": (NAME, ["UTF8"]),
    "SELECT pg_jit_available() AS v": (BOOL, ["f"]),
    "SELECT pg_notification_queue_usage() AS v": (FLOAT8, ["0"]),
    "SELECT pg_trigger_depth() AS v": (INT4, ["0"]),
    "SELECT pg_listening_channels() AS v": (TEXT, []),
    "SELECT pg_ls_waldir() AS v": (RECORD, []),
    "SELECT pg_lock_status() AS v": (RECORD, []),
    "SELECT pg_replication_origin_oid('o') AS v": (OID, [None]),
    "SELECT pg_replication_origin_session_is_setup() AS v": (BOOL, ["f"]),
    "SELECT pg_ls_dir('no_such_dir', true, false) AS v": (TEXT, []),
    "SELECT pg_relation_filepath(1) AS v": (TEXT, [None]),
    "SELECT pg_safe_snapshot_blocking_pids(1) AS v": (1007, ["{}"]),
}


@pytest.mark.parametrize("sql", list(ANSWERS))
def test_a_function_is_answered_as_postgresql_answers_it(conn: Any, sql: str):
    res = _run(conn, sql)
    assert res.status == pq.ExecStatus.TUPLES_OK, (res.error_message or b"").decode()
    oid, rows = ANSWERS[sql]
    assert res.ftype(0) == oid
    if oid == FLOAT8:  # its digits are the wire's, a known gap of its own: the value is compared
        assert [v and float(v) for v in _rows(res)] == [v and float(v) for v in rows]
    else:
        assert _rows(res) == rows


REFUSALS: dict[str, tuple[str, str]] = {
    "SELECT binary_upgrade_set_next_pg_type_oid(1)": (
        "55P02",
        "function can only be called when server is in binary upgrade mode",
    ),
    "SELECT pg_promote()": ("55000", "recovery is not in progress"),
    "SELECT pg_backup_start('x')": (
        "55000",
        "WAL level not sufficient for making an online backup",
    ),
    "SELECT pg_backup_stop()": ("55000", "backup is not in progress"),
    "SELECT pg_drop_replication_slot('s')": ("42704", 'replication slot "s" does not exist'),
    "SELECT pg_copy_physical_replication_slot('s', 't')": (
        "42704",
        'replication slot "s" does not exist',
    ),
    "SELECT pg_replication_origin_drop('o')": (
        "42704",
        'replication origin "o" does not exist',
    ),
    "SELECT pg_replication_origin_session_setup('o')": (
        "42704",
        'replication origin "o" does not exist',
    ),
    "SELECT pg_replication_origin_session_progress(false)": (
        "55000",
        "no replication origin is configured",
    ),
    "SELECT pg_replication_slot_advance('s', '0/0')": ("XX000", "invalid target WAL LSN"),
    "SELECT pg_create_physical_replication_slot('s')": (
        "55000",
        'replication slots can only be used if "max_replication_slots" > 0',
    ),
    "SELECT pg_replication_origin_create('o')": (
        "55000",
        'cannot query or manipulate replication origin when "max_replication_slots" is 0',
    ),
    "SELECT pg_replication_slot_advance('s', '0/1')": (
        "42704",
        'replication slot "s" does not exist',
    ),
    "SELECT pg_ls_dir('no_such_dir')": (
        "58P01",
        'could not open directory "no_such_dir": No such file or directory',
    ),
    "SELECT pg_ls_dir('/etc')": ("42501", "absolute path not allowed"),
    "SELECT pg_ls_replslotdir('s')": ("42704", 'replication slot "s" does not exist'),
    "SELECT pg_stat_reset_shared('nope')": ("22023", 'unrecognized reset target: "nope"'),
    "SELECT pg_stat_reset_replication_slot('s')": (
        "42704",
        'replication slot "s" does not exist',
    ),
    "SELECT pg_stat_have_stats('nope', 0, 0)": ("22023", 'invalid statistics kind: "nope"'),
    "SELECT pg_xact_commit_timestamp('5'::xid)": ("55000", "could not get commit timestamp data"),
    "SELECT pg_xact_status('99999999999'::xid8)": (
        "22023",
        "transaction ID 99999999999 is in the future",
    ),
    "SELECT lo_open(1, 262144)": ("42704", "large object 1 does not exist"),
    "SELECT lo_get(1)": ("42704", "large object 1 does not exist"),
    "SELECT lo_close(0)": ("42704", "invalid large-object descriptor: 0"),
    "SELECT lo_create(0)": ("25006", "cannot execute lo_create() in a read-only transaction"),
    "SELECT lo_import('x')": (
        "58P01",
        'could not open server file "x": No such file or directory',
    ),
    "SELECT aclinsert('{}'::aclitem[], '=r/postgres'::aclitem)": (
        "0A000",
        "aclinsert is no longer supported",
    ),
    "SELECT get_byte('\\x01'::bytea, 5)": ("2202E", "index 5 out of valid range, 0..0"),
    "SELECT set_bit('\\x00'::bytea, 0, 2)": ("22023", "new bit must be 0 or 1"),
    "SELECT convert('\\xff'::bytea, 'UTF8', 'LATIN1')": (
        "22021",
        'invalid byte sequence for encoding "UTF8": 0xff',
    ),
    "SELECT convert_to('a', 'NOPE')": ("22023", 'invalid destination encoding name "NOPE"'),
    "SELECT regexp_match('a', 'a', 'g')": (
        "22023",
        'regexp_match() does not support the "global" option',
    ),
    "SELECT pg_size_bytes('1 XB')": ("22023", 'invalid size: "1 XB"'),
    "SELECT factorial(-1::bigint)": ("22003", "factorial of a negative number is undefined"),
    "SELECT to_ascii('a')": ("0A000", "encoding conversion from UTF8 to ASCII not supported"),
    "SELECT pg_input_is_valid('1', 'no_such_type')": (
        "42704",
        'type "no_such_type" does not exist',
    ),
    "SELECT pg_get_object_address('table', '{no_such_table}', '{}')": (
        "42P01",
        'relation "no_such_table" does not exist',
    ),
    "SELECT pg_get_object_address('nope', '{r}', '{}')": (
        "22023",
        'unrecognized object type "nope"',
    ),
    "SELECT currtid2('no_such_table', '(0,1)'::tid)": (
        "42P01",
        'relation "no_such_table" does not exist',
    ),
    "SELECT numrange(2, 1)": (
        "22000",
        "range lower bound must be less than or equal to range upper bound",
    ),
    "SELECT makeaclitem(0, 10, 'NOPE', false)": ("22023", 'unrecognized privilege type: "NOPE"'),
    "SELECT json_object('{a}'::text[])": ("2202E", "array must have even number of elements"),
}


@pytest.mark.parametrize("sql", list(REFUSALS))
def test_a_function_refuses_as_postgresql_refuses_it(conn: Any, sql: str):
    res = _run(conn, sql)
    assert res.status == pq.ExecStatus.FATAL_ERROR, _rows(res)
    assert res.error_field(pq.DiagnosticField.SQLSTATE).decode() == REFUSALS[sql][0]
    assert res.error_field(pq.DiagnosticField.MESSAGE_PRIMARY).decode() == REFUSALS[sql][1]


def test_sleeping_for_a_while_and_until_a_time_answer_void(conn: Any):
    for sql in (
        "SELECT pg_sleep_for('0 seconds'::interval) AS v",
        "SELECT pg_sleep_until('2000-01-01 00:00:00+00'::timestamptz) AS v",
    ):
        res = _run(conn, sql)
        assert res.status == pq.ExecStatus.TUPLES_OK, (res.error_message or b"").decode()
        assert (res.ftype(0), _rows(res)) == (VOID, [""])
    res = _run(conn, "SELECT pg_sleep_for(NULL::interval) AS v")
    assert (res.ftype(0), _rows(res)) == (VOID, [None])


def test_time_zones_are_listed(conn: Any):
    for sql in ("SELECT pg_timezone_names() AS v", "SELECT pg_timezone_abbrevs() AS v"):
        res = _run(conn, sql)
        assert res.status == pq.ExecStatus.TUPLES_OK, (res.error_message or b"").decode()
        assert res.ftype(0) == RECORD and res.ntuples > 0


def test_an_acl_item_is_written_as_aclitemout_writes_it(conn: Any):
    res = _run(conn, "SELECT makeaclitem(0, 10, 'SELECT, INSERT', false) AS v")
    assert res.status == pq.ExecStatus.TUPLES_OK, (res.error_message or b"").decode()
    assert res.ftype(0) == 1033
    assert _rows(res)[0].startswith("=ra/")


def test_a_transaction_id_advances_and_a_snapshot_is_after_it(conn: Any):
    first = int(_rows(_run(conn, "SELECT pg_current_xact_id() AS v"))[0])
    res = _run(conn, "SELECT pg_current_snapshot() AS v")
    assert res.ftype(0) == 5038
    xmin, xmax, xip = _rows(res)[0].split(":")
    assert int(xmin) == int(xmax) > first and xip == ""


def test_a_past_transaction_is_committed(conn: Any):
    res = _run(conn, "SELECT pg_xact_status('1'::xid8) AS v")
    assert (res.ftype(0), _rows(res)) == (TEXT, ["committed"])


def test_a_record_of_the_control_file_reads_in_from(conn: Any):
    res = _run(conn, "SELECT database_block_size FROM pg_control_init()")
    assert _rows(res) == ["8192"]


@pytest.mark.parametrize(
    ("degrees", "sine", "tangent"),
    [
        (0.0, 0.0, 0.0),
        (30.0, 0.5, None),
        (150.0, 0.5, None),
        (210.0, -0.5, None),
        (-90.0, -1.0, None),
    ],
)
def test_sind_and_tand_are_exact_where_postgresql_is(degrees, sine, tangent):
    assert scalars.sind(degrees) == sine
    if tangent is not None:
        assert scalars.tand(degrees) == tangent


def test_tand_is_never_minus_zero():
    assert str(scalars.tand(180.0)) == "0.0"
    assert str(scalars.tand(-180.0)) == "0.0"


def test_a_catalog_relation_has_postgresqls_oid(conn: Any):
    res = conn.conn.pgconn.exec_params(b"SELECT to_regclass('pg_class') AS v", [], result_format=1)
    assert (res.ftype(0), res.get_value(0, 0).hex()) == (2205, "000004eb")


def test_tand_of_a_right_angle_is_infinite():
    assert scalars.tand(90.0) == float("inf")
    assert scalars.tand(270.0) == float("-inf")


@pytest.mark.parametrize(
    ("pattern", "expected"),
    [("a%", "^(?:a.*)$"), ("(a|b)", "^(?:(?:a|b))$"), ("a.b", "^(?:a\\.b)$"), ("[%]", "^(?:[%])$")],
)
def test_similar_escape_is_postgresqls_translation(pattern, expected):
    assert scalars._similar_escape(pattern, None) == expected


@pytest.mark.parametrize(
    ("text", "strict", "parts"),
    [
        ('"Some.Name".Tbl', True, ["Some.Name", "tbl"]),
        ("a . b", True, ["a", "b"]),
        ("a b", False, ["a"]),
    ],
)
def test_an_identifier_is_parsed_as_postgresql_parses_it(text, strict, parts):
    assert scalars.parse_ident(text, strict) == parts


@pytest.mark.parametrize("text", ["", ".a", "a.", '""', '"a', "a b"])
def test_a_bad_identifier_is_refused(text):
    with pytest.raises(ValueError, match="string is not a valid identifier"):
        scalars.parse_ident(text, True)


def test_a_cast_alone_stays_the_engines():
    from provisa.pgwire.catalog import classify

    assert classify("SELECT '(1,2)'::point AS v") == "PASS_THROUGH"
    assert classify("SELECT '1'::money AS v") == "PASS_THROUGH"
    assert classify("SELECT center('<(1,2),3>'::circle) AS v") == "INTERCEPT"


def test_a_statement_with_no_rewrite_is_returned_as_written():
    from provisa.transpiler.pg_functions import typed_calls

    sql = "select  length( a ) , sum(b)  from t"
    assert typed_calls(sql) == sql


def test_a_bytea_substring_before_its_start_uses_up_the_length():
    assert scalars.bytea_substr(b"\x01\x02\x03", -1, 3) == b"\x01"


def _expected_oid(name: str, args: tuple[str, ...]) -> int | None:
    returns = {r for params, r in FUNCTION_OVERLOADS.get(name, ()) if params == args}
    if len(returns) != 1:
        return None
    return next((oid for oid, entry in TYPES.items() if entry[-1] == next(iter(returns))), None)


def _signature_problem(conn: Any, name: str, args: tuple[str, ...], strict: bool) -> str | None:
    # As the conformance probe calls it: every argument a NULL of its type.
    listed = ", ".join(f"NULL::{t}" for t in args)
    res = _run(conn, f'SELECT pg_catalog."{name}"({listed}) AS v')
    if res.status != pq.ExecStatus.TUPLES_OK:
        return (res.error_message or b"").decode()
    expected = _expected_oid(name, args)
    if expected is not None and expected != ANYELEMENT and res.ftype(0) != expected:
        return f"type {res.ftype(0)}, PostgreSQL {expected}"
    if strict and args and any(v is not None for v in _rows(res)):
        return f"strict, but answered {_rows(res)}"
    return None


# One test over all of them: conn builds a DuckDB runtime per test, and hundreds of those in one
# worker exhaust it.
def test_every_signature_is_served_with_postgresqls_type(conn: Any):
    problems = {
        f"{name}({', '.join(args)})": problem
        for name, args, strict in SIGNATURES
        if (problem := _signature_problem(conn, name, args, strict)) is not None
    }
    assert not problems
    assert len(SIGNATURES) == 461


def _recorded_problem(conn: Any, sql: str) -> str | None:
    res = _run(conn, f"{sql} AS v")
    kind, first, second = RECORDED[sql]
    if kind == "err":
        if res.status != pq.ExecStatus.FATAL_ERROR:
            return f"answered {_rows(res)}"
        said = (
            res.error_field(pq.DiagnosticField.SQLSTATE).decode(),
            res.error_field(pq.DiagnosticField.MESSAGE_PRIMARY).decode(),
        )
        return None if said == (first, second) else str(said)
    if res.status != pq.ExecStatus.TUPLES_OK:
        return (res.error_message or b"").decode()
    if res.ftype(0) != first:
        return f"type {res.ftype(0)}"
    rows = _rows(res)
    return None if rows == second else str(rows)


def test_a_function_answers_what_postgresql_17_answered(conn: Any):
    problems = {
        sql: problem for sql in RECORDED if (problem := _recorded_problem(conn, sql)) is not None
    }
    assert not problems
