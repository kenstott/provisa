# Copyright (c) 2026 Kenneth Stott
# Canary: 6005d33f-115e-4f84-aafc-a2d916ed83bf
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The rest of the functions conformance run's wrong answers on DuckDB (outcome wrong_answer, after
test_duckdb_wrong_answers), each sent as the probe sends it through a real pgwire server on the
DuckDB engine, against PostgreSQL 17.11's answer to the same statement (REQ-2167)."""

# Requirements: REQ-2167

# ruff: noqa: F811 -- the fixtures are conn and pgwire of the module they are imported from

from __future__ import annotations

from typing import Any

import pytest
from psycopg import pq

from tests.unit.pgwire.test_duckdb_wrong_answers import (
    test_duckdb_answers_as_postgresql_17 as _answers,
)
from tests.unit.pgwire.test_type_conversions import conn, pgwire  # noqa: F401

INT4, FLOAT8, NUMERIC, TEXT, JSON, JSONB, BOOL, BOX = 23, 701, 1700, 25, 114, 3802, 16, 603

#: (statement, PostgreSQL 17's result type, its rows' text)
ANSWERS: list[tuple[str, int, list[Any]]] = [
    # A bit string's length is its bits, not the bytes of its text.
    ("SELECT pg_catalog.\"bit_length\"('1'::bit) AS v", INT4, ["1"]),
    ("SELECT pg_catalog.\"bit_length\"('101'::bit(3)) AS v", INT4, ["3"]),
    # A flag given as text is the flag: 'i' matches the first match whatever its case.
    (
        "SELECT pg_catalog.\"regexp_replace\"('Hello World'::text, '[a-z]+'::text, "
        "'Hello World'::text, 'i'::text) AS v",
        TEXT,
        ["Hello World World"],
    ),
    (
        "SELECT pg_catalog.\"regexp_replace\"('Hello World'::text, '[a-z]+'::text, 'X'::text, "
        "'gi'::text) AS v",
        TEXT,
        ["X X"],
    ),
    # regexp_instr's endoption 1 is the position just past the match.
    (
        "SELECT pg_catalog.\"regexp_instr\"('Hello World'::text, '[a-z]+'::text, 2::integer, "
        "1::integer, 1::integer) AS v",
        INT4,
        ["6"],
    ),
    (
        "SELECT pg_catalog.\"regexp_instr\"('Hello World'::text, '[a-z]+'::text, 2::integer, "
        "1::integer, 1::integer, 'c'::text) AS v",
        INT4,
        ["6"],
    ),
    (
        "SELECT pg_catalog.\"regexp_instr\"('Hello World'::text, '[a-z]+'::text, 2::integer, "
        "1::integer, 1::integer, 'c'::text, 0::integer) AS v",
        INT4,
        ["6"],
    ),
    (
        "SELECT pg_catalog.\"regexp_instr\"('Hello World'::text, '[a-z]+'::text, 2::integer, "
        "1::integer, 0::integer) AS v",
        INT4,
        ["2"],
    ),
    # A second keeps its fraction: date_part's is a float8, extract's a numeric of scale 6.
    (
        "SELECT pg_catalog.\"date_part\"('second'::text, '23:59:59.5'::time without time zone) "
        "AS v",
        FLOAT8,
        ["59.5"],
    ),
    (
        "SELECT pg_catalog.\"date_part\"('second'::text, '23:59:59.5-05'::time with time zone) "
        "AS v",
        FLOAT8,
        ["59.5"],
    ),
    (
        "SELECT pg_catalog.\"extract\"('second'::text, '23:59:59.5'::time without time zone) AS v",
        NUMERIC,
        ["59.500000"],
    ),
    (
        "SELECT pg_catalog.\"extract\"('second'::text, '23:59:59.5-05'::time with time zone) AS v",
        NUMERIC,
        ["59.500000"],
    ),
    # An anonymous row's fields are f1, f2, ...
    (
        "SELECT pg_catalog.\"row_to_json\"(ROW(1, 'a'::text)::record) AS v",
        JSON,
        ['{"f1":1,"f2":"a"}'],
    ),
    ("SELECT row_to_json(ROW(1, ROW(2, 'b'))) AS v", JSON, ['{"f1":1,"f2":{"f1":2,"f2":"b"}}']),
    (
        'SELECT pg_catalog."jsonb_populate_record_valid"(NULL::pg_catalog.pg_namespace, '
        "NULL::jsonb) AS v",
        BOOL,
        [None],
    ),
    ("SELECT pg_catalog.\"jsonb_in\"('12'::cstring) AS v", JSONB, ["12"]),
    (
        "SELECT pg_catalog.\"max\"(c1) AS v FROM (VALUES ('(0,1)'::tid), ('(5,3)'::tid), "
        "('(4294967295,65535)'::tid)) AS t(c1)",
        27,
        ["(4294967295,65535)"],
    ),
    # 1 * 0 is 0 and -1 * 0 is -0, as float8 multiplies them.
    (
        "SELECT pg_catalog.\"box_mul\"('(1,1),(-1,-1)'::box, '(0,0)'::point) AS v",
        BOX,
        ["(0,-0),(0,0)"],
    ),
]


@pytest.mark.parametrize(("sql", "oid", "rows"), ANSWERS, ids=[a[0] for a in ANSWERS])
def test_duckdb_answers_the_rest_as_postgresql_17(conn: Any, sql: str, oid: int, rows: list[Any]):
    _answers(conn, sql, oid, rows)


_TABLES = "'pg_catalog.pg_tables'::regclass::oid"

#: pg_get_viewdef at a wrap column (a catalog answers it), each against PostgreSQL 17.11's text:
#: a target starts a new line only where it would pass the column.
CATALOG_ANSWERS: list[tuple[str, int, list[Any]]] = [
    (
        f'SELECT pg_catalog."pg_get_viewdef"({_TABLES}, 80::integer) AS v',
        TEXT,
        [
            " SELECT n.nspname AS schemaname, c.relname AS tablename,\n    pg_get_userbyid(c.relowner) AS tableowner, t.spcname AS tablespace,\n    c.relhasindex AS hasindexes, c.relhasrules AS hasrules,\n    c.relhastriggers AS hastriggers, c.relrowsecurity AS rowsecurity\n   FROM pg_class c\n     LEFT JOIN pg_namespace n ON n.oid = c.relnamespace\n     LEFT JOIN pg_tablespace t ON t.oid = c.reltablespace\n  WHERE c.relkind = ANY (ARRAY['r'::\"char\", 'p'::\"char\"]);"
        ],
    ),
    (
        f'SELECT pg_catalog."pg_get_viewdef"({_TABLES}, 120::integer) AS v',
        TEXT,
        [
            " SELECT n.nspname AS schemaname, c.relname AS tablename, pg_get_userbyid(c.relowner) AS tableowner,\n    t.spcname AS tablespace, c.relhasindex AS hasindexes, c.relhasrules AS hasrules, c.relhastriggers AS hastriggers,\n    c.relrowsecurity AS rowsecurity\n   FROM pg_class c\n     LEFT JOIN pg_namespace n ON n.oid = c.relnamespace\n     LEFT JOIN pg_tablespace t ON t.oid = c.reltablespace\n  WHERE c.relkind = ANY (ARRAY['r'::\"char\", 'p'::\"char\"]);"
        ],
    ),
    (
        f'SELECT pg_catalog."pg_get_viewdef"({_TABLES}, 40::integer) AS v',
        TEXT,
        [
            " SELECT n.nspname AS schemaname,\n    c.relname AS tablename,\n    pg_get_userbyid(c.relowner) AS tableowner,\n    t.spcname AS tablespace,\n    c.relhasindex AS hasindexes,\n    c.relhasrules AS hasrules,\n    c.relhastriggers AS hastriggers,\n    c.relrowsecurity AS rowsecurity\n   FROM pg_class c\n     LEFT JOIN pg_namespace n ON n.oid = c.relnamespace\n     LEFT JOIN pg_tablespace t ON t.oid = c.reltablespace\n  WHERE c.relkind = ANY (ARRAY['r'::\"char\", 'p'::\"char\"]);"
        ],
    ),
    (
        f'SELECT pg_catalog."pg_get_viewdef"({_TABLES}, (-1)::integer) AS v',
        TEXT,
        [
            " SELECT n.nspname AS schemaname, c.relname AS tablename, pg_get_userbyid(c.relowner) AS tableowner, t.spcname AS tablespace, c.relhasindex AS hasindexes, c.relhasrules AS hasrules, c.relhastriggers AS hastriggers, c.relrowsecurity AS rowsecurity\n   FROM pg_class c\n     LEFT JOIN pg_namespace n ON n.oid = c.relnamespace\n     LEFT JOIN pg_tablespace t ON t.oid = c.reltablespace\n  WHERE c.relkind = ANY (ARRAY['r'::\"char\", 'p'::\"char\"]);"
        ],
    ),
    (
        "SELECT pg_catalog.\"pg_get_viewdef\"('pg_catalog.pg_roles'::regclass::oid, 80::integer) AS v",
        TEXT,
        [
            " SELECT pg_authid.rolname, pg_authid.rolsuper, pg_authid.rolinherit,\n    pg_authid.rolcreaterole, pg_authid.rolcreatedb, pg_authid.rolcanlogin,\n    pg_authid.rolreplication, pg_authid.rolconnlimit,\n    '********'::text AS rolpassword, pg_authid.rolvaliduntil,\n    pg_authid.rolbypassrls, s.setconfig AS rolconfig, pg_authid.oid\n   FROM pg_authid\n     LEFT JOIN pg_db_role_setting s ON pg_authid.oid = s.setrole AND s.setdatabase = 0::oid;"
        ],
    ),
]


@pytest.mark.parametrize(
    ("sql", "oid", "rows"), CATALOG_ANSWERS, ids=[a[0] for a in CATALOG_ANSWERS]
)
def test_a_catalog_answers_the_rest_as_postgresql_17(
    conn: Any, sql: str, oid: int, rows: list[Any]
):
    _answers(conn, sql, oid, rows)


def test_every_system_view_at_wrap_column_0_is_its_pretty_definition():
    """The wrap data and the definitions are read from PostgreSQL 17.11 apart: at wrap column 0
    every separator is wrapped, which is pg_get_viewdef(view, true)."""
    from provisa.pgwire.catalog_pg17_procs import VIEW_DEFINITIONS
    from provisa.pgwire.catalog_pg17_viewdefs import VIEW_WRAPS

    assert set(VIEW_WRAPS) == set(VIEW_DEFINITIONS)
    for oid, (texts, separators) in VIEW_WRAPS.items():
        wrapped = [texts[0]]
        for (form, _one_line, spans), text in zip(separators, texts[1:], strict=True):
            assert any(low <= 0 and (high is None or 0 <= high) for low, high in spans), oid
            wrapped += [form, text]
        assert "".join(wrapped) == VIEW_DEFINITIONS[oid][1], oid


#: pg_timezone_abbrevs' rows on PostgreSQL 17.11, as abbrev|utc_offset|is_dst.
PG17_ABBREVS = (
    "ACDT|10:30:00|true ACSST|10:30:00|true ACST|09:30:00|false ACT|-05:00:00|false "
    "ACWST|08:45:00|false ADT|-03:00:00|true AEDT|11:00:00|true AESST|11:00:00|true "
    "AEST|10:00:00|false AFT|04:30:00|false AKDT|-08:00:00|true AKST|-09:00:00|false "
    "ALMST|07:00:00|true ALMT|06:00:00|false AMST|04:00:00|false AMT|-04:00:00|false "
    "ANAST|12:00:00|false ANAT|12:00:00|false ARST|-03:00:00|false "
    "ART|-03:00:00|false AST|-04:00:00|false AWSST|09:00:00|true AWST|08:00:00|false "
    "AZOST|00:00:00|true AZOT|-01:00:00|false AZST|04:00:00|false AZT|04:00:00|false "
    "BDST|02:00:00|true BDT|06:00:00|false BNT|08:00:00|false BORT|08:00:00|false "
    "BOT|-04:00:00|false BRA|-03:00:00|false BRST|-02:00:00|true BRT|-03:00:00|false "
    "BST|01:00:00|true BTT|06:00:00|false CADT|10:30:00|true CAST|09:30:00|false "
    "CCT|08:00:00|false CDT|-05:00:00|true CEST|02:00:00|true CET|01:00:00|false "
    "CETDST|02:00:00|true CHADT|13:45:00|true CHAST|12:45:00|false "
    "CHUT|10:00:00|false CKT|-10:00:00|false CLST|-03:00:00|true CLT|-03:00:00|true "
    "COT|-05:00:00|false CST|-06:00:00|false CXT|07:00:00|false DAVT|07:00:00|false "
    "DDUT|10:00:00|false EASST|-05:00:00|true EAST|-05:00:00|true EAT|03:00:00|false "
    "EDT|-04:00:00|true EEST|03:00:00|true EET|02:00:00|false EETDST|03:00:00|true "
    "EGST|00:00:00|true EGT|-01:00:00|false EST|-05:00:00|false FET|03:00:00|false "
    "FJST|13:00:00|true FJT|12:00:00|false FKST|-03:00:00|false FKT|-03:00:00|false "
    "FNST|-01:00:00|true FNT|-02:00:00|false GALT|-06:00:00|false "
    "GAMT|-09:00:00|false GEST|04:00:00|false GET|04:00:00|false GFT|-03:00:00|false "
    "GILT|12:00:00|false GMT|00:00:00|false GYT|-04:00:00|false HKT|08:00:00|false "
    "HST|-10:00:00|false ICT|07:00:00|false IDT|03:00:00|true IOT|06:00:00|false "
    "IRKST|08:00:00|false IRKT|08:00:00|false IRT|03:30:00|false IST|02:00:00|false "
    "JAYT|09:00:00|false JST|09:00:00|false KDT|10:00:00|true KGST|06:00:00|true "
    "KGT|06:00:00|false KOST|11:00:00|false KRAST|07:00:00|false KRAT|07:00:00|false "
    "KST|09:00:00|false LHDT|11:00:00|true LHST|10:30:00|false LIGT|10:00:00|false "
    "LINT|14:00:00|false LKT|05:30:00|false MAGST|11:00:00|false MAGT|11:00:00|false "
    "MART|-09:30:00|false MAWT|05:00:00|false MDT|-06:00:00|true MEST|02:00:00|true "
    "MESZ|02:00:00|true MET|01:00:00|false METDST|02:00:00|true MEZ|01:00:00|false "
    "MHT|12:00:00|false MMT|06:30:00|false MPT|10:00:00|false MSD|04:00:00|true "
    "MSK|03:00:00|false MST|-07:00:00|false MUST|05:00:00|true MUT|04:00:00|false "
    "MVT|05:00:00|false MYT|08:00:00|false NDT|-02:30:00|true NFT|-03:30:00|false "
    "NOVST|07:00:00|false NOVT|07:00:00|false NPT|05:45:00|false NST|-03:30:00|false "
    "NUT|-11:00:00|false NZDT|13:00:00|true NZST|12:00:00|false NZT|12:00:00|false "
    "OMSST|06:00:00|false OMST|06:00:00|false PDT|-07:00:00|true PET|-05:00:00|false "
    "PETST|12:00:00|false PETT|12:00:00|false PGT|10:00:00|false PHT|08:00:00|false "
    "PKST|06:00:00|true PKT|05:00:00|false PMDT|-02:00:00|true PMST|-03:00:00|false "
    "PONT|11:00:00|false PST|-08:00:00|false PWT|09:00:00|false PYST|-03:00:00|true "
    "PYT|-03:00:00|false RET|04:00:00|false SADT|10:30:00|true SAST|02:00:00|false "
    "SCT|04:00:00|false SGT|08:00:00|false TAHT|-10:00:00|false TFT|05:00:00|false "
    "TJT|05:00:00|false TKT|13:00:00|false TMT|05:00:00|false TOT|13:00:00|false "
    "TRUT|10:00:00|false TVT|12:00:00|false UCT|00:00:00|false ULAST|09:00:00|true "
    "ULAT|08:00:00|false UT|00:00:00|false UTC|00:00:00|false UYST|-02:00:00|true "
    "UYT|-03:00:00|false UZST|06:00:00|true UZT|05:00:00|false VET|-04:00:00|false "
    "VLAST|10:00:00|false VLAT|10:00:00|false VOLT|03:00:00|false VUT|11:00:00|false "
    "WADT|08:00:00|true WAKT|12:00:00|false WAST|07:00:00|false WAT|01:00:00|false "
    "WDT|09:00:00|true WET|00:00:00|false WETDST|01:00:00|true WFT|12:00:00|false "
    "WGST|-02:00:00|true WGT|-03:00:00|false XJT|06:00:00|false YAKST|09:00:00|false "
    "YAKT|09:00:00|false YAPT|10:00:00|false YEKST|06:00:00|true YEKT|05:00:00|false "
    "Z|00:00:00|false ZULU|00:00:00|false "
).split()


def _rows(conn: Any, sql: str) -> tuple[int, list[str]]:
    res = conn.conn.pgconn.exec_params(sql.encode(), [], result_format=0)
    assert res.status == pq.ExecStatus.TUPLES_OK, (res.error_message or b"").decode()
    return res.ftype(0), [res.get_value(i, 0).decode() for i in range(res.ntuples)]


def test_the_abbreviations_are_postgresql_17s_default_set(conn: Any):
    oid, rows = _rows(conn, 'SELECT pg_catalog."pg_timezone_abbrevs"() AS v')
    said = [row.split("|") for row in PG17_ABBREVS]
    want = [f"({a},{o},{'t' if d == 'true' else 'f'})" for a, o, d in said]
    assert (oid, sorted(rows)) == (2249, sorted(want))
