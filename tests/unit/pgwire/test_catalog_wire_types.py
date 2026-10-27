# Copyright (c) 2026 Kenneth Stott
# Canary: 190e8b11-cf6f-4ecf-abec-7c9d6f7e5892
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A catalog column reaches the client as the type PostgreSQL 17 declares for it (REQ-2167):
information_schema's sql_identifier, character_data, yes_or_no and cardinal_number, and
pg_catalog's oid, name and "char", wherever the statement reads the column as it is."""

from __future__ import annotations

import io
import struct
from unittest.mock import MagicMock

from provisa.pgwire.catalog import answer
from provisa.pgwire.server import ProvisaQueryResult

SQL_IDENTIFIER, CHARACTER_DATA, YES_OR_NO, CARDINAL_NUMBER = 13302, 13300, 13310, 13297
OID, NAME, CHAR, TEXT, INT4, INT8, FLOAT8, TIMESTAMPTZ = 26, 19, 18, 25, 23, 20, 701, 1184
REGPROC, REGTYPE = 24, 2206


def _state():
    ctx = MagicMock()
    ctx.tables = {}
    state = MagicMock()
    state.multitenancy = False
    state.contexts = {"testrole": ctx}
    state.schema_build_cache = {"column_types": {}}
    return state


def _oids(sql: str) -> dict[str, int]:
    """Each column's name and the type OID the RowDescription gives the client."""
    from buenavista.postgres import BuenaVistaHandler

    result = ProvisaQueryResult(answer(sql, "testrole", _state()), sql)
    handler = BuenaVistaHandler.__new__(BuenaVistaHandler)
    handler.wfile = io.BytesIO()
    handler.send_row_description(result)
    out = handler.wfile.getvalue()
    (count,) = struct.unpack("!h", out[5:7])
    oids, at = {}, 7
    for _ in range(count):
        end = out.index(b"\x00", at)
        name = out[at:end].decode()
        _table, _attnum, type_oid, _len, _mod, _fmt = struct.unpack(
            "!ihihih", out[end + 1 : end + 19]
        )
        oids[name] = type_oid
        at = end + 19
    return oids


def test_information_schema_tables_are_named_by_their_domains():
    oids = _oids("SELECT * FROM information_schema.tables LIMIT 0")
    assert oids["table_catalog"] == SQL_IDENTIFIER
    assert oids["table_name"] == SQL_IDENTIFIER
    assert oids["table_type"] == CHARACTER_DATA
    assert oids["is_insertable_into"] == YES_OR_NO


def test_a_cardinal_number_is_its_domain():
    oids = _oids("SELECT ordinal_position, column_name FROM information_schema.columns LIMIT 0")
    assert oids == {"ordinal_position": CARDINAL_NUMBER, "column_name": SQL_IDENTIFIER}


def test_pg_catalog_columns_through_aliases_and_a_join():
    oids = _oids(
        "SELECT c.oid, c.relname AS r, c.relkind, n.nspname FROM pg_catalog.pg_class c "
        "JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace LIMIT 0"
    )
    assert oids == {"oid": OID, "r": NAME, "relkind": CHAR, "nspname": NAME}


def test_an_unqualified_catalog_relation_is_pg_catalogs():
    assert _oids("SELECT nspname FROM pg_namespace LIMIT 0") == {"nspname": NAME}


def test_a_column_read_through_a_subquery_or_a_cte_keeps_its_type():
    assert _oids("SELECT x.relname FROM (SELECT relname FROM pg_class) x LIMIT 0") == {
        "relname": NAME
    }
    assert _oids("WITH w AS (SELECT oid FROM pg_class) SELECT * FROM w LIMIT 0") == {"oid": OID}


def test_a_computed_column_is_the_type_it_computes():
    assert _oids("SELECT upper(relname) AS u, length(relname) AS l FROM pg_class LIMIT 0") == {
        "u": TEXT,
        "l": INT4,
    }


def test_a_described_type_is_in_pg_type():
    result = answer(
        "SELECT oid, typname, typtype, typbasetype FROM pg_catalog.pg_type "
        "WHERE oid IN (13302, 13297, 19, 26) ORDER BY oid",
        "testrole",
        _state(),
    )
    assert [tuple(r) for r in result.rows] == [
        (19, "name", "b", 0),
        (26, "oid", "b", 0),
        (13297, "cardinal_number", "d", 23),
        (13302, "sql_identifier", "d", 19),
    ]


def test_a_catalog_function_returns_the_type_postgresql_declares():
    def returned(call: str) -> int:
        return _oids(f"SELECT {call} AS v")["v"]

    assert returned("pg_catalog.pg_stat_get_numscans(NULL::oid)") == INT8
    assert returned("pg_catalog.pg_stat_get_function_total_time(NULL::oid)") == FLOAT8
    assert returned("pg_catalog.pg_stat_get_last_vacuum_time(NULL::oid)") == TIMESTAMPTZ
    assert returned("pg_catalog.pg_stat_get_backend_activity(NULL::integer)") == TEXT
    assert returned("pg_catalog.pg_stat_get_backend_dbid(NULL::integer)") == OID
    assert returned("pg_catalog.pg_get_expr(NULL::pg_node_tree, NULL::oid)") == TEXT
    assert returned('pg_catalog."current_database"()') == NAME
    assert returned('pg_catalog."current_user"()') == NAME


# The PostgreSQL types a served catalog column is stored and sent as, beyond the ones above.
_STORED_AS_POSTGRESQL_DOES = frozenset(
    {
        "timestamp with time zone",
        "information_schema.time_stamp",
        "interval",
        "numeric",
        "bytea",
        "boolean",
        "text[]",
        "smallint[]",
        "regproc",
        "regtype",
        "oid[]",
        "oidvector",
        "int2vector",
        "aclitem[]",
        '"char"[]',
        "inet",
        "pg_lsn",
    }
)


def test_every_served_catalog_column_of_a_stored_type_is_postgresqls():
    from provisa.pgwire.catalog_data import _TABLE_MAP
    from provisa.pgwire.catalog_pg17 import RELATIONS

    differing = []
    for schema, relation in sorted(_TABLE_MAP):
        if (schema, relation) not in RELATIONS:
            continue
        served = _oids(f"SELECT * FROM {schema}.{relation} LIMIT 0")
        for column, pg_type, pg_oid in RELATIONS[(schema, relation)]:
            if pg_type in _STORED_AS_POSTGRESQL_DOES and served.get(column, pg_oid) != pg_oid:
                differing.append(f"{schema}.{relation}.{column}: {served[column]}, not {pg_type}")
    assert differing == []


def _wire(sql: str) -> list[tuple[int, str, bytes]]:
    """Each column of the first row as (type OID, text form, binary form)."""
    from buenavista.postgres import BVTYPE_TO_PGTYPE

    result = ProvisaQueryResult(answer(sql, "testrole", _state()), sql)
    row = next(iter(result.rows()))
    out = []
    for i, value in enumerate(row):
        bvtype = result.column(i)[1]
        oid = result.column_type_oid(i)
        _own_oid, text, binary = BVTYPE_TO_PGTYPE[bvtype]
        out.append((_own_oid if oid is None else oid, text(value), binary(value)))
    return out


def test_a_regproc_is_its_function_name_as_text_and_its_oid_as_binary():
    assert _wire("SELECT amhandler FROM pg_catalog.pg_am WHERE amname = 'btree'") == [
        (REGPROC, "bthandler", (330).to_bytes(4, "big"))
    ]
    assert _wire("SELECT typinput FROM pg_catalog.pg_type WHERE oid = 23") == [
        (REGPROC, "-", (0).to_bytes(4, "big"))
    ]


def test_an_oid_array_and_the_vectors_are_written_as_postgresql_writes_them():
    from buenavista.core import BVType
    from buenavista.postgres import BVTYPE_TO_PGTYPE

    def header(element_oid: int, n: int) -> bytes:
        return struct.pack("!iiiii", 1, 0, element_oid, n, 1)

    oid_array, oidvector, int2vector = (
        BVTYPE_TO_PGTYPE[BVType.OIDARRAY],
        BVTYPE_TO_PGTYPE[BVType.OIDVECTOR],
        BVTYPE_TO_PGTYPE[BVType.INT2VECTOR],
    )
    assert oid_array[0] == 1028 and oid_array[1]([16, 3000000000]) == "{16,3000000000}"
    assert oid_array[2]([16]) == header(26, 1) + struct.pack("!iI", 4, 16)
    assert oidvector[0] == 30 and oidvector[1]([23, 25]) == "23 25"
    assert oidvector[2]([23]) == header(26, 1) + struct.pack("!iI", 4, 23)
    assert int2vector[0] == 22 and int2vector[1]([1, 3]) == "1 3"
    assert int2vector[2]([1]) == header(21, 1) + struct.pack("!ih", 2, 1)


def test_acl_char_array_inet_and_lsn_are_written_and_sent_as_postgresql_does():
    import pytest
    from buenavista.core import BVType
    from buenavista.postgres import BVTYPE_TO_PGTYPE

    acl = BVTYPE_TO_PGTYPE[BVType.ACLITEMARRAY]
    assert acl[0] == 1034 and acl[1](["=r/postgres", "app=arw/postgres"]) == (
        "{=r/postgres,app=arw/postgres}"
    )
    with pytest.raises(TypeError, match="aclitem"):  # PostgreSQL has no binary aclitem either
        acl[2](["=r/postgres"])
    chars = BVTYPE_TO_PGTYPE[BVType.CHARARRAY]
    assert chars[0] == 1002 and chars[1](["i", "o"]) == "{i,o}"
    assert chars[2](["i"]) == struct.pack("!iiiii", 1, 0, 18, 1, 1) + struct.pack("!i", 1) + b"i"
    inet = BVTYPE_TO_PGTYPE[BVType.INET]
    assert inet[0] == 869 and inet[1]("10.0.0.1") == "10.0.0.1"
    assert inet[2]("10.0.0.1") == bytes([2, 32, 0, 4, 10, 0, 0, 1])
    assert inet[2]("192.168.0.0/16") == bytes([2, 16, 0, 4, 192, 168, 0, 0])
    lsn = BVTYPE_TO_PGTYPE[BVType.PG_LSN]
    assert lsn[0] == 3220 and lsn[1]("16/B374D848") == "16/B374D848"
    assert lsn[2]("16/B374D848") == ((0x16 << 32) | 0xB374D848).to_bytes(8, "big")
    assert lsn[1]((0x16 << 32) | 0xB374D848) == "16/B374D848"  # asyncpg reads a pg_lsn as int


def test_every_client_relation_has_postgresqls_columns_and_types():
    from provisa.pgwire.catalog_pg17 import RELATIONS
    from provisa.pgwire.catalog_pg17_tables import CLIENT_RELATIONS

    for schema, relation in CLIENT_RELATIONS:
        served = _oids(f"SELECT * FROM {schema}.{relation} LIMIT 0")
        assert served == {col: oid for col, _, oid in RELATIONS[(schema, relation)]}, relation


def test_pg_authid_lists_the_roles_and_never_a_password():
    state = _state()
    state.roles = {"analyst": {"id": "analyst"}, "admin": {"id": "admin"}}
    rows = answer("SELECT rolname, rolpassword FROM pg_catalog.pg_authid", "testrole", state).rows
    assert rows and {name for name, _ in rows} >= {"analyst", "admin"}
    assert [password for _, password in rows] == [None] * len(rows)


def test_psqls_describe_table_footers_run():
    # psql 17's \d <table>: "Statistics objects" and "Publications" (describe.c).
    oid = "16394"
    statistics = (
        "SELECT oid, stxrelid::pg_catalog.regclass, stxnamespace::pg_catalog.regnamespace AS nsp, "
        "stxname,\n  (SELECT pg_catalog.string_agg(pg_catalog.quote_ident(attname),', ')\n"
        "   FROM pg_catalog.unnest(stxkeys) s(attnum)\n"
        "   JOIN pg_catalog.pg_attribute a ON (stxrelid = a.attrelid AND\n"
        "        a.attnum = s.attnum AND NOT attisdropped)) AS columns,\n"
        "  'd' = any(stxkind) AS ndist_enabled,\n  'f' = any(stxkind) AS deps_enabled,\n"
        "  'm' = any(stxkind) AS mcv_enabled,\n  stxstattarget\n"
        f"FROM pg_catalog.pg_statistic_ext\nWHERE stxrelid = '{oid}'\nORDER BY 1;"
    )
    publications = (
        "SELECT pubname\n     , NULL\n     , NULL\nFROM pg_catalog.pg_publication p\n"
        "     JOIN pg_catalog.pg_publication_namespace pn ON p.oid = pn.pnpubid\n"
        "     JOIN pg_catalog.pg_class pc ON pc.relnamespace = pn.pnnspid\n"
        f"WHERE pc.oid ='{oid}' and pg_catalog.pg_relation_is_publishable('{oid}')\n"
        "UNION\nSELECT pubname\n     , pg_get_expr(pr.prqual, c.oid)\n"
        "     , (CASE WHEN pr.prattrs IS NOT NULL THEN\n"
        "         (SELECT string_agg(attname, ', ')\n"
        "           FROM pg_catalog.generate_series(0, "
        "pg_catalog.array_upper(pr.prattrs::pg_catalog.int2[], 1)) s,\n"
        "                pg_catalog.pg_attribute\n"
        "          WHERE attrelid = pr.prrelid AND attnum = prattrs[s])\n"
        "        ELSE NULL END) FROM pg_catalog.pg_publication p\n"
        "     JOIN pg_catalog.pg_publication_rel pr ON p.oid = pr.prpubid\n"
        "     JOIN pg_catalog.pg_class c ON c.oid = pr.prrelid\n"
        f"WHERE pr.prrelid = '{oid}'\nUNION\nSELECT pubname\n     , NULL\n     , NULL\n"
        "FROM pg_catalog.pg_publication p\n"
        f"WHERE p.puballtables AND pg_catalog.pg_relation_is_publishable('{oid}')\nORDER BY 1;"
    )
    assert answer(statistics, "testrole", _state()).rows == []
    assert answer(publications, "testrole", _state()).rows == []


def test_dbeavers_sequence_query_runs():
    # DBeaver PostgreSequence.java.
    sql = "SELECT * from pg_catalog.pg_sequences WHERE schemaname='public' AND sequencename='s'"
    assert answer(sql, "testrole", _state()).rows == []


def test_every_statistics_relation_has_postgresqls_columns_and_no_rows():
    # Provisa keeps no statistics: each relation is served, empty, never a made-up figure.
    from provisa.pgwire.catalog_pg17 import RELATIONS
    from provisa.pgwire.catalog_pg17_tables import STATISTICS_RELATIONS

    for schema, relation in STATISTICS_RELATIONS:
        served = _oids(f"SELECT * FROM {schema}.{relation} LIMIT 0")
        assert served == {col: oid for col, _, oid in RELATIONS[(schema, relation)]}, relation
        assert answer(f"SELECT * FROM {schema}.{relation}", "testrole", _state()).rows == []


def test_pgadmins_statistics_queries_run():
    # pgAdmin dashboard/sql/default/dashboard_stats.sql (tps_stats) and
    # tables/sql/16_plus/coll_table_stats.sql, rendered.
    tps = (
        "SELECT 'tps_stats' AS chart_name, pg_catalog.row_to_json(t) AS chart_data\n"
        "FROM (SELECT\n"
        "   (SELECT sum(xact_commit) + sum(xact_rollback) FROM pg_catalog.pg_stat_database) "
        'AS "Transactions",\n'
        '   (SELECT sum(xact_commit) FROM pg_catalog.pg_stat_database) AS "Commits",\n'
        '   (SELECT sum(xact_rollback) FROM pg_catalog.pg_stat_database) AS "Rollbacks"\n) t'
    )
    assert [r[0] for r in answer(tps, "testrole", _state()).rows] == ["tps_stats"]
    tables = (
        'SELECT st.relname AS "Table name", n_tup_ins AS "Tuples inserted", '
        'last_vacuum AS "Last vacuum", vacuum_count AS "Vacuum counter"\n'
        "FROM pg_catalog.pg_stat_all_tables st\n"
        "JOIN pg_catalog.pg_class cl on cl.oid=st.relid and cl.relkind IN ('r','s','t','p')\n"
        "WHERE schemaname = 'public' ORDER BY st.relname;"
    )
    assert answer(tables, "testrole", _state()).rows == []


def test_every_object_type_relation_has_postgresqls_columns():
    from provisa.pgwire.catalog_pg17 import RELATIONS
    from provisa.pgwire.catalog_pg17_tables import OBJECT_TYPE_RELATIONS

    for schema, relation in OBJECT_TYPE_RELATIONS:
        served = _oids(f"SELECT * FROM {schema}.{relation} LIMIT 0")
        assert served == {col: oid for col, _, oid in RELATIONS[(schema, relation)]}, relation


def test_psqls_text_search_and_large_object_lists_are_empty():
    # Provisa runs no text search configuration and holds no large object: psql's \dFp and \dl
    # (describe.c) list none.
    parsers = (
        'SELECT\n  n.nspname as "Schema",\n  p.prsname as "Name",\n'
        "  pg_catalog.obj_description(p.oid, 'pg_ts_parser') as \"Description\"\n"
        "FROM pg_catalog.pg_ts_parser p\n"
        "LEFT JOIN pg_catalog.pg_namespace n ON n.oid = p.prsnamespace\n"
        "WHERE pg_catalog.pg_ts_parser_is_visible(p.oid)\nORDER BY 1, 2;"
    )
    large_objects = (
        'SELECT oid as "ID",\n  pg_catalog.pg_get_userbyid(lomowner) as "Owner",\n  '
        "pg_catalog.obj_description(oid, 'pg_largeobject') as \"Description\"\n"
        "FROM pg_catalog.pg_largeobject_metadata\nORDER BY oid"
    )
    assert answer(parsers, "testrole", _state()).rows == []
    assert answer(large_objects, "testrole", _state()).rows == []


def test_the_available_extensions_are_the_installed_ones(monkeypatch):
    from provisa.pgwire import catalog_populate

    monkeypatch.setattr(
        catalog_populate,
        "extension_rows",
        lambda: [(90000, "vector", 10, 11, False, "0.8.0", None, None)],
    )
    # DBeaver PostgreDatabase.java and pgAdmin extensions/sql/extensions.sql.
    dbeaver = (
        "SELECT name,default_version,installed_version,comment "
        "FROM pg_catalog.pg_available_extensions ORDER BY name"
    )
    pgadmin = (
        "SELECT\n    a.name, a.installed_version,\n"
        "    pg_catalog.array_agg(av.version) as version,\n"
        "    pg_catalog.array_agg(av.schema) as schema,\n"
        "    pg_catalog.array_agg(av.superuser) as superuser,\n"
        "    pg_catalog.array_agg(av.relocatable) as relocatable\nFROM\n"
        "    pg_catalog.pg_available_extensions a\n"
        "    LEFT JOIN pg_catalog.pg_available_extension_versions av ON (a.name = av.name)\n"
        "GROUP BY a.name, a.installed_version\nORDER BY a.name"
    )
    assert [tuple(r) for r in answer(dbeaver, "testrole", _state()).rows] == [
        ("vector", "0.8.0", "0.8.0", None)
    ]
    assert [tuple(r) for r in answer(pgadmin, "testrole", _state()).rows] == [
        ("vector", "0.8.0", ["0.8.0"], ["pg_catalog"], [False], [False])
    ]
