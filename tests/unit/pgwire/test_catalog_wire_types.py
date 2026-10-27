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
