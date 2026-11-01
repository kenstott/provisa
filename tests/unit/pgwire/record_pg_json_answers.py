# Copyright (c) 2026 Kenneth Stott
# Canary: 7b1e0c44-9f2a-4d36-8a51-c3e6d2f09b17
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Records tests/unit/pgwire/pg17_json_answers.py from a PostgreSQL 17 of the caller's, never
local-dev's: ``python -m tests.unit.pgwire.record_pg_json_answers <conninfo>``. Each statement's
answer is its columns' type OIDs and its rows' text, or its SQLSTATE and message."""

# Requirements: REQ-2167, REQ-2171

from __future__ import annotations

import pprint
import sys
from pathlib import Path

import psycopg
from psycopg import pq

_TARGET = Path(__file__).with_name("pg17_json_answers.py")
_HEADER = '''# Copyright (c) 2026 Kenneth Stott
# Canary: 3d9a6f70-14c8-4e2b-b7d5-08f1a2c6e93d
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""What PostgreSQL 17 answered: statement -> (column type OIDs, rows' text), or ("ERROR", SQLSTATE,
message). Recorded by record_pg_json_answers, not derived."""

# Requirements: REQ-2167, REQ-2171
# fmt: off

'''

_DOC = """'{"b": 1.50, "a": {"z": [1, 2, {"y": null}], "k": "é\\n"}, "n": null, "t": true}'"""
_LIST = """'[1.50, "x", null, {"b": 2, "a": [1]}, [3, 4], 1e2, true]'"""

STATEMENTS: tuple[str, ...] = (
    # readers, each with a nested value, a number's digits, a NULL and a miss
    *(
        f"SELECT {cast_} {op} {key}"
        for doc, keys in (
            (_DOC, ("'a'", "'b'", "'n'", "'t'", "'nope'")),
            (_LIST, ("0", "3", "-1", "9")),
        )
        for cast_ in (f"{doc}::json", f"{doc}::jsonb")
        for op in ("->", "->>")
        for key in keys
    ),
    *(
        f"SELECT {doc}::{kind} {op} '{path}'"
        for doc, paths in (
            (_DOC, ("{a,z,2}", "{a,k}", "{a,z,2,y}", "{b}", "{}", "{nope,x}")),
            (_LIST, ("{3,a,0}", "{4,1}", "{-1}")),
        )
        for kind in ("json", "jsonb")
        for op in ("#>", "#>>")
        for path in paths
    ),
    """SELECT '{"1": "k"}'::jsonb #> '{1}'""",
    """SELECT '{"a.b": 1, "$c": 2, "d\\"e": 3}'::jsonb -> 'a.b'""",
    """SELECT '{"a.b": 1, "$c": 2}'::jsonb -> '$c'""",
    """SELECT '{"a":1,"b":{"c":2}}'::json -> 'b'""",
    """SELECT '{"a": {"b": [1, 2]}, "c": 3}'::json -> 'a'""",
    """SELECT '{"a":  {"b":   1}}'::json -> 'a'""",
    """SELECT '{"z": {"y": 1, "b": 2}}'::jsonb ->> 'z'""",
    """SELECT '{"a": {"b": 1}}'::jsonb -> 'a' -> 'b'""",
    """SELECT NULL::jsonb -> 'a'""",
    """SELECT '[1]'::jsonb -> NULL::int""",
    *(
        f"SELECT {prefix}_{name}({doc}, {key})"
        for prefix in ("json", "jsonb")
        for name, doc, key in (
            ("array_element", _LIST, "3"),
            ("array_element", _LIST, "-1"),
            ("array_element", _LIST, "0"),
            ("array_element", _LIST, "9"),
            ("array_element", """'{"a":1}'""", "0"),
            ("array_element_text", _LIST, "1"),
            ("array_element_text", _LIST, "2"),
            ("array_element_text", _LIST, "3"),
            ("object_field", _DOC, "'a'"),
            ("object_field", _DOC, "'b'"),
            ("object_field", _DOC, "'nope'"),
            ("object_field", "'[1]'", "'a'"),
            ("object_field_text", _DOC, "'a'"),
            ("object_field_text", _DOC, "'n'"),
            ("object_field_text", _DOC, "'t'"),
            ("extract_path", _DOC, "'a', 'z', '2'"),
            ("extract_path", _DOC, "'a', 'nope'"),
            ("extract_path", _DOC, "VARIADIC ARRAY['a', 'k']"),
            ("extract_path", _DOC, "VARIADIC NULL::text[]"),
            ("extract_path_text", _DOC, "'a', 'k'"),
            ("extract_path_text", _DOC, "'a', 'z'"),
            ("extract_path_text", _DOC, "'n'"),
        )
    ),
    *(
        f"SELECT {prefix}_typeof('{value}')"
        for prefix in ("json", "jsonb")
        for value in ('{"a":1}', " [1]", '"a"', "1.5", "true", "false", "null")
    ),
    "SELECT json_typeof(NULL::json)",
    "SELECT jsonb_typeof(NULL::jsonb)",
    *(
        f"SELECT {prefix}_array_length('{value}')"
        for prefix in ("json", "jsonb")
        for value in ("[1,[2,3],4]", "[]", '{"a":1}', "1")
    ),
    "SELECT jsonb_array_length(NULL::jsonb)",
    # set-returning, in both places a statement can call them
    *(
        statement
        for prefix in ("json", "jsonb")
        for statement in (
            f"SELECT * FROM {prefix}_array_elements({_LIST})",
            f"SELECT {prefix}_array_elements({_LIST})",
            f"SELECT value FROM {prefix}_array_elements('[1, 2]') AS t",
            f"SELECT x FROM {prefix}_array_elements('[1, 2]') AS t(x)",
            f"SELECT * FROM {prefix}_array_elements_text({_LIST})",
            f"SELECT {prefix}_array_elements_text({_LIST})",
            f"SELECT * FROM {prefix}_array_elements('[]')",
            f"SELECT * FROM {prefix}_array_elements(NULL)",
            f"""SELECT * FROM {prefix}_array_elements('{{"a":1}}')""",
            f"SELECT * FROM {prefix}_array_elements('1')",
            f"SELECT * FROM {prefix}_each({_DOC})",
            f"SELECT key FROM {prefix}_each({_DOC})",
            f"SELECT k, w FROM {prefix}_each({_DOC}) AS t(k, w)",
            f"SELECT * FROM {prefix}_each_text({_DOC})",
            f"SELECT * FROM {prefix}_each('[1]')",
            f"SELECT * FROM {prefix}_each(NULL)",
            f"SELECT * FROM {prefix}_object_keys({_DOC})",
            f"SELECT {prefix}_object_keys({_DOC})",
            f"SELECT * FROM {prefix}_object_keys('[1]')",
            f"SELECT {prefix}_object_keys('1')",
            f"""SELECT * FROM {prefix}_to_record('{{"a":1,"b":"x","c":[1,2],"d":{{"e": 1}}}}') AS t(a int, b text, c int[], d json, z text)""",
            f"""SELECT * FROM {prefix}_to_recordset('[{{"a":1}},{{"a":2,"b":"y"}}]') AS t(a int, b text)""",
            f"SELECT * FROM {prefix}_to_recordset('[]') AS t(v integer)",
            f"SELECT count(*) FROM (VALUES ('[1,2]'::{prefix}), ('[3]'::{prefix})) s(d), {prefix}_array_elements(s.d) e",
            f"""SELECT s.i, e.key, e.value FROM (VALUES (1, '{{"a": 1.50}}'::{prefix}), (2, '{{"b": [1, 2]}}'::{prefix})) s(i, d), {prefix}_each(s.d) e ORDER BY 1""",
        )
    ),
    # constructors and aggregates
    *(
        statement
        for prefix in ("json", "jsonb")
        for statement in (
            f"SELECT {prefix}_build_array()",
            f"SELECT {prefix}_build_array(1, 'a', NULL, true, 1.50)",
            f"SELECT {prefix}_build_array(1, {prefix}_build_array(2, 3))",
            f"SELECT {prefix}_build_array(VARIADIC ARRAY[1, 2])",
            f"SELECT {prefix}_build_object()",
            f"SELECT {prefix}_build_object('b', 1, 'a', 'x', 'c', NULL)",
            f"SELECT {prefix}_build_object('b', 1.50, 'a', {prefix}_build_object('z', 1, 'y', 2))",
            f"SELECT {prefix}_build_object('a')",
            f"SELECT {prefix}_build_object(NULL, 1)",
            f"SELECT {prefix}_build_object(1, 2, true, 3)",
            f"SELECT {prefix}_agg(x) FROM (VALUES (1), (NULL), (2)) t(x)",
            f"SELECT {prefix}_agg(x) FROM (VALUES ('a'), ('b\"c')) t(x)",
            f"SELECT {prefix}_agg(x) FROM (VALUES (1.50)) t(x)",
            f"SELECT {prefix}_agg(x) FROM (VALUES (1)) t(x) WHERE false",
            f"SELECT {prefix}_agg(x ORDER BY x DESC) FROM (VALUES (1), (2)) t(x)",
            f"SELECT {prefix}_agg_strict(x) FROM (VALUES (1), (NULL), (2)) t(x)",
            f"SELECT {prefix}_agg_strict(x) FROM (VALUES (NULL::int)) t(x)",
            f"SELECT {prefix}_object_agg(k, x) FROM (VALUES ('b', 1), ('a', NULL)) t(k, x)",
            f"SELECT {prefix}_object_agg(k, x) FROM (VALUES ('b', 1), ('b', 3)) t(k, x)",
            f"SELECT {prefix}_object_agg(k, x) FROM (VALUES (NULL::text, 1)) t(k, x)",
            f"SELECT {prefix}_object_agg('k', x) FROM (VALUES (1)) t(x) WHERE false",
            f"SELECT {prefix}_object_agg_strict(k, x) FROM (VALUES ('b', 1), ('a', NULL)) t(k, x)",
            f"SELECT {prefix}_object_agg_unique(k, x) FROM (VALUES ('b', 1), ('b', 3)) t(k, x)",
            f"SELECT {prefix}_object_agg_unique(k, x) FROM (VALUES ('b', 1), ('a', 3)) t(k, x)",
            f"SELECT {prefix}_object_agg_unique_strict(k, x) FROM (VALUES ('b', 1), ('a', NULL)) t(k, x)",
            f"SELECT to_{prefix}(1.50)",
            f"SELECT to_{prefix}('a\"b'::text)",
            f"SELECT to_{prefix}(true)",
            f"SELECT to_{prefix}(NULL::int)",
            f"SELECT to_{prefix}(ARRAY[1, 2])",
            f"SELECT to_{prefix}(DATE '2024-01-02')",
            f"SELECT to_{prefix}(TIMESTAMP '2024-01-02 03:04:05')",
            f"SELECT {prefix}_object('{{a,1,b,2}}')",
            f"SELECT {prefix}_object('{{a,b}}', '{{1,2}}')",
            f"SELECT {prefix}_object('{{a,NULL}}')",
            f"SELECT {prefix}_object('{{a}}')",
        )
    ),
    "SELECT row_to_json(t) FROM (VALUES (1, 'a', NULL::int)) t(x, y, z)",
    "SELECT row_to_json(t, true) FROM (VALUES (1, 'a')) t(x, y)",
    # what a jsonb is, whatever it was written as
    """SELECT '{"b": 1, "a": 2, "b": 3}'::jsonb""",
    """SELECT '{"bb":1,"a":2,"é":3}'::jsonb""",
    """SELECT '[1e2, 1.0E+1, 1.50, -0, 0.10]'::jsonb""",
    """SELECT '"\\u00e9\\n\\/"'::jsonb""",
    """SELECT '{"a":   1}'::json""",
    """SELECT '{"a": "\\u00e9"}'::json""",
    # membership
    """SELECT '{"a":1}'::jsonb ? 'a'""",
    """SELECT '{"a":1}'::jsonb ? 'b'""",
    """SELECT '["a","b"]'::jsonb ? 'a'""",
    """SELECT '[1]'::jsonb ? '1'""",
    """SELECT '"a"'::jsonb ? 'a'""",
    """SELECT '1'::jsonb ? 'a'""",
    """SELECT NULL::jsonb ? 'a'""",
    """SELECT '{"a":1,"b":2}'::jsonb ?| ARRAY['b','c']""",
    """SELECT '{"a":1,"b":2}'::jsonb ?| ARRAY['x']""",
    """SELECT '{"a":1,"b":2}'::jsonb ?& ARRAY['a','b']""",
    """SELECT '{"a":1,"b":2}'::jsonb ?& ARRAY['a','c']""",
    """SELECT jsonb_exists('{"a":1}', 'a')""",
    """SELECT jsonb_exists_any('{"a":1}', '{a,b}')""",
    """SELECT jsonb_exists_all('{"a":1}', '{a,b}')""",
    # containment
    *(
        f"SELECT '{left}'::jsonb @> '{right}'::jsonb"
        for left, right in (
            ('{"a":1,"b":[1,2,{"c":3}]}', '{"a":1}'),
            ('{"a":1,"b":[1,2,{"c":3}]}', '{"b":[{"c":3},2]}'),
            ('{"a":1}', '{"a":2}'),
            ('{"a":{"b":1}}', '{"a":{}}'),
            ('{"a":1}', "[]"),
            ("[1,2]", "1"),
            ("[[1,2]]", "[1]"),
            ("[1.0]", "[1]"),
            ("1", "[1]"),
            ('["a"]', '"a"'),
            ("[]", "[]"),
            ('[{"a":1,"b":2}]', '[{"a":1},{"b":2}]'),
            ("{}", "{}"),
        )
    ),
    """SELECT '{"a":1}'::jsonb <@ '{"a":1,"b":2}'::jsonb""",
    """SELECT '[1]'::jsonb <@ '[1,2]'::jsonb""",
    """SELECT NULL::jsonb @> '[1]'::jsonb""",
    """SELECT jsonb_contains('{"a":1,"b":2}', '{"a":1}')""",
    """SELECT jsonb_contained('{"a":1}', '{"a":1,"b":2}')""",
    # what changes one
    """SELECT '{"a":1}'::jsonb || '{"a":2,"b":3}'::jsonb""",
    """SELECT '[1]'::jsonb || '[2]'::jsonb""",
    """SELECT '[1]'::jsonb || '{"a":1}'::jsonb""",
    """SELECT '1'::jsonb || '[2]'::jsonb""",
    """SELECT jsonb_concat('{"a":1}', '{"a":2,"b":3}')""",
    """SELECT jsonb_concat('[1, 2]', '[3]')""",
    """SELECT jsonb_delete('{"a":1,"b":2}', 'a')""",
    """SELECT jsonb_delete('["a","b",1]', 'a')""",
    """SELECT jsonb_delete('["a","b",1]', 1)""",
    """SELECT jsonb_delete('["a","b",1]', -1)""",
    """SELECT jsonb_delete('{"a":1}', 0)""",
    """SELECT jsonb_delete('1', 'a')""",
    """SELECT jsonb_delete('{"a":1,"b":2,"c":3}', 'a', 'c')""",
    """SELECT jsonb_delete('{"a":1,"b":2,"c":3}', VARIADIC ARRAY['a', 'c'])""",
    """SELECT jsonb_delete(NULL::jsonb, VARIADIC NULL::text[])""",
    """SELECT '{"a":1,"b":2}'::jsonb - 'a'""",
    """SELECT '["a","b"]'::jsonb - 1""",
    """SELECT '{"a":{"b":1,"c":2}}'::jsonb #- '{a,b}'""",
    """SELECT jsonb_delete_path('{"a":{"b":1,"c":2}}', '{a,b}')""",
    """SELECT jsonb_delete_path('[1,[2,3]]', '{1,-1}')""",
    """SELECT jsonb_set('{"a":{"b":1}}', '{a,b}', '9')""",
    """SELECT jsonb_set('{"a":{"b":1}}', '{a,c}', '9')""",
    """SELECT jsonb_set('{"a":1}', '{b}', '[1, 2]')""",
    """SELECT jsonb_set('{"a":1}', '{b}', '9', false)""",
    """SELECT jsonb_set('{"a":[1,2]}', '{a,0}', '9')""",
    """SELECT jsonb_set('{"a":[1,2]}', '{a,5}', '9')""",
    """SELECT jsonb_set('1', '{a}', '9')""",
    """SELECT jsonb_set_lax('{"a":1}', '{a}', NULL)""",
    """SELECT jsonb_set_lax('{"a":1}', '{a}', NULL, true, 'delete_key')""",
    """SELECT jsonb_insert('{"a":[1,2]}', '{a,1}', '9')""",
    """SELECT jsonb_insert('{"a":[1,2]}', '{a,1}', '9', true)""",
    """SELECT jsonb_insert('{"a":1}', '{b}', '9')""",
    """SELECT jsonb_insert('{"a":1}', '{a}', '9')""",
    """SELECT jsonb_strip_nulls('{"a":null,"b":{"c":null,"d":1},"e":[null,{"f":null}]}')""",
    """SELECT json_strip_nulls('{"a":null,"b":{"c":null,"d":1},"e":[null,{"f":null}]}')""",
    """SELECT jsonb_strip_nulls('[null,1]')""",
    """SELECT jsonb_pretty('{"a":[1,{"b":2}]}')""",
    # a strict function of a NULL, whatever else it was given
    *(
        f"SELECT {call}"
        for call in (
            "jsonb_delete(NULL::jsonb, 'a')",
            "jsonb_delete(NULL::jsonb, 0)",
            "jsonb_delete('{\"a\":1}'::jsonb, NULL::text)",
            "jsonb_delete_path(NULL::jsonb, '{a}')",
            "jsonb_delete_path('{\"a\":1}', NULL::text[])",
            "jsonb_set(NULL::jsonb, '{a}', '1')",
            "jsonb_set('{\"a\":1}', NULL::text[], '1')",
            "jsonb_set('{\"a\":1}', '{a}', NULL::jsonb)",
            "jsonb_insert(NULL::jsonb, '{a}', '1')",
            "jsonb_concat(NULL::jsonb, '[1]')",
            "jsonb_concat('[1]', NULL::jsonb)",
            "jsonb_contains(NULL::jsonb, NULL::jsonb)",
            "jsonb_contained(NULL::jsonb, NULL::jsonb)",
            "jsonb_exists(NULL::jsonb, 'a')",
            "jsonb_exists_any(NULL::jsonb, NULL::text[])",
            "jsonb_exists_all('{\"a\":1}', NULL::text[])",
            "jsonb_object(NULL::text[])",
            "jsonb_object(NULL::text[], NULL::text[])",
            "json_strip_nulls(NULL::json)",
            "jsonb_strip_nulls(NULL::jsonb)",
            "jsonb_array_length(NULL)",
            "json_array_length(NULL::json)",
        )
    ),
    "SELECT * FROM pg_catalog.\"json_to_recordset\"('[]') AS t(v integer)",
    "SELECT * FROM pg_catalog.\"jsonb_to_recordset\"('[]') AS t(v integer)",
    "SELECT * FROM pg_catalog.\"jsonb_array_elements\"('[1]')",
    "SELECT json_to_record(NULL::json)",
    "SELECT jsonb_to_record(NULL::jsonb)",
    """SELECT json_to_record('{"a":1}')""",
    """SELECT jsonb_to_record('{"a":1}')""",
    "SELECT json_build_array(VARIADIC NULL::int[])",
    "SELECT jsonb_build_array(VARIADIC NULL::int[])",
    "SELECT json_build_object(VARIADIC NULL::text[])",
    "SELECT jsonb_build_object(VARIADIC NULL::text[])",
    # what orders one
    """SELECT '1.0'::jsonb = '1'::jsonb""",
    """SELECT '{"a":1,"b":2}'::jsonb = '{"b":2,"a":1}'::jsonb""",
    """SELECT '[1,2]'::jsonb < '[1,3]'::jsonb""",
    """SELECT jsonb_cmp('2.5', '10')""",
    """SELECT jsonb_eq('1.0', '1')""",
    """SELECT jsonb_lt('1', '2')""",
    # jsonpath
    """SELECT jsonb_path_exists('{"a":[1,2,3]}', '$.a[*] ? (@ > 2)')""",
    """SELECT jsonb_path_query_first('[1,2,3]', '$[*] ? (@ > 1)')""",
    """SELECT '{"a":1}'::jsonb @? '$.a'""",
    # table columns, which no statement can say are literals
    *(
        f"SELECT d -> 'a' FROM (VALUES ('{value}'::{kind})) t(d)"
        for kind in ("json", "jsonb")
        for value in ('{"a":{"y":1,"b":[1.50]}}', '{"a":1.50}', '{"a":null}', '{"b":1}')
    ),
    """SELECT d @> '{"a":1}' FROM (VALUES ('{"a":1,"b":2}'::jsonb), ('{"a":2}'::jsonb)) t(d)""",
    """SELECT d ? 'a' FROM (VALUES ('{"a":1}'::jsonb), ('["a"]'::jsonb), ('1'::jsonb)) t(d)""",
    """SELECT jsonb_set(d, '{a}', '9') FROM (VALUES ('{"a":1,"b":2}'::jsonb)) t(d)""",
)


def answer(conn: psycopg.Connection, sql: str) -> tuple:
    res = conn.pgconn.exec_params(sql.encode(), [], result_format=0)
    if res.status != pq.ExecStatus.TUPLES_OK:
        return (
            "ERROR",
            (res.error_field(pq.DiagnosticField.SQLSTATE) or b"").decode(),
            (res.error_field(pq.DiagnosticField.MESSAGE_PRIMARY) or b"").decode(),
        )
    rows = [
        tuple(
            None if (v := res.get_value(i, j)) is None else bytes(v).decode()
            for j in range(res.nfields)
        )
        for i in range(res.ntuples)
    ]
    return ([res.ftype(j) for j in range(res.nfields)], rows)


def main(conninfo: str) -> None:
    with psycopg.connect(conninfo, autocommit=True) as conn:
        conn.execute("SET TimeZone = 'UTC'")
        recorded = {sql: answer(conn, sql) for sql in dict.fromkeys(STATEMENTS)}
    _TARGET.write_text(_HEADER + f"RECORDED: dict = {pprint.pformat(recorded, width=100)}\n")


if __name__ == "__main__":
    main(sys.argv[1])
