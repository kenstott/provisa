# Copyright (c) 2026 Kenneth Stott
# Canary: 7e2a94d1-0c6b-4f38-b5d2-91c3e4a7f608
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Which pg_catalog functions work today, per federation engine (REQ-2167, THE GOAL IS THE
GUARANTEED LIST; REQ-2171, AN ANSWER THAT DIFFERS IS REFUSED).

Run by .github/workflows/conformance-functions.yml, one leg per engine. Measurement only: it
writes no function form. ``measure`` calls every overload pg_proc lists in pg_catalog on a real
PostgreSQL and through Provisa over pgwire; ``merge`` joins the legs into function_support.json
and the guaranteed list.

Each overload gets one outcome per engine:

  pass          every call PostgreSQL answered, Provisa answered the same, or the same under a
                written rule, which the overload's ``rules`` name (function_report.RULES), and
                at least one answer was compared or one property checked
  not_checked   every call PostgreSQL answered, Provisa answered with the same result type, and
                none compared an answer or checked a property: a call that ran proves nothing,
                so this is no pass. ``unchecked`` says why
  wrong_answer  a call compares unequal: its result type, its rows, their values, their form or
                a property PostgreSQL documents
  error         PostgreSQL answered a call and the engine failed with no named refusal (its
                message is kept)
  refused       PostgreSQL answered a call and Provisa refused it by name
  not_callable  PostgreSQL itself answers no call of it
  not_reached   the leg ended, or its server went away, before this overload

A wrong answer outranks an error, which outranks a refusal, which outranks a pass or a not checked:
the overload is as bad as its worst call. function_report.py says what each outcome is on the
report (exact, equivalent, not checked, refused by name or differs); there is no overall count,
only one per engine.

The calls of an overload are the NULL call of pgwire_function_probe.py and, where ``value_calls``
can write them, up to three calls with real values. How far values go, exactly:

  * Every immutable and stable function is given values, compared under one TimeZone, DateStyle
    and IntervalStyle on the same fixture catalog, but for those whose answer is the answering
    server's own (``OWN_ANSWERS``: its process, its addresses, its clock, its statistics, its
    transactions, or a by-design session answer, REQ-588 and REQ-579). Volatile functions are
    never given values, because that would run them for real (they are the ones that act:
    setval, pg_terminate_backend, lo_unlink), but for ``PROPERTIES``.
  * Every argument must have a literal: ``SAMPLES`` and ``VALUE_SAMPLES`` by type, ``ARG_VALUES``
    by function where the type's literals are not valid input (``Raw`` ones are written as they
    stand), and ``INPUT_TYPES`` for a type's input function. Pseudo-types take the concrete types
    of pgwire_function_probe.py. An overload taking a type with no sample gets no value call;
    ``NO_LITERAL`` types are ones PostgreSQL takes no literal of.
  * A value call PostgreSQL refuses compares nothing: that it also refuses on Provisa is not
    checked, nor is Provisa answering what PostgreSQL refuses.

``answers_checked`` is an overload with at least one value call PostgreSQL answered, its value
compared. ``property_checked`` is one whose value cannot be compared and that had an exact property
checked against PostgreSQL's documentation or its own output (REQ-2167, WHAT GUARANTEED MEANS):

  * form: a call with no NULL argument whose answer has the form PostgreSQL's has. ``value_form``
    gives the form of PostgreSQL's own answer, a named function's, then its type's, then a record's
    number of fields, and otherwise the sample's shape. Each row Provisa answers must fit.
    PostgreSQL's NULL or void, or a set Provisa leaves empty, is no form to check.
  * ``PROPERTIES``: a volatile function's documented range or shape on real values (random(min,
    max) within its bounds, array_shuffle a permutation, array_sample a sub-multiset of the size
    asked), and a clock function's answer within the statement's bounds on PostgreSQL's clock.

The rest are not checked, and each says why (``unchecked``, which a property checked overload keeps:
its value is still not compared). A NULL call where PostgreSQL answers NULL holds Provisa to NULL
too, which can make a wrong answer and never a pass. Three literals, not a domain: a function that
differs only on values no sample reaches passes here.

Literals alone do not say a function works on a column: a form that folds a literal at transpile
time passes while a column of the same type is refused. So an overload given values gets one more
call (``column_calls``), which reads the same value from a column. ``seed_columns`` seeds and
registers ``conformance.function_columns``, with a column of each type of ``SAMPLES`` and a row of
each sample, so the column is a registered one of that type, on the same relation on both servers.
An argument a function needs a particular literal for (``ARG_VALUES``: a unit, a pattern, a format)
stays that literal; an overload with no other argument reads its first such argument from a VALUES
subquery, which has its value where the table does not. A direct argument of an ordered-set
aggregate stays a literal, because PostgreSQL requires one. The first row PostgreSQL answers is
compared, and the result is the overload's ``column``, beside its outcome: a pass whose column call
fails passes on literals only. It changes no outcome and no guaranteed list; function_report.py
counts and lists it per engine.

The result is text format under one set of session settings. ``compare`` says whether an answer is
the same, and under which rules when its text is not: rows as a multiset (no call has an ORDER BY of
its own to hold them to), float4 and float8 and their arrays within one part in 10^14, numerics by
value, JSON by ``json_rules``, a JSON value in a record, an enum label as text, and a listed
function's answer about its own database. The NULL call of a stable or volatile function compares
its type alone.

The session is UTC, and so are a runner and its PostgreSQL service: an engine that ignores the
session's TimeZone answers the same here and is not told apart. Run off UTC, the pg engine's
timestamptz answers come back in the server's zone. ``ZONED_FUNCTIONS`` alone are also called off
UTC, under ``ZONE``, where the zone is in the text they write.
"""

# Requirements: REQ-2167, REQ-2171

from __future__ import annotations

import argparse
import datetime
import decimal
import json
import math
import re
import select
import sys
import time
from collections import Counter
from collections.abc import Callable, Iterable, Iterator
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import psycopg
import yaml
from psycopg import pq

import function_report
import pgwire_catalog_types as catalog
import pgwire_function_probe as function_probe
from function_report import (
    ARRAY_ORDER,
    ENUM_LABEL,
    FLOAT_CLOSE,
    JSON_KEY_ORDER,
    JSON_SPACING,
    MODEL_ANSWER,
    NUMBER_VALUE,
    ROW_ORDER,
)
from pgwire_function_probe import Function

#: The session settings both servers compare under.
SETTINGS = (
    "SET TimeZone TO 'UTC'",
    "SET DateStyle TO 'ISO, MDY'",
    "SET IntervalStyle TO 'postgres'",
    "SET extra_float_digits TO 1",
)

#: How long one call may take on one server before it is an error.
CALL_TIMEOUT_S = 30.0

#: How long a leg may run before what remains is not reached: the job's own limit would lose it all.
LEG_BUDGET_S = 6300.0

#: Calls of an overload given values: one aggregate or window statement reads all of them as rows.
VALUE_ROWS = 3

#: The registered table a column call reads: a column of each type of ``SAMPLES``, a row of each
#: sample, ``id`` naming the row.
COLUMN_TABLE = "function_columns"
COLUMN_RELATION = f"{catalog.FIXTURE_SCHEMA}.{COLUMN_TABLE}"

#: Floats compare within one part in this (REQ-2171).
FLOAT_TOLERANCE = 1e-14

#: json and jsonb: they compare as parsed JSON (REQ-2171).
JSON_OIDS = frozenset({114, 3802})
#: A read of json that answers text, which for a number or a container is JSON still.
_JSON_TEXT_READ = re.compile(r'->>|#>>|jsonb?_\w*_text"?\(')
_JSON_NUMBER = re.compile(r"-?\d+(?:\.\d+)?(?:[eE][-+]?\d+)?")
#: What leaves an array's order open, and what closes it (REQ-2171, ARRAY ORDER NEEDS AN ORDER BY).
_ARRAY_AGGREGATE = re.compile(r"\b(?:jsonb?_agg|array_agg|json_arrayagg)", re.IGNORECASE)
_ORDER_BY = re.compile(r"\bORDER\s+BY\b", re.IGNORECASE)

#: Answers from the clock: their value is checked against the statement's bounds (``PROPERTIES``).
CLOCK_FUNCTIONS = frozenset(
    {"clock_timestamp", "now", "statement_timestamp", "transaction_timestamp"}
)

#: The functions whose answer is the answering server's own, by name, with why: never compared by
#: value, as no PostgreSQL counterpart has it (REQ-2167, WHAT GUARANTEED MEANS: a session function
#: is validated by its type and form). A by-design one cites its decision (by_design/pgwire.yaml).
_SERVER = "answers with its own server's state"
OWN_ANSWERS: dict[str, str] = {
    **dict.fromkeys(CLOCK_FUNCTIONS, "answers from the clock"),
    **dict.fromkeys(
        ("current_user", "session_user", "current_role", "getpgusername", "system_user"),
        "by design (REQ-588): names the role the session acts as",
    ),
    **dict.fromkeys(
        ("current_database", "current_catalog"),
        "by design (REQ-588): names the served database",
    ),
    "version": "by design (REQ-579): the server's version",
    **dict.fromkeys(
        (
            "inet_client_addr",
            "inet_client_port",
            "inet_server_addr",
            "inet_server_port",
            "pg_backend_pid",
            "pg_conf_load_time",
            "pg_postmaster_start_time",
            "pg_current_snapshot",
            "pg_current_xact_id",
            "pg_current_xact_id_if_assigned",
            "txid_current",
            "txid_current_if_assigned",
            "txid_current_snapshot",
            "pg_show_all_settings",
            "pg_config",
            "pg_available_extensions",
            "pg_available_extension_versions",
            "pg_relation_filenode",
            "pg_relation_filepath",
            "pg_stat_get_archiver",
            "pg_stat_get_slru",
            "pg_stat_get_wal",
            "pg_stat_get_wal_receiver",
            "pg_stat_get_wal_senders",
            "pg_stat_get_snapshot_timestamp",
            "pg_stat_get_buf_alloc",
            "pg_stat_get_activity",
            "mxid_age",
        ),
        _SERVER,
    ),
}
#: As ``OWN_ANSWERS``, by prefix: a backend's, the background writer's, the checkpointer's and a
#: database's statistics.
_OWN_PREFIXES = (
    "pg_stat_get_backend_",
    "pg_stat_get_bgwriter_",
    "pg_stat_get_checkpointer_",
    "pg_stat_get_db_",
)
#: As ``OWN_ANSWERS``, by signature: the age of a transaction id is the server's.
_OWN_SIGNATURES = {"age(xid)": _SERVER}


def own_answer(fn: Function) -> str | None:
    """Why ``fn``'s answer is its server's own (``OWN_ANSWERS``); None if it is not."""
    if fn.name in OWN_ANSWERS:
        return OWN_ANSWERS[fn.name]
    if fn.name.startswith(_OWN_PREFIXES):
        return _SERVER
    return _OWN_SIGNATURES.get(_key(fn))


def _key(fn: Function) -> str:
    """``fn`` by its name and argument types, with no argument names."""
    return f"{fn.name}({', '.join(fn.argtypes)})"


#: Written into their text as the session's zone abbreviation, which UTC cannot tell from its name.
ZONED_FUNCTIONS = frozenset({"timeofday"})
ZONE = "Asia/Tokyo"

#: Answers about the database they run on, whose contents are that database's: XML either way
#: (function_report.MODEL_ANSWER).
MODEL_FUNCTIONS = frozenset(
    {
        "database_to_xml",
        "database_to_xml_and_xmlschema",
        "database_to_xmlschema",
        "schema_to_xml",
        "schema_to_xml_and_xmlschema",
        "schema_to_xmlschema",
    }
)

_FLOAT_OIDS = frozenset({700, 701})
_FLOAT_ARRAY_OIDS = frozenset({1021, 1022})
_NUMERIC_OID = 1700
_NUMERIC_ARRAY_OID = 1231
_TEXT_OIDS = frozenset({25, 1009})
_ANYENUM_OID = 3500
_TEXT_OID = 25
_VOID_OID = 2278
_RECORD_OID = 2249

_DATE = r"\d{4,}-\d{2}-\d{2}"
_TIME = r"\d{2}:\d{2}:\d{2}(\.\d{1,6})?"
_ZONE_OFFSET = r"[+-]\d{2}(:\d{2}){0,2}"
_INTEGER = r"-?\d+"
_FLOAT = r"-?\d+(\.\d+)?(e[+-]?\d+)?|NaN|-?Infinity"
_SNAPSHOT = r"\d+:\d+:(\d+(,\d+)*)?"
_XID = r"\d+"

#: The text form of the types that have one, by oid: under ``SETTINGS``, an interval in the
#: ``postgres`` style. uuid is version 4's, the only one a PostgreSQL 17 function generates.
TYPE_FORMS = {
    16: "[tf]",
    20: _INTEGER,
    21: _INTEGER,
    23: _INTEGER,
    26: _XID,
    28: _XID,
    700: _FLOAT,
    701: _FLOAT,
    869: r"(\d{1,3}(\.\d{1,3}){3}|[0-9a-f]*:[0-9a-f:.]*)(/\d{1,3})?",
    1082: _DATE,
    1083: _TIME,
    1114: f"{_DATE} {_TIME}",
    1184: f"{_DATE} {_TIME}{_ZONE_OFFSET}",
    1186: r"(?=.)(-?\d+ (year|mon|day)s? ?)*(-?\d+:\d{2}:\d{2}(\.\d{1,6})?)?",
    1266: f"{_TIME}{_ZONE_OFFSET}",
    2950: "[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}",
    2970: _SNAPSHOT,
    3220: "[0-9A-F]+/[0-9A-F]+",
    5038: _SNAPSHOT,
    5069: _XID,
}

_POSITIVE = r"[1-9]\d*"

#: The form of a function's answer where it says more than its type's: random is in [0, 1); a
#: backend, a port and a transaction are never numbered 0; version's words after its number are the
#: build's.
FUNCTION_FORMS = {
    "inet_client_port": _POSITIVE,
    "inet_server_port": _POSITIVE,
    "pg_backend_pid": _POSITIVE,
    "pg_current_xact_id": _POSITIVE,
    "random": r"0(\.\d+)?|\d(\.\d+)?e-\d+",
    "txid_current": _POSITIVE,
    "version": r"PostgreSQL \d+\.\d+ .+",
}

#: timeofday's text, but for its last word: the zone's abbreviation, which is PostgreSQL's to say.
_TIMEOFDAY = r"[A-Z][a-z]{2} [A-Z][a-z]{2} \d{2} \d{2}:\d{2}:\d{2}\.\d{6} \d{4} "

#: One field of a record's text: record_out quotes anything but a plain word, doubling the quotes.
_FIELD = r'(?:"(?:[^"]|"")*"|[^,"()]*)'

_INTEGERS = ("3", "(-7)", "42")
_DECIMALS = ("0.5", "(-2.25)", "100")
_STRINGS = ("'abc'", "'Hello World'", "'12'")

#: Three literals of each type format_type names, cast to it where they are used. Enough to tell
#: a sign, a case and a digit string apart; the first of the floats is a valid fraction, which an
#: ordered-set aggregate's direct argument needs.
SAMPLES: dict[str, tuple[str, str, str]] = {
    "smallint": _INTEGERS,
    "integer": _INTEGERS,
    "bigint": ("3", "(-7)", "5000000000"),
    "numeric": ("2.5", "(-7.125)", "1000"),
    "real": _DECIMALS,
    "double precision": _DECIMALS,
    "boolean": ("true", "false", "true"),
    "text": _STRINGS,
    "character varying": _STRINGS,
    "character": _STRINGS,
    "name": _STRINGS,
    '"char"': ("'a'", "'B'", "'1'"),
    "bytea": ("'\\xdeadbeef'", "'abc'", "'\\x00ff'"),
    "date": ("'2024-06-15'", "'1999-12-31'", "'2000-02-29'"),
    "time without time zone": ("'12:30:45'", "'00:00:00'", "'23:59:59.5'"),
    "time with time zone": ("'12:30:45+02'", "'00:00:00+00'", "'23:59:59.5-05'"),
    "timestamp without time zone": (
        "'2024-06-15 12:30:45'",
        "'1999-12-31 23:59:59'",
        "'2000-02-29 00:00:00'",
    ),
    "timestamp with time zone": (
        "'2024-06-15 12:30:45+00'",
        "'1999-12-31 23:59:59+02'",
        "'2000-02-29 00:00:00-05'",
    ),
    "interval": ("'1 day 02:03:04'", "'3 mons'", "'-1 year 2 days'"),
    "uuid": (
        "'a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11'",
        "'00000000-0000-0000-0000-000000000000'",
        "'ffffffff-ffff-ffff-ffff-ffffffffffff'",
    ),
    "json": ('\'{"a": 1, "b": [1, 2]}\'', "'[1, \"two\", null]'", "'3'"),
    "jsonb": ('\'{"a": 1, "b": [1, 2]}\'', "'[1, \"two\", null]'", "'3'"),
    "jsonpath": ("'$.a'", "'$[0]'", "'$'"),
    "inet": ("'192.168.0.1/24'", "'10.1.2.3'", "'::1'"),
    "cidr": ("'192.168.0.0/24'", "'10.0.0.0/8'", "'::1/128'"),
    "macaddr": ("'08:00:2b:01:02:03'", "'00:00:00:00:00:00'", "'ff:ff:ff:ff:ff:ff'"),
    "macaddr8": (
        "'08:00:2b:01:02:03:04:05'",
        "'00:00:00:00:00:00:00:00'",
        "'ff:ff:ff:ff:ff:ff:ff:ff'",
    ),
    "money": ("'12.34'", "'-5.00'", "'1000.00'"),
    "point": ("'(1,2)'", "'(0,0)'", "'(-3.5,4)'"),
    "line": ("'{1,2,3}'", "'{1,0,0}'", "'{0,1,-2}'"),
    "lseg": ("'[(0,0),(3,4)]'", "'[(1,1),(1,5)]'", "'[(-1,0),(2,0)]'"),
    "box": ("'(3,4),(0,0)'", "'(1,1),(-1,-1)'", "'(5,2),(2,1)'"),
    "path": ("'((0,0),(1,0),(1,1))'", "'[(0,0),(2,2)]'", "'((1,1),(4,1),(4,5))'"),
    "polygon": ("'((0,0),(1,0),(1,1))'", "'((0,0),(4,0),(4,4),(0,4))'", "'((1,1),(2,3),(3,1))'"),
    "circle": ("'<(0,0),1>'", "'<(1,2),3>'", "'<(-1,-1),0.5>'"),
    "bit": ("'1'", "'0'", "'1'"),
    "bit varying": ("'101'", "'1'", "'0011'"),
    "tsvector": ("'cat:1 fat:2'", "'a b c'", "'rat:3A'"),
    "tsquery": ("'cat'", "'a & b'", "'fat | rat'"),
    "xml": ("'<a>1</a>'", "'<b x=\"y\"/>'", "'<c><d>2</d></c>'"),
    "pg_lsn": ("'0/16B3748'", "'1/0'", "'FF/FFFFFFFF'"),
    "oid": ("12345", "0", "1"),
    "regconfig": ("'english'", "'simple'", "'english'"),
    "interval[]": ("'{1 day,2 hours}'", "'{}'", "'{1 mon}'"),
    "integer[]": ("'{1,2,3}'", "'{}'", "'{-7,NULL,42}'"),
    "smallint[]": ("'{1,2,3}'", "'{}'", "'{-7,NULL,42}'"),
    "bigint[]": ("'{1,2,3}'", "'{}'", "'{-7,NULL,42}'"),
    "real[]": ("'{0.25,0.5}'", "'{}'", "'{-1.5,100}'"),
    "double precision[]": ("'{0.25,0.5}'", "'{}'", "'{-1.5,100}'"),
    "numeric[]": ("'{0.25,0.5}'", "'{}'", "'{-1.5,100}'"),
    "text[]": ("'{a,b,c}'", "'{}'", "'{x,NULL,z}'"),
    "int4range": ("'[1,5)'", "'empty'", "'(,3]'"),
    "int8range": ("'[1,5)'", "'empty'", "'(,3]'"),
    "numrange": ("'[1.5,2.5)'", "'empty'", "'(,3]'"),
    "daterange": ("'[2024-01-01,2024-02-01)'", "'empty'", "'(,2000-01-01]'"),
    "tsrange": ("'[2024-01-01 00:00,2024-02-01 00:00)'", "'empty'", "'(,2000-01-01 00:00]'"),
    "tstzrange": (
        "'[2024-01-01 00:00+00,2024-02-01 00:00+00)'",
        "'empty'",
        "'(,2000-01-01 00:00+00]'",
    ),
    "int4multirange": ("'{[1,5),[7,9)}'", "'{}'", "'{(,3]}'"),
    "int8multirange": ("'{[1,5),[7,9)}'", "'{}'", "'{(,3]}'"),
    "nummultirange": ("'{[1.5,2.5)}'", "'{}'", "'{(,3]}'"),
}

_RANGES = '\'{"[1,5)","[7,9)"}\'', "'{}'", "'{\"(,3]\"}'"

#: Literals of the types no registered column holds, each valid input on PostgreSQL itself: a
#: catalog name names an object every PostgreSQL 17 catalog has, a role a predefined one, a
#: relation the fixture's own. They are cast as ``SAMPLES`` is, and read from no column.
VALUE_SAMPLES: dict[str, tuple[str, str, str]] = {
    "cstring": _STRINGS,
    "unknown": _STRINGS,
    "void": ("''", "''", "''"),
    "record": ("ROW(1, 'a')", "ROW(2, 'b')", "ROW(NULL::integer, 'c')"),
    "tid": ("'(0,1)'", "'(5,3)'", "'(4294967295,65535)'"),
    "xid": ("'3'", "'100'", "'4294967295'"),
    "xid8": ("'3'", "'100'", "'18446744073709551615'"),
    "cid": ("'0'", "'5'", "'4294967295'"),
    function_probe.PROBE_ENUM: ("'a'", "'b'", "'a'"),
    "oidvector": ("'1 2 3'", "''", "'23 25'"),
    "int2vector": ("'1 2'", "''", "'3'"),
    "txid_snapshot": ("'10:20:10,14,15'", "'1:1:'", "'5:10:7'"),
    "pg_snapshot": ("'10:20:10,14,15'", "'1:1:'", "'5:10:7'"),
    "regclass": ("'pg_catalog.pg_class'", "'pg_catalog.pg_type'", f"'{COLUMN_RELATION}'"),
    "regtype": ("'integer'", "'text'", "'boolean'"),
    "regproc": ("'now'", "'int4in'", "'pg_backend_pid'"),
    "regprocedure": ("'now()'", "'abs(integer)'", "'lower(text)'"),
    "regoper": ("'||/'", "'|/'", "'||/'"),
    "regoperator": ("'+(integer,integer)'", "'=(text,text)'", "'-(NONE,integer)'"),
    "regnamespace": ("'pg_catalog'", "'public'", "'information_schema'"),
    "regrole": ("'pg_monitor'", "'pg_read_all_data'", "'pg_signal_backend'"),
    "regcollation": ("'\"C\"'", "'\"POSIX\"'", "'\"default\"'"),
    "regdictionary": ("'simple'", "'english_stem'", "'simple'"),
    "aclitem": (
        "'pg_monitor=r/pg_monitor'",
        "'=r/pg_monitor'",
        "'pg_read_all_data=arw/pg_monitor'",
    ),
    "aclitem[]": (
        "'{pg_monitor=r/pg_monitor}'",
        "'{}'",
        "'{=r/pg_monitor,pg_read_all_data=arw/pg_monitor}'",
    ),
    '"char"[]': ("'{a,b}'", "'{}'", "'{A}'"),
    "int4range[]": _RANGES,
    "int8range[]": _RANGES,
    "numrange[]": ("'{\"[1.5,2.5)\"}'", "'{}'", "'{\"(,3]\"}'"),
    "daterange[]": (
        '\'{"[2024-01-01,2024-02-01)","[2024-03-01,2024-04-01)"}\'',
        "'{}'",
        "'{empty}'",
    ),
    "tsrange[]": ("'{\"[2024-01-01 00:00,2024-02-01 00:00)\"}'", "'{}'", "'{empty}'"),
    "tstzrange[]": ("'{\"[2024-01-01 00:00+00,2024-02-01 00:00+00)\"}'", "'{}'", "'{empty}'"),
}

#: The types PostgreSQL takes no literal of: their input function refuses every value ("cannot
#: accept a value of type"), so only a value the server itself derived could be passed.
NO_LITERAL = frozenset(
    {
        "gtsvector",
        "pg_brin_bloom_summary",
        "pg_brin_minmax_multi_summary",
        "pg_ddl_command",
        "pg_dependencies",
        "pg_mcv_list",
        "pg_ndistinct",
        "pg_node_tree",
    }
)


class Raw(tuple):
    """Literals ``ARG_VALUES`` gives as they stand, cast or not by the literal itself: an argument
    of a pseudo-type, an expression, or several arguments of a variadic ``"any"``."""


#: The type each input function reads, so a cstring argument takes that type's samples as text.
INPUT_TYPES = {
    "bit_in": "bit",
    "boolin": "boolean",
    "box_in": "box",
    "bpcharin": "character",
    "byteain": "bytea",
    "cash_in": "money",
    "charin": '"char"',
    "cidin": "cid",
    "cidr_in": "cidr",
    "circle_in": "circle",
    "cstring_in": "text",
    "cstring_out": "text",
    "cstring_send": "text",
    "date_in": "date",
    "enum_in": function_probe.PROBE_ENUM,
    "float4in": "real",
    "float8in": "double precision",
    "inet_in": "inet",
    "int2in": "smallint",
    "int2vectorin": "int2vector",
    "int4in": "integer",
    "int8in": "bigint",
    "json_in": "json",
    "jsonpath_in": "jsonpath",
    "line_in": "line",
    "lseg_in": "lseg",
    "macaddr8_in": "macaddr8",
    "macaddr_in": "macaddr",
    "namein": "name",
    "numeric_in": "numeric",
    "oidin": "oid",
    "oidvectorin": "oidvector",
    "path_in": "path",
    "pg_lsn_in": "pg_lsn",
    "pg_snapshot_in": "pg_snapshot",
    "point_in": "point",
    "poly_in": "polygon",
    "range_in": "int4range",
    "multirange_in": "int4multirange",
    "array_in": "integer[]",
    "regclassin": "regclass",
    "regcollationin": "regcollation",
    "regconfigin": "regconfig",
    "regdictionaryin": "regdictionary",
    "regnamespacein": "regnamespace",
    "regoperatorin": "regoperator",
    "regoperin": "regoper",
    "regprocedurein": "regprocedure",
    "regprocin": "regproc",
    "regrolein": "regrole",
    "regtypein": "regtype",
    "aclitemin": "aclitem",
    "textin": "text",
    "tidin": "tid",
    "time_in": "time without time zone",
    "timestamp_in": "timestamp without time zone",
    "timestamptz_in": "timestamp with time zone",
    "timetz_in": "time with time zone",
    "tsqueryin": "tsquery",
    "tsvectorin": "tsvector",
    "txid_snapshot_in": "txid_snapshot",
    "uuid_in": "uuid",
    "varbit_in": "bit varying",
    "varcharin": "character varying",
    "xidin": "xid",
    "xml_in": "xml",
}

#: The element an input function's type-oid argument names, where it is not the type itself.
_INPUT_OID = {"array_in": "integer", "range_in": "int4range", "multirange_in": "int4multirange"}


def _samples_of(sample_type: str) -> tuple[str, str, str] | None:
    return SAMPLES.get(sample_type) or VALUE_SAMPLES.get(sample_type)


def _as_text(literal: str) -> str:
    """A literal as the quoted text a type's input function reads."""
    if literal.startswith("'"):
        return literal
    return "'" + literal.strip("()").replace("'", "''") + "'"


def _input_given(fn: Function) -> dict[int, tuple[str, ...]]:
    """An input function's arguments: its type's samples as cstring, then that type's oid and
    the typmod -1 (``INPUT_TYPES``)."""
    typ = INPUT_TYPES.get(fn.name)
    samples = None if typ is None else _samples_of(typ)
    if samples is None or not fn.argtypes or fn.argtypes[0] != "cstring":
        return {}
    given: dict[int, tuple[str, ...]] = {0: Raw(f"{_as_text(s)}::cstring" for s in samples)}
    if len(fn.argtypes) > 1:
        named = _INPUT_OID.get(fn.name, typ)
        given[1] = Raw((f"'{named}'::regtype::oid",) * VALUE_ROWS)
    if len(fn.argtypes) > 2:
        given[2] = Raw(("(-1)::integer",) * VALUE_ROWS)
    return given


_UNITS = ("'day'", "'month'", "'year'")
_FIELDS = ("'year'", "'month'", "'dow'")
_PATTERNS = ("'b'", "'[a-z]+'", "'^1'")
_ENCODINGS = ("'hex'", "'base64'", "'escape'")

_BOUNDS = ("'[)'", "'[]'", "'()'")
_START = ("1", "2", "1")

_FLAGS = ("'i'", "'c'", "'i'")
_ROLE_OIDS = Raw(f"'{r}'::regrole::oid" for r in ("pg_monitor", "pg_read_all_data", "pg_monitor"))
#: float8 transition states: N, the sum and the sum of squared deviations; a regression's adds Sy,
#: Syy and Sxy.
_FLOAT8_STATE = ("'{3,6,2}'", "'{1,0.5,0}'", "'{4,10,5}'")
_REGR_STATE = ("'{3,6,2,9,2,2}'", "'{1,0.5,0,2,0,0}'", "'{4,10,5,8,5,5}'")
_INT_AVG_STATE = ("'{3,6}'", "'{1,-7}'", "'{4,100}'")
_FORMS = ("'NFC'", "'NFD'", "'NFKC'")
_NAMESPACE_ROW = Raw(("NULL::pg_catalog.pg_namespace",) * 3)
_NAMESPACE_JSON = ('\'{"nspname": "x", "oid": 5}\'', "'{}'", "'{\"nspname\": null}'")
_NAMESPACE_JSONS = ('\'[{"nspname": "x", "oid": 5}]\'', "'[]'", "'[{}, {\"oid\": 1}]'")
_PARSER = {0: ("'default'",) * 3}
_PARSER_OID = {0: ("3722",) * 3}
_HEADLINE_OPTIONS = ("'MaxWords=2, MinWords=1'", "'ShortWord=1'", "''")

#: The literals of the overloads PostgreSQL refused every sample of, by ``_key``: the values each
#: one's own input rules take.
_REFUSED_VALUES: dict[str, dict[int, tuple[str, ...]]] = {
    'acldefault("char", oid)': {0: ("'r'", "'f'", "'n'"), 1: _ROLE_OIDS},
    **{
        f"{name}(text, {time})": {0: ("'hour'", "'minute'", "'second'")}
        for name in ("date_part", "extract")
        for time in ("time without time zone", "time with time zone")
    },
    "float4_accum(double precision[], real)": {0: _FLOAT8_STATE},
    "float8_accum(double precision[], double precision)": {0: _FLOAT8_STATE},
    "float8_combine(double precision[], double precision[])": {0: _FLOAT8_STATE, 1: _FLOAT8_STATE},
    "float8_regr_accum(double precision[], double precision, double precision)": {0: _REGR_STATE},
    "float8_regr_combine(double precision[], double precision[])": {
        0: _REGR_STATE,
        1: _REGR_STATE,
    },
    **{
        f"float8_{name}(double precision[])": {0: _FLOAT8_STATE}
        for name in ("avg", "stddev_pop", "stddev_samp", "var_pop", "var_samp")
    },
    **{
        f"float8_{name}(double precision[])": {0: _REGR_STATE}
        for name in (
            "corr",
            "covar_pop",
            "covar_samp",
            "regr_avgx",
            "regr_avgy",
            "regr_intercept",
            "regr_r2",
            "regr_slope",
            "regr_sxx",
            "regr_sxy",
            "regr_syy",
        )
    },
    "int2_avg_accum(bigint[], smallint)": {0: _INT_AVG_STATE},
    "int2_avg_accum_inv(bigint[], smallint)": {0: _INT_AVG_STATE},
    "int4_avg_accum(bigint[], integer)": {0: _INT_AVG_STATE},
    "int4_avg_accum_inv(bigint[], integer)": {0: _INT_AVG_STATE},
    "int4_avg_combine(bigint[], bigint[])": {0: _INT_AVG_STATE, 1: _INT_AVG_STATE},
    "int8_avg(bigint[])": {0: _INT_AVG_STATE},
    "int2int4_sum(bigint[])": {0: _INT_AVG_STATE},
    "bool(jsonb)": {0: ("'true'", "'false'", "'true'")},
    "btvarstrequalimage(oid)": {0: ("100", "950", "951")},
    "is_normalized(text, text)": {1: _FORMS},
    "normalize(text, text)": {1: _FORMS},
    "json_to_tsvector(json, jsonb)": {1: ("'[\"string\"]'", "'[\"numeric\"]'", "'[\"all\"]'")},
    "json_to_tsvector(regconfig, json, jsonb)": {
        2: ("'[\"string\"]'", "'[\"numeric\"]'", "'[\"all\"]'")
    },
    "jsonb_to_tsvector(jsonb, jsonb)": {1: ("'[\"string\"]'", "'[\"numeric\"]'", "'[\"all\"]'")},
    "jsonb_to_tsvector(regconfig, jsonb, jsonb)": {
        2: ("'[\"string\"]'", "'[\"numeric\"]'", "'[\"all\"]'")
    },
    'json_build_object("any")': {
        0: Raw(("1::integer, 2::integer", "'a'::text, 3::integer", "'k'::text, NULL::integer"))
    },
    'jsonb_build_object("any")': {
        0: Raw(("1::integer, 2::integer", "'a'::text, 3::integer", "'k'::text, NULL::integer"))
    },
    "json_populate_record(anyelement, json, boolean)": {0: _NAMESPACE_ROW, 1: _NAMESPACE_JSON},
    "json_populate_recordset(anyelement, json, boolean)": {
        0: _NAMESPACE_ROW,
        1: _NAMESPACE_JSONS,
    },
    "jsonb_populate_record(anyelement, jsonb)": {0: _NAMESPACE_ROW, 1: _NAMESPACE_JSON},
    "jsonb_populate_record_valid(anyelement, jsonb)": {0: _NAMESPACE_ROW, 1: _NAMESPACE_JSON},
    "jsonb_populate_recordset(anyelement, jsonb)": {0: _NAMESPACE_ROW, 1: _NAMESPACE_JSONS},
    "array_agg(anyarray)": {0: ("'{1,2}'", "'{3,4}'", "'{5,NULL}'")},
    "line(point, point)": {1: ("'(3,4)'", "'(1,1)'", "'(0,4)'")},
    "macaddr(macaddr8)": {
        0: ("'08:00:2b:ff:fe:01:02:03'", "'00:00:00:ff:fe:00:00:00'", "'ff:ff:ff:ff:fe:ff:ff:ff'")
    },
    "makeaclitem(oid, oid, text, boolean)": {
        0: _ROLE_OIDS,
        1: _ROLE_OIDS,
        2: ("'SELECT'", "'INSERT'", "'SELECT, UPDATE'"),
    },
    "regclass(text)": {0: VALUE_SAMPLES["regclass"]},
    "regexp_count(text, text, integer, text)": {1: _PATTERNS, 2: _START, 3: _FLAGS},
    "regexp_instr(text, text, integer, integer, integer)": {
        1: _PATTERNS,
        2: _START,
        3: ("1", "1", "2"),
        4: ("0", "1", "0"),
    },
    "regexp_instr(text, text, integer, integer, integer, text)": {
        1: _PATTERNS,
        2: _START,
        3: ("1", "1", "2"),
        4: ("0", "1", "0"),
        5: _FLAGS,
    },
    "regexp_instr(text, text, integer, integer, integer, text, integer)": {
        1: _PATTERNS,
        2: _START,
        3: ("1", "1", "2"),
        4: ("0", "1", "0"),
        5: _FLAGS,
        6: ("0", "0", "0"),
    },
    "regexp_like(text, text, text)": {1: _PATTERNS, 2: _FLAGS},
    "regexp_match(text, text, text)": {1: _PATTERNS, 2: _FLAGS},
    "regexp_matches(text, text, text)": {1: _PATTERNS, 2: ("'g'", "'i'", "'gi'")},
    "regexp_replace(text, text, text, text)": {1: _PATTERNS, 3: ("'g'", "'i'", "'gi'")},
    "regexp_replace(text, text, text, integer, integer, text)": {
        1: _PATTERNS,
        3: _START,
        4: ("1", "0", "2"),
        5: _FLAGS,
    },
    "regexp_split_to_array(text, text, text)": {1: _PATTERNS, 2: _FLAGS},
    "regexp_split_to_table(text, text, text)": {1: _PATTERNS, 2: _FLAGS},
    "regexp_substr(text, text, integer, integer, text)": {
        1: _PATTERNS,
        2: _START,
        3: ("1", "1", "2"),
        4: _FLAGS,
    },
    "regexp_substr(text, text, integer, integer, text, integer)": {
        1: _PATTERNS,
        2: _START,
        3: ("1", "1", "2"),
        4: _FLAGS,
        5: ("0", "0", "0"),
    },
    "row_security_active(text)": {0: VALUE_SAMPLES["regclass"]},
    "schema_to_xml(name, boolean, boolean, text)": {0: ("'conformance'",) * 3},
    "schema_to_xml_and_xmlschema(name, boolean, boolean, text)": {0: ("'conformance'",) * 3},
    "schema_to_xmlschema(name, boolean, boolean, text)": {0: ("'conformance'",) * 3},
    **{
        f"timezone(interval, {time})": {0: ("'1 hour'", "'-05:30'", "'00:00'")}
        for time in (
            "timestamp without time zone",
            "timestamp with time zone",
            "time with time zone",
        )
    },
    "to_ascii(text, integer)": {1: ("8", "9", "29")},
    "to_ascii(text, name)": {1: ("'LATIN1'", "'LATIN2'", "'WIN1250'")},
    "ts_headline(jsonb, tsquery, text)": {2: _HEADLINE_OPTIONS},
    "ts_headline(json, tsquery, text)": {2: _HEADLINE_OPTIONS},
    "ts_headline(text, tsquery, text)": {2: _HEADLINE_OPTIONS},
    "ts_parse(text, text)": _PARSER,
    "ts_parse(oid, text)": _PARSER_OID,
    "ts_token_type(text)": _PARSER,
    "ts_token_type(oid)": _PARSER_OID,
    **{
        f"{name}(real[], tsvector, tsquery{rest})": {0: ("'{0.1,0.2,0.4,1.0}'",) * 3}
        for name in ("ts_rank", "ts_rank_cd")
        for rest in ("", ", integer")
    },
    "xpath(text, xml, text[])": {
        0: ("'/a'", "'/b/@x'", "'/c/d/text()'"),
        2: ("'{}'", "'{{x,http://example.com}}'", "'{}'"),
    },
    "xpath_exists(text, xml, text[])": {
        0: ("'/a'", "'/b/@x'", "'/c/d/text()'"),
        2: ("'{}'", "'{{x,http://example.com}}'", "'{}'"),
    },
}

#: Catalog objects every PostgreSQL 17 catalog has, by what kind each is: its name as text, then
#: its oid. A privilege function asks of these; a description or definition function reads them.
_TABLE_NAMES = ("'pg_catalog.pg_class'", "'pg_catalog.pg_namespace'", f"'{COLUMN_RELATION}'")
_OBJECTS: dict[str, tuple[tuple[str, ...], tuple[str, ...]]] = {
    "table": (_TABLE_NAMES, tuple(f"{t}::regclass::oid" for t in _TABLE_NAMES)),
    "sequence": (
        (f"'{function_probe.PROBE_SEQUENCE}'",) * 3,
        (f"'{function_probe.PROBE_SEQUENCE}'::regclass::oid",) * 3,
    ),
    "database": (
        ("current_database()",) * 3,
        ("(SELECT oid FROM pg_catalog.pg_database WHERE datname = current_database())",) * 3,
    ),
    "schema": (
        VALUE_SAMPLES["regnamespace"],
        tuple(f"{n}::regnamespace::oid" for n in VALUE_SAMPLES["regnamespace"]),
    ),
    "function": (
        ("'pg_catalog.now()'", "'pg_catalog.abs(integer)'", "'pg_catalog.lower(text)'"),
        tuple(f"{p}::regprocedure::oid" for p in VALUE_SAMPLES["regprocedure"]),
    ),
    "language": (("'sql'", "'internal'", "'c'"), ("14", "12", "13")),
    "tablespace": (("'pg_default'", "'pg_global'", "'pg_default'"), ("1663", "1664", "1663")),
    "type": (
        VALUE_SAMPLES["regtype"],
        tuple(f"{t}::regtype::oid" for t in VALUE_SAMPLES["regtype"]),
    ),
    "parameter": (("'work_mem'", "'TimeZone'", "'search_path'"), ()),
    "role": (VALUE_SAMPLES["regrole"], tuple(_ROLE_OIDS)),
}
_OBJECTS["column"] = _OBJECTS["any_column"] = _OBJECTS["table"]
_PRIVILEGES = {
    "table": ("'SELECT'", "'INSERT'", "'UPDATE'"),
    "column": ("'SELECT'", "'UPDATE'", "'REFERENCES'"),
    "any_column": ("'SELECT'", "'UPDATE'", "'REFERENCES'"),
    "sequence": ("'USAGE'", "'SELECT'", "'UPDATE'"),
    "database": ("'CONNECT'", "'CREATE'", "'TEMPORARY'"),
    "schema": ("'USAGE'", "'CREATE'", "'USAGE'"),
    "function": ("'EXECUTE'",) * 3,
    "language": ("'USAGE'",) * 3,
    "tablespace": ("'CREATE'",) * 3,
    "type": ("'USAGE'",) * 3,
    "parameter": ("'SET'", "'ALTER SYSTEM'", "'SET'"),
    "role": ("'MEMBER'", "'USAGE'", "'MEMBER'"),
}
#: A column of each of ``_TABLE_NAMES``, by name and by number.
_COLUMN_NAMES = ("'relname'", "'nspname'", "'id'")
_COLUMN_NUMBERS = ("2", "2", "1")
_PRIVILEGE_FUNCTION = re.compile(r"has_(\w+)_privilege|pg_has_role")


def _object(kind: str, argtype: str) -> Raw:
    names, oids = _OBJECTS[kind]
    return Raw(oids) if argtype == "oid" else Raw(f"{n}::{argtype}" for n in names)


def _privilege_given(fn: Function) -> dict[int, tuple[str, ...]]:
    """A privilege function's arguments: a role, an object of its kind and a privilege of it."""
    matched = _PRIVILEGE_FUNCTION.fullmatch(fn.name)
    if matched is None:
        return {}
    kind = matched.group(1) or "role"
    if kind not in _OBJECTS:
        return {}
    types = fn.argtypes
    objects = 2 if kind == "column" else 1
    given: dict[int, tuple[str, ...]] = {
        len(types) - 1: Raw(f"{p}::text" for p in _PRIVILEGES[kind])
    }
    first = len(types) - 1 - objects
    if first == 1:
        given[0] = _object("role", types[0])
    given[first] = _object(kind, types[first])
    if kind == "column":
        column = types[first + 1]
        names = _COLUMN_NUMBERS if column == "smallint" else _COLUMN_NAMES
        given[first + 1] = Raw(f"{n}::{column}" for n in names)
    return given


#: Catalog and session functions, by ``_key``: an object their PostgreSQL 17 catalog has, or the
#: settings both servers have under ``SETTINGS``.
_CATALOG_VALUES: dict[str, dict[int, tuple[str, ...]]] = {
    "current_setting(text)": {0: ("'TimeZone'", "'DateStyle'", "'IntervalStyle'")},
    "current_setting(text, boolean)": {0: ("'TimeZone'", "'DateStyle'", "'no_such.setting'")},
    "obj_description(oid)": {0: Raw(_OBJECTS["function"][1])},
    "obj_description(oid, name)": {
        0: Raw((*_OBJECTS["function"][1][:2], "'integer'::regtype::oid")),
        1: ("'pg_proc'", "'pg_proc'", "'pg_type'"),
    },
    "col_description(oid, integer)": {0: Raw(_OBJECTS["table"][1]), 1: _COLUMN_NUMBERS},
    "shobj_description(oid, name)": {0: ("1663", "1664", "1663"), 1: ("'pg_tablespace'",) * 3},
    "format_type(oid, integer)": {
        0: Raw(f"{t}::regtype::oid" for t in ("'integer'", "'numeric'", "'character varying'")),
        1: ("(-1)", "655366", "14"),
    },
    "pg_get_userbyid(oid)": {0: _ROLE_OIDS},
    **{
        f"pg_get_function_{part}(oid)": {0: Raw(_OBJECTS["function"][1])}
        for part in ("arguments", "identity_arguments", "result", "sqlbody")
    },
    "pg_get_functiondef(oid)": {0: Raw(_OBJECTS["function"][1])},
    "pg_get_function_arg_default(oid, integer)": {
        0: Raw(
            (
                "'pg_catalog.make_interval(integer, integer, integer, integer, integer, integer, double precision)'::regprocedure::oid",
            )
            * 3
        ),
        1: ("1", "7", "8"),
    },
    "pg_get_viewdef(oid)": {0: Raw(("'pg_catalog.pg_tables'::regclass::oid",) * 3)},
    "pg_get_viewdef(oid, boolean)": {0: Raw(("'pg_catalog.pg_tables'::regclass::oid",) * 3)},
    "pg_get_viewdef(oid, integer)": {
        0: Raw(("'pg_catalog.pg_tables'::regclass::oid",) * 3),
        1: ("0", "40", "80"),
    },
    "pg_get_viewdef(text)": {0: ("'pg_catalog.pg_tables'", "'pg_catalog.pg_views'", "'pg_tables'")},
    "pg_get_viewdef(text, boolean)": {
        0: ("'pg_catalog.pg_tables'", "'pg_catalog.pg_views'", "'pg_tables'")
    },
    "pg_get_indexdef(oid)": {
        0: Raw(
            f"'pg_catalog.{i}'::regclass::oid"
            for i in ("pg_class_oid_index", "pg_type_oid_index", "pg_namespace_nspname_index")
        )
    },
    "pg_get_indexdef(oid, integer, boolean)": {
        0: Raw(
            f"'pg_catalog.{i}'::regclass::oid"
            for i in ("pg_class_oid_index", "pg_type_oid_index", "pg_namespace_nspname_index")
        ),
        1: ("0", "1", "1"),
    },
    **{
        f"pg_get_constraintdef(oid{rest})": {
            0: Raw(
                f"(SELECT oid FROM pg_catalog.pg_constraint WHERE conname = '{c}')"
                for c in ("pg_class_oid_index", "pg_type_oid_index", "pg_namespace_nspname_index")
            )
        }
        for rest in ("", ", boolean")
    },
    "pg_get_serial_sequence(text, text)": {0: _TABLE_NAMES, 1: _COLUMN_NAMES},
    "pg_table_is_visible(oid)": {0: Raw(_OBJECTS["table"][1])},
    "pg_type_is_visible(oid)": {0: Raw(_OBJECTS["type"][1])},
    "pg_function_is_visible(oid)": {0: Raw(_OBJECTS["function"][1])},
    "pg_describe_object(oid, oid, integer)": {
        0: Raw(
            (
                "'pg_catalog.pg_class'::regclass::oid",
                "'pg_catalog.pg_proc'::regclass::oid",
                "'pg_catalog.pg_type'::regclass::oid",
            )
        ),
        1: Raw((_OBJECTS["table"][1][0], _OBJECTS["function"][1][0], _OBJECTS["type"][1][0])),
        2: ("0", "0", "0"),
    },
    "pg_identify_object(oid, oid, integer)": {
        0: Raw(
            (
                "'pg_catalog.pg_class'::regclass::oid",
                "'pg_catalog.pg_proc'::regclass::oid",
                "'pg_catalog.pg_type'::regclass::oid",
            )
        ),
        1: Raw((_OBJECTS["table"][1][0], _OBJECTS["function"][1][0], _OBJECTS["type"][1][0])),
        2: ("0", "0", "0"),
    },
    "pg_identify_object_as_address(oid, oid, integer)": {
        0: Raw(
            (
                "'pg_catalog.pg_class'::regclass::oid",
                "'pg_catalog.pg_proc'::regclass::oid",
                "'pg_catalog.pg_type'::regclass::oid",
            )
        ),
        1: Raw((_OBJECTS["table"][1][0], _OBJECTS["function"][1][0], _OBJECTS["type"][1][0])),
        2: ("0", "0", "0"),
    },
    "pg_get_object_address(text, text[], text[])": {
        0: ("'table'", "'function'", "'type'"),
        1: ("'{pg_catalog,pg_class}'", "'{pg_catalog,now}'", "'{integer}'"),
        2: ("'{}'", "'{}'", "'{}'"),
    },
    "pg_index_has_property(regclass, text)": {
        0: ("'pg_catalog.pg_class_oid_index'",) * 3,
        1: ("'clusterable'", "'index_scan'", "'bitmap_scan'"),
    },
    "pg_index_column_has_property(regclass, integer, text)": {
        0: ("'pg_catalog.pg_class_oid_index'",) * 3,
        1: ("1", "1", "1"),
        2: ("'asc'", "'orderable'", "'returnable'"),
    },
    "pg_indexam_has_property(oid, text)": {
        0: ("403", "405", "403"),
        1: ("'can_order'", "'can_unique'", "'can_multi_col'"),
    },
    "pg_input_is_valid(text, text)": {
        0: ("'42'", "'abc'", "'2024-02-30'"),
        1: ("'integer'", "'integer'", "'date'"),
    },
    "pg_input_error_info(text, text)": {
        0: ("'42'", "'abc'", "'2024-02-30'"),
        1: ("'integer'", "'integer'", "'date'"),
    },
    "pg_settings_get_flags(text)": {0: ("'work_mem'", "'TimeZone'", "'no_such_setting'")},
    "pg_char_to_encoding(name)": {0: ("'UTF8'", "'LATIN1'", "'SQL_ASCII'")},
    "pg_encoding_to_char(integer)": {0: ("6", "8", "0")},
    "pg_basetype(regtype)": {0: VALUE_SAMPLES["regtype"]},
    "pg_options_to_table(text[])": {0: ("'{a=1,b=2}'", "'{}'", "'{fillfactor=70}'")},
    **{
        f"to_reg{kind}(text)": {0: (*names[:2], "'no_such_object'")}
        for kind, names in (
            ("class", VALUE_SAMPLES["regclass"]),
            ("collation", VALUE_SAMPLES["regcollation"]),
            ("namespace", VALUE_SAMPLES["regnamespace"]),
            ("oper", VALUE_SAMPLES["regoper"]),
            ("operator", VALUE_SAMPLES["regoperator"]),
            ("proc", VALUE_SAMPLES["regproc"]),
            ("procedure", VALUE_SAMPLES["regprocedure"]),
            ("role", VALUE_SAMPLES["regrole"]),
            ("type", VALUE_SAMPLES["regtype"]),
            ("typemod", ("'integer'", "'numeric(10,2)'", "'varchar(12)'")),
        )
    },
    "fmgr_internal_validator(oid)": {0: Raw(("'pg_catalog.now()'::regprocedure::oid",) * 3)},
    "fmgr_sql_validator(oid)": {
        0: Raw(("'pg_catalog.ts_debug(regconfig, text)'::regprocedure::oid",) * 3)
    },
    "fmgr_c_validator(oid)": {
        0: Raw(("'pg_catalog.plpgsql_call_handler()'::regprocedure::oid",) * 3)
    },
    'pg_collation_for("any")': {0: Raw(("'abc'::text", "'abc'::text COLLATE \"C\"", "'x'::name"))},
}

#: Values of the volatile functions ``PROPERTIES`` checks, by ``_key``.
_PROPERTY_VALUES: dict[str, dict[int, tuple[str, ...]]] = {
    "array_sample(anyarray, integer)": {1: ("2", "0", "3")},
    "random(integer, integer)": {0: ("1", "(-5)", "7"), 1: ("10", "5", "7")},
    "random(bigint, bigint)": {0: ("1", "(-5000000000)", "7"), 1: ("10", "5", "7")},
    "random(numeric, numeric)": {0: ("1.5", "(-2.25)", "7"), 1: ("2.25", "5.125", "7.0")},
    "random_normal(double precision, double precision)": {
        0: ("5", "(-2.5)", "0"),
        1: ("0", "0", "0"),
    },
}

#: Where a function needs literals its argument's type cannot give, by name and argument position;
#: ``name(type)`` for the overloads whose first argument is that type. Raw literals, cast as
#: ``SAMPLES`` is.
ARG_VALUES: dict[str, dict[int, tuple[str, ...]]] = {
    "date_trunc": {0: _UNITS},
    "date_part": {0: _FIELDS},
    "extract": {0: _FIELDS},
    "date_bin": {0: ("'15 minutes'", "'1 hour'", "'1 day'")},
    "to_char": {1: ("'YYYY-MM-DD'", "'HH24:MI:SS'", "'FM999.00'")},
    "to_date": {
        0: ("'2024-06-15'", "'15 Jun 2024'", "'1999'"),
        1: ("'YYYY-MM-DD'", "'DD Mon YYYY'", "'YYYY'"),
    },
    "to_timestamp": {
        0: ("'2024-06-15 12:30:45'", "'15 Jun 2024'", "'1999'"),
        1: ("'YYYY-MM-DD HH24:MI:SS'", "'DD Mon YYYY'", "'YYYY'"),
    },
    "to_number": {0: ("'12,345.5'", "'-7'", "'0042'"), 1: ("'99G999D9'", "'S9'", "'0000'")},
    "make_date": {0: ("2024", "1999", "2000"), 1: ("6", "12", "2"), 2: ("15", "31", "29")},
    "make_time": {0: ("12", "0", "23"), 1: ("30", "0", "59"), 2: ("45.5", "0", "59.5")},
    "make_timestamp": {
        0: ("2024", "1999", "2000"),
        1: ("6", "12", "2"),
        2: ("15", "31", "29"),
        3: ("12", "0", "23"),
        4: ("30", "0", "59"),
        5: ("45.5", "0", "59.5"),
    },
    "make_timestamptz": {
        0: ("2024", "1999", "2000"),
        1: ("6", "12", "2"),
        2: ("15", "31", "29"),
        3: ("12", "0", "23"),
        4: ("30", "0", "59"),
        5: ("45.5", "0", "59.5"),
    },
    "regexp_count": {1: _PATTERNS},
    "regexp_instr": {1: _PATTERNS},
    "regexp_like": {1: _PATTERNS},
    "regexp_match": {1: _PATTERNS},
    "regexp_matches": {1: _PATTERNS},
    "regexp_replace": {1: _PATTERNS},
    "regexp_split_to_array": {1: _PATTERNS},
    "regexp_split_to_table": {1: _PATTERNS},
    "regexp_substr": {1: _PATTERNS},
    "encode": {1: _ENCODINGS},
    "decode": {0: ("'deadbeef'", "'3q2+7w=='", "'abc'"), 1: _ENCODINGS},
    "convert_from": {1: ("'UTF8'", "'LATIN1'", "'UTF8'")},
    "convert_to": {1: ("'UTF8'", "'LATIN1'", "'UTF8'")},
    "convert": {1: ("'UTF8'", "'LATIN1'", "'UTF8'"), 2: ("'LATIN1'", "'UTF8'", "'LATIN1'")},
    "nth_value": {1: ("1", "2", "1")},
    "lag": {1: ("1", "2", "1")},
    "lead": {1: ("1", "2", "1")},
    "ntile": {0: ("2", "2", "2")},
    "percentile_cont": {0: ("0.25", "0.5", "0.75")},
    "percentile_disc": {0: ("0.25", "0.5", "0.75")},
    "width_bucket(numeric)": {1: ("0", "(-10)", "0"), 2: ("10", "10", "100"), 3: ("5", "4", "10")},
    "width_bucket(double precision)": {
        1: ("0", "(-10)", "0"),
        2: ("10", "10", "100"),
        3: ("5", "4", "10"),
    },
    "repeat": {1: ("2", "0", "3")},
    "lpad": {1: ("6", "2", "5")},
    "rpad": {1: ("6", "2", "5")},
    "set_byte": {1: ("0", "1", "0"), 2: ("65", "66", "67")},
    "get_byte": {1: ("0", "1", "0")},
    "set_bit": {1: ("0", "1", "0"), 2: ("1", "0", "1")},
    "get_bit": {1: ("0", "1", "0")},
    "split_part": {1: ("'b'", "' '", "'1'"), 2: ("1", "2", "1")},
    "json_extract_path": {1: ("'{a}'", "'{0}'", "'{a}'")},
    "json_extract_path_text": {1: ("'{a}'", "'{0}'", "'{a}'")},
    "jsonb_extract_path": {1: ("'{a}'", "'{0}'", "'{a}'")},
    "jsonb_extract_path_text": {1: ("'{a}'", "'{0}'", "'{a}'")},
    "timezone(text)": {0: ("'UTC'", "'America/New_York'", "'Asia/Tokyo'")},
    "substring": {1: _START, 2: ("2", "3", "1")},
    "overlay": {2: _START, 3: ("1", "2", "1")},
    "like_escape": {1: ("'\\'", "'#'", "''")},
    "similar_escape": {1: ("'\\'", "'#'", "''")},
    "similar_to_escape": {1: ("'\\'", "'#'", "''")},
    "ts_headline": {3: ("'MaxWords=2, MinWords=1'", "'ShortWord=1'", "''")},
    "daterange": {2: _BOUNDS},
    "int4range": {2: _BOUNDS},
    "int8range": {2: _BOUNDS},
    "numrange": {2: _BOUNDS},
    "tsrange": {2: _BOUNDS},
    "tstzrange": {2: _BOUNDS},
    **_REFUSED_VALUES,
    **_CATALOG_VALUES,
    **_PROPERTY_VALUES,
}


@dataclass(frozen=True)
class Call:
    """One statement of an overload. ``answers``: a real answer, compared by value; ``values``:
    whether the rows compare at all (the NULL call of a volatile function, or of one whose answer
    is its server's own: no); ``zone``: the session's TimeZone for this call, if not ``SETTINGS``'
    UTC; ``prop``: the key of the ``PROPERTIES`` check its answer is held to, with ``args``, the
    literals it was called with."""

    sql: str
    answers: bool = False
    values: bool = True
    zone: str | None = None
    prop: str | None = None
    args: tuple[str, ...] = ()

    @property
    def form(self) -> bool:
        """Whether the answer's form is checked: its value cannot be, no NULL made it, and no
        property it is held to says more."""
        return not self.values and self.prop is None and "NULL::" not in self.sql


def _cast(literal: str, argtype: str) -> str:
    concrete = function_probe.CONCRETE.get(argtype, argtype)
    return f"{literal}::{concrete}"


def _given(fn: Function) -> dict[int, tuple[str, ...]]:
    """The arguments of ``fn`` that take literals of their own, by position: ``ARG_VALUES`` by
    ``_key``, then by name and first argument type, then by name; an input function's, then a
    privilege function's."""
    return (
        ARG_VALUES.get(_key(fn))
        or ARG_VALUES.get(f"{fn.name}({fn.argtypes[0]})")
        or ARG_VALUES.get(fn.name)
        or _input_given(fn)
        or _privilege_given(fn)
    )


def _sample(fn: Function, position: int, row: int) -> str | None:
    """Row ``row``'s literal of ``fn``'s argument ``position``, cast; None if it has none."""
    argtype = fn.argtypes[position]
    given = _given(fn).get(position)
    if isinstance(given, Raw):
        return given[row]
    if given is None:
        given = _samples_of(function_probe.CONCRETE.get(argtype, argtype))
        if given is None:
            return None
    return _cast(given[row], argtype)


def unchecked(fn: Function, family: str) -> str | None:
    """Why ``fn`` is given no values; None if it is."""
    if function_probe.UNCALLABLE in fn.argtypes:
        return function_probe.NO_CALL
    if fn.signature in function_probe.STATEMENTS:
        return "its one statement takes no value"
    if fn.volatility == "v":
        return "volatile"
    own = own_answer(fn)
    if own is not None:
        return own
    missing = sorted(
        {
            function_probe.CONCRETE.get(t, t)
            for i, t in enumerate(fn.argtypes)
            if _sample(fn, i, 0) is None
        }
    )
    unliteral = [t for t in missing if t in NO_LITERAL]
    if unliteral:
        return f"PostgreSQL takes no literal of {', '.join(unliteral)}"
    return f"no sample of {', '.join(missing)}" if missing else None


def _rows(fn: Function) -> list[list[str]]:
    return [
        [_sample(fn, i, row) or "" for i in range(len(fn.argtypes))] for row in range(VALUE_ROWS)
    ]


def _values(rows: Iterable[Iterable[str]]) -> str:
    return "VALUES " + ", ".join(f"({', '.join(row)})" for row in rows)


def _columns(count: int) -> list[str]:
    return [f"c{i + 1}" for i in range(count)]


def _aggregate(fn: Function) -> str:
    """An aggregate over the rows of a VALUES list, which reads no table and so reaches the engine."""
    rows = _rows(fn)
    named = f'pg_catalog."{fn.name}"'
    if fn.aggkind == "h":
        relation = "(VALUES (1), (2), (3)) AS t(c1)"
        return f"SELECT {named}({rows[0][0]}) WITHIN GROUP (ORDER BY c1) AS v FROM {relation}"
    if not fn.argtypes:
        return f"SELECT {function_probe.statement(fn, [])[len('SELECT ') :]} FROM (VALUES (1), (2), (3)) AS t(c1)"
    if fn.aggkind == "o":
        direct = ", ".join(rows[0][: fn.direct])
        ordered = [row[fn.direct :] for row in rows]
        columns = _columns(len(ordered[0]))
        relation = f"({_values(ordered)}) AS t({', '.join(columns)})"
        return (
            f"SELECT {named}({direct}) WITHIN GROUP (ORDER BY {', '.join(columns)}) AS v "
            f"FROM {relation}"
        )
    columns = _columns(len(fn.argtypes))
    relation = f"({_values(rows)}) AS t({', '.join(columns)})"
    # The order of an aggregate that keeps its rows' order is the call's to give.
    order = " ORDER BY c1" if "_agg" in fn.name else ""
    return f"SELECT {named}({', '.join(columns)}{order}) AS v FROM {relation}"


def _window(fn: Function) -> str:
    named = f'pg_catalog."{fn.name}"'
    rows = _rows(fn)
    numbered = [[str(i + 1), *row] for i, row in enumerate(rows)]
    columns = _columns(len(fn.argtypes) + 1)
    relation = f"({_values(numbered)}) AS t({', '.join(columns)})"
    return f"SELECT {named}({', '.join(columns[1:])}) OVER (ORDER BY c1) AS v FROM {relation}"


def value_calls(fn: Function, family: str) -> list[Call]:
    """The calls of ``fn`` with real values, none if ``unchecked`` says so."""
    if unchecked(fn, family) is not None:
        return []
    if fn.kind == "a":
        return [Call(_aggregate(fn), answers=True)]
    if fn.kind == "w":
        return [Call(_window(fn), answers=True)]
    if not fn.argtypes:
        return [Call(function_probe.statement(fn, []), answers=True)]
    calls = []
    for row in _rows(fn):
        if fn.variadic:
            row = [*row[:-1], f"VARIADIC {row[-1]}"]
        calls.append(Call(function_probe.statement(fn, row), answers=True))
    return calls


def sample_column(sample_type: str) -> str:
    """The column of ``COLUMN_TABLE`` holding the samples of ``sample_type``."""
    return "c_" + re.sub(r"\W+", "_", sample_type.replace("[]", " array")).strip("_")


def _column_reads(fn: Function, rows: Iterable[int]) -> tuple[dict[int, str], str]:
    """The arguments of ``fn`` a column call of ``rows`` reads from a column, each position's
    column, and the relation they are columns of, with its ``id``. A VALUES subquery holds those
    rows alone: one sample that casts says nothing of another that does not."""
    given = _given(fn)
    first = 0 if fn.aggkind == "h" else fn.direct  # the one argument is direct and ordered both
    reads = {
        i: sample_column(function_probe.CONCRETE.get(argtype, argtype))
        for i, argtype in enumerate(fn.argtypes)
        if i >= first
        and i not in given
        and function_probe.CONCRETE.get(argtype, argtype) in SAMPLES
    }
    if reads:
        return reads, COLUMN_RELATION
    numbered = [[str(row + 1), _sample(fn, first, row) or ""] for row in rows]
    return {first: "c1"}, f"({_values(numbered)}) AS t(id, c1)"


def _column_args(fn: Function, reads: dict[int, str], row: int) -> list[str]:
    return [reads.get(i) or _sample(fn, i, row) or "" for i in range(len(fn.argtypes))]


def _column_aggregate(fn: Function) -> str:
    reads, relation = _column_reads(fn, range(VALUE_ROWS))
    named = f'pg_catalog."{fn.name}"'
    args = _column_args(fn, reads, 0)
    if fn.aggkind == "h":
        direct = _sample(fn, 0, 0)
        return f"SELECT {named}({direct}) WITHIN GROUP (ORDER BY {args[0]}) AS v FROM {relation}"
    if fn.aggkind == "o":
        direct = ", ".join(args[: fn.direct])
        ordered = ", ".join(args[fn.direct :])
        return f"SELECT {named}({direct}) WITHIN GROUP (ORDER BY {ordered}) AS v FROM {relation}"
    order = " ORDER BY id" if "_agg" in fn.name else ""
    return f"SELECT {named}({', '.join(args)}{order}) AS v FROM {relation}"


def column_calls(fn: Function, family: str) -> list[Call]:
    """``fn`` reading the values of ``value_calls`` from columns, in the order to try them: the
    first PostgreSQL answers is the overload's column call. None without an argument to read."""
    if unchecked(fn, family) is not None or not fn.argtypes:
        return []
    if fn.kind == "a":
        return [Call(_column_aggregate(fn), answers=True)]
    if fn.kind == "w":
        reads, relation = _column_reads(fn, range(VALUE_ROWS))
        args = ", ".join(_column_args(fn, reads, 0))
        named = f'pg_catalog."{fn.name}"'
        sql = f"SELECT {named}({args}) OVER (ORDER BY id) AS v FROM {relation}"
        return [Call(sql, answers=True)]
    out = []
    for row in range(VALUE_ROWS):
        reads, relation = _column_reads(fn, (row,))
        args = _column_args(fn, reads, row)
        if fn.variadic:
            args = [*args[:-1], f"VARIADIC {args[-1]}"]
        sql = f"{function_probe.statement(fn, args)} FROM {relation} WHERE id = {row + 1}"
        out.append(Call(sql, answers=True))
    return out


def property_calls(fn: Function) -> list[Call]:
    """The calls of a volatile function of ``PROPERTIES`` with real values, each held to its
    property: one per row of literals."""
    prop = _key(fn)
    if prop not in PROPERTIES:
        return []
    out = []
    for row in _rows(fn):
        sql = function_probe.statement(fn, row)
        out.append(Call(sql, values=False, prop=prop, args=tuple(row)))
    return out


def calls(fn: Function, family: str) -> list[Call]:
    """Every call of ``fn``: its NULL call, then its value calls or its property calls. The NULL
    call compares its rows unless the function is volatile or its answer its server's own; a
    clock function's is held to the statement's bounds (``CLOCK``)."""
    valued = value_calls(fn, family) or property_calls(fn)
    null = function_probe.call(fn)
    if null is None or any(c.sql == null for c in valued):
        return valued
    zoned = [Call(null, values=False, zone=ZONE)] if fn.name in ZONED_FUNCTIONS else []
    if fn.name in CLOCK_FUNCTIONS and not fn.argtypes:
        return [Call(null, values=False, prop=CLOCK)]
    compared = fn.volatility != "v" and own_answer(fn) is None
    return [Call(null, values=compared), *zoned, *valued]


# --- one call on one server -------------------------------------------------------------------


class Session(catalog.Server):
    """A server under the session settings, whose every call has a deadline: a statement that
    hangs costs its own overload, not the leg."""

    def __init__(self, dsn: str) -> None:
        super().__init__(dsn)
        self._set = None
        self._ensure()

    def _ensure(self) -> None:
        super()._ensure()
        if self._set is not self.conn:
            self._set = self.conn
            for sql in SETTINGS:
                self.conn.pgconn.exec_(sql.encode())

    def answer(self, sql: str, timeout: float = CALL_TIMEOUT_S) -> dict[str, Any]:
        """One statement's every row as text: its fields and rows, or its error. A lost
        connection says so (``lost``): the next call reconnects, and raises if it cannot."""
        self._ensure()
        conn = self.conn.pgconn
        deadline = time.monotonic() + timeout
        last = None
        try:
            conn.send_query_params(sql.encode(), [], result_format=0)
            while True:
                conn.consume_input()
                if conn.is_busy():
                    left = deadline - time.monotonic()
                    if left <= 0 or not select.select([conn.socket], [], [], left)[0]:
                        self.conn.close()
                        return {"ok": False, "error": f"no answer in {timeout:.0f}s"}
                    continue
                res = conn.get_result()
                if res is None:
                    break
                if last is None or res.status != pq.ExecStatus.TUPLES_OK:
                    last = res
        except psycopg.Error as exc:
            self.conn.close()
            return {"ok": False, "error": f"connection: {exc}".strip(), "lost": True}
        if last is None:
            return {"ok": False, "error": "no result"}
        return _result(last)


def _result(res: Any) -> dict[str, Any]:
    if res.status not in (pq.ExecStatus.TUPLES_OK, pq.ExecStatus.COMMAND_OK):
        return {
            "ok": False,
            "error": (res.error_message or b"").decode(errors="replace").strip()[:400],
        }
    fields = [res.ftype(i) for i in range(res.nfields)]
    rows = [
        tuple(
            None if (v := res.get_value(r, c)) is None else v.decode(errors="replace")
            for c in range(res.nfields)
        )
        for r in range(res.ntuples)
    ]
    return {"ok": True, "fields": fields, "rows": rows}


# --- comparison -------------------------------------------------------------------------------


#: What a comparison answers: None if the two differ, else the rules that made them the same, none
#: when their text is.
Rules = frozenset[str] | None

_SAME: frozenset[str] = frozenset()


def _floats_equal(a: str, b: str) -> bool:
    try:
        x, y = float(a), float(b)
    except ValueError:
        return False
    if not (math.isfinite(x) and math.isfinite(y)):
        return False
    return abs(x - y) <= FLOAT_TOLERANCE * max(abs(x), abs(y))


def _float_rules(want: str, got: str) -> Rules:
    if not _floats_equal(want, got):
        return None
    return frozenset({NUMBER_VALUE if float(want) == float(got) else FLOAT_CLOSE})


def _numeric_rules(want: str, got: str) -> Rules:
    try:
        return frozenset({NUMBER_VALUE}) if decimal.Decimal(want) == decimal.Decimal(got) else None
    except decimal.InvalidOperation:
        return None


def _array_rules(want: str, got: str, element: Callable[[str, str], Rules]) -> Rules:
    """An array of numbers, element by element."""
    a, b = want.strip("{}").split(","), got.strip("{}").split(",")
    if len(a) != len(b):
        return None
    found = [_SAME if x == y else element(x, y) for x, y in zip(a, b, strict=True)]
    return None if None in found else frozenset().union(*found)


def _unmatched(want: Iterable, got: Iterable, same: Callable) -> tuple[list, list]:
    """What is left of each multiset once every item of ``want`` has taken its ``same`` in ``got``."""
    missing, left = [], list(got)
    for item in want:
        match = next((i for i, other in enumerate(left) if same(item, other)), None)
        if match is None:
            missing.append(item)
        else:
            del left[match]
    return missing, left


def _json_number(value: Any) -> bool:
    return isinstance(value, int | float | decimal.Decimal) and not isinstance(value, bool)


def _parsed_rules(a: Any, b: Any) -> Rules:
    """Two parsed JSON values: objects by key, numbers by value, arrays in order."""
    if _json_number(a) and _json_number(b):
        if str(a) == str(b):
            return _SAME
        if isinstance(a, int) and isinstance(b, int):
            return None
        if decimal.Decimal(a) == decimal.Decimal(b):
            return frozenset({NUMBER_VALUE})
        return frozenset({FLOAT_CLOSE}) if _floats_equal(str(a), str(b)) else None
    if type(a) is not type(b):
        return None
    if isinstance(a, dict):
        if a.keys() != b.keys():
            return None
        found = [_parsed_rules(a[key], b[key]) for key in a]
        if None in found:
            return None
        return frozenset().union(*found, {JSON_KEY_ORDER} if list(a) != list(b) else ())
    if isinstance(a, list):
        if len(a) != len(b):
            return None
        found = list(map(_parsed_rules, a, b))
        return None if None in found else frozenset().union(*found)
    return _SAME if a == b else None


def _array_in_any_order(a: list, b: list) -> Rules:
    """An aggregate's array: its elements as a multiset, what let each match kept."""
    found: set[str] = {ARRAY_ORDER}

    def same(x: Any, y: Any) -> bool:
        rules = _parsed_rules(x, y)
        if rules is None:
            return False
        found.update(rules)
        return True

    return None if any(_unmatched(a, b, same)) else frozenset(found)


def _spacing(text: str) -> int:
    """How much whitespace ``text``, which is JSON, has between its tokens."""
    return sum(ch.isspace() for ch in re.sub(r'"(?:[^"\\]|\\.)*"', '""', text))


def json_rules(want: str, got: str, sql: str) -> Rules:
    """Whether two answers of ``sql`` are the same JSON (REQ-2171), and under which rules. The array
    an aggregate answers with, in a statement with no ORDER BY, has no order to hold it to. A side
    that does not parse compares as text."""
    try:
        a = json.loads(want, parse_float=decimal.Decimal)
        b = json.loads(got, parse_float=decimal.Decimal)
    except ValueError:
        return _SAME if want == got else None
    rules = _parsed_rules(a, b)
    aggregate = _ARRAY_AGGREGATE.search(sql) and not _ORDER_BY.search(sql)
    if rules is None and type(a) is type(b) is list and aggregate:
        rules = _array_in_any_order(a, b)
    if rules is None:
        return None
    if want != got and (not rules or _spacing(want) != _spacing(got)):
        return rules | {JSON_SPACING}
    return rules


def _record_fields(text: str) -> list[str] | None:
    """The fields of a record's text, unquoted; None if ``text`` is no record."""
    if len(text) < 2 or text[0] != "(" or text[-1] != ")":
        return None
    fields, field, quoted, i = [], "", False, 1
    body = text[:-1]
    while i < len(body):
        ch = body[i]
        pair = body[i : i + 2]
        if quoted and pair in ('""', "\\\\"):
            field += ch
            i += 1
        elif ch == '"':
            quoted = not quoted
        elif ch == "," and not quoted:
            fields.append(field)
            field = ""
        else:
            field += ch
        i += 1
    return [*fields, field]


def _record_rules(want: str, got: str, sql: str) -> Rules:
    """A record whose fields are the same, a JSON container comparing as JSON."""
    a, b = _record_fields(want), _record_fields(got)
    if a is None or b is None or len(a) != len(b):
        return None
    found: set[str] = set()
    for x, y in zip(a, b, strict=True):
        rules = json_rules(x, y, sql) if x[:1] in ("{", "[") else (_SAME if x == y else None)
        if rules is None:
            return None
        found |= rules
    return frozenset(found)


def _json_read(sql: str, oid: int, want: str) -> bool:
    """Whether ``want`` is the text a read of json gives for a number or a container. A json string
    stays a string: the statement's literal has it in quotes."""
    if oid != _TEXT_OID or _JSON_TEXT_READ.search(sql) is None or f'"{want}"' in sql:
        return False
    return want[:1] in ("{", "[") or _JSON_NUMBER.fullmatch(want) is not None


def _value_rules(want: str | None, got: str | None, oid: int, sql: str) -> Rules:
    if want == got:
        return _SAME
    if want is None or got is None:
        return None
    if oid in _FLOAT_OIDS:
        return _float_rules(want, got)
    if oid == _NUMERIC_OID:
        return _numeric_rules(want, got)
    if oid in _FLOAT_ARRAY_OIDS:
        return _array_rules(want, got, _float_rules)
    if oid == _NUMERIC_ARRAY_OID:
        return _array_rules(want, got, _numeric_rules)
    if oid in JSON_OIDS or _json_read(sql, oid, want):
        return json_rules(want, got, sql)
    if oid == _RECORD_OID:
        return _record_rules(want, got, sql)
    return None


def _model_answer(function: str, want: str | None, got: str | None) -> bool:
    """An answer about the database it ran on, which is XML on both sides."""
    if function not in MODEL_FUNCTIONS or not want or not got:
        return False
    return all(text.startswith("<") and text.endswith(">") for text in (want.strip(), got.strip()))


def _field_rules(want: list[int], got: list[int], enums: frozenset[int]) -> Rules:
    """The result types: the same, or an enum's on PostgreSQL, sent as text."""
    if len(want) != len(got):
        return None
    labels = enums | {_ANYENUM_OID}
    found: set[str] = set()
    for w, g in zip(want, got, strict=True):
        if w == g:
            continue
        if w not in labels or g not in _TEXT_OIDS:
            return None
        found.add(ENUM_LABEL)
    return frozenset(found)


def _pair_rows(
    want: list[tuple], got: list[tuple], rules_of: Callable[[tuple, tuple], Rules]
) -> tuple[set[str], list[tuple], list[tuple]]:
    """Each of ``want`` with the row of ``got`` it equals, an identical one before any other: the
    rules used, and the rows of each left over."""
    left = dict(enumerate(got))
    used: set[str] = set()
    missing: list[tuple] = []
    for at, row in enumerate(want):
        match, rules = next(((i, _SAME) for i, other in left.items() if other == row), (None, None))
        if match is None:
            pairs = ((i, rules_of(row, other)) for i, other in left.items())
            match, rules = next(((i, r) for i, r in pairs if r is not None), (None, None))
        if match is None or rules is None:
            missing.append(row)
            continue
        used |= rules
        if match != at:
            used.add(ROW_ORDER)
        del left[match]
    return used, missing, list(left.values())


@dataclass(frozen=True)
class Compared:
    """``difference``: how an answer differs, None if it does not. ``rules``: what it took."""

    difference: str | None = None
    rules: frozenset[str] = _SAME


def compare(
    want: dict[str, Any],
    got: dict[str, Any],
    *,
    values: bool,
    sql: str,
    function: str = "",
    enums: frozenset[int] = frozenset(),
) -> Compared:
    """Provisa's answer to ``sql``, a call of ``function``, against PostgreSQL's. ``values`` False
    compares the result types alone. ``enums``: the oids of PostgreSQL's enum types and arrays."""
    typed = _field_rules(want["fields"], got["fields"], enums)
    if typed is None:
        return Compared(f"result type oids {got['fields']}, PostgreSQL {want['fields']}")
    if not values:
        return Compared(rules=typed)
    fields = want["fields"]
    a, b = want["rows"], got["rows"]
    if len(a) != len(b):
        return Compared(f"{len(b)} rows, PostgreSQL {len(a)}")

    def value(wv: str | None, gv: str | None, oid: int) -> Rules:
        rules = _value_rules(wv, gv, oid, sql)
        if rules is None and _model_answer(function, wv, gv):
            return frozenset({MODEL_ANSWER})
        return rules

    def row_rules(x: tuple, y: tuple) -> Rules:
        found = [value(wv, gv, oid) for wv, gv, oid in zip(x, y, fields, strict=True)]
        return None if None in found else frozenset().union(*found)

    def order(row: tuple) -> list[tuple[bool, str]]:
        return [(value_ is None, value_ or "") for value_ in row]

    used, missing, left = _pair_rows(a, b, row_rules)
    if not missing:
        return Compared(rules=typed | used)
    missing, left = sorted(missing, key=order), sorted(left, key=order)
    wv, gv = next(
        (wv, gv)
        for wv, gv, oid in zip(missing[0], left[0], fields, strict=True)
        if value(wv, gv, oid) is None
    )
    return Compared(f"{gv!r}, PostgreSQL {wv!r}")


def difference(want: dict[str, Any], got: dict[str, Any], *, values: bool, sql: str) -> str | None:
    """How Provisa's answer to ``sql`` differs from PostgreSQL's; None if it does not."""
    return compare(want, got, values=values, sql=sql).difference


def _shape(sample: str) -> str:
    """The form of a string with none of its own: its words any word, the rest as written."""
    parts = re.split(r"(\w+)", sample)
    return "".join(r"\w+" if i % 2 else re.escape(part) for i, part in enumerate(parts))


def value_form(name: str, oid: int, sample: str) -> str:
    """The regular expression an answer of function ``name``, of type ``oid``, must match, from
    ``sample``, PostgreSQL's answer."""
    if name == "timeofday":
        return _TIMEOFDAY + re.escape(sample.rsplit(" ", 1)[1])
    if name in FUNCTION_FORMS:
        return FUNCTION_FORMS[name]
    if oid in TYPE_FORMS:
        return TYPE_FORMS[oid]
    if oid == _RECORD_OID:
        fields = len(re.findall(rf"{_FIELD}(?:,|\)$)", sample[1:]))
        return rf"\({_FIELD}(?:,{_FIELD}){{{fields - 1}}}\)"
    return _shape(sample)


def form_check(name: str, want: dict[str, Any], got: dict[str, Any]) -> tuple[bool, str | None]:
    """Whether a form was checked, and how Provisa's answer differs from PostgreSQL's in it. A set's
    rows are its server's own, so one Provisa leaves empty differs in nothing and checks nothing."""
    oid = want["fields"][0]
    sample = next((row[0] for row in want["rows"] if row[0] is not None), None)
    if sample is None or oid == _VOID_OID:
        return False, None
    values = [row[0] for row in got["rows"] if row[0] is not None]
    if not values:
        if oid == _RECORD_OID or len(want["rows"]) > 1:
            return False, None
        return True, f"None, PostgreSQL {sample!r}"
    form = re.compile(value_form(name, oid, sample), re.DOTALL)
    for value in values:
        if form.fullmatch(value) is None:
            return True, f"{value!r} is not the form of PostgreSQL's {sample!r}"
    return True, None


# --- the properties of a value that cannot be compared ---------------------------------------


def _literal(arg: str) -> str:
    """The value a cast literal of ``value_calls`` writes: ``'{1,2}'::integer[]`` is ``{1,2}``."""
    text = arg.rsplit("::", 1)[0].strip()
    if text.startswith("'") and text.endswith("'"):
        return text[1:-1].replace("''", "'")
    return text.strip("()")


def _elements(array: str) -> list[str] | None:
    """A one-dimensional array's elements as text; None if ``array`` is none."""
    if not (array.startswith("{") and array.endswith("}")):
        return None
    body = array[1:-1]
    return body.split(",") if body else []


def _single(got: dict[str, Any]) -> str | None:
    rows = got["rows"]
    return rows[0][0] if len(rows) == 1 else None


def _permutation(args: tuple[str, ...], _want: dict[str, Any], got: dict[str, Any]) -> str | None:
    """array_shuffle: the input's elements, each as often, in any order."""
    given, value = _elements(_literal(args[0])), _single(got)
    answered = None if value is None else _elements(value)
    if given is None or answered is None or sorted(given) != sorted(answered):
        return f"{value!r} is no permutation of {_literal(args[0])!r}"
    return None


def _sub_multiset(args: tuple[str, ...], _want: dict[str, Any], got: dict[str, Any]) -> str | None:
    """array_sample: as many of the input's elements as asked, none more often than it holds."""
    given, value = _elements(_literal(args[0])), _single(got)
    answered = None if value is None else _elements(value)
    size = int(_literal(args[1]))
    if (
        given is None
        or answered is None
        or len(answered) != size
        or Counter(answered) - Counter(given)
    ):
        return f"{value!r} is no sample of {size} of {_literal(args[0])!r}"
    return None


def _scale(number: str) -> int:
    return len(number.split(".", 1)[1]) if "." in number else 0


def _within(args: tuple[str, ...], _want: dict[str, Any], got: dict[str, Any]) -> str | None:
    """random(min, max): in [min, max]; a numeric one has the larger scale of the two."""
    low, high, value = _literal(args[0]), _literal(args[1]), _single(got)
    try:
        inside = value is not None and decimal.Decimal(low) <= decimal.Decimal(
            value
        ) <= decimal.Decimal(high)
    except decimal.InvalidOperation:
        inside = False
    if not inside:
        return f"{value!r} is not in [{low}, {high}]"
    scale = max(_scale(low), _scale(high))
    if args[0].endswith("::numeric") and value is not None and _scale(value) != scale:
        return f"{value!r} does not have the scale {scale} of its bounds"
    return None


def _mean_at_no_spread(
    args: tuple[str, ...], _want: dict[str, Any], got: dict[str, Any]
) -> str | None:
    """random_normal(mean, 0): the mean itself, within ``FLOAT_TOLERANCE``."""
    mean, value = _literal(args[0]), _single(got)
    if value is None or not (
        value == mean or _floats_equal(mean, value) or float(mean) == float(value)
    ):
        return f"{value!r} is not the mean {mean} of a distribution with no spread"
    return None


#: A volatile function's documented property, by ``_key``: each takes the call's literals,
#: PostgreSQL's answer and Provisa's, and says how Provisa's breaks it, None if it holds.
PROPERTIES: dict[str, Callable[[tuple[str, ...], dict[str, Any], dict[str, Any]], str | None]] = {
    "array_shuffle(anyarray)": _permutation,
    "array_sample(anyarray, integer)": _sub_multiset,
    "random(integer, integer)": _within,
    "random(bigint, bigint)": _within,
    "random(numeric, numeric)": _within,
    "random_normal(double precision, double precision)": _mean_at_no_spread,
}

#: The property of a clock function's answer: within the statement's bounds on PostgreSQL's clock.
CLOCK = "clock"
CLOCK_SQL = "SELECT pg_catalog.clock_timestamp()::text AS v"
#: The clock of the runner the two servers share is one, but each reads it at its own instant: an
#: answer may be this far outside the bounds PostgreSQL reads before and after it.
CLOCK_SLACK = datetime.timedelta(seconds=1)


def _instant(text: str | None) -> datetime.datetime | None:
    try:
        return None if text is None else datetime.datetime.fromisoformat(text)
    except ValueError:
        return None


def clock_check(before: dict[str, Any], got: dict[str, Any], after: dict[str, Any]) -> str | None:
    """How Provisa's answer of a clock function falls outside the bounds PostgreSQL's clock read
    before and after the call; None if it is within them."""
    low, high = (_instant(_single(b)) if b["ok"] else None for b in (before, after))
    if low is None or high is None:
        return f"PostgreSQL's clock gave no bounds: {before}, {after}"
    value = _single(got)
    instant = _instant(value)
    if value is None or instant is None or re.fullmatch(TYPE_FORMS[1184], value) is None:
        return f"{value!r} is no timestamp with time zone in PostgreSQL's text form"
    if not low - CLOCK_SLACK <= instant <= high + CLOCK_SLACK:
        return f"{value!r} is not between {_single(before)!r} and {_single(after)!r}"
    return None


# --- one overload on one engine ---------------------------------------------------------------


def _answer(server: Session, call_: Call) -> dict[str, Any]:
    if call_.zone is None:
        return server.answer(call_.sql)
    server.answer(f"SET TimeZone TO '{call_.zone}'")
    answer = server.answer(call_.sql)
    server.answer(SETTINGS[0])
    return answer


def measure_column(
    fn: Function, family: str, pg: Session, provisa: Session, enums: frozenset[int] = frozenset()
) -> dict[str, Any] | None:
    """The first of ``column_calls`` PostgreSQL answers, on both servers; None if it answers none.
    ``outcome`` is function_support's own, of this call alone; a column's rules are its own too."""
    for call_ in column_calls(fn, family):
        want = _answer(pg, call_)
        if not want["ok"] or not want["fields"]:
            continue
        got = _answer(provisa, call_)
        if not got["ok"]:
            named = function_probe.refused_by_name(got["error"])
            return {
                "outcome": "refused" if named else "error",
                "detail": {"statement": call_.sql, "error": got["error"]},
            }
        compared = compare(want, got, values=True, sql=call_.sql, function=fn.name, enums=enums)
        if compared.difference is not None:
            detail = {"statement": call_.sql, "difference": compared.difference}
            return {"outcome": "wrong_answer", "detail": detail}
        return {"outcome": "pass", "rules": sorted(compared.rules)}
    return None


def _all_null(want: dict[str, Any]) -> bool:
    return bool(want["rows"]) and all(v is None for row in want["rows"] for v in row)


def measure_overload(
    fn: Function,
    family: str,
    pg: Session,
    provisa: Session,
    classify: Any,
    enums: frozenset[int] = frozenset(),
) -> dict[str, Any]:
    """Every call of ``fn`` on both servers, as this engine's one outcome of it."""
    planned = calls(fn, family)
    entry: dict[str, Any] = {
        "function": fn.name,
        "overload": fn.overload,
        "family": family,
        "kind": fn.kind,
        "volatility": fn.volatility,
    }
    accepted = answered = checked = 0
    wrong: list[dict[str, str]] = []
    errors: list[dict[str, str]] = []
    named: list[dict[str, str]] = []
    refused: list[str] = []
    routes: set[str] = set()
    rules: set[str] = set()
    lost = False
    for call_ in planned:
        want = _answer(pg, call_)
        if not want["ok"] or not want["fields"]:
            refused.append(want.get("error") or "no result column")
            continue
        accepted += 1
        answered += call_.answers
        routes.add("catalog" if classify(call_.sql) == "INTERCEPT" else "engine")
        before = pg.answer(CLOCK_SQL) if call_.prop == CLOCK else None
        got = _answer(provisa, call_)
        after = pg.answer(CLOCK_SQL) if call_.prop == CLOCK else None
        if not got["ok"]:
            said = {"statement": call_.sql, "error": got["error"]}
            (named if function_probe.refused_by_name(got["error"]) else errors).append(said)
            lost = lost or got.get("lost", False)
            continue
        compared = compare(
            want, got, values=call_.values, sql=call_.sql, function=fn.name, enums=enums
        )
        differs = compared.difference
        rules |= compared.rules
        if differs is None and not call_.values and _all_null(want):
            # PostgreSQL's NULL holds Provisa to NULL: a wrong answer if not, never a pass.
            differs = compare(want, got, values=True, sql=call_.sql).difference
        if differs is None and call_.form:
            formed, differs = form_check(fn.name, want, got)
            checked += formed
        if differs is None and call_.prop is not None:
            checked += 1
            if call_.prop == CLOCK:
                differs = clock_check(before or {}, got, after or {})
            else:
                differs = PROPERTIES[call_.prop](call_.args, want, got)
        if differs is not None:
            wrong.append({"statement": call_.sql, "difference": differs})
    entry["calls"] = accepted
    entry["answers_compared"] = answered
    entry["answers_checked"] = answered > 0
    entry["property_checked"] = checked > 0 and not answered
    entry["served_by"] = "+".join(sorted(routes)) or None
    if not answered:
        why = unchecked(fn, family)
        entry["unchecked"] = why or "PostgreSQL refused every value call"
    if lost:
        entry["connection_lost"] = True
    if accepted == 0:
        entry["outcome"] = "not_callable"
        entry["detail"] = (
            f"PostgreSQL refuses the call: {refused[0]}" if refused else function_probe.NO_CALL
        )
    elif wrong:
        entry["outcome"] = "wrong_answer"
        entry["detail"] = wrong[0]
        entry["wrong_calls"] = len(wrong)
    elif errors:
        entry["outcome"] = "error"
        entry["detail"] = errors[0]
        entry["error_calls"] = len(errors)
    elif named:
        entry["outcome"] = "refused"
        entry["detail"] = named[0]
        entry["refused_calls"] = len(named)
    elif answered or checked:
        entry["outcome"] = "pass"
        entry["rules"] = sorted(rules)
    else:
        entry["outcome"] = "not_checked"
        entry["rules"] = sorted(rules)
    column = measure_column(fn, family, pg, provisa, enums)
    if column is not None:
        entry["column"] = column
    return entry


def seed_columns(pg_dsn: str, out: Path) -> None:
    """``COLUMN_TABLE`` in the job's PostgreSQL, and ``out``, its model table entry (the instance's
    ``extra-tables``). It comes before the instance starts, as any job's own fixture does."""
    types = list(SAMPLES)
    columns = [(sample_column(t), t) for t in types]
    declared = ", ".join(["id integer", *(f"{name} {typ}" for name, typ in columns)])
    with psycopg.connect(pg_dsn, autocommit=True) as conn:
        conn.execute(f"CREATE SCHEMA IF NOT EXISTS {catalog.FIXTURE_SCHEMA}")
        conn.execute(f"DROP TABLE IF EXISTS {COLUMN_RELATION}")
        conn.execute(f"CREATE TABLE {COLUMN_RELATION} ({declared})")
        for row in range(VALUE_ROWS):
            values = [str(row + 1), *(_cast(SAMPLES[t][row], t) for t in types)]
            conn.execute(f"INSERT INTO {COLUMN_RELATION} VALUES ({', '.join(values)})")
    registered = catalog.registered_columns(
        [("id", "integer"), *columns],
        lambda column: catalog._sized_numeric(pg_dsn, column, COLUMN_TABLE),
    )
    entry = {
        "source_id": "conformance-pg",
        "schema": catalog.FIXTURE_SCHEMA,
        "table": COLUMN_TABLE,
        "alias": COLUMN_TABLE,
        "domain_id": catalog.FIXTURE_SCHEMA,
        "columns": [
            {"name": name, **typing, "visible_to": ["org_admin"]}
            for name, _typ, typing in registered
        ],
    }
    out.write_text(yaml.safe_dump([entry]))
    print(f"seeded {COLUMN_RELATION}: {len(columns)} columns, {VALUE_ROWS} rows")


def require_column_table(provisa: Session) -> None:
    """Stops a leg whose model lacks ``COLUMN_TABLE``: every column call would be refused, and
    every overload would read as passing on literals only."""
    found = provisa.answer(f"SELECT id FROM {COLUMN_RELATION}")
    if not found["ok"] or len(found["rows"]) != VALUE_ROWS:
        raise SystemExit(f"{COLUMN_RELATION} is not served: {found}")


def _classifier() -> Any:
    from provisa.pgwire.catalog import classify

    return classify


def measure(
    engine: str, pg_dsn: str, provisa_dsn: str, out_json: Path, out_md: Path, limit: int | None
) -> int:
    """One leg. Written whatever the leg reaches: what it did not reach is ``not_reached``."""
    pg = Session(pg_dsn)
    fns = catalog.pg_functions(pg)
    enums = frozenset(
        oid
        for row in pg.rows("SELECT oid, typarray FROM pg_catalog.pg_type WHERE typtype = 'e'")
        for oid in map(int, row)
    )
    if limit is not None:
        fns = fns[:: max(1, len(fns) // limit)]
    declared = function_probe.families()
    classify = _classifier()
    provisa = Session(provisa_dsn)
    require_column_table(provisa)
    started = time.monotonic()
    overloads: list[dict[str, Any]] = []
    server_gone: str | None = None
    for i, fn in enumerate(fns):
        family = function_probe.family(fn, declared)
        if server_gone is None and time.monotonic() - started > LEG_BUDGET_S:
            server_gone = f"the leg ran out of its {LEG_BUDGET_S:.0f}s at {fn.signature}"
            print(f"::error::{server_gone}")
        if server_gone is None:
            try:
                overloads.append(measure_overload(fn, family, pg, provisa, classify, enums))
            except psycopg.OperationalError as exc:
                server_gone = f"Provisa stopped answering at {fn.signature}: {exc}".strip()
                print(f"::error::{server_gone}")
        if server_gone is not None and len(overloads) == i:
            overloads.append(
                {
                    "function": fn.name,
                    "overload": fn.overload,
                    "family": family,
                    "kind": fn.kind,
                    "volatility": fn.volatility,
                    "outcome": "not_reached",
                    "detail": server_gone,
                }
            )
        if i % 200 == 0:
            print(f"{i}/{len(fns)} after {time.monotonic() - started:.0f}s", flush=True)
    report = {
        "engine": engine,
        "postgres_version": pg.rows("SHOW server_version")[0][0],
        "seconds": round(time.monotonic() - started, 1),
        "server_gone": server_gone,
        "counts": counts(overloads),
        "overloads": overloads,
    }
    out_json.write_text(json.dumps(report, indent=1) + "\n")
    out_md.write_text(engine_markdown(report) + "\n")
    print(engine_markdown(report))
    return 1 if server_gone else 0


# --- counts and the merged data file ----------------------------------------------------------

OUTCOMES = (
    "pass",
    "not_checked",
    "wrong_answer",
    "error",
    "refused",
    "not_callable",
    "not_reached",
)


def counts(overloads: list[dict[str, Any]]) -> dict[str, int]:
    """The outcomes, with a pass split by what was checked: an answer or a property."""
    tally = Counter(o["outcome"] for o in overloads)
    out = {name: tally.get(name, 0) for name in OUTCOMES}
    out["total"] = len(overloads)
    out["pass_answers_checked"] = sum(
        1 for o in overloads if o["outcome"] == "pass" and o.get("answers_checked")
    )
    out["pass_property_checked"] = out["pass"] - out["pass_answers_checked"]
    # Passed without reaching the engine: the catalog DB answers it on every engine alike.
    out["pass_catalog_answered"] = sum(
        1 for o in overloads if o["outcome"] == "pass" and o.get("served_by") == "catalog"
    )
    return out


def not_checked_reasons(overloads: Iterable[dict[str, Any]]) -> dict[str, int]:
    """Why the overloads not checked were not, most first."""
    tally = Counter(o.get("unchecked", "") for o in overloads if o["outcome"] == "not_checked")
    return dict(sorted(tally.items(), key=lambda item: (-item[1], item[0])))


def _row(label: str, c: dict[str, int]) -> str:
    return (
        f"| {label} | {c['total']} | {c['pass_answers_checked']} | {c['pass_property_checked']} "
        f"| {c['not_checked']} | {c['refused']} "
        f"| {c['wrong_answer']} | {c['error']} | {c['not_callable']} | {c['not_reached']} |"
    )


_HEADER = (
    "| | overloads | pass, answers checked | pass, property checked | not checked "
    "| refused by name | wrong answer | engine error | not callable | not reached |",
    "|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|",
)


def _reason_lines(reasons: dict[str, int]) -> list[str]:
    """The not-checked reasons as a table: none of these is a pass."""
    if not reasons:
        return ["Not checked: none.", ""]
    return [
        "| not checked because | overloads |",
        "|---|---:|",
        *(f"| {why.replace('|', chr(92) + '|')} | {n} |" for why, n in reasons.items()),
        "",
    ]


def _families(overloads: list[dict[str, Any]]) -> Iterator[tuple[str, list[dict[str, Any]]]]:
    by: dict[str, list[dict[str, Any]]] = {}
    for o in overloads:
        by.setdefault(o["family"], []).append(o)
    yield from sorted(by.items())


def engine_markdown(report: dict[str, Any]) -> str:
    """One leg's summary: the engine's counts, then each family's."""
    lines = [
        f"## pg_catalog functions on {report['engine']} (PostgreSQL {report['postgres_version']})",
        "",
        *_HEADER,
        _row(report["engine"], report["counts"]),
        *(_row(family, counts(group)) for family, group in _families(report["overloads"])),
        "",
        *_reason_lines(not_checked_reasons(report["overloads"])),
    ]
    columns = function_report.column_results(
        [{**o, "outcomes": {report["engine"]: o}} for o in report["overloads"]], report["engine"]
    )
    lines += [
        f"Passes on literals and columns: {columns['pass_literal_and_column']}. "
        f"On literals only: {columns['pass_literal_only']}.",
        "",
    ]
    if report["server_gone"]:
        lines += [f"**{report['server_gone']}**", ""]
    return "\n".join(lines)


def _outcome(entry: dict[str, Any] | None) -> dict[str, Any]:
    """An overload's outcome on one engine, as the data file keeps it."""
    if entry is None:
        return {
            "outcome": "not_reached",
            "result": function_report.NOT_REACHED,
            "detail": "the engine's leg left no result",
        }
    kept: dict[str, Any] = {
        "outcome": entry["outcome"],
        "result": function_report.result_of(entry["outcome"], entry.get("rules", ())),
    }
    for key in ("detail", "served_by", "column"):
        if entry.get(key) is not None:
            kept[key] = entry[key]
    if entry.get("rules"):
        kept["rules"] = entry["rules"]
    if entry["outcome"] == "pass":
        kept["check"] = "answers" if entry.get("answers_checked") else "property"
    if entry["outcome"] == "not_checked":
        kept["unchecked"] = entry.get("unchecked", "")
    return kept


def merged(reports: dict[str, dict[str, Any] | None]) -> dict[str, Any]:
    """The legs as one document. ``reports`` holds every engine the workflow runs, None for a leg
    that left no result: it counts as not reached, so nothing is guaranteed by its absence.

    An overload passing on every engine is on the guaranteed list, by what every engine checked of
    it (``proof``): ``answers_checked``, or ``property_checked`` where a property stood in on any of
    them (REQ-2167, WHAT GUARANTEED MEANS). One not checked on any engine is not on it: a pass is
    never a call alone. A function is guaranteed when each of its overloads PostgreSQL can call
    is."""
    by_engine = {
        engine: {
            (o["function"], o["overload"]): o for o in (report or {"overloads": []})["overloads"]
        }
        for engine, report in reports.items()
    }
    keys = sorted({key for overloads in by_engine.values() for key in overloads})
    functions: list[dict[str, Any]] = []
    for name, overload in keys:
        entries = {
            engine: overloads.get((name, overload)) for engine, overloads in by_engine.items()
        }
        any_entry = next(e for e in entries.values() if e is not None)
        functions.append(
            {
                "function": name,
                "overload": overload,
                "family": any_entry["family"],
                "kind": any_entry["kind"],
                "volatility": any_entry["volatility"],
                "answers_checked": any(
                    e is not None and e.get("answers_checked") for e in entries.values()
                ),
                "property_checked": any(
                    e is not None and e.get("property_checked") for e in entries.values()
                ),
                "proof": _proof(entries),
                "unchecked": next(
                    (e["unchecked"] for e in entries.values() if e and "unchecked" in e), None
                ),
                "outcomes": {engine: _outcome(entry) for engine, entry in entries.items()},
            }
        )
    guaranteed: list[dict[str, Any]] = [
        f for f in functions if all(o["outcome"] == "pass" for o in f["outcomes"].values())
    ]
    held = {(f["function"], f["overload"]) for f in guaranteed}
    callable_by_name: dict[str, list[dict[str, Any]]] = {}
    for f in functions:
        if not all(o["outcome"] == "not_callable" for o in f["outcomes"].values()):
            callable_by_name.setdefault(f["function"], []).append(f)
    missing = sorted(engine for engine, report in reports.items() if report is None)
    incomplete = sorted(
        engine for engine, report in reports.items() if report is None or report.get("server_gone")
    )
    per_engine = {
        engine: counts([by_engine[engine].get(key) or _absent() for key in keys])
        for engine in reports
    }
    engines = sorted(reports)
    return {
        "postgres_version": next((r["postgres_version"] for r in reports.values() if r), None),
        "engines": engines,
        "engines_without_a_result": missing,
        "engines_incomplete": incomplete,
        "comparison": (
            "text format under one TimeZone, DateStyle and IntervalStyle; exact where the text is "
            "the same, equivalent under the numbered rules"
        ),
        # function_report.RESULT_KEYS per engine: what a floor reads (REQ-2171, RESULTS ARE BY
        # ENGINE). No key adds the engines up.
        "by_engine": {
            engine: {
                **function_report.result_counts(f["outcomes"][engine] for f in functions),
                **function_report.column_results(functions, engine),
            }
            for engine in engines
        },
        "by_family": function_report.by_family(functions, engines),
        "no_engine": function_report.no_engine(functions),
        "rules": function_report.rule_coverage(functions, engines),
        "counts": per_engine,
        "not_checked_reasons": {
            engine: not_checked_reasons((report or {"overloads": []})["overloads"])
            for engine, report in sorted(reports.items())
        },
        "guaranteed": {
            **{
                proof: sorted(
                    f"{f['function']}({f['overload']})" for f in guaranteed if f["proof"] == proof
                )
                for proof in ("answers_checked", "property_checked")
            },
            "functions": sorted(
                name
                for name, overloads in callable_by_name.items()
                if all((f["function"], f["overload"]) in held for f in overloads)
            ),
        },
        "functions": functions,
    }


def _proof(entries: dict[str, dict[str, Any] | None]) -> str:
    """What every engine checked of an overload."""
    present = [e or {} for e in entries.values()]
    if all(e.get("answers_checked") for e in present):
        return "answers_checked"
    return "property_checked"


def _absent() -> dict[str, Any]:
    return {"outcome": "not_reached", "family": ""}


def merged_markdown(doc: dict[str, Any]) -> str:
    """The run's summary: per engine, per family, and the guaranteed list's size."""
    engines = doc["engines"]
    guaranteed = doc["guaranteed"]
    lines = [
        f"## pg_catalog functions per engine (PostgreSQL {doc['postgres_version']})",
        "",
        function_report.summary_block(doc),
        "",
        "### What each engine's pass checked",
        "",
        *_HEADER,
        *(_row(engine, doc["counts"][engine]) for engine in engines),
        "",
        f"**Guaranteed list:** {len(guaranteed['answers_checked'])} overloads with answers "
        f"compared and {len(guaranteed['property_checked'])} property checked, passing on all "
        f"{len(engines)} engines. {len(guaranteed['functions'])} functions have every callable "
        "overload on the list.",
        "",
        "Comparison: " + doc["comparison"] + ". A pass, exact or equivalent, is answers checked "
        "when a call with real values was compared, and property checked when a value that cannot "
        "be compared has the type and the form of PostgreSQL's, or a property PostgreSQL "
        "documents. Any other overload PostgreSQL answers is not checked, and no pass: a call "
        "that ran proves nothing.",
        "",
    ]
    for engine in engines:
        lines += [
            f"#### Not checked on {engine}",
            "",
            *_reason_lines(doc["not_checked_reasons"][engine]),
        ]
    answered = {engine: doc["counts"][engine]["pass_catalog_answered"] for engine in engines}
    lines += [
        "Passes the catalog DB answers, without reaching the engine: "
        + ", ".join(f"{engine} {n}" for engine, n in answered.items())
        + ".",
        "",
    ]
    if doc["engines_incomplete"]:
        lines += [f"**Incomplete legs:** {', '.join(doc['engines_incomplete'])}.", ""]
    lines += [
        "### Per family: pass (answers checked + property checked), then not checked, of callable "
        "overloads",
        "",
        "| family | overloads | " + " | ".join(engines) + " | on all |",
        "|---|---:|" + "---:|" * (len(engines) + 1),
    ]
    by_family: dict[str, list[dict[str, Any]]] = {}
    for f in doc["functions"]:
        by_family.setdefault(f["family"], []).append(f)
    for family, members in sorted(by_family.items()):
        cells = [_family_cell([f["outcomes"][engine] for f in members]) for engine in engines]
        on_all = sum(
            1 for f in members if all(o["outcome"] == "pass" for o in f["outcomes"].values())
        )
        lines.append(f"| {family} | {len(members)} | {' | '.join(cells)} | {on_all} |")
    lines.append("")
    return "\n".join(lines)


def _family_cell(outcomes: list[dict[str, Any]]) -> str:
    """One engine's family: answers checked + property checked, then not checked, of callable."""
    passes = [o for o in outcomes if o["outcome"] == "pass"]
    checked = sum(1 for o in passes if o.get("check") == "answers")
    unchecked_ = sum(1 for o in outcomes if o["outcome"] == "not_checked")
    callable_ = sum(1 for o in outcomes if o["outcome"] != "not_callable")
    return f"{checked} + {len(passes) - checked}, {unchecked_} not checked, of {callable_}"


def merge(engines: list[str], indir: Path, out: Path, out_md: Path) -> int:
    """``function-support-<engine>.json`` of each engine, anywhere under ``indir``, as one file.
    Fails when a leg left no result or stopped short: the list is then not a guarantee."""
    reports: dict[str, dict[str, Any] | None] = {}
    for engine in engines:
        found = sorted(indir.rglob(f"function-support-{engine}.json"))
        if len(found) > 1:
            raise SystemExit(f"{engine}: more than one result under {indir}: {found}")
        reports[engine] = json.loads(found[0].read_text()) if found else None
    doc = merged(reports)
    out.write_text(json.dumps(doc, indent=1) + "\n")
    text = merged_markdown(doc)
    out_md.write_text(text + "\n")
    print(text)
    return 1 if doc["engines_incomplete"] else 0


def main(argv: list[str]) -> int:
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    sub = parser.add_subparsers(dest="command", required=True)
    m = sub.add_parser("measure")
    m.add_argument("--engine", required=True)
    m.add_argument("--pg", required=True)
    m.add_argument("--provisa", required=True)
    m.add_argument("--out-json", required=True, type=Path)
    m.add_argument("--out-md", required=True, type=Path)
    m.add_argument("--limit", type=int, help="an even sample of about this many overloads")
    s = sub.add_parser("seed-columns")
    s.add_argument("--pg", required=True)
    s.add_argument("--out", required=True, type=Path)
    g = sub.add_parser("merge")
    g.add_argument("--engines", required=True, help="every engine of the run, comma separated")
    g.add_argument("--in", dest="indir", required=True, type=Path)
    g.add_argument("--out", required=True, type=Path)
    g.add_argument("--out-md", required=True, type=Path)
    args = parser.parse_args(argv[1:])
    if args.command == "measure":
        return measure(args.engine, args.pg, args.provisa, args.out_json, args.out_md, args.limit)
    if args.command == "seed-columns":
        seed_columns(args.pg, args.out)
        return 0
    return merge(args.engines.split(","), args.indir, args.out, args.out_md)


if __name__ == "__main__":
    sys.exit(main(sys.argv))
