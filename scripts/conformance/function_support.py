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
                something was compared: an answer, a type and form, or a side effect
  not_checked   every call PostgreSQL answered, Provisa answered with the same result type, and
                nothing was compared: a call that ran proves nothing, so this is no pass, and it
                counts as failed. ``unchecked`` says why
  by_design     skipped by design under a decision of REQ-2167 (``GROUPS``): a side-effect
                function on an engine but ``SIDE_EFFECT_ENGINE``, one whose side effect no client
                can see (``NOT_TESTABLE``), a refusal by name a decision makes
                (``DECIDED_REFUSALS``), or a function a decision skips whatever it answers
                (``decided_function``: PostgreSQL's own server state, the types no SQL value can
                have, and the others amended 2026-11-03 and 2026-11-04 name)
  wrong_answer  a call compares unequal: its result type, its rows, their values, their form,
                or the side effect it had
  error         PostgreSQL answered a call and the engine failed with no named refusal (its
                message is kept)
  refused       PostgreSQL answered a call and Provisa refused it by name
  not_callable  PostgreSQL itself answers no call of it: every call was refused, or none answered
                anything but NULL from NULL arguments and every call with a real value was refused
                for the call itself. ``refused_for`` says which (REQ-2167, amended 2026-11-04, A
                call PostgreSQL cannot make is skipped by design): ``function_probe.THE_CALL``
                when every refusal was of the call, read from its SQLSTATE
                (``function_probe.refused_for``); ``function_probe.ITS_ARGUMENTS`` when a refusal
                was of the values or types the check gave it, a gap in its samples, which is
                ``not_checked`` instead
  not_reached   the leg ended, or its server went away, before this overload

A wrong answer outranks an error, which outranks a refusal, which outranks a pass or a not checked:
the overload is as bad as its worst call. function_report.py says what each outcome is on the
report (exact, equivalent, not checked, skipped by design, refused by name or differs); there is no
overall count, only one per engine.

The calls of an overload are the NULL call of pgwire_function_probe.py and, where ``value_calls``
can write them, up to three calls with real values. How far values go, exactly:

  * Every immutable and stable function is given values, compared under one TimeZone, DateStyle
    and IntervalStyle on the same fixture catalog, but for those whose answer is the answering
    server's own (``OWN_ANSWERS``: its process, its addresses, its clock, its statistics, its
    transactions, or a by-design session answer, REQ-588 and REQ-579). Those get real values for
    their type and form alone (``form_calls``): the session's own database, backend and process,
    an object of the catalog or of the fixture, a transaction and a multixact of the fixture
    (``FIXTURE``, resolved from PostgreSQL when a leg starts). A volatile function is given values
    only where it acts on nothing (``_FORM_VALUES``): it reads an object, a file or a directory of
    the data directory, or its answer differs by nature. Giving one that acts values would run it
    for real.
  * Every argument must have a literal: ``SAMPLES`` and ``VALUE_SAMPLES`` by type, ``ARG_VALUES``
    by function where the type's literals are not valid input (``Raw`` ones are written as they
    stand), and ``INPUT_TYPES`` for a type's input function. Pseudo-types take the concrete types
    of pgwire_function_probe.py. An overload taking a type with no sample gets no value call;
    ``NO_LITERAL`` types are ones PostgreSQL takes no literal of, and ``DERIVED_SAMPLES`` reads the
    values of those its catalog holds (an expression tree, the statistics ANALYZE built). A record
    of no declared type is called with a column definition list (``VALUE_STATEMENTS``), and a
    cursor is opened in a transaction around its call (``Call.setup``).
  * ``seed_fixture`` makes what those values read (``FIXTURE_SQL``), beside the column table.
  * A value call PostgreSQL refuses compares nothing: that it also refuses on Provisa is not
    checked, nor is Provisa answering what PostgreSQL refuses.

What a pass compared, exactly:

  * ``answers_checked``: at least one value call PostgreSQL answered, its value compared.
  * ``type_and_form_checked``: a value that cannot be compared (volatile, or the server's own),
    from a call with no NULL argument, has PostgreSQL's result type and the form of PostgreSQL's own
    answer (REQ-2167, WHAT GUARANTEED MEANS). ``value_form`` gives that form: a named function's,
    then its type's, then a record's number of fields, and otherwise the shape of PostgreSQL's
    string. Each row Provisa answers must fit. PostgreSQL's NULL or void, or a set Provisa leaves
    empty, is no form to check. A volatile overload is never left not tested for being volatile
    (amended 2026-11-04 (4)): any call of it, a NULL call too, whose result type and, in a single
    row, NULLs are PostgreSQL's has its type and form checked; one acting is tested by its side
    effect.
  * ``side_effect_checked``: a function returning void or acting (``side_effect``) is tested by
    its side effect (REQ-2167, amended 2026-11-02, Side-effect functions): ``EFFECTS`` performs the
    call on each server and then observes what it did through the same client (a setting read
    back, the session's advisory locks, a large object, a listener's notification, the time a sleep
    took, the random sequence a seed fixes), and Provisa's must be PostgreSQL's.

The rest are not checked, and each says why (``unchecked``, which an overload whose type and form
were compared keeps: its value is still not compared). One given real values for its form that
still has none checked says too whether PostgreSQL refused them or answered NULL or no row. A NULL call where PostgreSQL answers NULL
holds Provisa to NULL too, which can make a wrong answer and never a pass. Three literals, not a
domain: a function that differs only on values no sample reaches passes here.

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
import decimal
import functools
import html
import importlib.resources
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
from typing import Any, LiteralString

import psycopg
import yaml
from psycopg import pq
from psycopg import sql as psql

import function_report
import pgwire_catalog_types as catalog
import pgwire_function_probe as function_probe
from function_report import (
    ARRAY_ORDER,
    CLOCK_FIELD,
    ENUM_LABEL,
    FLOAT_CLOSE,
    JSON_KEY_ORDER,
    JSON_SPACING,
    MODEL_ANSWER,
    NUMBER_VALUE,
    ROW_ORDER,
    ZONE_PACKAGING,
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

#: How many overloads apart the leg says where it is: a leg the runner gives up on mid-step leaves
#: its log alone, which then names the overload it last reached.
PROGRESS_EVERY = 50

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

#: Answers from the clock: their type and form are compared, never their value.
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
    # REQ-2167, amended 2026-11-04, Server state the client tools read is answered as
    # Provisa's own: compared by type and form.
    **dict.fromkeys(
        ("pg_stat_get_numscans", "pg_tablespace_location", "pg_relation_is_publishable"),
        "by design (REQ-2167): answers with Provisa's own server state",
    ),
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
_NUMERIC_OID = 1700
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

#: The result type a function's form is of, where another overload answers another type: random()
#: is a fraction in [0, 1), random(min, max) an integer or a numeric between its bounds.
_FUNCTION_FORM_OIDS = {"random": 701}

#: timeofday's text, but for its last word: the zone's abbreviation, which is PostgreSQL's to say.
_TIMEOFDAY = r"[A-Z][a-z]{2} [A-Z][a-z]{2} \d{2} \d{2}:\d{2}:\d{2}\.\d{6} \d{4} "

#: One field of a record's text: record_out quotes anything but a plain word, doubling the quotes.
_FIELD = r'(?:"(?:[^"]|"")*"|[^,"()]*)'

_INTEGERS: tuple[LiteralString, LiteralString, LiteralString] = ("3", "(-7)", "42")
_DECIMALS: tuple[LiteralString, LiteralString, LiteralString] = ("0.5", "(-2.25)", "100")
_STRINGS: tuple[LiteralString, LiteralString, LiteralString] = ("'abc'", "'Hello World'", "'12'")

#: Three literals of each type format_type names, cast to it where they are used. Enough to tell
#: a sign, a case and a digit string apart; the first of the floats is a valid fraction, which an
#: ordered-set aggregate's direct argument needs.
_SAMPLE_ROWS: dict[LiteralString, tuple[LiteralString, LiteralString, LiteralString]] = {
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

SAMPLES: dict[str, tuple[str, str, str]] = dict(_SAMPLE_ROWS.items())

_RANGES = '\'{"[1,5)","[7,9)"}\'', "'{}'", "'{\"(,3]\"}'"

#: Literals of the types no registered column holds, each valid input on PostgreSQL itself: a
#: catalog name names an object every PostgreSQL 17 catalog has, a role a predefined one, a
#: relation the fixture's own. They are cast as ``SAMPLES`` is, and read from no column.
VALUE_SAMPLES: dict[str, tuple[str, str, str]] = {
    "cstring": _STRINGS,
    "unknown": _STRINGS,
    "void": ("''", "''", "''"),
    # Typed fields: a field of type unknown has no comparison, hash or image of its own.
    "record": ("ROW(1, 'a'::text)", "ROW(2, 'b'::text)", "ROW(NULL::integer, 'c'::text)"),
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


def _catalog_read(column: str, relation: str, where: str) -> str:
    return f"(SELECT {column} FROM pg_catalog.{relation} WHERE {where})"


def _statistics(kind: str) -> tuple[str, str, str]:
    """A statistics object's ``kind`` as ANALYZE built it, from each of ``STATISTICS``."""
    first, second, third = (
        _catalog_read(
            f"d.stxd{kind}",
            "pg_statistic_ext_data AS d JOIN pg_catalog.pg_statistic_ext AS s ON s.oid = d.stxoid",
            f"s.stxname = '{name}'",
        )
        for name in STATISTICS
    )
    return first, second, third


#: The fixture ``seed_fixture`` makes: a table with a column default and statistics objects,
#: and a hash-partitioned table, each named in pg_class alone so a server's own catalog finds it.
STATISTICS_TABLE = "stats_fixture"
STATISTICS = {"stats_fixture_ab": "a, b", "stats_fixture_ac": "a, c", "stats_fixture_bc": "b, c"}
PARTITIONED_TABLE = "hash_fixture"
#: A foreign-data wrapper and a server of it, which PostgreSQL has none of until one is made.
FOREIGN_DATA_WRAPPER = "conformance_fdw"
FOREIGN_SERVER = "conformance_server"
#: A large object, a replication slot and a table row two transactions share-locked, so its xmax
#: is a multixact.
FIXTURE_LARGE_OBJECT = 424243
REPLICATION_SLOT = "conformance_slot"
MULTIXACT_TABLE = "multixact_fixture"
#: A sequence that has answered once: the probe's own is made again after the fixture.
FIXTURE_SEQUENCE = "fixture_sequence"
FIXTURE_DOMAIN = "fixture_domain"
_RELATION_OID = "(SELECT oid FROM pg_catalog.pg_class WHERE relname = '{}')"

#: Values of the ``NO_LITERAL`` types the server itself derived, read from its catalog: an
#: expression tree (a column default, a partition bound, a view's rule), and the statistics
#: ANALYZE built for ``STATISTICS``. They are cast as ``SAMPLES`` is.
DERIVED_SAMPLES: dict[str, tuple[str, str, str]] = {
    "pg_node_tree": (
        _catalog_read(
            "d.adbin",
            "pg_attrdef AS d JOIN pg_catalog.pg_class AS c ON c.oid = d.adrelid",
            f"c.relname = '{STATISTICS_TABLE}'",
        ),
        _catalog_read("relpartbound", "pg_class", f"relname = '{PARTITIONED_TABLE}_0'"),
        _catalog_read(
            "r.ev_action",
            "pg_rewrite AS r JOIN pg_catalog.pg_class AS c ON c.oid = r.ev_class",
            "c.relname = 'pg_tables' AND c.relnamespace = 'pg_catalog'::regnamespace",
        ),
    ),
    "pg_ndistinct": _statistics("ndistinct"),
    "pg_dependencies": _statistics("dependencies"),
    "pg_mcv_list": _statistics("mcv"),
}


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
    "interval_in": "interval",
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
    return (
        SAMPLES.get(sample_type)
        or VALUE_SAMPLES.get(sample_type)
        or DERIVED_SAMPLES.get(sample_type)
    )


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
_FRACTIONS = ("'{0.25,0.5}'", "'{0.5}'", "'{0,0.75,1}'")
#: A type's modifier as the parser hands it to the type's typmodin, by function: interval's names
#: its fields and precision (INTERVAL(3)), as PostgreSQL refuses a bare number for it.
_TYPMODS: dict[str, dict[int, tuple[str, ...]]] = {
    **{
        f"{name}typmodin": {0: ("'{10}'",) * VALUE_ROWS}
        for name in (
            "bit",
            "bpchar",
            "timestamp",
            "timestamptz",
            "time",
            "timetz",
            "varbit",
            "varchar",
        )
    },
    "numerictypmodin": {0: ("'{12,2}'",) * VALUE_ROWS},
    "intervaltypmodin": {0: ("'{32767,3}'",) * VALUE_ROWS},
}
_NAMESPACE_ROW = Raw(("NULL::pg_catalog.pg_namespace",) * 3)
_NAMESPACE_JSON = ('\'{"nspname": "x", "oid": 5}\'', "'{}'", "'{\"nspname\": null}'")
_NAMESPACE_JSONS = ('\'[{"nspname": "x", "oid": 5}]\'', "'[]'", "'[{}, {\"oid\": 1}]'")
_PARSER = {0: ("'default'",) * 3}
_PARSER_OID = {0: ("3722",) * 3}
#: A timestamp's year, month, day, hour, minute and seconds.
_TIMESTAMP_PARTS: dict[int, tuple[str, ...]] = {
    0: ("2024", "1999", "2000"),
    1: ("6", "12", "2"),
    2: ("15", "31", "29"),
    3: ("12", "0", "23"),
    4: ("30", "0", "59"),
    5: ("45.5", "0", "59.5"),
}
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
    "make_timestamptz(integer, integer, integer, integer, integer, double precision, text)": {
        **_TIMESTAMP_PARTS,
        6: ("'UTC'", "'America/New_York'", "'+05:30'"),
    },
    "record_in(cstring, oid, integer)": {
        0: Raw(f"{r}::cstring" for r in ("'(1,x,10,)'", "'(2,\"a b\",10,{})'", "'(3,,10,)'")),
        1: Raw(("'pg_catalog.pg_namespace'::regtype::oid",) * 3),
        2: Raw(("(-1)::integer",) * 3),
    },
    "pg_extension_update_paths(name)": {0: ("'hstore'", "'plpgsql'", "'pg_trgm'")},
    "pg_sequence_parameters(oid)": {
        0: Raw((f"'{function_probe.PROBE_SEQUENCE}'::regclass::oid",) * 3)
    },
    "pg_split_walfile_name(text)": {
        0: (
            "'000000010000000000000001'",
            "'00000002000000A1000000FF'",
            "'000000010000000000000000'",
        )
    },
    'satisfies_hash_partition(oid, integer, integer, "any")': {
        0: Raw((_RELATION_OID.format(PARTITIONED_TABLE),) * 3),
        1: ("2", "2", "2"),
        2: ("0", "1", "0"),
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
    "foreign_data_wrapper": (
        (f"'{FOREIGN_DATA_WRAPPER}'",) * 3,
        (_catalog_read("oid", "pg_foreign_data_wrapper", f"fdwname = '{FOREIGN_DATA_WRAPPER}'"),)
        * 3,
    ),
    "server": (
        (f"'{FOREIGN_SERVER}'",) * 3,
        (_catalog_read("oid", "pg_foreign_server", f"srvname = '{FOREIGN_SERVER}'"),) * 3,
    ),
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
    "foreign_data_wrapper": ("'USAGE'", "'USAGE WITH GRANT OPTION'", "'USAGE'"),
    "server": ("'USAGE'", "'USAGE WITH GRANT OPTION'", "'USAGE'"),
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

#: The job's PostgreSQL's own values that no literal names before it runs, resolved from it when
#: a leg starts (``resolve_fixture``) and written into a literal where ``{fixture:<name>}`` stands:
#: a committed transaction of the fixture, and a multixact. A literal naming one unresolved is no
#: sample.
FIXTURE: dict[str, str] = {}
FIXTURE_QUERIES = {
    "xid": f"SELECT xmin FROM {COLUMN_RELATION} WHERE id = 1",
    "multixact": f"SELECT xmax FROM {catalog.FIXTURE_SCHEMA}.{MULTIXACT_TABLE}",
    "domain": (
        f"SELECT oid FROM pg_catalog.pg_type WHERE typname = '{FIXTURE_DOMAIN}' "
        f"AND typnamespace = '{catalog.FIXTURE_SCHEMA}'::regnamespace"
    ),
}
_FIXTURE_TOKEN = re.compile(r"\{fixture:(\w+)\}")


def _resolved(literal: str) -> str | None:
    """``literal`` with each ``FIXTURE`` value written in; None if one is not resolved."""
    names = _FIXTURE_TOKEN.findall(literal)
    if any(name not in FIXTURE for name in names):
        return None
    return _FIXTURE_TOKEN.sub(lambda m: FIXTURE[m.group(1)], literal)


_DATABASE_OID = _OBJECTS["database"][1]
_BACKEND_PID = Raw(("pg_catalog.pg_backend_pid()",) * VALUE_ROWS)
#: The answering session's own backend id, which pg_stat_get_backend_idset numbers from 1.
_BACKEND_ID = Raw(
    (
        "(SELECT b FROM pg_catalog.pg_stat_get_backend_idset() AS b "
        "WHERE pg_catalog.pg_stat_get_backend_pid(b) = pg_catalog.pg_backend_pid())",
    )
    * VALUE_ROWS
)
_FIXTURE_XID = ("'{fixture:xid}'",) * VALUE_ROWS
_RELATIONS = ("'pg_catalog.pg_class'", "'pg_catalog.pg_type'", f"'{COLUMN_RELATION}'")
_FILES = ("'PG_VERSION'", "'base/1/PG_VERSION'", "'PG_VERSION'")
#: Directories of the data directory whose entries are alike in form: databases' oids, WAL
#: segments with their status and summary directories.
_DIRECTORIES = ("'base'", "'pg_wal'", "'base'")
_TS_QUERIES = (
    "'SELECT ''cat:1 fat:2''::tsvector'",
    "'SELECT ''a:1A b:2B''::tsvector'",
    "'SELECT ''rat:3''::tsvector UNION ALL SELECT ''cat:1''::tsvector'",
)
_XML_QUERIES = {
    0: ("'SELECT 1 AS a'", "'SELECT ''x''::text AS b, 2 AS c'", "'SELECT 1 AS a WHERE false'"),
    1: ("true", "false", "true"),
    2: ("false", "true", "false"),
    3: ("''", "'urn:conformance'", "''"),
}
_CURSOR = "conformance_cursor"
_COLLATIONS = Raw(
    _catalog_read("oid", "pg_collation", f"collname = '{c}'")
    for c in ("und-x-icu", "en_US.utf8", "en_US")
)

#: Values of the volatile functions with no side effect that ``form_calls`` gives values, by
#: ``_key``: each array's elements alike in form, so any order of them has the form of another.
#: An object each reads is a catalog's own, or one ``seed_fixture`` made, named or read as each
#: server's catalog has it; a file or a directory is the data directory's own.
_FORM_VALUES: dict[str, dict[int, tuple[str, ...]]] = {
    "amvalidate(oid)": {
        0: Raw(
            _catalog_read("oid", "pg_opclass", f"opcname = '{name}' AND opcmethod = {am}")
            for name, am in (("int4_ops", 403), ("text_ops", 403), ("int4_ops", 405))
        )
    },
    "currtid2(text, tid)": {
        0: (f"'{COLUMN_RELATION}'",) * VALUE_ROWS,
        1: ("'(0,1)'", "'(0,2)'", "'(0,3)'"),
    },
    **{
        f"cursor_to_xml{schema}(refcursor{count}, boolean, boolean, text)": {
            0: (f"'{_CURSOR}'",) * VALUE_ROWS,
            **({1: ("1", "2", "0")} if count else {}),
            **{i + bool(count): _XML_QUERIES[i] for i in (1, 2, 3)},
        }
        for schema, count in (("", ", integer"), ("schema", ""))
    },
    "lo_get(oid)": {0: (str(FIXTURE_LARGE_OBJECT),) * VALUE_ROWS},
    "lo_get(oid, bigint, integer)": {
        0: (str(FIXTURE_LARGE_OBJECT),) * VALUE_ROWS,
        1: ("0", "1", "2"),
        2: ("2", "1", "5"),
    },
    "pg_get_multixact_members(xid)": {0: ("'{fixture:multixact}'",) * VALUE_ROWS},
    "pg_blocking_pids(integer)": {0: _BACKEND_PID},
    "pg_safe_snapshot_blocking_pids(integer)": {0: _BACKEND_PID},
    "pg_isolation_test_session_is_blocked(integer, integer[])": {
        0: _BACKEND_PID,
        1: ("'{}'", "'{1}'", "'{}'"),
    },
    "pg_collation_actual_version(oid)": {0: _COLLATIONS},
    "pg_database_collation_actual_version(oid)": {0: Raw(_DATABASE_OID)},
    "pg_current_logfile(text)": {0: ("'stderr'", "'csvlog'", "'jsonlog'")},
    "pg_database_size(name)": {0: Raw(("pg_catalog.current_database()",) * VALUE_ROWS)},
    "pg_database_size(oid)": {0: Raw(_DATABASE_OID)},
    "pg_tablespace_size(name)": {0: ("'pg_default'", "'pg_global'", "'pg_default'")},
    "pg_tablespace_size(oid)": {0: ("1663", "1664", "1663")},
    **{
        f"{name}(regclass)": {0: _RELATIONS}
        for name in (
            "pg_indexes_size",
            "pg_relation_size",
            "pg_table_size",
            "pg_total_relation_size",
        )
    },
    "pg_relation_size(regclass, text)": {0: _RELATIONS, 1: ("'main'", "'fsm'", "'vm'")},
    "pg_ls_dir(text)": {0: _DIRECTORIES},
    "pg_ls_dir(text, boolean, boolean)": {
        0: (*_DIRECTORIES[:2], "'no_such_directory'"),
        1: ("false", "true", "true"),
        2: ("false", "false", "false"),
    },
    "pg_ls_replslotdir(text)": {0: (f"'{REPLICATION_SLOT}'",) * VALUE_ROWS},
    "pg_ls_tmpdir(oid)": {0: ("1663",) * VALUE_ROWS},
    "pg_partition_ancestors(regclass)": {
        0: Raw(
            f"{_RELATION_OID.format(t)}::regclass"
            for t in (f"{PARTITIONED_TABLE}_0", f"{PARTITIONED_TABLE}_1", PARTITIONED_TABLE)
        )
    },
    "pg_partition_tree(regclass)": {
        0: Raw(
            f"{_RELATION_OID.format(t)}::regclass"
            for t in (PARTITIONED_TABLE, f"{PARTITIONED_TABLE}_1", PARTITIONED_TABLE)
        )
    },
    **{
        f"{name}(text{rest})": {0: _FILES, **given}
        for name in ("pg_read_file", "pg_read_binary_file")
        for rest, given in (
            ("", {}),
            (", boolean", {0: (*_FILES[:2], "'no_such_file'"), 1: ("false", "true", "true")}),
            (", bigint, bigint", {1: ("0", "1", "0"), 2: ("2", "1", "1")}),
            (
                ", bigint, bigint, boolean",
                {
                    0: (*_FILES[:2], "'no_such_file'"),
                    1: ("0", "1", "0"),
                    2: ("2", "1", "1"),
                    3: ("false", "true", "true"),
                },
            ),
        )
    },
    "pg_stat_file(text)": {0: ("'PG_VERSION'", "'base'", "'postmaster.pid'")},
    "pg_stat_file(text, boolean)": {
        0: ("'PG_VERSION'", "'base'", "'no_such_file'"),
        1: ("false", "true", "true"),
    },
    "pg_sequence_last_value(regclass)": {
        0: Raw((f"{_RELATION_OID.format(FIXTURE_SEQUENCE)}::regclass",) * VALUE_ROWS)
    },
    "pg_stat_have_stats(text, oid, oid)": {
        0: ("'database'", "'relation'", "'database'"),
        1: Raw((*_DATABASE_OID[:2], "0::oid")),
        2: Raw(("0::oid", "'pg_catalog.pg_class'::regclass::oid", "0::oid")),
    },
    "pg_wal_summary_contents(bigint, pg_lsn, pg_lsn)": {
        0: ("1",) * VALUE_ROWS,
        1: ("'0/0'",) * VALUE_ROWS,
        2: ("'0/1000000'", "'0/2000000'", "'1/0'"),
    },
    "pg_xact_commit_timestamp(xid)": {0: _FIXTURE_XID},
    "pg_xact_commit_timestamp_origin(xid)": {0: _FIXTURE_XID},
    "pg_xact_status(xid8)": {0: _FIXTURE_XID},
    "txid_status(bigint)": {0: _FIXTURE_XID},
    **{
        f"query_to_xml{part}(text, boolean, boolean, text)": _XML_QUERIES
        for part in ("", "schema", "_and_xmlschema")
    },
    "ts_rewrite(tsquery, text)": {
        0: ("'a & b'", "'cat'", "'a | c'"),
        1: ("'SELECT ''a''::tsquery, ''c''::tsquery'",) * VALUE_ROWS,
    },
    "ts_stat(text)": {0: _TS_QUERIES},
    "ts_stat(text, text)": {0: _TS_QUERIES, 1: ("'a'", "'ab'", "'d'")},
    "array_shuffle(anyarray)": {0: ("'{1,2,3}'", "'{}'", "'{4,5,6}'")},
    "array_sample(anyarray, integer)": {0: ("'{1,2,3}'", "'{}'", "'{4,5,6}'"), 1: ("2", "0", "3")},
    "random(integer, integer)": {0: ("1", "(-5)", "7"), 1: ("10", "5", "7")},
    "random(bigint, bigint)": {0: ("1", "(-5000000000)", "7"), 1: ("10", "5", "7")},
    "random(numeric, numeric)": {0: ("1.5", "(-2.25)", "7"), 1: ("2.25", "5.125", "7.0")},
    "random_normal(double precision, double precision)": {
        0: ("5", "(-2.5)", "0"),
        1: ("0", "0", "0"),
    },
    # Answers with the server's own state (``OWN_ANSWERS``), of an object it has.
    "age(xid)": {0: _FIXTURE_XID},
    "mxid_age(xid)": {0: ("'{fixture:multixact}'",) * VALUE_ROWS},
    "pg_relation_filenode(regclass)": {0: _RELATIONS},
    "pg_relation_filepath(regclass)": {0: _RELATIONS},
    "pg_stat_get_activity(integer)": {0: _BACKEND_PID},
    "pg_stat_get_numscans(oid)": {
        0: Raw(
            f"'pg_catalog.{i}'::regclass::oid"
            for i in ("pg_class_oid_index", "pg_type_oid_index", "pg_namespace_nspname_index")
        )
    },
    "pg_tablespace_location(oid)": {0: ("1663", "1664", "1663")},
    "pg_relation_is_publishable(regclass)": {0: _RELATIONS},
}

#: As ``_FORM_VALUES``, by prefix: a database's statistics of the session's own database, a
#: backend's of the session's own backend.
_OWN_PREFIX_VALUES: dict[str, dict[int, tuple[str, ...]]] = {
    "pg_stat_get_db_": {0: Raw(_DATABASE_OID)},
    "pg_stat_get_backend_": {0: _BACKEND_ID},
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
    "make_timestamp": _TIMESTAMP_PARTS,
    "make_timestamptz": _TIMESTAMP_PARTS,
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
    # The array forms take an array of fractions (REQ-2167, amended 2026-11-04 (5)).
    "percentile_cont(double precision[])": {0: _FRACTIONS},
    "percentile_disc(double precision[])": {0: _FRACTIONS},
    **_TYPMODS,
    # domain_in reads its text as the fixture's domain (``FIXTURE_DOMAIN``), with no typmod: a
    # NULL among them, as its answer is compared by whether it IS NULL (``domain_in_calls``).
    "domain_in": {
        0: Raw(("'5'::cstring", "NULL::cstring", "'1'::cstring")),
        1: ("'{fixture:domain}'",) * VALUE_ROWS,
        2: Raw(("(-1)::integer",) * VALUE_ROWS),
    },
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
    **_FORM_VALUES,
}


@dataclass(frozen=True)
class Call:
    """One statement of an overload. ``answers``: a real answer, compared by value; ``values``:
    whether the rows compare at all (the NULL call of a volatile function, or of one whose answer
    is its server's own: no); ``zone``: the session's TimeZone for this call, if not ``SETTINGS``'
    UTC; ``before``: what the session must have done first, run just before it
    (function_probe.BEFORE); ``setup`` and ``teardown``: statements around it on the same session,
    as an open cursor needs its transaction; ``refusal``: PostgreSQL's answer is its refusal,
    which Provisa must answer with the same SQLSTATE and message."""

    sql: str
    answers: bool = False
    values: bool = True
    refusal: bool = False
    zone: str | None = None
    before: str | None = None
    setup: tuple[str, ...] = ()
    teardown: tuple[str, ...] = ()

    @property
    def form(self) -> bool:
        """Whether the answer's type and form are checked: its value cannot be, and no NULL made
        it."""
        return not self.values and "NULL::" not in self.sql


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
        or _own_given(fn)
    )


def _own_given(fn: Function) -> dict[int, tuple[str, ...]]:
    """The arguments of a stable function answering its server's own of the session's own
    object (``_OWN_PREFIX_VALUES``)."""
    if fn.volatility == "v" or own_answer(fn) is None:
        return {}
    return next((g for p, g in _OWN_PREFIX_VALUES.items() if fn.name.startswith(p)), {})


def _sample(fn: Function, position: int, row: int) -> str | None:
    """Row ``row``'s literal of ``fn``'s argument ``position``, cast; None if it has none."""
    argtype = fn.argtypes[position]
    given = _given(fn).get(position)
    if isinstance(given, Raw):
        return _resolved(given[row])
    if given is None:
        given = _samples_of(function_probe.CONCRETE.get(argtype, argtype))
        if given is None:
            return None
    literal = _resolved(given[row])
    return None if literal is None else _cast(literal, argtype)


def unchecked(fn: Function, family: str) -> str | None:
    """Why ``fn`` is given no values; None if it is."""
    if function_probe.UNCALLABLE in fn.argtypes:
        return function_probe.NO_CALL
    if fn.signature in function_probe.STATEMENTS and _key(fn) not in VALUE_STATEMENTS:
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


#: Value calls written whole, by ``_key``: a function returning a record of no declared type is
#: called where a column definition list gives it one, a set of them in FROM (REQ-2167, amended
#: 2026-11-04 (5)).
VALUE_STATEMENTS: dict[str, tuple[str, ...]] = {
    **{
        f"{name}({typ})": tuple(
            f'SELECT t.a, t.b FROM pg_catalog."{name}"({doc}::{typ}) AS t(a integer, b text)'
            for doc in ('\'{"a": 1, "b": "x"}\'', "'{\"b\": null}'", '\'{"a": -7, "c": true}\'')
        )
        for name, typ in (("json_to_record", "json"), ("jsonb_to_record", "jsonb"))
    },
    **{
        f"{name}({typ})": tuple(
            f'SELECT * FROM pg_catalog."{name}"({doc}::{typ}) AS t(a integer, b text)'
            for doc in (
                '\'[{"a": 1, "b": "x"}, {"a": 2}]\'',
                "'[]'",
                '\'[{"b": null}, {"a": -7, "c": true}]\'',
            )
        )
        for name, typ in (("json_to_recordset", "json"), ("jsonb_to_recordset", "jsonb"))
    },
}


def value_calls(fn: Function, family: str) -> list[Call]:
    """The calls of ``fn`` with real values, none if ``unchecked`` says so."""
    if unchecked(fn, family) is not None:
        return []
    if _key(fn) in VALUE_STATEMENTS:
        return [Call(sql, answers=True) for sql in VALUE_STATEMENTS[_key(fn)]]
    if fn.name == "domain_in":
        return domain_in_calls(fn)
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


#: A value breaking the fixture domain's CHECK (VALUE > 0).
DOMAIN_BREAKING = "'-1'::cstring"


def domain_in_calls(fn: Function) -> list[Call]:
    """domain_in's calls: its answer, of the pseudo-type any, which no client can display, is
    compared by whether it IS NULL, and a value breaking the fixture domain's CHECK by the refusal
    it gets (REQ-2167, amended 2026-11-04, A call PostgreSQL cannot make is skipped by design)."""
    named = f'pg_catalog."{fn.name}"'
    rows = _rows(fn)
    breaking = [DOMAIN_BREAKING, *rows[0][1:]]
    return [
        *(Call(f"SELECT {named}({', '.join(row)}) IS NULL AS v", answers=True) for row in rows),
        Call(f"SELECT {named}({', '.join(breaking)}) IS NULL AS v", answers=True, refusal=True),
    ]


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


#: The statements around a call reading ``_CURSOR``: its transaction, and the cursor in it.
_CURSOR_SETUP = ("BEGIN", f"DECLARE {_CURSOR} CURSOR FOR SELECT 1 AS a UNION ALL SELECT 2")
_CURSOR_TEARDOWN = ("ROLLBACK",)


def form_calls(fn: Function) -> list[Call]:
    """A function whose value cannot be compared, with real values, its answer held to its type
    and form (REQ-2167, WHAT GUARANTEED MEANS): a volatile one of ``_FORM_VALUES``, which acts on
    nothing, or a stable one answering its server's own, of ``_FORM_VALUES`` or
    ``_OWN_PREFIX_VALUES``. None if a value is not resolved (``FIXTURE``)."""
    if not fn.argtypes or not (_key(fn) in _FORM_VALUES or _own_given(fn)):
        return []
    rows = range(VALUE_ROWS)
    if any(_sample(fn, i, row) is None for i in range(len(fn.argtypes)) for row in rows):
        return []
    cursor = fn.argtypes[0] == "refcursor"
    setup, teardown = (_CURSOR_SETUP, _CURSOR_TEARDOWN) if cursor else ((), ())
    return [
        Call(function_probe.statement(fn, row), values=False, setup=setup, teardown=teardown)
        for row in _rows(fn)
    ]


def calls(fn: Function, family: str) -> list[Call]:
    """Every call of ``fn``: its NULL call, then its value calls or its form calls. The NULL call
    compares its rows unless the function is volatile or its answer its server's own."""
    valued = value_calls(fn, family) or form_calls(fn)
    null = function_probe.call(fn)
    if null is None or any(c.sql == null for c in valued):
        return valued
    zoned = [Call(null, values=False, zone=ZONE)] if fn.name in ZONED_FUNCTIONS else []
    compared = fn.volatility != "v" and own_answer(fn) is None
    before = function_probe.BEFORE.get(fn.signature)
    return [Call(null, values=compared, before=before), *zoned, *valued]


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
        state = res.error_field(pq.DiagnosticField.SQLSTATE)
        return {
            "ok": False,
            "error": (res.error_message or b"").decode(errors="replace").strip()[:400],
            "sqlstate": state.decode() if state else "",
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


def _union(found: Iterable[Rules]) -> Rules:
    """The rules every part needed, or None when any part differs."""
    parts = list(found)
    rules: frozenset[str] = frozenset()
    for part in parts:
        if part is None:
            return None
        rules |= part
    return rules


def _array_rules(want: str, got: str, element: Callable[[str, str], Rules]) -> Rules:
    """An array of numbers, element by element."""
    a, b = want.strip("{}").split(","), got.strip("{}").split(",")
    if len(a) != len(b):
        return None
    found = [_SAME if x == y else element(x, y) for x, y in zip(a, b, strict=True)]
    return _union(found)


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
        return _union([*found, frozenset({JSON_KEY_ORDER}) if list(a) != list(b) else frozenset()])
    if isinstance(a, list):
        if len(a) != len(b):
            return None
        found = list(map(_parsed_rules, a, b))
        return _union(found)
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


#: The fields of a function's record read from the clock, by position, with their type's oid:
#: they compare by that type's form (REQ-2167, WHAT GUARANTEED MEANS), the others by value.
CLOCK_FIELDS = {"pg_cursor": {5: 1184}}


def _record_rules(want: str, got: str, sql: str, function: str = "") -> Rules:
    """A record whose fields are the same, a JSON container comparing as JSON, a clock's field
    (``CLOCK_FIELDS``) by its form."""
    a, b = _record_fields(want), _record_fields(got)
    if a is None or b is None or len(a) != len(b):
        return None
    clocks = CLOCK_FIELDS.get(function, {})
    found: set[str] = set()
    for at, (x, y) in enumerate(zip(a, b, strict=True)):
        if at in clocks and x != y:
            if re.fullmatch(TYPE_FORMS[clocks[at]], y) is None:
                return None
            found.add(CLOCK_FIELD)
            continue
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


def _value_rules(
    want: str | None, got: str | None, oid: int, sql: str, function: str = ""
) -> Rules:
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
        return _record_rules(want, got, sql, function)
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


# --- time zones (REQ-2167, amended 2026-11-03, Time zones follow upstream tzdata) --------------

#: The functions listing time zones by name, its record's first field: Provisa's list is the
#: pinned upstream IANA tzdata, PostgreSQL's its distribution's packaging of one.
ZONE_LISTS = frozenset({"pg_timezone_names"})
#: The names a distribution's tzdata adds to the release's: Debian's.
DISTRIBUTION_ZONES = frozenset({"localtime", "posixrules"})
#: The zones tzdata 2024b made backward links, each to the zone it names: a distribution dropping
#: the backward links drops them (Debian's, run 42935119044), and the pinned release compiles each
#: apart from its zone (their rules before 1970), so its bytes do not show it a link.
BACKWARD_LINKS = {
    "CST6CDT": "America/Chicago",
    "EST5EDT": "America/New_York",
    "MST7MDT": "America/Denver",
    "PST8PDT": "America/Los_Angeles",
    "EST": "America/Panama",
    "MST": "America/Phoenix",
    "HST": "Pacific/Honolulu",
}


def _zone_name(row: tuple) -> str | None:
    fields = _record_fields(row[0]) if row and row[0] is not None else None
    return fields[0] if fields else None


@functools.cache
def _pinned_zone(name: str) -> bytes | None:
    """The pinned release's compiled zone of ``name``; a link's is its zone's."""
    zone = importlib.resources.files("tzdata").joinpath("zoneinfo", *name.split("/"))
    return zone.read_bytes() if zone.is_file() else None


def shared_zones(
    want: list[tuple], got: list[tuple]
) -> tuple[list[tuple], list[tuple], frozenset[str]] | str:
    """The rows of the zones both lists name, and the rule that skipped the rest, or how the lists
    differ. A name PostgreSQL alone lists is skipped when its distribution adds it
    (``DISTRIBUTION_ZONES``); one Provisa alone lists when it is a link of the pinned release to a
    zone both name, a backward link a distribution drops."""
    named = {_zone_name(row) for row in want}
    shared = named & {_zone_name(row) for row in got}
    zones = {zone for name in shared if name and (zone := _pinned_zone(name)) is not None}
    only_pg = sorted(n or "" for n in named - shared if n not in DISTRIBUTION_ZONES)
    if only_pg:
        return f"zones PostgreSQL lists and Provisa does not: {only_pg}"
    only_provisa = sorted(
        name or ""
        for row in got
        if (name := _zone_name(row)) not in shared
        and (name is None or _pinned_zone(name) not in zones)
        and BACKWARD_LINKS.get(name or "") not in shared
    )
    if only_provisa:
        return f"zones Provisa lists and PostgreSQL does not: {only_provisa}"
    rules = _SAME if len(shared) == len(want) == len(got) else frozenset({ZONE_PACKAGING})
    kept = [row for row in want if _zone_name(row) in shared]
    return kept, [row for row in got if _zone_name(row) in shared], rules


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
    packaged = _SAME
    if function in ZONE_LISTS:
        zoned = shared_zones(a, b)
        if isinstance(zoned, str):
            return Compared(zoned)
        a, b, packaged = zoned
    if len(a) != len(b):
        return Compared(f"{len(b)} rows, PostgreSQL {len(a)}")

    def value(wv: str | None, gv: str | None, oid: int) -> Rules:
        rules = _value_rules(wv, gv, oid, sql, function)
        if rules is None and _model_answer(function, wv, gv):
            return frozenset({MODEL_ANSWER})
        return rules

    def row_rules(x: tuple, y: tuple) -> Rules:
        found = [value(wv, gv, oid) for wv, gv, oid in zip(x, y, fields, strict=True)]
        return _union(found)

    def order(row: tuple) -> list[tuple[bool, str]]:
        return [(value_ is None, value_ or "") for value_ in row]

    used, missing, left = _pair_rows(a, b, row_rules)
    if not missing:
        return Compared(rules=typed | used | packaged)
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


def _numeric_form(sample: str) -> str:
    """A numeric's form: its scale, the digits PostgreSQL writes after the point, of either sign.
    random(min, max)'s answer is of the larger scale of its bounds; its sign is the draw's, which
    the shape of PostgreSQL's own draw held it to (run 42527458223: '1.023' was not the form of
    '-0.803'). NaN and the infinities are words of their own."""
    if sample in ("NaN", "Infinity", "-Infinity"):
        return re.escape(sample)
    scale = len(sample.partition(".")[2])
    return _INTEGER + (rf"\.\d{{{scale}}}" if scale else "")


def value_form(name: str, oid: int, sample: str) -> str:
    """The regular expression an answer of function ``name``, of type ``oid``, must match, from
    ``sample``, PostgreSQL's answer."""
    if name == "timeofday":
        return _TIMEOFDAY + re.escape(sample.rsplit(" ", 1)[1])
    if name in FUNCTION_FORMS and _FUNCTION_FORM_OIDS.get(name, oid) == oid:
        return FUNCTION_FORMS[name]
    if oid in TYPE_FORMS:
        return TYPE_FORMS[oid]
    if oid == _NUMERIC_OID:
        return _numeric_form(sample)
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


# --- one overload on one engine ---------------------------------------------------------------


def _answer(server: Session, call_: Call) -> dict[str, Any]:
    """``call_`` on ``server``: the first of what must run before it (its ``before``, then its
    setup) that fails is its answer, and its teardown runs whatever came before."""
    if call_.zone is not None:
        server.answer(f"SET TimeZone TO '{call_.zone}'")
    try:
        if call_.before is not None:
            done = server.answer(call_.before)
            if not done["ok"]:
                return {**done, "error": f"{call_.before}: {done['error']}"}
        for sql in call_.setup:
            done = server.answer(sql)
            if not done["ok"]:
                return done
        return server.answer(call_.sql)
    finally:
        for sql in call_.teardown:
            server.answer(sql)
        if call_.zone is not None:
            server.answer(SETTINGS[0])


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
            group = decided_error(fn.name, got["error"])
            if group is not None:
                return {"outcome": "by_design", "group": group}
            named = function_probe.refused_by_name(got["error"])
            return {
                "outcome": "refused" if named else "error",
                "detail": {"statement": call_.sql, "error": got["error"]},
            }
        compared = compare(want, got, values=True, sql=call_.sql, function=fn.name, enums=enums)
        if compared.difference is not None:
            group = decided_difference(fn.name, call_.sql, compared.difference) or decided_answer(
                fn.name, call_.sql, want, got
            )
            if group is not None:
                return {"outcome": "by_design", "group": group}
            detail = {"statement": call_.sql, "difference": compared.difference}
            return {"outcome": "wrong_answer", "detail": detail}
        return {"outcome": "pass", "rules": sorted(compared.rules)}
    return None


def _all_null(want: dict[str, Any]) -> bool:
    return bool(want["rows"]) and all(v is None for row in want["rows"] for v in row)


def _null_difference(want: dict[str, Any], got: dict[str, Any]) -> str | None:
    """How the NULLs of Provisa's one row differ from those of PostgreSQL's; None if they do not.
    A set's rows are its server's own, so a set differs in none."""
    if len(want["rows"]) != 1 or len(got["rows"]) != 1:
        return None
    for wv, gv in zip(want["rows"][0], got["rows"][0], strict=False):
        if (wv is None) != (gv is None):
            return f"{gv!r}, PostgreSQL {wv!r}"
    return None


# --- side effects (REQ-2167, amended 2026-11-02, Side-effect functions) -------------------------

#: The engine side-effect functions are expected to work on: on every other they are skipped by
#: design.
SIDE_EFFECT_ENGINE = "pg"

#: The skipped-by-design groups of this check, each a decision of REQ-2167.
SIDE_EFFECT_GROUP = "Side-effect functions"
NOT_TESTABLE = "Not testable"
SEQUENCE_GROUP = "Sequence functions are not supported"
REGEXP_GROUP = "Six-argument regexp_replace is not supported"
SYSTEM_CATALOG_WRITE_GROUP = "A system catalog is written by no role"
NOTIFICATION_GROUP = "LISTEN, NOTIFY and pg_notify are refused"
LARGE_OBJECT_GROUP = "Large objects as a read-only transaction"
CATALOG_IS_THE_MODEL_GROUP = "The catalog is the model's"
STORAGE_GROUP = "Provisa holds no storage"
SERVER_FILES_GROUP = "No function reads the server's files"
SERVER_ADMINISTRATION_GROUP = "Server administration functions"
SERVER_VERSION_GROUP = "The server's version"
REFERENCES_GROUP = "REFERENCES is never granted"
TABLE_XML_GROUP = "A table's XML holds the model's arrays and database"
SERVER_STATE_GROUP = "PostgreSQL's own server state"
SESSION_CURSORS_GROUP = "The session's own cursors"
SERVER_EXTENSIONS_GROUP = "The server's installed extensions"
SINGLE_BYTE_DATABASE_GROUP = "One-argument to_ascii needs a single-byte database"
NO_SQL_VALUE_GROUP = "Types no SQL value can have"
INTERNAL_TYPE_GROUP = "Types that exist only inside PostgreSQL"
GROUPS = {
    # REQ-2167, amended 2026-11-02, Side-effect functions.
    SIDE_EFFECT_GROUP: (
        "A function that returns void or acts is expected to work only on the PostgreSQL engine."
    ),
    # REQ-2167, amended 2026-11-02, Side-effect functions: a side effect no client observes.
    NOT_TESTABLE: "Its side effect cannot be observed through a client.",
    # REQ-2167, amended 2026-11-02, Sequence functions are not supported.
    SEQUENCE_GROUP: "nextval, currval and setval are refused by name: Provisa does not model a "
    "source's sequences. pg_sequence_last_value is skipped: Provisa does not drive a source's "
    "sequences (amended 2026-11-04, Server state, internal types and the check's own call shapes "
    "(2)).",
    # REQ-2167, amended 2026-11-02, Six-argument regexp_replace is not supported.
    REGEXP_GROUP: "The PostgreSQL 16 form with a start position and an occurrence number is "
    "refused by name.",
    # REQ-2167, amended 2026-11-03, Privileges, visibility and notifications (1).
    SYSTEM_CATALOG_WRITE_GROUP: "has_table_privilege and the column variants answer false for a "
    "write on PostgreSQL's own system catalogs, for every role: nobody writes a system catalog "
    "through Provisa (REQ-127).",
    # REQ-2167, amended 2026-11-03, Privileges, visibility and notifications (4).
    NOTIFICATION_GROUP: "LISTEN, NOTIFY and pg_notify are refused by name: Provisa's pgwire has "
    "no notification channel.",
    # REQ-2167, amended 2026-10-29, LOCKS, FLOATS AND SERVER ANSWERS.
    LARGE_OBJECT_GROUP: "A large object function answers as PostgreSQL does in a read-only "
    "transaction: Provisa keeps no large objects.",
    # REQ-2167, amended 2026-10-25, PGWIRE CATALOG AND TYPES: catalog content is the role's
    # governed view of the model; REQ-2190, META IS A SUPERSET OF THE CATALOG: a relation's oid
    # and a catalog's rows are the model's, so a regclass is compared by its name.
    CATALOG_IS_THE_MODEL_GROUP: "A relation's oid and a catalog's rows are the model's "
    "(REQ-2190): a regclass is compared by its name, not its oid, and a catalog serialized "
    "lists the model; an object only the PostgreSQL fixture server creates (a foreign-data "
    "wrapper, a foreign server, statistics objects, partitions, a replication slot, a "
    "statistics reset, a large object, a transaction id, an available extension, a system "
    "view's rule) is in no model.",
    # REQ-2167, amended 2026-11-03, Storage, fixture objects and server files (1).
    STORAGE_GROUP: "Storage functions answer NULL: Provisa holds no storage of its own, and so "
    "no tuple ids (currtid2 finds no relation's).",
    # REQ-2167, amended 2026-11-03, Storage, fixture objects and server files (3); amended
    # 2026-10-27, PGWIRE'S CATALOG FUNCTIONS: no function reads the server's files.
    SERVER_FILES_GROUP: "pg_read_file and pg_read_binary_file read no file, and pg_ls_dir and "
    "pg_stat_file answer as a server without it: no function reads the server's files.",
    # REQ-2167, amended 2026-11-03, Server administration functions; amended 2026-11-03, Backend
    # session statistics are the engine's.
    SERVER_ADMINISTRATION_GROUP: "PostgreSQL refuses these for the test server's own state or "
    "configuration; each names PostgreSQL's refusal as its reason. pg_stat_get_backend_* and "
    "pg_stat_get_backend_idset report on the engine's own sessions, not the model: Provisa refuses "
    "them by name, as serving them would show one tenant other sessions' activity.",
    # REQ-579: the server reports its own version, and a collation's version is the server's.
    SERVER_VERSION_GROUP: "A collation's version is the server's own, as the server's version "
    "is (REQ-579).",
    # REQ-2167, amended 2026-11-03, Time zones follow upstream tzdata; REFERENCES is never granted.
    REFERENCES_GROUP: "Provisa never grants the REFERENCES privilege, so has_table_privilege and "
    "the column variants answer false for it.",
    # REQ-1965: a source's array column is the JSON it is registered as; REQ-588: the database is
    # the served one.
    TABLE_XML_GROUP: "table_to_xml and its kin write a source's array column as the JSON array it "
    "is registered as (REQ-1965), and name the served database in a type's name (REQ-588): the "
    "document differs from PostgreSQL's in these alone.",
    # REQ-2167, amended 2026-11-04, Server state, internal types and the check's own call shapes
    # (1).
    SERVER_STATE_GROUP: "These read PostgreSQL's own server state (WAL, replication and "
    "recovery, log files, the hba and ident files, shared memory and memory contexts, locks, I/O "
    "and SLRU statistics, wait events, server directory listings, prepared transactions, the "
    "pg_stat_get_xact_* transaction counters): there is no single server to test, since the "
    "answer is whichever engine's runs.",
    # REQ-2167, amended 2026-11-04, Server state, internal types and the check's own call shapes
    # (2).
    SESSION_CURSORS_GROUP: "pg_cursor lists the session's own cursors.",
    SERVER_EXTENSIONS_GROUP: "pg_extension_update_paths reads the server's installed extensions.",
    SINGLE_BYTE_DATABASE_GROUP: "to_ascii(text) converts only from a LATIN1, LATIN2, LATIN9 or "
    "WIN1250 database; the forms naming the encoding are compared.",
    # REQ-2167, amended 2026-11-04, Server state, internal types and the check's own call shapes
    # (3).
    NO_SQL_VALUE_GROUP: "The input and output functions of types no SQL value can have: "
    "PostgreSQL errors on every call (the any* and anycompatible* pseudo-types, shell_out, "
    "pg_ddl_command, pg_node_tree_in, the extended statistics inputs, the BRIN bloom and "
    "multi-minmax summaries, gtsvector).",
    # REQ-2167, amended 2026-11-13, AN INTERNAL TYPE IS BY DESIGN (INTERNAL_TYPES).
    INTERNAL_TYPE_GROUP: "The functions of a type that exists only inside PostgreSQL (the reg* "
    "types, record, aclitem, xid and xid8), a type's input by oid and an aggregate's transition "
    "state are refused by name: no engine holds such a value. A function that merely takes or "
    "returns one, such as row_to_json(record) or a record comparison, is a gap and stays refused.",
}

#: The requirement each group's decision is of, where it is not REQ-2167.
GROUP_REQUIREMENTS = {SERVER_VERSION_GROUP: "REQ-579", TABLE_XML_GROUP: "REQ-1965, REQ-588"}

#: The functions a decision skips by design whatever they answer, by name or ``_key``.
DECIDED_FUNCTIONS = {
    **dict.fromkeys(
        (
            "pg_relation_size",
            "pg_table_size",
            "pg_indexes_size",
            "pg_total_relation_size",
            "pg_database_size",
            "pg_tablespace_size",
            "pg_relation_filenode",
            "pg_relation_filepath",
        ),
        STORAGE_GROUP,
    ),
    **dict.fromkeys(("pg_read_file", "pg_read_binary_file"), SERVER_FILES_GROUP),
    # With missing_ok, pg_stat_file answers NULL as a server without the file does; the fixture
    # server has it. Without, its refusal is the decision (_DECIDED_ERRORS).
    "pg_stat_file(text, boolean)": SERVER_FILES_GROUP,
    **dict.fromkeys(
        (
            "pg_wal_replay_pause",
            "pg_wal_replay_resume",
            "pg_is_wal_replay_paused",
            "pg_get_wal_replay_pause_state",
            "pg_logical_slot_get_changes",
            "pg_logical_slot_peek_changes",
            "pg_logical_slot_get_binary_changes",
            "pg_logical_slot_peek_binary_changes",
            "pg_sync_replication_slots",
            "pg_last_committed_xact",
            "pg_ls_logdir",
            "pg_replication_origin_session_reset",
            "pg_stop_making_pinned_objects",
        ),
        SERVER_ADMINISTRATION_GROUP,
    ),
    **dict.fromkeys(
        ("pg_collation_actual_version", "pg_database_collation_actual_version"),
        SERVER_VERSION_GROUP,
    ),
    **dict.fromkeys(
        (
            # WAL
            "pg_available_wal_summaries",
            "pg_wal_summary_contents",
            "pg_get_wal_resource_managers",
            "pg_stat_get_wal",
            "pg_stat_get_archiver",
            # replication and recovery
            "pg_last_wal_receive_lsn",
            "pg_last_wal_replay_lsn",
            "pg_last_xact_replay_timestamp",
            "pg_stat_get_wal_receiver",
            "pg_stat_get_wal_senders",
            "pg_stat_get_recovery_prefetch",
            "pg_show_replication_origin_status",
            # log files, the hba and ident files
            "pg_current_logfile",
            "pg_hba_file_rules",
            "pg_ident_file_mappings",
            # shared memory and memory contexts
            "pg_get_shmem_allocations",
            "pg_get_backend_memory_contexts",
            # locks, I/O and SLRU statistics, wait events
            "pg_lock_status",
            "pg_stat_get_io",
            "pg_stat_get_slru",
            "pg_get_wait_events",
            # server directory listings
            "pg_ls_waldir",
            "pg_ls_archive_statusdir",
            "pg_ls_tmpdir",
            "pg_ls_logicalmapdir",
            "pg_ls_logicalsnapdir",
            # prepared transactions
            "pg_prepared_xact",
            # REQ-2167, amended 2026-11-04, A call PostgreSQL cannot make is skipped by design:
            # the server's build and extensions, its transaction ids, its activity, checksum and
            # progress statistics
            "pg_config",
            "pg_available_extensions",
            "pg_available_extension_versions",
            "pg_current_xact_id_if_assigned",
            "txid_current_if_assigned",
            "pg_stat_get_activity",
            "pg_stat_get_db_checksum_failures",
            "pg_stat_get_db_checksum_last_failure",
            "pg_stat_get_snapshot_timestamp",
            "pg_stat_get_progress_info",
        ),
        SERVER_STATE_GROUP,
    ),
    "pg_cursor": SESSION_CURSORS_GROUP,
    "pg_sequence_last_value": SEQUENCE_GROUP,
    "pg_extension_update_paths": SERVER_EXTENSIONS_GROUP,
    "to_ascii(text)": SINGLE_BYTE_DATABASE_GROUP,
    **dict.fromkeys(
        (
            *(
                f"{pseudo}_{io}"
                for pseudo, ios in (
                    ("any", ("in", "out")),
                    ("anyarray", ("in",)),
                    ("anycompatible", ("in", "out")),
                    ("anycompatiblearray", ("in",)),
                    ("anycompatiblemultirange", ("in",)),
                    ("anycompatiblenonarray", ("in", "out")),
                    ("anycompatiblerange", ("in",)),
                    ("anyelement", ("in", "out")),
                    ("anyenum", ("in",)),
                    ("anymultirange", ("in",)),
                    ("anynonarray", ("in", "out")),
                    ("anyrange", ("in",)),
                )
                for io in ios
            ),
            "shell_out",
            "pg_ddl_command_in",
            "pg_ddl_command_out",
            "pg_ddl_command_send",
            "pg_node_tree_in",
            "pg_ndistinct_in",
            "pg_dependencies_in",
            "pg_mcv_list_in",
            # the index summaries
            "brin_bloom_summary_in",
            "brin_bloom_summary_out",
            "brin_bloom_summary_send",
            "brin_minmax_multi_summary_in",
            "brin_minmax_multi_summary_out",
            "brin_minmax_multi_summary_send",
            "gtsvectorin",
            "gtsvectorout",
        ),
        NO_SQL_VALUE_GROUP,
    ),
}
#: As ``DECIDED_FUNCTIONS``, by prefix: the engine's backend statistics (amended 2026-11-03,
#: Backend session statistics are the engine's) and its transaction counters (amended 2026-11-04
#: (1)).
_DECIDED_PREFIXES = {
    "pg_stat_get_backend_": SERVER_ADMINISTRATION_GROUP,
    "pg_stat_get_xact_": SERVER_STATE_GROUP,
}


def decided_function(fn: Function) -> str | None:
    """The group of the decision that skips ``fn`` whatever it answers, by ``_key``, name or
    prefix; None if no decision does."""
    group = DECIDED_FUNCTIONS.get(_key(fn)) or DECIDED_FUNCTIONS.get(fn.name)
    if group is not None:
        return group
    return next((g for p, g in _DECIDED_PREFIXES.items() if fn.name.startswith(p)), None)


@dataclass(frozen=True)
class InternalType:
    """A type that exists only inside PostgreSQL: the stems its own functions are named by, and
    whether its comparison operators are its own (a record's compare the values it holds)."""

    stems: tuple[str, ...]
    operators: bool = True


#: REQ-2167, amended 2026-11-13, AN INTERNAL TYPE IS BY DESIGN: the types that exist only inside
#: PostgreSQL. Their own functions are by design; a function that merely takes or returns one is
#: not (``internal_types_own``).
INTERNAL_TYPES = {
    **{
        oid_alias: InternalType((oid_alias,))
        for oid_alias in (
            "regclass",
            "regcollation",
            "regconfig",
            "regdictionary",
            "regnamespace",
            "regoper",
            "regoperator",
            "regproc",
            "regprocedure",
            "regrole",
            "regtype",
        )
    },
    "record": InternalType(("record",), operators=False),
    "aclitem": InternalType(("aclitem", "acl")),
    "xid": InternalType(("xid",)),
    "xid8": InternalType(("xid8",)),
}

#: How a type's own functions are named after its stem: its input, output, receive and send
#: functions, its hash and B-tree support functions, and its operators and functions on it.
_OWN_IO = ("{s}in", "{s}out", "{s}recv", "{s}send", "{s}_in", "{s}_out", "{s}_recv", "{s}_send")
_OWN_SUPPORT = ("hash_{s}", "hash_{s}_extended", "bt{s}cmp", "bt{s}imagecmp", "{s}cmp")
_OWN_OPERATORS = (
    *(f"{{s}}{op}" for op in ("eq", "ne", "lt", "le", "gt", "ge")),
    "{s}_larger",
    "{s}_smaller",
    *(f"{{s}}{fn}" for fn in ("contains", "explode", "insert", "remove")),
)

#: A type's input by oid: the type to read is an argument and the result a pseudo-type, so the
#: call is PostgreSQL's own. A type with a typmod reads it by the same arguments and is not one.
_INPUT_BY_OID = ("cstring", "oid", "integer")
_READ_BY_OID = frozenset({"anyarray", '"any"', "anyrange", "anymultirange", "record"})

#: The internal types' own functions not named after a stem, by ``_key``: a transaction id's age
#: and its cast from xid8, and the aggregates' transition states.
_OWN_BY_KEY = frozenset(
    {
        "age(xid)",
        "xid(xid8)",
        "int8_avg(bigint[])",
        "int8inc_float8_float8(bigint, double precision, double precision)",
    }
)


def _element(typ: str) -> str:
    return typ.removesuffix("[]")


def internal_types_own(fn: Function) -> bool:
    """Whether ``fn`` is one of an internal type's own functions (``INTERNAL_TYPES``): decided by
    the types it takes and returns and by what kind of function it is, never by a refusal's text.
    An aggregate, or a function that merely takes or returns one (row_to_json(record), a record
    comparison, ts_headline(regconfig, ...), pg_xact_status(xid8)), is not."""
    if fn.kind != "f":
        return False
    if _key(fn) in _OWN_BY_KEY:
        return True
    if fn.argtypes == _INPUT_BY_OID and fn.rettype in _READ_BY_OID:
        return True
    for typ in {_element(t) for t in (*fn.argtypes, fn.rettype)}:
        internal = INTERNAL_TYPES.get(typ)
        if internal is None:
            continue
        forms = _OWN_IO + _OWN_SUPPORT + (_OWN_OPERATORS if internal.operators else ())
        if any(fn.name == form.format(s=stem) for stem in internal.stems for form in forms):
            return True
    return False


#: The refusals a decision makes, by name or ``_key``: Provisa refusing one by name is that
#: decision, skipped by design.
DECIDED_REFUSALS = {
    "nextval": SEQUENCE_GROUP,
    "currval": SEQUENCE_GROUP,
    "setval": SEQUENCE_GROUP,
    "regexp_replace(text, text, text, integer, integer, text)": REGEXP_GROUP,
    "pg_notify": NOTIFICATION_GROUP,
}

#: The errors a decision makes, by function and the error's wording: Provisa answering one so is
#: that decision, skipped by design.
_DECIDED_ERRORS: tuple[tuple[re.Pattern[str], re.Pattern[str], str], ...] = (
    (
        re.compile(r"^lo_|^lo(read|write)$"),
        re.compile(r"cannot execute lo_\w+\(\) in a read-only transaction"),
        LARGE_OBJECT_GROUP,
    ),
    (
        re.compile(r"^(has_sequence_privilege|pg_sequence_parameters)$"),
        re.compile(rf'relation "{re.escape(function_probe.PROBE_SEQUENCE)}" does not exist'),
        SEQUENCE_GROUP,
    ),
    # No function reads the server's files: each answers as a server without the file.
    (
        re.compile(r"^(pg_ls_dir|pg_stat_file)$"),
        re.compile(r'could not (open directory|stat file) "[^"]*": No such file or directory'),
        SERVER_FILES_GROUP,
    ),
    # An object only the PostgreSQL fixture server creates is in no model.
    (
        re.compile(r"^lo_get$"),
        re.compile(rf"large object {FIXTURE_LARGE_OBJECT} does not exist"),
        CATALOG_IS_THE_MODEL_GROUP,
    ),
    (
        re.compile(r"^pg_ls_replslotdir$"),
        re.compile(rf'replication slot "{re.escape(REPLICATION_SLOT)}" does not exist'),
        CATALOG_IS_THE_MODEL_GROUP,
    ),
    (
        re.compile(r"^(pg_xact_status|txid_status)$"),
        re.compile(r"transaction ID \d+ is in the future"),
        CATALOG_IS_THE_MODEL_GROUP,
    ),
    (
        re.compile(r"^pg_extension_update_paths$"),
        re.compile(r'extension "hstore" is not available'),
        CATALOG_IS_THE_MODEL_GROUP,
    ),
    # Provisa holds no storage, so no tuple ids.
    (
        re.compile(r"^currtid2$"),
        re.compile(r'relation "[^"]+" does not exist'),
        STORAGE_GROUP,
    ),
)

#: The differences a decision makes, by function, statement and difference.
_PRIVILEGES_OF_A_RELATION = re.compile(r"^has_(table|column|any_column)_privilege$")
_WRITE_ON_A_CATALOG = re.compile(
    r"'pg_catalog\.\w+'.*'(INSERT|UPDATE|DELETE|TRUNCATE|REFERENCES|TRIGGER|MAINTAIN)'::text",
    re.IGNORECASE,
)
_TABLE_TO_XML = re.compile(r"^table_to_xml(schema|_and_xmlschema)?$")
_REFERENCES = re.compile(r"'REFERENCES'::text", re.IGNORECASE)
#: A call reading an object only the PostgreSQL fixture server creates (``FIXTURE_SQL``).
_FIXTURE_OBJECT = re.compile(
    "'(?:"
    + "|".join(
        re.escape(name)
        for name in (
            *STATISTICS,
            STATISTICS_TABLE,
            f"{PARTITIONED_TABLE}_0",
            f"{PARTITIONED_TABLE}_1",
            PARTITIONED_TABLE,
            FOREIGN_DATA_WRAPPER,
            FOREIGN_SERVER,
            REPLICATION_SLOT,
        )
    )
    + ")'"
)
#: Functions answering about such an object with no argument naming it: the fixture's
#: replication slot, its statistics reset, and a collation oid that is the model catalog's.
_FIXTURE_ANSWERS = frozenset(
    {"pg_get_replication_slots", "pg_stat_get_db_stat_reset_time", "pg_collation_is_visible"}
)


#: A rule's node tree, which a call reads from pg_rewrite.
_NODE_TREES = frozenset({"pg_node_tree_out", "pg_node_tree_send"})


def decided_error(name: str, error: str) -> str | None:
    """The group of the decision ``name``'s ``error`` is, None if it is no decision's."""
    for function, wording, group in _DECIDED_ERRORS:
        if function.search(name) and wording.search(error):
            return group
    return None


def decided_difference(name: str, sql: str, difference: str) -> str | None:
    """The group of the decision a call's ``difference`` from PostgreSQL is, None if none."""
    if (
        _PRIVILEGES_OF_A_RELATION.match(name)
        and _WRITE_ON_A_CATALOG.search(sql)
        and difference.startswith("'f', PostgreSQL 't'")
    ):
        return SYSTEM_CATALOG_WRITE_GROUP
    if (
        _PRIVILEGES_OF_A_RELATION.match(name)
        and _REFERENCES.search(sql)
        and difference.startswith("'f', PostgreSQL 't'")
    ):
        return REFERENCES_GROUP
    if name in ("regclasssend", "regclassrecv"):
        return CATALOG_IS_THE_MODEL_GROUP  # the oid's bytes: the model's oid
    if _TABLE_TO_XML.match(name) and "'pg_catalog." in sql:
        return CATALOG_IS_THE_MODEL_GROUP  # a catalog's rows: the model's
    if name in _NODE_TREES and "pg_catalog.pg_rewrite" in sql and difference.startswith("None,"):
        return CATALOG_IS_THE_MODEL_GROUP  # the model's pg_rewrite holds no system view's rule
    if name in _FIXTURE_ANSWERS or _FIXTURE_OBJECT.search(sql):
        return CATALOG_IS_THE_MODEL_GROUP  # an object only the PostgreSQL fixture makes
    return None


#: A name of the XML Schema a table maps to, up to the database it names (REQ-588).
_DATABASE_IN_A_NAME = re.compile(r'"(UDT|RowType|TableType|SchemaType|CatalogType)\.[^".]+')
#: One line of a table's XML that is one column's element.
_COLUMN_ELEMENT = re.compile(r"(\s*<([\w.:-]+)>)(.*)(</\2>)")


def _element_text(item: Any) -> str:
    return html.escape(item, quote=False) if isinstance(item, str) else json.dumps(item)


def _array_as_json(want: str, got: str) -> bool:
    """Whether ``got`` is the column element ``want`` is, its array written as the JSON array its
    column is registered as (REQ-1965): the same elements, a NULL one not written."""
    w, g = _COLUMN_ELEMENT.fullmatch(want), _COLUMN_ELEMENT.fullmatch(got)
    if w is None or g is None or w.group(1) != g.group(1):
        return False
    try:
        items = json.loads(html.unescape(g.group(3)))
    except ValueError:
        return False
    if not isinstance(items, list):
        return False
    written = "".join(f"<element>{_element_text(i)}</element>" for i in items if i is not None)
    return written == w.group(3)


def decided_answer(name: str, sql: str, want: dict[str, Any], got: dict[str, Any]) -> str | None:
    """The group of the decision a table's XML differs from PostgreSQL's by, None if none: line
    by line, the database named in a type's name and an array as JSON alone."""
    if not _TABLE_TO_XML.match(name) or len(want["rows"]) != 1 or len(got["rows"]) != 1:
        return None
    ((wanted,),), ((answered,),) = want["rows"], got["rows"]
    if wanted is None or answered is None:
        return None
    a = _DATABASE_IN_A_NAME.sub(r'"\1.', wanted).split("\n")
    b = _DATABASE_IN_A_NAME.sub(r'"\1.', answered).split("\n")
    if len(a) != len(b) or any(x != y and not _array_as_json(x, y) for x, y in zip(a, b)):
        return None
    return TABLE_XML_GROUP if wanted != answered else None


#: Volatile functions whose purpose is what they do rather than what they answer, beside every
#: function returning void.
_ACTING = frozenset(
    {
        "currval",
        "nextval",
        "setval",
        "set_config",
        "pg_notify",
        "pg_cancel_backend",
        "pg_terminate_backend",
        "pg_reload_conf",
        "pg_rotate_logfile",
        "pg_switch_wal",
        "pg_create_restore_point",
        "pg_backup_start",
        "pg_backup_stop",
        "pg_promote",
        "pg_drop_replication_slot",
        "pg_replication_slot_advance",
        "pg_log_backend_memory_contexts",
        "pg_log_standby_snapshot",
        "pg_logical_emit_message",
        "pg_import_system_collations",
        "pg_nextoid",
        "brin_summarize_new_values",
        "brin_summarize_range",
        "brin_desummarize_range",
        "gin_clean_pending_list",
        "lo_close",
        "lo_creat",
        "lo_create",
        "lo_export",
        "lo_from_bytea",
        "lo_import",
        "lo_lseek",
        "lo_lseek64",
        "lo_open",
        "lo_put",
        "lo_tell",
        "lo_tell64",
        "lo_truncate",
        "lo_truncate64",
        "lo_unlink",
        "loread",
        "lowrite",
    }
)
_ACTING_PREFIXES = (
    "pg_advisory_",
    "pg_try_advisory_",
    "pg_create_",
    "pg_copy_",
    "pg_replication_origin_",
    "pg_stat_reset",
    "binary_upgrade_",
)


def side_effect(fn: Function) -> bool:
    """Whether ``fn`` returns void or acts: it is tested by its side effect."""
    if fn.rettype == "void":
        return True
    return fn.volatility == "v" and (fn.name in _ACTING or fn.name.startswith(_ACTING_PREFIXES))


@dataclass(frozen=True)
class Effect:
    """How a side effect is seen through a client: ``setup``, then ``call``, then ``observe``,
    whose answer shows the effect, then ``teardown``, all on one session. ``elapsed``: the call
    takes at least this many seconds. ``listen``: a second session listens on this channel, and
    what it receives is the effect."""

    call: str
    observe: str | None = None
    setup: tuple[str, ...] = ()
    teardown: tuple[str, ...] = ()
    elapsed: float | None = None
    listen: str | None = None


_LOCKS = (
    "SELECT mode, granted FROM pg_catalog.pg_locks "
    "WHERE locktype = 'advisory' AND pid = pg_catalog.pg_backend_pid()"
)
_UNLOCK_ALL = ("SELECT pg_catalog.pg_advisory_unlock_all()",)
_LOCK_KEYS = {"bigint": "4242::bigint", "integer, integer": "42, 42"}
_LARGE_OBJECT = 424242
_LO_COUNT = (
    f"SELECT count(*) AS v FROM pg_catalog.pg_largeobject_metadata WHERE oid = {_LARGE_OBJECT}"
)
_LO_GET = f"SELECT pg_catalog.lo_get({_LARGE_OBJECT}) AS v"
_LO_UNLINK = (f"SELECT pg_catalog.lo_unlink({_LARGE_OBJECT})",)
_LO_ABC = f"SELECT pg_catalog.lo_from_bytea({_LARGE_OBJECT}, '\\x616263'::bytea)"
#: How long a sleep is asked for, and how far short of it a measured one may end: the client's
#: clock reads the statement's round trip, which holds the sleep.
SLEEP_S = 0.2
SLEEP_TOLERANCE_S = 0.01


def _effects() -> dict[str, Effect]:
    out: dict[str, Effect] = {
        "set_config(text, text, boolean)": Effect(
            "SELECT pg_catalog.set_config('conformance.probe', 'on', false) AS v",
            "SELECT pg_catalog.current_setting('conformance.probe') AS v",
            teardown=("SELECT pg_catalog.set_config('conformance.probe', '', false)",),
        ),
        "pg_notify(text, text)": Effect(
            "SELECT pg_catalog.pg_notify('conformance_probe', 'payload') AS v",
            listen="conformance_probe",
        ),
        "pg_sleep(double precision)": Effect(
            f"SELECT pg_catalog.pg_sleep({SLEEP_S}) AS v", elapsed=SLEEP_S
        ),
        "pg_sleep_for(interval)": Effect(
            f"SELECT pg_catalog.pg_sleep_for('{SLEEP_S} seconds'::interval) AS v", elapsed=SLEEP_S
        ),
        "pg_sleep_until(timestamp with time zone)": Effect(
            "SELECT pg_catalog.pg_sleep_until("
            f"pg_catalog.clock_timestamp() + '{SLEEP_S} seconds'::interval) AS v",
            elapsed=SLEEP_S,
        ),
        "setseed(double precision)": Effect(
            "SELECT pg_catalog.setseed(0.5) AS v", "SELECT pg_catalog.random() AS v"
        ),
        "lo_create(oid)": Effect(
            f"SELECT pg_catalog.lo_create({_LARGE_OBJECT}) AS v", _LO_COUNT, teardown=_LO_UNLINK
        ),
        "lo_from_bytea(oid, bytea)": Effect(_LO_ABC + " AS v", _LO_GET, teardown=_LO_UNLINK),
        "lo_unlink(oid)": Effect(
            f"SELECT pg_catalog.lo_unlink({_LARGE_OBJECT}) AS v",
            _LO_COUNT,
            setup=(f"SELECT pg_catalog.lo_create({_LARGE_OBJECT})",),
        ),
        "lo_put(oid, bigint, bytea)": Effect(
            f"SELECT pg_catalog.lo_put({_LARGE_OBJECT}, 1, '\\x7a'::bytea) AS v",
            _LO_GET,
            setup=(_LO_ABC,),
            teardown=_LO_UNLINK,
        ),
        "pg_advisory_unlock_all()": Effect(
            "SELECT pg_catalog.pg_advisory_unlock_all() AS v",
            _LOCKS,
            setup=("SELECT pg_catalog.pg_advisory_lock(4242::bigint)",),
        ),
    }
    for types, key in _LOCK_KEYS.items():
        for lock in ("lock", "lock_shared"):
            for name in (f"pg_advisory_{lock}", f"pg_try_advisory_{lock}"):
                out[f"{name}({types})"] = Effect(
                    f"SELECT pg_catalog.{name}({key}) AS v", _LOCKS, teardown=_UNLOCK_ALL
                )
            # A transaction's lock is held until its statement ends: seen from inside it.
            for name in (f"pg_advisory_xact_{lock}", f"pg_try_advisory_xact_{lock}"):
                out[f"{name}({types})"] = Effect(
                    f"SELECT ({_LOCKS.replace('SELECT mode, granted', 'SELECT count(*)')}) AS v "
                    f"FROM (SELECT pg_catalog.{name}({key}) OFFSET 0) s",
                    _LOCKS,
                )
            unlock = lock.replace("lock", "unlock")
            out[f"pg_advisory_{unlock}({types})"] = Effect(
                f"SELECT pg_catalog.pg_advisory_{unlock}({key}) AS v",
                _LOCKS,
                setup=(f"SELECT pg_catalog.pg_advisory_{lock}({key})",),
                teardown=_UNLOCK_ALL,
            )
    return out


#: How each side-effect function's effect is seen, by ``_key``. A side-effect function with none is
#: not testable (``NOT_TESTABLE``).
EFFECTS = _effects()

#: How long a listening session waits for a notification.
LISTEN_TIMEOUT_S = 2.0


def _run_effect(server: Session, effect: Effect) -> dict[str, Any]:
    """``effect`` on ``server``: the call's answer, the observation's, how long the call took and
    what a listener received, or the first statement that failed. The teardown runs whatever
    failed before it, so no effect outlives its check."""
    listener = None
    try:
        if effect.listen is not None:
            try:
                listener = psycopg.connect(server.dsn, autocommit=True)
                listener.execute(f"LISTEN {effect.listen}".encode())
            except psycopg.Error as exc:
                return {"ok": False, "statement": f"LISTEN {effect.listen}", "error": str(exc)}
        result = _act(server, effect, listener)
    finally:
        if listener is not None:
            listener.close()
        torn = [(sql, server.answer(sql)) for sql in effect.teardown]
    failed = next(((sql, done) for sql, done in torn if not done["ok"]), None)
    if result["ok"] and failed is not None:
        return {"ok": False, "statement": failed[0], "error": failed[1]["error"]}
    return result


def _act(server: Session, effect: Effect, listener: psycopg.Connection | None) -> dict[str, Any]:
    for sql in effect.setup:
        done = server.answer(sql)
        if not done["ok"]:
            return {"ok": False, "statement": sql, "error": done["error"]}
    started = time.monotonic()
    called = server.answer(effect.call)
    took = time.monotonic() - started
    if not called["ok"]:
        return {"ok": False, "statement": effect.call, "error": called["error"]}
    seen = None if effect.observe is None else server.answer(effect.observe)
    if seen is not None and not seen["ok"]:
        return {"ok": False, "statement": effect.observe, "error": seen["error"]}
    heard = None
    if listener is not None:
        notes = listener.notifies(timeout=LISTEN_TIMEOUT_S, stop_after=1)
        heard = [(n.channel, n.payload) for n in notes]
    return {"ok": True, "called": called, "seen": seen, "took": took, "heard": heard}


def effect_difference(effect: Effect, want: dict[str, Any], got: dict[str, Any]) -> str | None:
    """How Provisa's side effect differs from PostgreSQL's; None if it is the same."""
    differs = compare(want["called"], got["called"], values=True, sql=effect.call).difference
    if differs is not None:
        return f"the call: {differs}"
    if effect.observe is not None:
        differs = compare(want["seen"], got["seen"], values=True, sql=effect.observe).difference
        if differs is not None:
            return f"after it, {effect.observe}: {differs}"
    if effect.elapsed is not None and got["took"] < effect.elapsed - SLEEP_TOLERANCE_S:
        return f"the call took {got['took']:.3f}s, asked for {effect.elapsed}s"
    if effect.listen is not None and got["heard"] != want["heard"]:
        return f"a listener received {got['heard']}, PostgreSQL's {want['heard']}"
    return None


def measure_effect(fn: Function, pg: Session, provisa: Session) -> dict[str, Any] | None:
    """``fn``'s side effect on both servers, as an outcome; None if it has no ``EFFECTS`` entry.
    One PostgreSQL itself does not produce is not checked: the check, not the function, failed."""
    effect = EFFECTS.get(_key(fn))
    if effect is None:
        return None
    want = _run_effect(pg, effect)
    if not want["ok"]:
        said = f"PostgreSQL did not produce the side effect: {want['statement']}: {want['error']}"
        return {"outcome": "not_checked", "unchecked": said}
    got = _run_effect(provisa, effect)
    if not got["ok"]:
        named = function_probe.refused_by_name(got["error"])
        detail = {"statement": got["statement"], "error": got["error"]}
        return {"outcome": "refused" if named else "error", "detail": detail}
    differs = effect_difference(effect, want, got)
    if differs is not None:
        return {
            "outcome": "wrong_answer",
            "detail": {"statement": effect.call, "difference": differs},
        }
    return {"outcome": "pass", "side_effect_checked": True}


def _by_design(entry: dict[str, Any], group: str) -> dict[str, Any]:
    entry["outcome"] = "by_design"
    entry["group"] = group
    entry["requirement"] = GROUP_REQUIREMENTS.get(group, "REQ-2167")
    entry["detail"] = GROUPS[group]
    return entry


def _decided_function(entry: dict[str, Any], group: str) -> dict[str, Any]:
    """``entry`` skipped by design under ``group``, a decision about its function by name. One
    PostgreSQL refuses keeps the refusal as its reason (Server administration functions)."""
    if entry["outcome"] == "not_callable":
        entry["reason"] = entry["detail"]
    return _by_design(entry, group)


def measure_overload(
    fn: Function,
    family: str,
    pg: Session,
    provisa: Session,
    classify: Any,
    enums: frozenset[int] = frozenset(),
    *,
    engine: str,
) -> dict[str, Any]:
    """``fn`` on ``engine``, as one outcome. A side-effect function is tested by its side effect on
    ``SIDE_EFFECT_ENGINE`` and skipped by design elsewhere, and is not testable where no client can
    see its effect; a refusal a decision makes is skipped by design under it, as is a refusal of an
    internal type's own function (``internal_types_own``)."""
    entry = _measure_calls(fn, family, pg, provisa, classify, enums)
    group = decided_function(fn)
    if group is not None:
        return _decided_function(entry, group)
    if entry["outcome"] == "not_callable" or "refused_for" in entry:
        return entry
    decided = DECIDED_REFUSALS.get(_key(fn)) or DECIDED_REFUSALS.get(fn.name)
    if decided is not None and entry["outcome"] == "refused":
        return _by_design(entry, decided)
    if entry["outcome"] == "refused" and internal_types_own(fn):
        return _by_design(entry, INTERNAL_TYPE_GROUP)
    if not side_effect(fn):
        return entry
    if engine != SIDE_EFFECT_ENGINE:
        return _by_design(entry, SIDE_EFFECT_GROUP)
    effect = measure_effect(fn, pg, provisa)
    if effect is not None:
        entry.update(effect)
        entry["answers_checked"] = False
        entry["type_and_form_checked"] = False
        entry["side_effect_checked"] = effect["outcome"] == "pass"
        if decided is not None and effect["outcome"] == "refused":
            return _by_design(entry, decided)
        if effect["outcome"] == "error":
            group = decided_error(fn.name, effect["detail"]["error"])
            if group is not None:
                return _by_design(entry, group)
        return entry
    if entry["outcome"] in ("pass", "not_checked"):
        return _by_design(entry, NOT_TESTABLE)
    return entry


def _nothing(want: dict[str, Any]) -> bool:
    """Whether PostgreSQL answered nothing but NULL, or no row."""
    return all(v is None for row in want["rows"] for v in row)


def _refused_for(fn: Function, refused: list[dict[str, str]]) -> str | None:
    """What PostgreSQL's refusals of ``fn`` are of: the call itself only when every one is
    (REQ-2167, amended 2026-11-04, A call PostgreSQL cannot make is skipped by design), its
    arguments when any one is; None if they are neither, as one for the server's state is."""
    kinds = [function_probe.refused_for(fn, r["sqlstate"], r["error"]) for r in refused]
    if all(k == function_probe.THE_CALL for k in kinds):
        return function_probe.THE_CALL
    if function_probe.ITS_ARGUMENTS in kinds:
        return function_probe.ITS_ARGUMENTS
    return None


def _primary(answer: dict[str, Any]) -> str:
    return answer["error"].split("\n", 1)[0]


def _said(call_: Call, got: dict[str, Any]) -> dict[str, str]:
    """A call Provisa failed: its statement, its error and, where it has one, its SQLSTATE, which
    the polyfill check holds a refusal's grid row to (scripts/conformance/polyfill_check.py)."""
    said = {"statement": call_.sql, "error": got["error"]}
    if got.get("sqlstate"):
        said["sqlstate"] = got["sqlstate"]
    return said


def _refusal_answer(
    call_: Call, want: dict[str, Any], got: dict[str, Any]
) -> tuple[str, dict[str, str]] | None:
    """A call whose answer is PostgreSQL's refusal (``Call.refusal``), compared: None if Provisa
    refuses it with the same SQLSTATE and message, else the kind of what it did (wrong, error or
    named) with what it said: another SQLSTATE or message is a wrong answer."""
    expected = f"{want.get('sqlstate', '')} {_primary(want)}"
    if got["ok"]:
        difference = f"answered {got['rows']!r}, PostgreSQL refused: {expected}"
        return "wrong", {"statement": call_.sql, "difference": difference}
    if got.get("sqlstate", "") == want.get("sqlstate", "") and _primary(got) == _primary(want):
        return None
    said = _said(call_, got)
    if function_probe.refused_by_name(got["error"]):
        return "named", said
    if got.get("sqlstate"):
        difference = f"refused: {got['sqlstate']} {_primary(got)}, PostgreSQL {expected}"
        return "wrong", {"statement": call_.sql, "difference": difference}
    return "error", said


#: Why an overload given real values for its form still had none checked.
_FORM_REFUSED = "PostgreSQL refused every call with real values"
_FORMLESS = "with real values PostgreSQL answers NULL or no row"


def _measure_calls(
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
    # The form calls of real values, and how many of them PostgreSQL answered at all.
    form_sqls = {c.sql for c in form_calls(fn)}
    formed_accepted = 0
    wrong: list[dict[str, str]] = []
    errors: list[dict[str, str]] = []
    named: list[dict[str, str]] = []
    decided: list[str] = []  # the groups of the calls a decision answers
    refused: list[dict[str, str]] = []
    made = 0  # the calls PostgreSQL answered with a real value or anything but NULL
    routes: set[str] = set()
    rules: set[str] = set()
    lost = False
    volatile_typed = fn.volatility == "v" and not side_effect(fn)
    for call_ in planned:
        want = _answer(pg, call_)
        if call_.refusal and want["ok"]:
            # A sample that should break the fixture's rule and does not: a gap in the check.
            refused.append({"sqlstate": "", "error": f"PostgreSQL answered {call_.sql}"})
            continue
        if call_.refusal:
            accepted += 1
            made += 1
            answered += call_.answers
            got = _answer(provisa, call_)
            lost = lost or got.get("lost", False)
            found = _refusal_answer(call_, want, got)
            if found is not None:
                kind, said = found
                {"wrong": wrong, "error": errors, "named": named}[kind].append(said)
            continue
        if not want["ok"] or not want["fields"]:
            refused.append(
                {
                    "sqlstate": want.get("sqlstate", ""),
                    "error": want.get("error") or "no result column",
                }
            )
            continue
        accepted += 1
        made += call_.answers or call_.form or not _nothing(want)
        answered += call_.answers
        formed_accepted += call_.sql in form_sqls
        routes.add("catalog" if classify(call_.sql) == "INTERCEPT" else "engine")
        got = _answer(provisa, call_)
        if not got["ok"]:
            said = _said(call_, got)
            group = decided_error(fn.name, got["error"])
            if group is not None:
                decided.append(group)
            else:
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
        formed = False
        if differs is None and call_.form:
            formed, differs = form_check(fn.name, want, got)
        # A volatile overload is never left not tested for being volatile: an answer whose type
        # and NULL-ness are PostgreSQL's is its type and form checked (REQ-2167, amended
        # 2026-11-04 (4)). One acting is tested by its side effect instead.
        typed = differs is None and not call_.values and volatile_typed
        if typed and not formed:
            differs = _null_difference(want, got)
        checked += formed or typed
        if differs is not None:
            group = decided_difference(fn.name, call_.sql, differs) or decided_answer(
                fn.name, call_.sql, want, got
            )
            if group is not None:
                decided.append(group)
            else:
                wrong.append({"statement": call_.sql, "difference": differs})
    entry["calls"] = accepted
    entry["answers_compared"] = answered
    entry["answers_checked"] = answered > 0
    entry["type_and_form_checked"] = checked > 0 and not answered
    entry["served_by"] = "+".join(sorted(routes)) or None
    if not answered:
        why = unchecked(fn, family) or "PostgreSQL refused every value call"
        if form_sqls and not checked:
            why += f"; {_FORMLESS if formed_accepted else _FORM_REFUSED}"
        entry["unchecked"] = why
    if lost:
        entry["connection_lost"] = True
    why = _refused_for(fn, refused) if not made and refused else None
    if why == function_probe.THE_CALL or (accepted == 0 and why is None):
        entry["outcome"] = "not_callable"
        entry["detail"] = (
            f"PostgreSQL refuses the call: {refused[0]['error']}"
            if refused
            else function_probe.NO_CALL
        )
        if why is not None:
            entry["refused_for"] = why
            entry["refusal_sqlstates"] = sorted({r["sqlstate"] for r in refused})
    elif accepted == 0:
        # Refused for the values the check gave it: a gap in its samples, not tested.
        entry["outcome"] = "not_checked"
        entry["rules"] = []
        entry["refused_for"] = why
        entry["refusal_sqlstates"] = sorted({r["sqlstate"] for r in refused})
        entry["unchecked"] = f"PostgreSQL refused every call for {why}: {refused[0]['error']}"
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
    elif decided:
        _by_design(entry, decided[0])
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
    types = list(_SAMPLE_ROWS)
    columns: list[tuple[str, LiteralString]] = [(sample_column(t), t) for t in types]
    relation = psql.Identifier(catalog.FIXTURE_SCHEMA, COLUMN_TABLE)
    declared = psql.SQL(", ").join(
        [
            psql.SQL("id integer"),
            *(
                psql.SQL("{} {}").format(psql.Identifier(name), psql.SQL(typ))
                for name, typ in columns
            ),
        ]
    )
    with psycopg.connect(pg_dsn, autocommit=True) as conn:
        conn.execute(
            psql.SQL("CREATE SCHEMA IF NOT EXISTS {}").format(
                psql.Identifier(catalog.FIXTURE_SCHEMA)
            )
        )
        conn.execute(psql.SQL("DROP TABLE IF EXISTS {}").format(relation))
        conn.execute(psql.SQL("CREATE TABLE {} ({})").format(relation, declared))
        for row in range(VALUE_ROWS):
            # Each sample is cast to its own type: no sample type is one CONCRETE rewrites.
            values = [
                psql.Literal(row + 1),
                *(
                    psql.SQL("{}::{}").format(psql.SQL(_SAMPLE_ROWS[t][row]), psql.SQL(t))
                    for t in types
                ),
            ]
            conn.execute(
                psql.SQL("INSERT INTO {} VALUES ({})").format(relation, psql.SQL(", ").join(values))
            )
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
        # The probe's role writes the table, as the PostgreSQL probe does as its owner, so the
        # privilege functions compare like with like (REQ-2167, amended 2026-11-03, Storage,
        # fixture objects and server files (4)).
        "write_grants": [
            *(
                {"role": "org_admin", "operation": op, "columns": [n for n, _t, _y in registered]}
                for op in ("insert", "update")
            ),
            {"role": "org_admin", "operation": "delete"},
        ],
    }
    out.write_text(yaml.safe_dump([entry]))
    print(f"seeded {COLUMN_RELATION}: {len(columns)} columns, {VALUE_ROWS} rows")


_SCHEMA = catalog.FIXTURE_SCHEMA

#: What the values PostgreSQL takes no literal of, and the objects a call reads, are made of: a
#: table with a column default and statistics objects ANALYZE fills, a hash-partitioned table, a
#: foreign-data wrapper with a server, a large object, a replication slot, a sequence that has
#: answered, database statistics that were reset, so they have a time, a row to share-lock, and a
#: domain for domain_in.
FIXTURE_SQL = (
    f"DROP TABLE IF EXISTS {_SCHEMA}.{STATISTICS_TABLE}, {_SCHEMA}.{PARTITIONED_TABLE}, "
    f"{_SCHEMA}.{MULTIXACT_TABLE}",
    f"CREATE TABLE {_SCHEMA}.{STATISTICS_TABLE} (a integer, b integer, c text DEFAULT 'x')",
    f"INSERT INTO {_SCHEMA}.{STATISTICS_TABLE} "
    "SELECT i % 10, i % 5, 'v' || (i % 3) FROM generate_series(1, 1000) AS i",
    *(
        f"CREATE STATISTICS {_SCHEMA}.{name} (ndistinct, dependencies, mcv) "
        f"ON {columns} FROM {_SCHEMA}.{STATISTICS_TABLE}"
        for name, columns in STATISTICS.items()
    ),
    f"ANALYZE {_SCHEMA}.{STATISTICS_TABLE}",
    f"CREATE TABLE {_SCHEMA}.{PARTITIONED_TABLE} (id integer) PARTITION BY HASH (id)",
    *(
        f"CREATE TABLE {_SCHEMA}.{PARTITIONED_TABLE}_{r} PARTITION OF "
        f"{_SCHEMA}.{PARTITIONED_TABLE} FOR VALUES WITH (MODULUS 2, REMAINDER {r})"
        for r in (0, 1)
    ),
    f"DROP SERVER IF EXISTS {FOREIGN_SERVER}",
    f"DROP FOREIGN DATA WRAPPER IF EXISTS {FOREIGN_DATA_WRAPPER}",
    f"CREATE FOREIGN DATA WRAPPER {FOREIGN_DATA_WRAPPER}",
    f"CREATE SERVER {FOREIGN_SERVER} FOREIGN DATA WRAPPER {FOREIGN_DATA_WRAPPER}",
    f"SELECT pg_catalog.lo_unlink(oid) FROM pg_catalog.pg_largeobject_metadata "
    f"WHERE oid = {FIXTURE_LARGE_OBJECT}",
    f"SELECT pg_catalog.lo_from_bytea({FIXTURE_LARGE_OBJECT}, '\\x616263'::bytea)",
    "SELECT pg_catalog.pg_drop_replication_slot(slot_name) FROM pg_catalog.pg_replication_slots "
    f"WHERE slot_name = '{REPLICATION_SLOT}'",
    f"SELECT pg_catalog.pg_create_physical_replication_slot('{REPLICATION_SLOT}', true)",
    f"DROP SEQUENCE IF EXISTS {_SCHEMA}.{FIXTURE_SEQUENCE}",
    f"CREATE SEQUENCE {_SCHEMA}.{FIXTURE_SEQUENCE}",
    f"SELECT pg_catalog.nextval('{_SCHEMA}.{FIXTURE_SEQUENCE}')",
    "SELECT pg_catalog.pg_stat_reset()",
    f"CREATE TABLE {_SCHEMA}.{MULTIXACT_TABLE} (id integer)",
    f"INSERT INTO {_SCHEMA}.{MULTIXACT_TABLE} VALUES (1)",
    f"DROP DOMAIN IF EXISTS {_SCHEMA}.{FIXTURE_DOMAIN}",
    f"CREATE DOMAIN {_SCHEMA}.{FIXTURE_DOMAIN} AS integer CHECK (VALUE > 0)",
)


def seed_fixture(pg_dsn: str) -> None:
    """``FIXTURE_SQL`` in the job's PostgreSQL, then two transactions share-locking the row of
    ``MULTIXACT_TABLE`` at once, which makes its xmax a multixact."""
    with psycopg.connect(pg_dsn, autocommit=True) as conn:
        for sql in FIXTURE_SQL:
            conn.execute(sql.encode())
    lock = f"SELECT id FROM {_SCHEMA}.{MULTIXACT_TABLE} FOR SHARE".encode()
    with psycopg.connect(pg_dsn) as first, psycopg.connect(pg_dsn) as second:
        first.execute(lock)
        second.execute(lock)
        first.commit()
        second.commit()
    print(f"seeded the fixture: {len(FIXTURE_SQL)} statements and a multixact")


def resolve_fixture(pg: Session) -> dict[str, str]:
    """``FIXTURE``'s values, from the job's PostgreSQL: one it does not answer stays unresolved."""
    resolved = {}
    for name, sql in FIXTURE_QUERIES.items():
        found = pg.answer(sql)
        if found["ok"] and found["rows"] and found["rows"][0][0] is not None:
            resolved[name] = found["rows"][0][0]
    return resolved


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
    FIXTURE.update(resolve_fixture(pg))
    print(f"fixture: {FIXTURE}")
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
                overloads.append(
                    measure_overload(fn, family, pg, provisa, classify, enums, engine=engine)
                )
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
        if i % PROGRESS_EVERY == 0:
            print(
                f"{i}/{len(fns)} after {time.monotonic() - started:.0f}s at {fn.signature}",
                flush=True,
            )
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
    "by_design",
    "wrong_answer",
    "error",
    "refused",
    "not_callable",
    "not_reached",
)


def counts(overloads: list[dict[str, Any]]) -> dict[str, int]:
    """The outcomes, with a pass split by what was compared: an answer, a type and form, or a side
    effect."""
    tally = Counter(o["outcome"] for o in overloads)
    verdicts = Counter(function_report.verdict(o) for o in overloads)
    out = {
        name: verdicts.get(name, 0)
        for name in ("passed", "skipped_by_design", "failed", "not_tested")
    }
    out |= {name: tally.get(name, 0) for name in OUTCOMES}
    out["total"] = len(overloads)
    out["pass_answers_checked"] = sum(
        1 for o in overloads if o["outcome"] == "pass" and o.get("answers_checked")
    )
    out["pass_side_effect_checked"] = sum(
        1 for o in overloads if o["outcome"] == "pass" and o.get("side_effect_checked")
    )
    out["pass_type_and_form_checked"] = (
        out["pass"] - out["pass_answers_checked"] - out["pass_side_effect_checked"]
    )
    # Passed without reaching the engine: the catalog DB answers it on every engine alike.
    out["pass_catalog_answered"] = sum(
        1 for o in overloads if o["outcome"] == "pass" and o.get("served_by") == "catalog"
    )
    return out


def by_design_groups(overloads: Iterable[dict[str, Any]]) -> dict[str, list[str]]:
    """The overloads skipped by design, by the group whose decision skips them."""
    out: dict[str, list[str]] = {}
    for o in overloads:
        if o["outcome"] == "by_design":
            named = f"{o['function']}({o['overload']})"
            reason = o.get("reason")
            out.setdefault(o["group"], []).append(f"{named}: {reason}" if reason else named)
    return {group: sorted(names) for group, names in sorted(out.items())}


def not_checked_reasons(overloads: Iterable[dict[str, Any]]) -> dict[str, int]:
    """Why the overloads not checked were not, most first."""
    tally = Counter(o.get("unchecked", "") for o in overloads if o["outcome"] == "not_checked")
    return dict(sorted(tally.items(), key=lambda item: (-item[1], item[0])))


def _row(label: str, c: dict[str, int]) -> str:
    return (
        f"| {label} | {c['total']} | {c['passed']} | {c['skipped_by_design']} | {c['failed']} "
        f"| {c['not_tested']} | {c['pass_answers_checked']} "
        f"| {c['pass_type_and_form_checked']} | {c['pass_side_effect_checked']} "
        f"| {c['not_checked']} | {c['by_design']} | {c['refused']} "
        f"| {c['wrong_answer']} | {c['error']} | {c['not_callable']} | {c['not_reached']} |"
    )


_HEADER = (
    "| | overloads | passed | skipped by design | failed | not tested "
    "| pass, answers compared | pass, type and form compared "
    "| pass, side effect compared | not checked | skipped by design | refused by name "
    "| wrong answer | engine error | not callable | not reached |",
    "|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|",
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
    for key in ("detail", "served_by", "column", "refused_for", "refusal_sqlstates"):
        if entry.get(key) is not None:
            kept[key] = entry[key]
    if entry.get("rules"):
        kept["rules"] = entry["rules"]
    if entry["outcome"] == "pass":
        kept["check"] = (
            "answers"
            if entry.get("answers_checked")
            else "side_effect"
            if entry.get("side_effect_checked")
            else "type_and_form"
        )
    if entry["outcome"] == "by_design":
        kept["group"] = entry["group"]
        kept["requirement"] = entry["requirement"]
        if entry.get("reason"):
            kept["reason"] = entry["reason"]
    if entry["outcome"] == "not_checked":
        kept["unchecked"] = entry.get("unchecked", "")
    return kept


def merged(reports: dict[str, dict[str, Any] | None]) -> dict[str, Any]:
    """The legs as one document. ``reports`` holds every engine the workflow runs, None for a leg
    that left no result: it counts as not reached, so nothing is guaranteed by its absence.

    An overload passing on every engine is on the guaranteed list, by what every engine checked of
    it (``proof``): ``answers_checked`` on every engine, else ``side_effect_checked`` where a
    side effect was compared on any, else ``type_and_form_checked`` (REQ-2167, WHAT GUARANTEED
    MEANS). One not checked or skipped by design on any engine is not on it: a pass is never a
    call alone. A function is guaranteed when each of its overloads PostgreSQL can call is."""
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
                "type_and_form_checked": any(
                    e is not None and e.get("type_and_form_checked") for e in entries.values()
                ),
                "side_effect_checked": any(
                    e is not None and e.get("side_effect_checked") for e in entries.values()
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
        "by_design_groups": {
            engine: by_design_groups((report or {"overloads": []})["overloads"])
            for engine, report in sorted(reports.items())
        },
        "guaranteed": {
            **{
                proof: sorted(
                    f"{f['function']}({f['overload']})" for f in guaranteed if f["proof"] == proof
                )
                for proof in ("answers_checked", "type_and_form_checked", "side_effect_checked")
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
    if any(e.get("side_effect_checked") for e in present):
        return "side_effect_checked"
    return "type_and_form_checked"


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
        f"compared, {len(guaranteed['type_and_form_checked'])} with type and form compared and "
        f"{len(guaranteed['side_effect_checked'])} with side effect compared, passing on all "
        f"{len(engines)} engines. {len(guaranteed['functions'])} functions have every callable "
        "overload on the list.",
        "",
        "Comparison: " + doc["comparison"] + ". A pass, exact or equivalent, is answers compared "
        "when a call with real values was compared; type and form compared when a value that "
        "cannot be compared (volatile, or the server's own) has PostgreSQL's result type and, for "
        "a string, its canonical form; and side effect compared when what a void or acting "
        "function did, seen through a client, is what PostgreSQL did. Any other overload "
        "PostgreSQL answers is not checked, and no pass: a call that ran proves nothing.",
        "",
    ]
    for engine in engines:
        groups = doc["by_design_groups"][engine]
        lines += [
            f"#### Skipped by design on {engine}",
            "",
            *(
                [
                    f"- {group} ({GROUP_REQUIREMENTS.get(group, 'REQ-2167')}): {len(names)}"
                    for group, names in groups.items()
                ]
                or ["None."]
            ),
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
        "### Per family: pass (answers + type and form + side effect compared), then not checked, "
        "of callable overloads",
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
    """One engine's family: answers + type and form + side effect compared, then not checked, of
    callable."""
    passes = Counter(o.get("check") for o in outcomes if o["outcome"] == "pass")
    unchecked_ = sum(1 for o in outcomes if o["outcome"] == "not_checked")
    callable_ = sum(1 for o in outcomes if o["outcome"] != "not_callable")
    return (
        f"{passes['answers']} + {passes['type_and_form']} + {passes['side_effect']}, "
        f"{unchecked_} not checked, of {callable_}"
    )


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


def _doc() -> str:
    if __doc__ is None:
        raise RuntimeError("function_support has no module docstring")
    return __doc__


def main(argv: list[str]) -> int:
    parser = argparse.ArgumentParser(description=_doc().split("\n\n")[0])
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
        seed_fixture(args.pg)
        return 0
    return merge(args.engines.split(","), args.indir, args.out, args.out_md)


if __name__ == "__main__":
    sys.exit(main(sys.argv))
