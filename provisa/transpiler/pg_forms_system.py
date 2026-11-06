# Copyright (c) 2026 Kenneth Stott
# Canary: 1f6b3e94-82c7-4a5d-9e10-c7d45a0b2e68
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""What a server answers of its own state, and regexp_replace's Nth match, as forms on DuckDB, Trino
and ClickHouse (REQ-2167, REQ-2171), replayed against postgres:17.11's recorded answers.

A Provisa server is what those answers describe: never in recovery, with no logical decoding, no
commit timestamps, no replication origin, no log directory and nobody listening. So each function
below says what PostgreSQL says in that state, its error among them. A call of a function of a
stored value is told by the statement: a value no table holds has never been compressed or toasted
and has the default collation, and a column is refused by name, for its storage and collation are
its source's.

The dialects write an error differently, and ClickHouse folds a constant call in a branch not
taken, so there a check is ``throwIf`` beside the answer, whose message is a constant: PostgreSQL's
message written from a value is refused by name there. A dialect with no entry refuses the call by
name."""

# Requirements: REQ-2167, REQ-2171

from __future__ import annotations

from dataclasses import dataclass

from provisa.core.pg_error import END, MARK
from provisa.transpiler.pg_forms_types import REFUSED

_DUCKDB = frozenset({"duckdb"})
_TRINO = frozenset({"trino", "athena", "presto"})
_CLICKHOUSE = frozenset({"clickhouse"})

_RECOVERY = ("55000", "recovery is not in progress")
_SLOT_ARITIES = range(3, 9)  # slot, upto_lsn, upto_nchanges, then its VARIADIC options
_LOGICAL = ("55000", 'logical decoding requires "wal_level" >= "logical"')


def _quoted(text: str) -> str:
    return "'" + text.replace("'", "''") + "'"


@dataclass(frozen=True)
class _Engine:
    """An engine's spelling of what a form needs: ``{}`` for each operand."""

    text: str  # the type of a text
    bigint: str
    fail: str  # the marked message, as a value
    bytes: str  # a text's length in bytes
    matches_nothing: str  # pattern: whether it matches the empty text
    contains: str  # text, what it holds


_DUCKDB_ENGINE = _Engine(
    "VARCHAR",
    "BIGINT",
    "error({})",
    "strlen({})",
    "regexp_matches('', {})",
    "contains({}, {})",
)
_TRINO_ENGINE = _Engine(
    "VARCHAR",
    "BIGINT",
    "fail({})",
    "length(to_utf8({}))",
    "regexp_like('', {})",
    "strpos({}, {}) > 0",
)
_CLICKHOUSE_ENGINE = _Engine(
    "Nullable(String)",
    "Nullable(Int64)",
    "throwIf(1, {})",
    "length({})",
    "match('', assumeNotNull({}))",
    "position(ifNull({}, ''), {}) > 0",
)
_ENGINES = (
    (_DUCKDB, _DUCKDB_ENGINE),
    (_TRINO, _TRINO_ENGINE),
    (_CLICKHOUSE, _CLICKHOUSE_ENGINE),
)


def _marked(sqlstate: str, message: str) -> str:
    return _quoted(f"{MARK} {sqlstate}: {message} {END}")


def _raised(engine: _Engine, sqlstate: str, message: str, sql_type: str) -> str:
    """PostgreSQL's error, with its message, as a value of ``sql_type``."""
    return f"CAST({engine.fail.format(_marked(sqlstate, message))} AS {sql_type})"


def _checked(
    engine: _Engine, checks: tuple[tuple[str, str, str], ...], answer: str, sql_type: str
) -> str:
    """``answer``, or the error of the first of ``checks`` (condition, sqlstate, message) to hold."""
    if engine is _CLICKHOUSE_ENGINE:
        thrown = " + ".join(
            f"throwIf(coalesce({condition}, 0), {_marked(sqlstate, message)})"
            for condition, sqlstate, message in checks
        )
        return f"if({thrown} = 0, {answer}, NULL)"
    arms = " ".join(
        f"WHEN {condition} THEN {_raised(engine, sqlstate, message, sql_type)}"
        for condition, sqlstate, message in checks
    )
    return f"CASE {arms} ELSE {answer} END"


def _per_engine(form) -> dict[frozenset[str], str]:
    return {dialects: form(engine) for dialects, engine in _ENGINES}


#: Function -> its argument counts and PostgreSQL's error, in the state every Provisa server is in.
_REFUSALS: dict[str, tuple[tuple[int, ...], tuple[str, str]]] = {
    "pg_get_wal_replay_pause_state": ((0,), _RECOVERY),
    "pg_is_wal_replay_paused": ((0,), _RECOVERY),
    "pg_wal_replay_pause": ((0,), _RECOVERY),
    "pg_wal_replay_resume": ((0,), _RECOVERY),
    "pg_last_committed_xact": ((0,), ("55000", "could not get commit timestamp data")),
    "pg_replication_origin_session_reset": ((0,), ("55000", "no replication origin is configured")),
    "pg_sync_replication_slots": (
        (0,),
        ("55000", "replication slots can only be synchronized to a standby server"),
    ),
    "lastval": ((0,), ("55000", "lastval is not yet defined in this session")),
    "pg_ls_logdir": ((0,), ("58P01", 'could not open directory "log": No such file or directory')),
    **{
        f"pg_logical_slot_{verb}": (tuple(_SLOT_ARITIES), _LOGICAL)
        for verb in ("get_changes", "peek_changes", "get_binary_changes", "peek_binary_changes")
    },
}
#: What a statement may call on an engine though it would read a server (metadata_functions).
SERVER_STATE = frozenset(_REFUSALS) | {
    # Refused by name on every engine (pg_forms_enum.NOT_SUPPORTED): the engine's refusal.
    "pg_notify",
    "pg_column_compression",
    "pg_get_publication_tables",
}


#: Each said type no collation applies to, as the statement spells it -> as format_type does.
_NOT_COLLATABLE = {
    "int": "integer",
    "integer": "integer",
    "bigint": "bigint",
    "smallint": "smallint",
    "decimal": "numeric",
    "numeric": "numeric",
    "real": "real",
    "double precision": "double precision",
    "boolean": "boolean",
    "date": "date",
    "timestamp": "timestamp without time zone",
    "timestamptz": "timestamp with time zone",
    "bytea": "bytea",
    "uuid": "uuid",
    "interval": "interval",
    "json": "json",
    "jsonb": "jsonb",
}
_COLLATABLE = ("text", "varchar", "character varying", "bpchar", "char", "character")
_COLLATION_OF_NAME = '"C"'
_DEFAULT_COLLATION = '"default"'


def _collation_row(format_name: str) -> str:
    return "pg_collation_for_" + format_name.replace(" ", "_")


#: (function, argument count, the type said of the value) -> its row.
OVERLOADS: dict[tuple[str, int, str], str] = {
    **{("pg_collation_for", 1, said): "pg_collation_for_text" for said in _COLLATABLE},
    ("pg_collation_for", 1, "name"): "pg_collation_for_name",
    **{
        ("pg_collation_for", 1, said): _collation_row(name)
        for said, name in _NOT_COLLATABLE.items()
    },
}
#: The functions of what a stored value is: a statement says of a value, and a column is not told.
OF_A_STORED_VALUE = frozenset(
    {"pg_collation_for", "pg_column_compression", "pg_column_toast_chunk_id"}
)


def _regexp_refused(engine: _Engine) -> str:
    """What this form cannot answer as PostgreSQL does, whose search keeps its place in the text: an
    anchor, a pattern that matches nothing at all, a replacement of more than a group's number."""
    has = engine.contains.format

    def text(value: str) -> str:
        reads_escapes = engine is _CLICKHOUSE_ENGINE
        return _quoted(value.replace("\\", "\\\\") if reads_escapes else value)

    return " OR ".join(
        (
            has("pattern", "'^'"),
            *(has("pattern", text("\\" + anchor)) for anchor in "AbBmMyYZ"),
            has("replacement", text("\\&")),
            has("replacement", text("\\\\")),
            engine.matches_nothing.format("pattern"),
        )
    )


_DUCKDB_TAIL = "substring(value, CAST(start AS BIGINT))"
_DUCKDB_FOUND = f"regexp_extract_all({_DUCKDB_TAIL}, pattern)"
_DUCKDB_HEAD = (
    f"coalesce(list_sum(list_transform(list_slice(regexp_split_to_array({_DUCKDB_TAIL}, pattern), "
    "1, n), LAMBDA _p : length(_p))), 0) + "
    f"coalesce(list_sum(list_transform(list_slice({_DUCKDB_FOUND}, 1, n - 1), "
    "LAMBDA _m : length(_m))), 0)"
)
_DUCKDB_REPLACED = (
    f"CASE WHEN n = 0 THEN regexp_replace({_DUCKDB_TAIL}, pattern, replacement, 'g') "
    f"WHEN n > len({_DUCKDB_FOUND}) THEN {_DUCKDB_TAIL} ELSE "
    f"left({_DUCKDB_TAIL}, CAST({_DUCKDB_HEAD} AS BIGINT)) || "
    f"regexp_replace(list_extract({_DUCKDB_FOUND}, n), pattern, replacement) || "
    f"substring({_DUCKDB_TAIL}, CAST({_DUCKDB_HEAD} AS BIGINT) + "
    f"length(list_extract({_DUCKDB_FOUND}, n)) + 1) END"
)
_TRINO_TAIL = "substr(value, start)"
#: PostgreSQL's \1 as Trino reads it, and a dollar as nothing but a dollar.
_TRINO_REPLACEMENT = (
    r"regexp_replace(regexp_replace(replacement, '[$]', '\\$0'), '\\([1-9])', '\$$1')"
)
_TRINO_AT = f"regexp_position({_TRINO_TAIL}, pattern, 1, n)"
_TRINO_FOUND = f"element_at(regexp_extract_all({_TRINO_TAIL}, pattern), n)"
_TRINO_REPLACED = (
    f"CASE WHEN n = 0 THEN regexp_replace({_TRINO_TAIL}, pattern, {_TRINO_REPLACEMENT}) "
    f"WHEN {_TRINO_AT} < 1 THEN {_TRINO_TAIL} ELSE "
    f"substr({_TRINO_TAIL}, 1, {_TRINO_AT} - 1) || "
    f"regexp_replace({_TRINO_FOUND}, pattern, {_TRINO_REPLACEMENT}) || "
    f"substr({_TRINO_TAIL}, {_TRINO_AT} + length({_TRINO_FOUND})) END"
)
# Every branch here is compiled whatever the value: no array may be Nullable, so nothing it is made
# from is, and a NULL or empty pattern, which the checks answer, is one the engine will compile.
_CLICKHOUSE_PATTERN = "if(assumeNotNull(pattern) = '', '.', assumeNotNull(pattern))"
_CLICKHOUSE_RESULT = "assumeNotNull(replacement)"
_CLICKHOUSE_START = "assumeNotNull(start)"
_CLICKHOUSE_N = "assumeNotNull(n)"
_CLICKHOUSE_TAIL = f"substringUTF8(assumeNotNull(value), {_CLICKHOUSE_START})"
_CLICKHOUSE_FOUND = f"extractAll({_CLICKHOUSE_TAIL}, concat('(', {_CLICKHOUSE_PATTERN}, ')'))"
_CLICKHOUSE_HEAD = (
    "arraySum(arrayMap(_p -> length(_p), "
    f"arraySlice(splitByRegexp({_CLICKHOUSE_PATTERN}, {_CLICKHOUSE_TAIL}), 1, {_CLICKHOUSE_N}))) + "
    f"arraySum(arrayMap(_m -> length(_m), "
    f"arraySlice({_CLICKHOUSE_FOUND}, 1, {_CLICKHOUSE_N} - 1)))"
)
_CLICKHOUSE_NTH = f"arrayElement({_CLICKHOUSE_FOUND}, {_CLICKHOUSE_N})"
_CLICKHOUSE_REPLACED = (
    f"if({_CLICKHOUSE_N} = 0, "
    f"replaceRegexpAll({_CLICKHOUSE_TAIL}, {_CLICKHOUSE_PATTERN}, {_CLICKHOUSE_RESULT}), "
    f"if({_CLICKHOUSE_N} > length({_CLICKHOUSE_FOUND}), {_CLICKHOUSE_TAIL}, concat("
    f"substring({_CLICKHOUSE_TAIL}, 1, {_CLICKHOUSE_HEAD}), "
    f"replaceRegexpOne({_CLICKHOUSE_NTH}, {_CLICKHOUSE_PATTERN}, "
    f"{_CLICKHOUSE_RESULT}), "
    f"substring({_CLICKHOUSE_TAIL}, {_CLICKHOUSE_HEAD} + "
    f"length({_CLICKHOUSE_NTH}) + 1))))"
)
_REPLACED = {
    id(_DUCKDB_ENGINE): ("left(value, start - 1)", _DUCKDB_REPLACED),
    id(_TRINO_ENGINE): ("substr(value, 1, start - 1)", _TRINO_REPLACED),
    id(_CLICKHOUSE_ENGINE): (
        f"leftUTF8(assumeNotNull(value), {_CLICKHOUSE_START} - 1)",
        _CLICKHOUSE_REPLACED,
    ),
}


def _regexp_replace_nth(engine: _Engine) -> str:
    """regexp_replace(value, pattern, replacement, start, n): strict; n 0 is every match, else the
    Nth, found by the pieces between matches. PostgreSQL checks start, then n."""
    before, replaced = _REPLACED[id(engine)]
    nulls = " OR ".join(f"{p} IS NULL" for p in _REGEXP_PARAMS)
    if engine is _CLICKHOUSE_ENGINE:
        answer = f"concat({before}, {replaced})"
        return (
            f"if({nulls}, NULL, if("
            f"throwIf(coalesce(NOT ({nulls}) AND "
            f"(start < 1 OR n < 0 OR {_regexp_refused(engine)}), 0), "
            f"{_marked('0A000', f'regexp_replace {REFUSED}')}) = 0, "
            f"{answer}, NULL))"
        )
    raised = engine.fail.format
    prefix = _quoted(f"{MARK} 22023: invalid value for parameter ")
    message = "{} || " + _quoted('"{}": ') + " || CAST({} AS VARCHAR) || " + _quoted(f" {END}")
    return (
        f"CASE WHEN {nulls} THEN CAST(NULL AS VARCHAR) "
        f"WHEN start < 1 THEN CAST({raised(message.format(prefix, 'start', 'start'))} AS VARCHAR) "
        f"WHEN n < 0 THEN CAST({raised(message.format(prefix, 'n', 'n'))} AS VARCHAR) "
        f"WHEN {_regexp_refused(engine)} THEN "
        f"{_raised(engine, '0A000', f'regexp_replace {REFUSED}', 'VARCHAR')} "
        f"ELSE {before} || {replaced} END"
    )


_REGEXP_PARAMS = ("value", "pattern", "replacement", "start", "n")

#: repeat on Trino, whose own builds an array: strict, and nothing for a count below one. rpad takes
#: the length in characters and refuses an empty padding.
TRINO_REPEAT = (
    "CASE WHEN value IS NULL OR n IS NULL THEN CAST(NULL AS VARCHAR) "
    "WHEN n < 1 OR value = '' THEN '' "
    "ELSE rpad('', n * length(value), value) END"
)


def _rows() -> tuple[tuple[str, tuple[str, ...], dict[frozenset[str], str]], ...]:
    refusals = [
        (
            name,
            tuple(f"a{i}" for i in range(arity)),
            _per_engine(lambda e, why=why: _raised(e, why[0], why[1], e.text)),
        )
        for name, (arities, why) in _REFUSALS.items()
        for arity in arities
    ]
    collation = [
        (
            _collation_row(name),
            ("value",),
            _per_engine(
                lambda e, name=name: _raised(
                    e, "42804", f"collations are not supported by type {name}", e.text
                )
            ),
        )
        for name in sorted(set(_NOT_COLLATABLE.values()))
    ]
    constant = {
        "pg_collation_for_text": _DEFAULT_COLLATION,
        "pg_collation_for_name": _COLLATION_OF_NAME,
    }
    return (
        *refusals,
        *collation,
        *(
            (name, ("value",), _per_engine(lambda _, said=said: _quoted(said)))
            for name, said in constant.items()
        ),
        # A literal of no said type has no collation.
        (
            "pg_collation_for",
            ("value",),
            _per_engine(lambda e: f"CAST(NULL AS {e.text})"),
        ),
        ("pg_column_compression", ("value",), _per_engine(lambda e: f"CAST(NULL AS {e.text})")),
        (
            "pg_column_toast_chunk_id",
            ("value",),
            _per_engine(lambda e: f"CAST(NULL AS {e.bigint})"),
        ),
        ("regexp_replace", _REGEXP_PARAMS, _per_engine(_regexp_replace_nth)),
        ("regexp_replace_at", _REGEXP_PARAMS, _per_engine(_regexp_replace_nth)),
    )


#: A row: name, parameters, each dialect's form.
ROWS = _rows()
