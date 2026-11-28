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

# Requirements: REQ-2167, REQ-2171, REQ-2236

from __future__ import annotations

import re
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


def _regexp_refused(engine: _Engine, *, empty: bool = True) -> str:
    """What this form cannot answer as PostgreSQL does, whose search keeps its place in the text: an
    anchor, a pattern that matches nothing at all, a replacement of more than a group's number.

    The simple ``^`` anchor is included for DuckDB (which emits Provisa's own 0A000 refusal) but
    excluded for Trino and ClickHouse: commit 1850a660e7 changed those MATRIX entries from
    "refused" to "error", so their engines now raise their own error for a ``^``-anchored pattern
    on the 5-arg form.  The 4-arg DuckDB-only Macro guards ``^`` inline anyway, but the 5-arg
    form uses this function for all three engines, so the distinction is engine-scoped here."""
    has = engine.contains.format

    def text(value: str) -> str:
        reads_escapes = engine is _CLICKHOUSE_ENGINE
        return _quoted(value.replace("\\", "\\\\") if reads_escapes else value)

    # Trino and ClickHouse: no '^' guard — let the engine return its own error (REQ-2167).
    caret = () if engine in (_TRINO_ENGINE, _CLICKHOUSE_ENGINE) else (has("pattern", "'^'"),)
    return " OR ".join(
        (
            *caret,
            *(has("pattern", text("\\" + anchor)) for anchor in "AbBmMyYZ"),
            has("replacement", text("\\&")),
            has("replacement", text("\\\\")),
            *((engine.matches_nothing.format("pattern"),) if empty else ()),
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
#: A pattern that matches the empty text, on ClickHouse (REQ-2236): PostgreSQL searches from start
#: and, past an empty match, copies the next character and searches again, where extractAll and
#: splitByRegexp skip an empty match. Its search is written here: at each character from start, the
#: pattern's match there is the Nth (or n is 0) and replaced, or copied; an empty match copies the
#: character after it; a character where nothing matches is copied. The pattern is read at the
#: text's first character as it is, and past it behind one character of the text (?s:.), so its ^
#: and \\A match only at the text's start, as PostgreSQL's do. The replacement's \\1 to \\9 are
#: the match's groups (replaceRegexpOne of the text from there).
_CH_V = "assumeNotNull(value)"
_CH_P = "assumeNotNull(pattern)"
_CH_AT = "substringUTF8(" + _CH_V + ", _i - 1)"
_CH_LENGTH = (
    f"if(_i = 1, if(match({_CH_V}, concat('^(?:', {_CH_P}, ')')), "
    f"toInt64(lengthUTF8(extract({_CH_V}, concat('^((?:', {_CH_P}, '))')))), -1), "
    f"if(match({_CH_AT}, concat('^(?s:.)(?:', {_CH_P}, ')')), "
    f"toInt64(lengthUTF8(extract({_CH_AT}, concat('^(?s:.)((?:', {_CH_P}, '))')))), -1))"
)
_CH_SAID = (
    f"if(_i = 1, replaceRegexpOne({_CH_V}, concat('^(?:', {_CH_P}, ')(?s:.*)'), "
    f"{_CLICKHOUSE_RESULT}), replaceRegexpOne({_CH_AT}, "
    f"concat('^(?s:.)(?:', {_CH_P}, ')(?s:.*)'), {_CLICKHOUSE_RESULT}))"
)
_CH_POSITIONS = (
    f"range(toUInt64(greatest({_CLICKHOUSE_START}, 1)), toUInt64(lengthUTF8({_CH_V})) + 2)"
)
_CLICKHOUSE_EMPTY_MATCH = (
    f"concat(leftUTF8({_CH_V}, {_CLICKHOUSE_START} - 1), tupleElement(arrayFold("
    "(_acc, _i, _l) -> if(_i < tupleElement(_acc, 2), _acc, if(_l < 0, "
    f"tuple(concat(tupleElement(_acc, 1), substringUTF8({_CH_V}, _i, 1)), toUInt64(_i + 1), "
    "tupleElement(_acc, 3)), "
    f"tuple(concat(tupleElement(_acc, 1), if({_CLICKHOUSE_N} = 0 OR "
    f"tupleElement(_acc, 3) + 1 = {_CLICKHOUSE_N}, {_CH_SAID}, "
    f"substringUTF8({_CH_V}, _i, _l)), if(_l = 0, substringUTF8({_CH_V}, _i, 1), '')), "
    "toUInt64(_i + greatest(_l, 1)), tupleElement(_acc, 3) + 1))), "
    f"{_CH_POSITIONS}, arrayMap(_i -> {_CH_LENGTH}, {_CH_POSITIONS}), "
    f"tuple('', toUInt64(greatest({_CLICKHOUSE_START}, 1)), toUInt64(0))), 1))"
)

#: The empty-match search of _CLICKHOUSE_EMPTY_MATCH on DuckDB and Trino (REQ-2236): RE2's and
#: Trino's searches go past an empty match that ends where a non-empty match ended
#: (regexp_replace('abc', 'a|', 'X', 'g') is 'XXbXcX' on PostgreSQL, 'XbXcX' there), so each
#: character's match is read on its own, as there, and the matches are chosen as PostgreSQL chooses
#: them. ``_i`` is a character's place; the pattern is read behind one character of the text from
#: the second on, so its ^ and \A match only at the text's start.
_WALK_HEAD = "'\\A(?:' || pattern || ')'"
_WALK_BEHIND = "'\\A(?s:.)(?:' || pattern || ')'"
_WALK_HEAD_GROUP = "'\\A((?:' || pattern || '))'"
_WALK_BEHIND_GROUP = "'\\A(?s:.)((?:' || pattern || '))'"
_WALK_HEAD_REST = "'\\A(?:' || pattern || ')(?s:.*)'"
_WALK_BEHIND_REST = "'\\A(?s:.)(?:' || pattern || ')(?s:.*)'"


@dataclass(frozen=True)
class _Walk:
    """An engine's spelling of the empty-match search: ``{}`` for each operand."""

    matches: str  # text, pattern
    group: str  # text, pattern: its first group
    replaced: str  # text, pattern: the text with its first match replaced by the replacement
    tail: str  # the text from a place
    char: str  # the character at a place
    span: str  # place, length: the text there
    #: the fold of each place from start to the text's end (``{places}``, ``{place}``, ``{length}``
    #: of the item, ``{step}`` of the state ``_a``, ``{start}``): the text it builds
    fold: str
    #: the state of the text built ``{0}``, the next place read ``{1}`` and the matches ``{2}``
    state: str


_DUCKDB_WALK = _Walk(
    "regexp_matches({}, {})",
    "regexp_extract({}, {}, 1)",
    "regexp_replace({}, {}, replacement)",
    "substring(value, {})",
    "substring(value, {}, 1)",
    "substring(value, {}, {})",
    "list_reduce(list_transform(range({start}, length(value) + 2), LAMBDA _i : "
    "struct_pack(o := '', nx := _i, k := {length})), LAMBDA _a, _x : {step}, "
    "struct_pack(o := '', nx := CAST({start} AS BIGINT), k := CAST(0 AS BIGINT))).o",
    "struct_pack(o := {0}, nx := {1}, k := {2})",
)
#: Trino's sequence holds at most 10000 places: a longer text is refused by name (_WALK_BOUND).
_TRINO_WALK = _Walk(
    "regexp_like({}, {})",
    "regexp_extract({}, {}, 1)",
    f"regexp_replace({{}}, {{}}, {_TRINO_REPLACEMENT})",
    "substr(value, {})",
    "substr(value, {}, 1)",
    "substr(value, {}, {})",
    "reduce(transform(sequence({start}, length(value) + 1), _i -> "
    "CAST(ROW(_i, {length}) AS ROW(nx BIGINT, k BIGINT))), "
    "CAST(ROW('', {start}, 0) AS ROW(o VARCHAR, nx BIGINT, k BIGINT)), (_a, _x) -> {step}, "
    "_a -> _a.o)",
    "CAST(ROW({0}, {1}, {2}) AS ROW(o VARCHAR, nx BIGINT, k BIGINT))",
)
#: The longest text the walk reads on Trino, whose sequence of places (start to the text's end and
#: one past it) holds at most 10000.
_WALK_BOUND = 9998


def _walk_length(walk: _Walk) -> str:
    """The length of the match at place ``_i``, or -1 where none starts there."""
    behind = walk.tail.format("_i - 1")
    return (
        f"CASE WHEN _i = 1 THEN CASE WHEN {walk.matches.format('value', _WALK_HEAD)} "
        f"THEN length({walk.group.format('value', _WALK_HEAD_GROUP)}) ELSE -1 END "
        f"WHEN {walk.matches.format(behind, _WALK_BEHIND)} "
        f"THEN length({walk.group.format(behind, _WALK_BEHIND_GROUP)}) ELSE -1 END"
    )


def _empty_match_walk(walk: _Walk) -> str:
    """regexp_replace(value, pattern, replacement, start, n) where the pattern matches the empty
    text, as PostgreSQL searches: the text before start, then from each place the match there is
    the Nth (or n is 0) and replaced, or copied; an empty match copies the character after it; a
    character where nothing matches is copied; a non-empty match is followed by the search at its
    end, where an empty match is one."""
    behind = walk.tail.format("_x.nx - 1")
    said = (
        f"CASE WHEN _x.nx = 1 THEN {walk.replaced.format('value', _WALK_HEAD_REST)} "
        f"ELSE {walk.replaced.format(behind, _WALK_BEHIND_REST)} END"
    )
    char = walk.char.format("_x.nx")
    copied = walk.state.format(f"_a.o || {char}", "_x.nx + 1", "_a.k")
    matched = walk.state.format(
        f"_a.o || CASE WHEN n = 0 OR _a.k + 1 = n THEN {said} "
        f"ELSE {walk.span.format('_x.nx', '_x.k')} END "
        f"|| CASE WHEN _x.k = 0 THEN {char} ELSE '' END",
        "_x.nx + greatest(_x.k, 1)",
        "_a.k + 1",
    )
    step = f"CASE WHEN _x.nx < _a.nx THEN _a WHEN _x.k < 0 THEN {copied} ELSE {matched} END"
    return walk.fold.format(start="start", length=_walk_length(walk), step=step)


_WALKS = {id(_DUCKDB_ENGINE): _DUCKDB_WALK, id(_TRINO_ENGINE): _TRINO_WALK}
#: The replacement the walk reads otherwise than PostgreSQL: RE2's \0 is the text from the
#: character behind the match to the end, where PostgreSQL copies \0 as written.
_WALK_UNREAD = {id(_DUCKDB_ENGINE): "'\\0'"}
#: The walk's operands, each a hole of a polyfill grid row's pattern ($v the text, $p the pattern,
#: $r the replacement), start 1 and every match.
_WALK_HOLES = {"value": "$v", "pattern": "$p", "replacement": "$r", "start": "1", "n": "0"}
_WALK_OPERAND = re.compile(r"\b(?:value|pattern|replacement|start|n)\b")


def duckdb_every_match() -> str:
    """regexp_replace(text, pattern, replacement, 'g') on DuckDB, of the holes $v, $p and $r: where
    the pattern matches the empty text and the text holds a match that is not empty, PostgreSQL's
    search (_empty_match_walk), which a replacement holding \\0 is refused by name in; RE2's every
    match otherwise, which is PostgreSQL's there (each match empty, or none)."""
    walk = _WALK_OPERAND.sub(lambda m: _WALK_HOLES[m.group()], _empty_match_walk(_DUCKDB_WALK))
    refused = _raised(_DUCKDB_ENGINE, "0A000", f"regexp_replace {REFUSED}", "VARCHAR")
    return (
        "CASE WHEN regexp_matches('', $p) AND regexp_replace($v, $p, '', 'g') <> $v THEN "
        f"CASE WHEN contains($r, '\\0') THEN {refused} ELSE {walk} END "
        "ELSE regexp_replace($v, $p, $r, 'g') END"
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
    Nth, found by the pieces between matches. PostgreSQL checks start, then n. On ClickHouse a
    pattern that matches the empty text is searched as PostgreSQL searches it
    (_CLICKHOUSE_EMPTY_MATCH)."""
    before, replaced = _REPLACED[id(engine)]
    nulls = " OR ".join(f"{p} IS NULL" for p in _REGEXP_PARAMS)
    if engine is _CLICKHOUSE_ENGINE:
        answer = (
            f"if(match('', assumeNotNull(pattern)), {_CLICKHOUSE_EMPTY_MATCH}, "
            f"concat({before}, {replaced}))"
        )
        return (
            f"if({nulls}, NULL, if("
            f"throwIf(coalesce(NOT ({nulls}) AND "
            f"(start < 1 OR n < 0 OR {_regexp_refused(engine, empty=False)}), 0), "
            f"{_marked('0A000', f'regexp_replace {REFUSED}')}) = 0, "
            f"{answer}, NULL))"
        )
    raised = engine.fail.format
    prefix = _quoted(f"{MARK} 22023: invalid value for parameter ")
    message = "{} || " + _quoted('"{}": ') + " || CAST({} AS VARCHAR) || " + _quoted(f" {END}")
    refused = _raised(engine, "0A000", f"regexp_replace {REFUSED}", "VARCHAR")
    empty = engine.matches_nothing.format("pattern")
    # A pattern that matches the empty text is searched as PostgreSQL searches it (_WALKS); what
    # the walk cannot read is refused by name there.
    unread = []
    if id(engine) in _WALK_UNREAD:
        unread.append(engine.contains.format("replacement", _WALK_UNREAD[id(engine)]))
    if engine is _TRINO_ENGINE:
        unread.append(f"length(value) > {_WALK_BOUND}")
    walk_refused = f"WHEN {empty} AND ({' OR '.join(unread)}) THEN {refused} " if unread else ""
    return (
        f"CASE WHEN {nulls} THEN CAST(NULL AS VARCHAR) "
        f"WHEN start < 1 THEN CAST({raised(message.format(prefix, 'start', 'start'))} AS VARCHAR) "
        f"WHEN n < 0 THEN CAST({raised(message.format(prefix, 'n', 'n'))} AS VARCHAR) "
        f"WHEN {_regexp_refused(engine, empty=False)} THEN {refused} "
        f"{walk_refused}"
        f"WHEN {empty} THEN {before} || {_empty_match_walk(_WALKS[id(engine)])} "
        f"ELSE {before} || {replaced} END"
    )


_REGEXP_PARAMS = ("value", "pattern", "replacement", "start", "n")
#: regexp_replace's Nth match on ClickHouse, its parameters REGEXP_PARAMS, which the polyfill grid
#: writes with a literal pattern read as PostgreSQL reads it (provisa.transpiler.polyfill.regex,
#: REQ-2236): ClickHouse's RE2 reads the pattern it is given at run time.
REGEXP_PARAMS = _REGEXP_PARAMS
CLICKHOUSE_REGEXP_REPLACE_NTH = _regexp_replace_nth(_CLICKHOUSE_ENGINE)

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
