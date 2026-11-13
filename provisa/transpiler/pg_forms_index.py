# Copyright (c) 2026 Kenneth Stott
# Canary: c81f5e02-7ab3-4d19-86e4-0b92d7a3e6f5
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The index access methods' support functions a client can call (REQ-2171): a btree comparison is
a comparison, whatever the index. A cmp function promises its sign; ``btint2cmp``, ``btboolcmp`` and
``btcharcmp`` promise the difference PostgreSQL's source computes, and a form of them answers it.

A function of a value only PostgreSQL's index machinery holds is strict: NULL is NULL, its input
function raises PostgreSQL's refusal, and any other value is refused by name, since a client has
no way to make one. Each form is in PostgreSQL's syntax but the two that read a list, which are
written per engine with provisa.transpiler.pg_forms_hash's vocabulary."""

# Requirements: REQ-2167, REQ-2171

from __future__ import annotations

from dataclasses import dataclass

from provisa.transpiler.pg_forms_hash import ENGINES, Engine
from provisa.transpiler.pg_forms_types import REFUSED, Form, refused


def _compare(a: str, b: str) -> str:
    return f"CASE WHEN {a} < {b} THEN -1 WHEN {a} > {b} THEN 1 WHEN {a} = {b} THEN 0 END"


def _strict(params: str, sql_type: str, answer: str) -> str:
    nulls = " OR ".join(f"{p} IS NULL" for p in params.split(", "))
    return f"CASE WHEN {nulls} THEN CAST(NULL AS {sql_type}) ELSE {answer} END"


def _integer(name: str, left: str = "BIGINT", right: str = "BIGINT") -> Form:
    return Form(name, "a, b", _compare(f"CAST(a AS {left})", f"CAST(b AS {right})"))


def _float(name: str) -> Form:
    """btfloat8cmp: a NaN sorts above every other value and equals itself."""
    a, b = "CAST(a AS DOUBLE PRECISION)", "CAST(b AS DOUBLE PRECISION)"
    sql = (
        f"CASE WHEN isnan({a}) AND isnan({b}) THEN 0 WHEN isnan({a}) AND b IS NOT NULL THEN 1 "
        f"WHEN isnan({b}) AND a IS NOT NULL THEN -1 ELSE {_compare(a, b)} END"
    )
    return Form(name, "a, b", sql)


def _text(name: str, a: str = "CAST(a AS TEXT)", b: str = "CAST(b AS TEXT)") -> Form:
    """Bytes in order, the C collation's: a name's, and every pattern operator's."""
    return Form(name, "a, b", _compare(a, b))


#: bcTruelen: a character value's trailing spaces are padding.
_UNPADDED = "RTRIM(CAST({} AS TEXT), ' ')"
#: What sorts alike in the C collation and in every collation a server's default may be.
_COLLATES_ALIKE = "'^[0-9a-z]*$'"


def _collated(name: str) -> Form:
    """bttextcmp sorts by the server's collation, which only values of these characters do not
    depend on: any other pair that differs is refused by name."""
    a, b = "CAST(a AS TEXT)", "CAST(b AS TEXT)"
    alike = f"{a} ~ {_COLLATES_ALIKE} AND {b} ~ {_COLLATES_ALIKE}"
    message = refused("0A000", f"'{name} {REFUSED}'", "INTEGER")
    return Form(
        name,
        "a, b",
        _strict(
            "a, b",
            "INTEGER",
            f"CASE WHEN {a} = {b} THEN 0 WHEN {alike} THEN {_compare(a, b)} ELSE {message} END",
        ),
    )


def _of_no_client(name: str, params: str, sql_type: str) -> Form:
    """A value no client can make: NULL, or refused by name, in a test since a refusal is no
    value of every type."""
    message = refused("0A000", f"'{name} {REFUSED}'", "INTEGER")
    values = " AND ".join(f"{p} IS NOT NULL" for p in params.split(", "))
    return Form(
        name, params, f"CASE WHEN {values} AND {message} = 0 THEN CAST(NULL AS {sql_type}) END"
    )


def _input(name: str, type_name: str) -> Form:
    """Such a type's input function, which PostgreSQL refuses by the type's name."""
    message = refused("0A000", f"'cannot accept a value of type {type_name}'", "VARCHAR")
    return Form(name, "a", _strict("a", "VARCHAR", message))


_NO_COLLATION = refused(
    "42P22", "'could not determine which collation to use for string comparison'", "BOOLEAN"
)

SUPPORT_FUNCTIONS: tuple[Form, ...] = (
    Form(
        "btboolcmp",
        "a, b",
        "(CASE WHEN a IS NULL THEN NULL WHEN a THEN 1 ELSE 0 END) - "
        "(CASE WHEN b IS NULL THEN NULL WHEN b THEN 1 ELSE 0 END)",
    ),
    Form("btint2cmp", "a, b", "CAST(a AS INTEGER) - CAST(b AS INTEGER)"),
    _integer("btint4cmp"),
    _integer("btint8cmp"),
    _integer("btint24cmp"),
    _integer("btint28cmp"),
    _integer("btint42cmp"),
    _integer("btint48cmp"),
    _integer("btint82cmp"),
    _integer("btint84cmp"),
    _integer("btoidcmp"),
    _float("btfloat4cmp"),
    _float("btfloat8cmp"),
    _float("btfloat48cmp"),
    _float("btfloat84cmp"),
    _text("btnamecmp"),
    _text("btnametextcmp"),
    _text("bttextnamecmp"),
    _text("bttext_pattern_cmp"),
    _text("btbpchar_pattern_cmp", _UNPADDED.format("a"), _UNPADDED.format("b")),
    _text("gin_cmp_tslexeme"),
    _text("gin_compare_jsonb"),
    _collated("bttextcmp"),
    Form("btarraycmp", "a, b", _compare("a", "b")),
    Form("btequalimage", "a", _strict("a", "BOOLEAN", "TRUE")),
    Form("btvarstrequalimage", "a", _strict("a", "BOOLEAN", _NO_COLLATION)),
    _of_no_client("btrecordcmp", "a, b", "INTEGER"),
    _of_no_client("btrecordimagecmp", "a, b", "INTEGER"),
    _input("brin_bloom_summary_in", "pg_brin_bloom_summary"),
    _input("brin_minmax_multi_summary_in", "brin_minmax_multi_summary"),
    _input("gtsvectorin", "gtsvector"),
    _of_no_client("brin_bloom_summary_out", "a", "VARCHAR"),
    _of_no_client("brin_bloom_summary_send", "a", "BYTEA"),
    _of_no_client("brin_minmax_multi_summary_out", "a", "VARCHAR"),
    _of_no_client("brin_minmax_multi_summary_send", "a", "BYTEA"),
    _of_no_client("gtsvectorout", "a", "VARCHAR"),
)
#: The forms that read an argument's text or a value of a type only PostgreSQL has.
HELD = frozenset(
    {
        *(form.name for form in SUPPORT_FUNCTIONS),
        "btoidvectorcmp",
        "bttidcmp",
        "btcharcmp",
        "spg_poly_quad_compress",
    }
) - {"btboolcmp", "btarraycmp", "btfloat4cmp", "btfloat8cmp", "btfloat48cmp", "btfloat84cmp"}
ARRAY_INPUT = frozenset({"btarraycmp"})


def _tid_cmp(e: Engine) -> str:
    def part(value: str, which: int) -> str:
        inner = f"replace(replace({e.text.format(value)}, '(', ''), ')', '')"
        return e.big.format(e.part.format(inner, "','", which))

    return (
        "CASE WHEN a IS NULL OR b IS NULL THEN NULL "
        f"WHEN {part('a', 1)} <> {part('b', 1)} THEN {_compare(part('a', 1), part('b', 1))} "
        f"ELSE {_compare(part('a', 2), part('b', 2))} END"
    )


def _char_cmp(e: Engine) -> str:
    """btcharcmp: the bytes' difference, unsigned; a text past ASCII is no "char"'s one byte."""

    def byte(value: str) -> str:
        return e.ascii.format(f"substr({e.text.format(value)}, 1, 1)")

    return f"{e.big.format(byte('a'))} - {e.big.format(byte('b'))}"


def _oidvector_key(e: Engine, value: str) -> str:
    """Each oid at a fixed width, so the text sorts as the oids do."""
    return e.join.format(
        e.map.format(
            e.split.format(e.text.format(value), "' '"), "_o", e.lpad.format("_o", 10, "'0'")
        )
    )


def _oidvector_cmp(e: Engine) -> str:
    """btoidvectorcmp: the shorter first, then oid by oid."""

    def size(value: str) -> str:
        text = e.text.format(value)
        return f"(length({text}) - length(replace({text}, ' ', '')))"

    return (
        f"CASE WHEN a IS NULL OR b IS NULL THEN NULL "
        f"WHEN {size('a')} < {size('b')} THEN -1 WHEN {size('a')} > {size('b')} THEN 1 "
        f"ELSE {_compare(_oidvector_key(e, 'a'), _oidvector_key(e, 'b'))} END"
    )


#: A number as float8out writes it, the only text a form may hand back as it read it.
_SHOWN_AS_POSTGRESQL_DOES = "'^-?(0|[1-9][0-9]*)([.][0-9]*[1-9])?$'"
_POINT_NUMBER = "'-?[0-9.]+(?:e[-+]?[0-9]+)?'"


@dataclass(frozen=True)
class _Lists:
    """An engine's spelling of what reading a polygon's numbers needs."""

    tokens: str
    number: str
    highest: str
    lowest: str
    keep: str  # list, name, condition
    matches: str


_LISTS = {
    "duckdb": _Lists(
        "regexp_extract_all({}, {})",
        "CAST({} AS DOUBLE)",
        "list_max",
        "list_min",
        "list_filter({0}, {1} -> {2})",
        "regexp_matches({}, {})",
    ),
    "trino": _Lists(
        "regexp_extract_all({}, {})",
        "CAST({} AS DOUBLE)",
        "array_max",
        "array_min",
        "filter({0}, {1} -> {2})",
        "regexp_like({}, {})",
    ),
}


def _polygon_box(e: Engine, lists: _Lists) -> str:
    """spg_poly_quad_compress: the bounding box, (high x, high y),(low x, low y). A polygon's own
    number is the answer's, so one PostgreSQL would write otherwise is refused by name."""
    tokens = lists.tokens.format(e.text.format(e.value), _POINT_NUMBER)

    def of(side: int, chosen: str) -> str:
        indexes = e.zero_to.format(e.quotient.format(e.size.format("_n"), 2))
        numbers = e.map.format(indexes, "_j", lists.number.format(f"_n[2 * _j + {side}]"))
        is_it = f"{lists.number.format('_y')} = {chosen}({numbers})"
        return lists.keep.format("_n", "_y", is_it) + "[1]"

    box = (
        f"'(' || {of(1, lists.highest)} || ',' || {of(2, lists.highest)} || '),(' "
        f"|| {of(1, lists.lowest)} || ',' || {of(2, lists.lowest)} || ')'"
    )
    shown = lists.matches.format("_y", _SHOWN_AS_POSTGRESQL_DOES)
    every = f"{e.size.format(lists.keep.format('_n', '_y', f'NOT {shown}'))} = 0"
    message = refused("0A000", f"'spg_poly_quad_compress {REFUSED}'", "VARCHAR")
    return _strict(
        "value",
        "VARCHAR",
        e.let.format("_n", tokens, f"CASE WHEN {every} THEN {box} ELSE {message} END"),
    )


#: The forms written per engine, strict, and what a NULL argument answers as.
PER_ENGINE_RESULTS = {
    "btoidvectorcmp": "INT",
    "bttidcmp": "INT",
    "btcharcmp": "INT",
    "spg_poly_quad_compress": "TEXT",
}


def per_engine() -> dict[tuple[str, str], dict[str, str]]:
    """Each form written per engine, by its function and parameters."""
    return {
        ("btoidvectorcmp", "a, b"): {name: _oidvector_cmp(e) for name, e in ENGINES},
        ("bttidcmp", "a, b"): {name: _tid_cmp(e) for name, e in ENGINES},
        ("btcharcmp", "a, b"): {name: _char_cmp(e) for name, e in ENGINES},
        # ClickHouse's is the polyfill grid's row (provisa.transpiler.polyfill.clickhouse).
        ("spg_poly_quad_compress", "value"): {
            name: _polygon_box(e, _LISTS[name]) for name, e in ENGINES if name in _LISTS
        },
    }
