# Copyright (c) 2026 Kenneth Stott
# Canary: 3a9f1c62-8e4d-4b07-9d25-6c1e7b0a5f38
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.
"""REQ-2142: a mask's regex is written once in the universal dialect, refused by name when it is
not, and lowered to each engine's own flavour, where it masks a value the same way everywhere."""

from __future__ import annotations

import re

import duckdb
import pytest
from pydantic import ValidationError

from provisa.api.admin.masks_router import pattern_report
from provisa.compiler.sql_literals import sql_literal
from provisa.core.models import Mask
from provisa.security.mask_presets import PRESETS, preset_pattern
from provisa.security.masking import (
    MaskingRule,
    MaskType,
    apply_mask_to_value,
    build_mask_expression,
)
from provisa.security.universal_regex import FLAVOURS, NotUniversal, check, lower, parse, warnings
from provisa.transpiler.transpile import transpile

ENGINES = sorted(d for d in FLAVOURS if d != "python")


def _presets() -> list[tuple[str, str, str, str, str]]:
    out = []
    for p in PRESETS:
        ex = p["example"]
        out.append(
            (
                p["id"],
                preset_pattern(p, **ex.get("params", {})),
                p["replace"],
                ex["input"],
                ex["output"],
            )
        )
    return out


# (pattern, replace, input, masked output): the presets, then the corners where engines differ.
CORPUS = [(p, r, x, y) for _id, p, r, x, y in _presets()] + [
    # . is any character but a newline, everywhere (PostgreSQL's own . takes one)
    (r"a.c", "X", "a\nc abc", "a\nc X"),
    # ^ and $ anchor to the whole value, never to a line, nor before a final newline
    # (neither matches, so each value is fully masked)
    (r"^x$", "Y", "x\n", "****"),
    (r"^b", "Y", "a\nb", "****"),
    # every match is replaced
    (r"o", "0", "foo boo", "f00 b00"),
    # \d \w \s are ASCII
    (r"\w", "_", "é1", "é_"),
    (r"\s+", " ", "a \t\n b", "a b"),
    (r"\d", "#", "٣3", "٣#"),
    # a set's special members, escaped
    (r"[\]\-\\^]", "!", "a]b-c\\d^e", "a!b!c!d!e"),
    (r"[^a-c]", ".", "abcd", "abc."),
    # non-capturing groups keep the numbering of the groups that capture
    (r"(?:x|y)(\d)(?:z)?(\w)", "<$2$1>", "x1a y2bz", "<a1> <b2>z"),
    # literal $ and backslash in a replacement; a digit after a reference stays a digit
    (r"(\d)", "$$\\$12", "7", "$\\72"),
    # bounds
    (r"a{2,3}", "-", "aaaaaaa", "--a"),
    (r"(ab){2}", "[$1]", "ababab", "[ab]ab"),
    # metacharacters as literals
    (r"\.\*\+\?\(\)\{\}\|\$", "ok", "x.*+?(){}|$y", "xoky"),
    # alternation whose longer branch comes first
    (r"(cat|ca)t?", "<$1>", "cat ca", "<cat> <ca>"),
]


# --------------------------------------------------------------------------- #
# The dialect: what is refused, by name
# --------------------------------------------------------------------------- #


@pytest.mark.parametrize(
    ("pattern", "construct"),
    [
        (r"a(?=b)", "lookahead"),
        (r"a(?!b)", "lookahead"),
        (r"(?<=a)b", "lookbehind"),
        (r"(?<!a)b", "lookbehind"),
        (r"(a)\1", "backreference"),
        (r"a*?", "lazy_quantifier"),
        (r"a{2,3}?", "lazy_quantifier"),
        (r"a++", "possessive_quantifier"),
        (r"(?>a)", "possessive_quantifier"),
        (r"(?<n>a)", "named_group"),
        (r"(?P<n>a)", "named_group"),
        (r"\bx", "word_boundary"),
        (r"\Bx", "word_boundary"),
        (r"\p{L}", "unicode_class"),
        (r"[[:alpha:]]", "posix_class"),
        (r"(?i)a", "inline_flags"),
        (r"(?i:a)", "inline_flags"),
        (r"\x41", "unknown_escape"),
        (r"\A", "unknown_escape"),
        (r"[\D]", "negated_class_escape_in_set"),
        (r"a{,3}", "open_bound"),
        (r"a{", "open_bound"),
        (r"a{1001}", "bound_too_large"),
        (r"(a", "unbalanced_paren"),
        (r"a)", "unbalanced_paren"),
        (r"[a", "unbalanced_bracket"),
        (r"[a[b]", "unbalanced_bracket"),
        (r"[]", "empty_class"),
        (r"[z-a]", "bad_range"),
        (r"a{3,2}", "bad_range"),
        (r"*a", "nothing_to_repeat"),
        (r"a**", "nothing_to_repeat"),
        (r"^*", "nothing_to_repeat"),
        (r"(a|ab)", "alternation_prefix"),
        (r"(|a)", "alternation_prefix"),
    ],
)
def test_a_construct_outside_the_dialect_is_refused_by_name(pattern: str, construct: str) -> None:
    with pytest.raises(NotUniversal) as caught:
        parse(pattern)
    assert caught.value.construct == construct
    assert str(caught.value)  # a sentence: what, where and why


@pytest.mark.parametrize(
    ("replace", "construct"),
    [
        ("$2", "replace_group_out_of_range"),
        ("$0", "replace_bad_escape"),
        ("$x", "replace_bad_escape"),
        ("cost $", "replace_bad_escape"),
        ("${n}", "replace_named_ref"),
    ],
)
def test_a_replacement_outside_the_dialect_is_refused_by_name(replace: str, construct: str) -> None:
    with pytest.raises(NotUniversal) as caught:
        check(r"(a)", replace)
    assert (caught.value.construct, getattr(caught.value, "field")) == (construct, "replace")


def test_a_group_past_the_ninth_counting_non_capturing_ones_cannot_be_referenced() -> None:
    pattern = "(?:a)" * 9 + "(b)"
    with pytest.raises(NotUniversal) as caught:
        check(pattern, "$1")
    assert caught.value.construct == "replace_group_out_of_range"


def test_an_alternation_whose_branches_begin_alike_is_allowed_with_a_warning() -> None:
    assert [w.construct for w in warnings(parse(r"(ab|a)"))] == ["alternation_overlap"]
    assert [w.construct for w in warnings(parse(r"x(a.|ab)"))] == ["alternation_overlap"]
    assert warnings(parse(r"(cat|dog)")) == []
    assert warnings(parse(r"(\d+|-)")) == []


# --------------------------------------------------------------------------- #
# Lowering: every engine's form
# --------------------------------------------------------------------------- #


@pytest.mark.parametrize("engine", ENGINES)
@pytest.mark.parametrize(("pattern", "replace", "value", "masked"), CORPUS)
def test_every_universal_pattern_lowers_for_every_engine(
    engine, pattern, replace, value, masked
) -> None:
    sql = transpile(_select(pattern, replace, "x"), engine)
    assert "provisa_regex_replace" not in sql.lower()
    assert "REGEXP_REPLACE" in sql.upper()


def test_each_engine_gets_its_own_flavour() -> None:
    pattern, replace = r"^(?:x)(\d+)\.(.)$", "$2-$1$$"
    assert lower(pattern, replace, "trino") == (r"\A(?:x)([0-9]+)\.(.)\z", r"$2-$1\$")
    assert lower(pattern, replace, "databricks") == lower(pattern, replace, "trino")
    assert lower(pattern, replace, "duckdb") == (r"^(?:x)([0-9]+)\.(.)$", r"\2-\1$")
    assert lower(pattern, replace, "bigquery") == lower(pattern, replace, "duckdb")
    assert lower(pattern, replace, "postgres") == ("^(?:x)([0-9]+)\\.([^\n])$", r"\2-\1$")
    # Snowflake has no (?: ): the group captures, and the references count it
    assert lower(pattern, replace, "snowflake") == (r"^(x)([0-9]+)\.(.)$", r"\3-\2$")


def test_every_match_is_replaced_in_each_engines_sql() -> None:
    sql = _select(r"\d", "#", "x")
    assert "'g'" in transpile(sql, "postgres") and "'g'" in transpile(sql, "duckdb")
    for engine in ("trino", "snowflake", "bigquery", "databricks", "spark", "clickhouse"):
        # their three-argument REGEXP_REPLACE replaces every match
        assert transpile(sql, engine).upper().count("REGEXP_REPLACE(") == 1
        assert "'g'" not in transpile(sql, engine)


def test_the_sql_strings_carry_the_engine_pattern_escaped_for_each_dialect() -> None:
    sql = _select(r"\d", "#", "x")
    assert r"'[0-9]'" in transpile(sql, "postgres")
    assert r"'\\A" not in transpile(sql, "bigquery")  # RE2: ^ $ as written
    assert r"'\\A[0-9]" not in transpile(_select(r"^\d", "#", "x"), "bigquery")
    assert r"'\\A[0-9]" in transpile(_select(r"^\d", "#", "x"), "databricks")  # backslash doubled
    assert r"'\A[0-9]" in transpile(_select(r"^\d", "#", "x"), "trino")


def test_an_engine_with_no_lowering_is_refused_by_name() -> None:
    with pytest.raises(ValueError, match="no lowering for the tsql engine"):
        transpile(_select("a", "b", "x"), "tsql")


# --------------------------------------------------------------------------- #
# Execution: the same masked value on every engine that runs here
# --------------------------------------------------------------------------- #


def _select(pattern: str, replace: str, value: str | None) -> str:
    rule = MaskingRule(mask_type=MaskType.regex, pattern=pattern, replace=replace)
    expr = build_mask_expression(rule, '"t"."v"', "varchar")
    literal = "CAST(NULL AS VARCHAR)" if value is None else sql_literal(value, "postgres")
    return f'SELECT {expr} AS "m" FROM (SELECT {literal} AS "v") AS "t"'


@pytest.mark.parametrize(("pattern", "replace", "value", "masked"), CORPUS)
def test_python_masks_as_the_corpus_says(pattern, replace, value, masked) -> None:
    rule = MaskingRule(mask_type=MaskType.regex, pattern=pattern, replace=replace)
    assert apply_mask_to_value(rule, value, "varchar") == masked


@pytest.mark.parametrize(("pattern", "replace", "value", "masked"), CORPUS)
def test_duckdb_masks_as_python_does(pattern, replace, value, masked) -> None:
    [(got,)] = duckdb.sql(transpile(_select(pattern, replace, value), "duckdb")).fetchall()
    assert got == masked


@pytest.fixture(scope="module")
def pg(embedded_postgres):
    import psycopg

    with psycopg.connect(embedded_postgres.dsn(), autocommit=True) as conn:
        yield conn


@pytest.mark.parametrize(("pattern", "replace", "value", "masked"), CORPUS)
def test_postgres_masks_as_python_does(pg, pattern, replace, value, masked) -> None:
    [(got,)] = pg.execute(transpile(_select(pattern, replace, value), "postgres")).fetchall()
    assert got == masked


# --------------------------------------------------------------------------- #
# Save, the editor's check, the presets
# --------------------------------------------------------------------------- #


def test_a_mask_saved_with_a_pattern_that_is_not_universal_is_refused() -> None:
    with pytest.raises(
        ValidationError, match=r"mask pattern is not universal: the lookahead \(\?="
    ):
        Mask(type="regex", pattern=r"\d(?=\d{4})", replace="*")
    with pytest.raises(ValidationError, match=r"mask replace is not universal: \$3"):
        Mask(type="regex", pattern=r"(\d)", replace="$3")
    assert Mask(type="regex", pattern=r"^(\d{3})", replace="$1").pattern == r"^(\d{3})"


def test_the_editors_check_reports_each_entry_and_warns_without_refusing() -> None:
    masks = [
        {"to": ["a"], "mask": {"type": "regex", "pattern": r"(?i)x", "replace": "y"}},
        {"to": ["b"], "mask": {"type": "regex", "pattern": r"(ab|a)", "replace": "y"}},
        {"to": ["c"], "mask": {"type": "regex", "pattern": r"(\d)", "replace": "$2"}},
        {"to": ["d"], "mask": {"type": "constant", "value": None}},
        {"to": ["*"], "mask": None},
    ]
    report = pattern_report(masks)
    assert [(p["entry"], p["field"], p["construct"], p["ok"]) for p in report["patterns"]] == [
        (0, "pattern", "inline_flags", False),
        (1, "pattern", "alternation_overlap", True),
        (2, "replace", "replace_group_out_of_range", False),
    ]
    assert all(p["message"] for p in report["patterns"])
    assert report["patterns"][0]["position"] == 0


@pytest.mark.parametrize("preset", PRESETS, ids=[p["id"] for p in PRESETS])
def test_every_preset_is_universal_and_warns_of_nothing(preset) -> None:
    ex = preset["example"]
    parsed, _ = check(preset_pattern(preset, **ex.get("params", {})), preset["replace"])
    assert warnings(parsed) == []
    py_pattern, py_replace = lower(
        preset_pattern(preset, **ex.get("params", {})), preset["replace"], "python"
    )
    assert re.sub(py_pattern, py_replace, ex["input"]) == ex["output"]


def test_keep_first_last_fills_its_counts() -> None:
    [preset] = [p for p in PRESETS if p["id"] == "keep_first_last"]
    assert preset_pattern(preset, first=1, last=3) == r"^(.{1}).*(.{3})$"


# --------------------------------------------------------------------------- #
# Fail closed: a value the pattern does not match is fully masked
# --------------------------------------------------------------------------- #

# For each preset, a value its pattern does not match.
UNMATCHED = {
    "ssn_last4": "12-3456",
    "card_last4": "no card on file",
    "phone_last4": "ext. 12",
    "email_initial_domain": "not an email",
    "all_but_last4": "abc",
    "digits_keep_separators": "no digits",
    "zip3": "SW1A 1AA",
    "birth_year": "12/07/1984",
    "iban_last4": "gb82 west",
    "keep_first_last": "abc",
}


def _unmatched() -> list[tuple[str, str, str]]:
    return [
        (preset_pattern(p, **p["example"].get("params", {})), p["replace"], UNMATCHED[p["id"]])
        for p in PRESETS
    ]


def test_every_preset_has_an_unmatched_value() -> None:
    assert set(UNMATCHED) == {p["id"] for p in PRESETS}
    for pattern, _replace, value in _unmatched():
        assert re.search(lower(pattern, "x", "python")[0], value) is None


@pytest.mark.parametrize(("pattern", "replace", "value"), _unmatched())
def test_an_unmatched_value_is_fully_masked_in_python(pattern, replace, value) -> None:
    rule = MaskingRule(mask_type=MaskType.regex, pattern=pattern, replace=replace)
    assert apply_mask_to_value(rule, value, "varchar") == "****"


@pytest.mark.parametrize(("pattern", "replace", "value"), _unmatched())
def test_an_unmatched_value_is_fully_masked_on_duckdb(pattern, replace, value) -> None:
    assert duckdb.sql(transpile(_select(pattern, replace, value), "duckdb")).fetchall() == [
        ("****",)
    ]


@pytest.mark.parametrize(("pattern", "replace", "value"), _unmatched())
def test_an_unmatched_value_is_fully_masked_on_postgres(pg, pattern, replace, value) -> None:
    sql = transpile(_select(pattern, replace, value), "postgres")
    assert pg.execute(sql).fetchall() == [("****",)]


def test_null_stays_null() -> None:
    rule = MaskingRule(mask_type=MaskType.regex, pattern=r"\d", replace="#")
    assert apply_mask_to_value(rule, None, "varchar") is None
    assert duckdb.sql(transpile(_select(r"\d", "#", None), "duckdb")).fetchall() == [(None,)]


def test_null_stays_null_on_postgres(pg) -> None:
    assert pg.execute(transpile(_select(r"\d", "#", None), "postgres")).fetchall() == [(None,)]


def test_a_column_that_is_not_text_is_fully_masked_as_null() -> None:
    rule = MaskingRule(mask_type=MaskType.regex, pattern=r"^x", replace="y")
    assert apply_mask_to_value(rule, b"abc", "varbinary") is None
    assert build_mask_expression(rule, '"t"."v"', "varbinary").endswith(", NULL)")
    assert build_mask_expression(rule, '"t"."v"', "varchar(20)").endswith(", '****')")


@pytest.mark.parametrize("engine", ENGINES)
def test_every_engine_masks_an_unmatched_value_fully(engine) -> None:
    """The compiled SQL of the engines not run here: the replacement only where the pattern is
    found, the full mask otherwise, NULL kept."""
    sql = transpile(_select(r"^\d{3}", "###", "x"), engine)
    upper = sql.upper()
    assert "IS NULL THEN NULL WHEN" in upper and "ELSE '****' END" in upper
    found = upper.split(" WHEN ")[2].split(" THEN ")[0]
    assert any(
        f in found
        for f in (
            "REGEXP_LIKE(",
            "REGEXP_MATCHES(",
            "REGEXP_CONTAINS(",
            "MATCH(",
            " ~ ",
            " RLIKE ",
            "REGEXP_INSTR(",
        )
    )
    if engine == "snowflake":
        # Snowflake's REGEXP_LIKE matches the whole value; the pattern is found by its position
        assert "REGEXP_INSTR(" in found and "> 0" in found
