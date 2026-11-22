# Copyright (c) 2026 Kenneth Stott
# Canary: ee4eef8b-c804-43ab-ae02-c20466420ac9
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The function table's rows of strings, LIKE and regular expressions (REQ-2167, REQ-2171):
``ROWS``, which provisa.transpiler.rows collects into provisa.transpiler.function_macros.MACROS."""

# Requirements: REQ-2167, REQ-2171, REQ-2236

from __future__ import annotations

from provisa.transpiler import pg_forms_system, pg_forms_types
from provisa.transpiler.rows.base import (
    _CLICKHOUSE,
    _DUCKDB,
    _EVERY,
    _EVERY_BUT_DUCKDB,
    _NO_CASE_SENSITIVE_LIKE,
    _NO_REGEX,
    _REGEXP_SUBSTRS,
    _TRINO,
    MAPPED,
    Macro,
    Pg,
    _macro,
    _pg,
    _udf,
)

_BACKSLASH = "\\"
_LIKE = f"(a LIKE b ESCAPE '{_BACKSLASH}')"
_ILIKE = f"(a ILIKE b ESCAPE '{_BACKSLASH}')"
#: REGEXP_LIKE matches the whole value on Snowflake (transpile._WHOLE_VALUE_REGEXP_LIKE).
_SNOWFLAKE = frozenset({"snowflake"})


def _text_matches() -> tuple[Macro, ...]:
    operators = {
        "like": _LIKE,
        "nlike": f"NOT {_LIKE}",
        "iclike": _ILIKE,
        "icnlike": f"NOT {_ILIKE}",
    }
    functions = {
        "regexeq": ("(a ~ b)", "(REGEXP_INSTR(a, b) > 0)"),
        "regexne": ("NOT (a ~ b)", "NOT (REGEXP_INSTR(a, b) > 0)"),
        "icregexeq": ("(a ~* b)", "(REGEXP_INSTR(a, b, 1, 1, 0, 'i') > 0)"),
        "icregexne": ("NOT (a ~* b)", "NOT (REGEXP_INSTR(a, b, 1, 1, 0, 'i') > 0)"),
    }

    def of(prefix: str, form: str) -> str:
        """A bpchar is its text: an engine's char type takes no regular expression or LIKE."""
        return form.replace("(a ", "(CAST(a AS TEXT) ") if prefix == "bpchar" else form

    return (
        *(
            Macro(
                f"{prefix}{suffix}",
                ("a", "b"),
                operators={_EVERY: Pg(of(prefix, form))},
                differs_on=_NO_CASE_SENSITIVE_LIKE,
            )
            for prefix in ("text", "name", "bpchar")
            for suffix, form in operators.items()
        ),
        *(
            Macro(
                f"{prefix}{suffix}",
                ("a", "b"),
                {_SNOWFLAKE: variation, _EVERY: Pg(of(prefix, form))},
                differs_on=_NO_REGEX,
            )
            for prefix in ("text", "name", "bpchar")
            for suffix, (form, variation) in functions.items()
        ),
    )


#: quote_literal: an E'' string, its backslashes doubled, when it holds one.
_QUOTED_LITERAL = (
    "CAST(CASE "
    f"WHEN contains(CAST(value AS VARCHAR), '{_BACKSLASH}') THEN 'E''' || "
    f"replace(replace(CAST(value AS VARCHAR), '{_BACKSLASH}', '{_BACKSLASH * 2}'), '''', '''''') "
    "|| '''' ELSE '''' || replace(CAST(value AS VARCHAR), '''', '''''') || '''' END AS VARCHAR)"
)
_PG_QUOTED_LITERAL = (
    "CAST(CASE "
    f"WHEN STRPOS(CAST(value AS TEXT), '{_BACKSLASH}') > 0 THEN 'E''' || "
    f"REPLACE(REPLACE(CAST(value AS TEXT), '{_BACKSLASH}', '{_BACKSLASH * 2}'), '''', '''''') "
    "|| '''' ELSE '''' || REPLACE(CAST(value AS TEXT), '''', '''''') || '''' END AS TEXT)"
)


_QUOTED_IDENTIFIER = (
    "CASE WHEN CAST(value AS TEXT) ~ '^[a-z_][a-z0-9_$]*$' THEN CAST(value AS TEXT) "
    "ELSE '\"' || REPLACE(CAST(value AS TEXT), '\"', '\"\"') || '\"' END"
)


#: PostgreSQL's substr counts positions before the first: a start at or below 0 takes fewer.
_SUBSTR_START = "GREATEST(start, 1)"

ROWS: tuple[Macro, ...] = (
    *_text_matches(),
    Macro("substr", ("value", "start"), {_EVERY: Pg(f"SUBSTRING(value, {_SUBSTR_START})")}),
    Macro(
        "substr",
        ("value", "start", "n"),
        {
            # text_substring: a negative length is PostgreSQL's 22011, which the form below reads
            # as none (run 44670380355 answered '' of substr('abc', 3, -2147483648) on Trino).
            _TRINO: Pg(
                "CASE WHEN n < 0 THEN "
                f"{pg_forms_types.fails('22011', 'negative substring length not allowed', 'TEXT')} "
                f"ELSE SUBSTRING(value, {_SUBSTR_START}, GREATEST(start + n - {_SUBSTR_START}, 0)) "
                "END"
            ),
            _EVERY: Pg(
                f"SUBSTRING(value, {_SUBSTR_START}, GREATEST(start + n - {_SUBSTR_START}, 0))"
            ),
        },
    ),
    # The session's collation, en_US.UTF-8 as pg_database says: DuckDB compares bytes.
    _macro("max_collated", "value", "arg_max(value, provisa_en_us_key(value, 'max'))"),
    _macro("min_collated", "value", "arg_min(value, provisa_en_us_key(value, 'min'))"),
    _pg("like", "a, b", _LIKE, _NO_CASE_SENSITIVE_LIKE),
    _pg("notlike", "a, b", f"NOT {_LIKE}", _NO_CASE_SENSITIVE_LIKE),
    _pg("textcat", "a, b", "CAST(a || b AS TEXT)"),
    _pg("quote_ident", "value", _QUOTED_IDENTIFIER),
    Macro(
        "repeat",
        ("value", "n"),
        {
            _TRINO: pg_forms_system.TRINO_REPEAT,
            _EVERY: Pg(
                "CASE WHEN n > 0 THEN REPEAT(value, n) "
                "WHEN n IS NOT NULL AND value IS NOT NULL THEN '' END"
            ),
        },
    ),
    _pg("textanycat", "a, b", "CAST(CAST(a AS TEXT) || CAST(b AS TEXT) AS TEXT)"),
    _pg("anytextcat", "a, b", "CAST(CAST(a AS TEXT) || CAST(b AS TEXT) AS TEXT)"),
    _pg("text", "value", "CAST(value AS TEXT)"),
    _pg("name", "value", "LEFT(CAST(value AS TEXT), 63)"),
    Macro(
        "ascii",
        ("value",),
        {_EVERY: Pg("ASCII(value)")},
        own={
            _TRINO: "CASE WHEN value = '' THEN 0 ELSE codepoint(cast(substr(value, 1, 1) AS varchar(1))) END"
        },
    ),
    Macro(
        "octet_length",
        ("value",),
        {_EVERY: Pg("OCTET_LENGTH(value)")},
        own={_TRINO: "length(to_utf8(value))"},
    ),
    Macro(
        "bit_length",
        ("value",),
        {
            # A bit string's length is its bits; a text's and a bytea's, their bytes times 8.
            _DUCKDB: "CASE WHEN typeof(value) = 'BIT' THEN bit_length(TRY_CAST(value AS BIT)) "
            "ELSE 8 * OCTET_LENGTH(value) END",
            _EVERY: Pg("8 * OCTET_LENGTH(value)"),
        },
        own={_TRINO: "8 * length(to_utf8(value))", _CLICKHOUSE: "8 * length(value)"},
    ),
    _pg("quote_literal", "value", _PG_QUOTED_LITERAL),
    _pg(
        "quote_nullable",
        "value",
        f"CASE WHEN value IS NULL THEN 'NULL' ELSE {_PG_QUOTED_LITERAL} END",
    ),
    _macro(
        "pg_convert",
        "value, source, destination",
        "provisa_convert(value, source, destination)",
    ),
    _macro("convert_from", "value, source", "decode(provisa_convert(value, source, 'UTF8'))"),
    _macro(
        "convert_to", "value, destination", "provisa_convert(encode(value), 'UTF8', destination)"
    ),
    _udf("regexp_match", "value, pattern", "provisa_regexp_match(value, pattern, '')"),
    _udf("regexp_match", "value, pattern, flags", "provisa_regexp_match(value, pattern, flags)"),
    *(
        Macro(
            "regexp_substr",
            params,
            {_DUCKDB: f"provisa_regexp_substr(value, pattern, {rest})"},
            others,
        )
        for params, rest, others in (
            (
                ("value", "pattern"),
                "1, 1, '', 0",
                # Trino has no regexp_substr, which sqlglot writes; regexp_extract is its first match.
                {_TRINO: "regexp_extract(value, pattern)", _REGEXP_SUBSTRS: MAPPED},
            ),
            (("value", "pattern", "start"), "start, 1, '', 0", {_REGEXP_SUBSTRS: MAPPED}),
            (("value", "pattern", "start", "n"), "start, n, '', 0", {_REGEXP_SUBSTRS: MAPPED}),
            (
                ("value", "pattern", "start", "n", "flags"),
                "start, n, flags, 0",
                {_REGEXP_SUBSTRS - {"bigquery"}: MAPPED},  # BigQuery's stops at the occurrence
            ),
            (
                ("value", "pattern", "start", "n", "flags", "subexpr"),
                "start, n, flags, subexpr",
                {frozenset({"snowflake", "oracle"}): MAPPED},  # the others take no group
            ),
        )
    ),
    *(
        # sqlglot's DuckDB regexp_instr reads no endoption, flags or subexpression. Every other
        # dialect keeps sqlglot's own, and the federation engines their refusal (engine_refusals).
        Macro(
            "regexp_instr",
            params,
            {_DUCKDB: f"provisa_regexp_instr(value, pattern, {rest})"},
            {_EVERY_BUT_DUCKDB: MAPPED},
            differs_on=_TRINO | _CLICKHOUSE,
        )
        for params, rest in (
            (("value", "pattern"), "1, 1, 0, '', 0"),
            (("value", "pattern", "start"), "start, 1, 0, '', 0"),
            (("value", "pattern", "start", "n"), "start, n, 0, '', 0"),
            (("value", "pattern", "start", "n", "endoption"), "start, n, endoption, '', 0"),
            (
                ("value", "pattern", "start", "n", "endoption", "flags"),
                "start, n, endoption, flags, 0",
            ),
            (
                ("value", "pattern", "start", "n", "endoption", "flags", "subexpr"),
                "start, n, endoption, flags, subexpr",
            ),
        )
    ),
    _macro(
        "string_to_table",
        "value, delimiter",
        "unnest(provisa_string_to_array(value, delimiter, NULL))",
    ),
    _macro(
        "string_to_table",
        "value, delimiter, null_string",
        "unnest(provisa_string_to_array(value, delimiter, null_string))",
    ),
    _udf("similar_escape", "pattern, esc", "provisa_similar_escape(pattern, esc)"),
    _macro(
        "similar_to_escape",
        "pattern",
        "CASE WHEN pattern IS NOT NULL THEN provisa_similar_escape(pattern, NULL) END",
    ),
    _macro(
        "similar_to_escape",
        "pattern, esc",
        "CASE WHEN pattern IS NOT NULL AND esc IS NOT NULL THEN "
        "provisa_similar_escape(pattern, esc) END",
    ),
    _udf("is_normalized", "value, form", "provisa_is_normalized(value, form)"),
    _udf("unicode_assigned", "value", "provisa_unicode_assigned(value)"),
    Macro("unicode_version", (), {_EVERY: Pg("'15.1'")}),
    Macro("icu_unicode_version", (), {_EVERY: Pg("'16.0'")}),  # postgres:17's ICU
    _udf("unistr", "value", "provisa_unistr(value)"),
    # The database's encoding is UTF8, which to_ascii refuses; a named single-byte one it strips.
    _udf("to_ascii", "value", "provisa_to_ascii(value, 'UTF8')"),
    _macro("to_ascii", "value, encoding", "provisa_to_ascii(value, CAST(encoding AS VARCHAR))"),
    _udf("parse_ident", "value", "provisa_parse_ident(value, true)"),
    _udf("parse_ident", "value, strict_", "provisa_parse_ident(value, strict_)"),
    _pg(
        "nameconcatoid",
        "name, oid",
        "CAST(LEFT(name, 63 - LENGTH('_' || CAST(oid AS TEXT))) || '_' || CAST(oid AS TEXT) AS TEXT)",
    ),
    # A character value of a "char".
    _pg("bpchar", "value", "CAST(value AS TEXT)"),
)
