# Copyright (c) 2026 Kenneth Stott
# Canary: 85831f13-ddc3-44a1-ab5c-9a95189acc81
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The server's transaction ids and encoding names (REQ-2236; the no-form investigation's CAT-0:
the calls a tool sends that reach the engine).

age(xid): xid_age answers INT_MAX for an xid that is no normal transaction id (0, 1 and 2), and
otherwise the current transaction id less it, as an int32. DataGrip sends ``age(0::varchar::xid)``
on its own. DuckDB's form (provisa.transpiler.pg_functions, function_macros' provisa_xid_age) takes
DuckDB's own txid_current() for the current transaction; Trino and ClickHouse hold no transaction id,
so their forms answer the special xids and refuse a normal one by name when it runs. age of a
timestamp has no form on either, and is refused by name, as engine_refusals refused every age.

mxid_age: MultiXactIdIsValid is every multixact but 0, and its age is the next multixact less it,
as an int32. Provisa makes no multixact, so the next one is 1: DuckDB's form, which the old table
(function_macros) keeps. An xid's input reads -2^31 to 2^32 - 1, a negative one wrapping.

pg_char_to_encoding: PostgreSQL's pg_encname_tbl (src/common/encnames.c, PostgreSQL 16.11) maps
each name, cleaned as clean_encoding_name cleans it (only its letters and digits, lower case), to
its encoding (pg_enc, src/include/mb/pg_wchar.h); any other is -1. A name holds 63 bytes: a longer
text is cut to them as it is read, which no form here does, so it is refused by name."""

# Requirements: REQ-2236, REQ-2171

from __future__ import annotations

import sqlglot.expressions as exp

from provisa.core.pg_error import END, MARK
from provisa.transpiler.metadata_functions import ENCODINGS
from provisa.transpiler.polyfill import trino
from provisa.transpiler.polyfill.grid import (
    DIALECTS,
    NATIVE,
    Feature,
    Limitation,
    Rule,
    Side,
    written_in,
)
from provisa.transpiler.polyfill.matching import graft
from provisa.transpiler.polyfill.notation import Refusal, Refused, Rewrite, replacement

FAMILY = "the server's transaction ids and encodings"

_CLICKHOUSE = "clickhouse"
#: The dialects whose forms are written here: Trino's SQL and ClickHouse's.
_ENGINES = (*trino.TRINO, _CLICKHOUSE)


def _value_refused(function: str) -> Refusal:
    return Refusal("0A000", f"{function} does not read this value")


def _raised(refusal: Refusal, dialect: str) -> str:
    """``dialect``'s raise of ``refusal`` where it runs (provisa.core.pg_error)."""
    text = f"{MARK} {refusal.sqlstate}: {refusal.message}{END}".replace("'", "''")
    if dialect == "duckdb":
        return f"error('{text}')"
    return f"fail('{text}')"


def _int32_unless(condition: str, refusal: Refusal, value: str) -> str:
    """ClickHouse's ``value`` as an int32, raising ``refusal`` where ``condition`` holds (a NULL
    does not). The raise is outside the cast: transpile.clickhouse_semantics reads a refusal under
    a cast to an integer as one of a text, and casts it to an Int32 no NULL fits."""
    text = f"{MARK} {refusal.sqlstate}: {refusal.message} {END}".replace("'", "''")
    thrown = f"throwIf(ifNull({condition}, 0), '{text}')"
    return f"if({thrown} = 0, CAST({value} AS Nullable(Int32)), NULL)"


# --- age(xid) ----------------------------------------------------------------------------------

_AGE_OF_XID = _value_refused("age")
#: xid_age's answer of an xid that is no normal transaction id.
_INT_MAX = "2147483647"
_AGE_PATTERN = "age($x)"
_AGE_FORMS = {
    **dict.fromkeys(
        trino.TRINO,
        f"CASE WHEN $x IS NULL THEN CAST(NULL AS INTEGER) WHEN $x IN (0, 1, 2) THEN {_INT_MAX} "
        f"ELSE {_raised(_AGE_OF_XID, 'trino')} END",
    ),
    _CLICKHOUSE: _int32_unless(
        "$x NOT IN (0, 1, 2)", _AGE_OF_XID, f"CASE WHEN $x IN (0, 1, 2) THEN {_INT_MAX} END"
    ),
}
#: What a form writes its argument within: the walk does not descend into what a rule writes.
_PLAIN = (
    exp.Cast,
    exp.Literal,
    exp.Boolean,
    exp.Null,
    exp.Neg,
    exp.Paren,
    exp.DataType,
    exp.DataTypeParam,
)
_PLAIN_COLUMN = (exp.Column, exp.Identifier)


def plain(value: exp.Expr) -> bool:
    """Whether ``value`` holds no call: a literal, a column or a cast of one, which a form may
    hold as it stands."""
    return all(isinstance(node, _PLAIN + _PLAIN_COLUMN) for node in value.walk())


def age_of_xid(node: exp.Expr, dialect: str) -> exp.Expr | None:
    """age(xid): the special xids answered, a normal one refused where it runs; age of anything
    else refused by name, as no form of an interval is held here."""
    call = trino.called(node)
    if call is None or "age" not in call[0]:
        return None
    args = call[1]
    if len(args) != 1 or trino.said(args[0]) != "xid" or not plain(args[0]):
        raise Refused(trino.no_form("age", dialect))
    form = replacement(_AGE_FORMS[dialect], written_in(dialect), _AGE_PATTERN)
    return graft(form, {"x": args[0]})


AGE = Feature(
    "age",
    FAMILY,
    "The age of a transaction id: INT_MAX for one that is no normal id (0, 1, 2).",
    read=Side(
        rules=(
            Rule(
                None,
                dict.fromkeys(_ENGINES, Rewrite(age_of_xid, mentions=r"\bage\s*\(")),
            ),
        ),
        limitations=dict.fromkeys(
            _ENGINES,
            (
                Limitation(
                    "Of a normal xid, refused by name where it runs: its age is the current "
                    "transaction id less it, and the engine holds no transaction id.",
                    _AGE_OF_XID,
                ),
                Limitation(
                    "Of a timestamp, or of an xid within a call: refused by name, as no form of "
                    "an interval is held here."
                ),
            ),
        ),
    ),
)

# --- mxid_age ----------------------------------------------------------------------------------

_MXID_AGE = _value_refused("mxid_age")
#: The xid ``$x`` as the 32-bit unsigned number its input reads: a negative one wraps.
_UNSIGNED = {
    "trino": "(CASE WHEN $x < 0 THEN $x + 4294967296 ELSE $x END)",
    _CLICKHOUSE: "if($x < 0, $x + CAST(4294967296 AS Int64), $x)",
}
_OUT_OF_RANGE = "$x < -2147483648 OR $x > 4294967295"


def _mxid_age(dialect: str) -> str:
    """(int32)(1 - x): the next multixact is 1, as Provisa makes none."""
    age = f"1 - {_UNSIGNED[dialect]}"
    wrapped = f"WHEN {age} < -2147483648 THEN {age} + 4294967296"
    if dialect == _CLICKHOUSE:
        return _int32_unless(
            _OUT_OF_RANGE, _MXID_AGE, f"CASE WHEN $x = 0 THEN {_INT_MAX} {wrapped} ELSE {age} END"
        )
    return (
        "CASE WHEN $x IS NULL THEN CAST(NULL AS INTEGER) "
        f"WHEN {_OUT_OF_RANGE} THEN {_raised(_MXID_AGE, dialect)} "
        f"WHEN $x = 0 THEN {_INT_MAX} "
        f"ELSE CAST(CASE {wrapped} ELSE {age} END AS INTEGER) END"
    )


MXID_AGE = Feature(
    "mxid_age",
    FAMILY,
    "The age of a multixact id: INT_MAX for 0, and 1 less it, as Provisa makes no multixact.",
    read=Side(
        rules=(
            Rule(
                "mxid_age($x)",
                {**trino.trino_only(_mxid_age("trino")), _CLICKHOUSE: _mxid_age(_CLICKHOUSE)},
            ),
        ),
        limitations=dict.fromkeys(
            _ENGINES,
            (
                Limitation(
                    "An xid past what xid's input reads (-2^31 to 2^32 - 1) is refused by name "
                    "where it runs.",
                    _MXID_AGE,
                ),
            ),
        ),
    ),
)

# --- pg_char_to_encoding -----------------------------------------------------------------------

#: pg_encname_tbl (src/common/encnames.c, PostgreSQL 16.11): each cleaned name and its encoding.
PG_ENCNAME_TBL = {
    "abc": "WIN1258", "alt": "WIN866", "big5": "BIG5", "euccn": "EUC_CN",
    "eucjis2004": "EUC_JIS_2004", "eucjp": "EUC_JP", "euckr": "EUC_KR", "euctw": "EUC_TW",
    "gb18030": "GB18030", "gbk": "GBK", "iso88591": "LATIN1", "iso885910": "LATIN6",
    "iso885913": "LATIN7", "iso885914": "LATIN8", "iso885915": "LATIN9", "iso885916": "LATIN10",
    "iso88592": "LATIN2", "iso88593": "LATIN3", "iso88594": "LATIN4", "iso88595": "ISO_8859_5",
    "iso88596": "ISO_8859_6", "iso88597": "ISO_8859_7", "iso88598": "ISO_8859_8",
    "iso88599": "LATIN5", "johab": "JOHAB", "koi8": "KOI8R", "koi8r": "KOI8R", "koi8u": "KOI8U",
    "latin1": "LATIN1", "latin10": "LATIN10", "latin2": "LATIN2", "latin3": "LATIN3",
    "latin4": "LATIN4", "latin5": "LATIN5", "latin6": "LATIN6", "latin7": "LATIN7",
    "latin8": "LATIN8", "latin9": "LATIN9", "mskanji": "SJIS", "muleinternal": "MULE_INTERNAL",
    "shiftjis": "SJIS", "shiftjis2004": "SHIFT_JIS_2004", "sjis": "SJIS", "sqlascii": "SQL_ASCII",
    "tcvn": "WIN1258", "tcvn5712": "WIN1258", "uhc": "UHC", "unicode": "UTF8", "utf8": "UTF8",
    "vscii": "WIN1258", "win": "WIN1251", "win1250": "WIN1250", "win1251": "WIN1251",
    "win1252": "WIN1252", "win1253": "WIN1253", "win1254": "WIN1254", "win1255": "WIN1255",
    "win1256": "WIN1256", "win1257": "WIN1257", "win1258": "WIN1258", "win866": "WIN866",
    "win874": "WIN874", "win932": "SJIS", "win936": "GBK", "win949": "UHC", "win950": "BIG5",
    "windows1250": "WIN1250", "windows1251": "WIN1251", "windows1252": "WIN1252",
    "windows1253": "WIN1253", "windows1254": "WIN1254", "windows1255": "WIN1255",
    "windows1256": "WIN1256", "windows1257": "WIN1257", "windows1258": "WIN1258",
    "windows866": "WIN866", "windows874": "WIN874", "windows932": "SJIS", "windows936": "GBK",
    "windows949": "UHC", "windows950": "BIG5",
}  # fmt: skip

_ENCODING = _value_refused("pg_char_to_encoding")
#: A name's most bytes (NAMEDATALEN - 1).
_NAME_BYTES = 63
#: Each dialect's clean_encoding_name of ``$n``, and its count of ``$n``'s bytes.
_CLEANED = {
    "duckdb": ("lower(regexp_replace($n, '[^A-Za-z0-9]', '', 'g'))", "strlen($n)"),
    "trino": ("lower(regexp_replace($n, '[^A-Za-z0-9]', ''))", "length(to_utf8($n))"),
    _CLICKHOUSE: ("lower(replaceRegexpAll($n, '[^A-Za-z0-9]', ''))", "length($n)"),
}


def _encoding(dialect: str) -> str:
    cleaned, size = _CLEANED[dialect]
    numbers = " ".join(
        f"WHEN '{name}' THEN {ENCODINGS.index(encoding)}"
        for name, encoding in PG_ENCNAME_TBL.items()
    )
    number = f"CASE {cleaned} {numbers} ELSE -1 END"
    if dialect == _CLICKHOUSE:
        return _int32_unless(
            f"{size} > {_NAME_BYTES}",
            _ENCODING,
            f"CASE WHEN $n IS NULL THEN NULL ELSE {number} END",
        )
    return (
        f"CASE WHEN $n IS NULL THEN CAST(NULL AS INTEGER) "
        f"WHEN {size} > {_NAME_BYTES} THEN {_raised(_ENCODING, dialect)} "
        f"ELSE CAST({number} AS INTEGER) END"
    )


PG_CHAR_TO_ENCODING = Feature(
    "pg_char_to_encoding",
    FAMILY,
    "An encoding's number by any of PostgreSQL's names of it; -1 for a name of none.",
    read=Side(
        rules=(
            Rule(
                "pg_char_to_encoding($n)",
                {
                    **{
                        d: trino.no_form("pg_char_to_encoding", d)
                        for d in DIALECTS
                        if d not in {*NATIVE, "duckdb", *_ENGINES}
                    },
                    "duckdb": _encoding("duckdb"),
                    **dict.fromkeys(trino.TRINO, _encoding("trino")),
                    _CLICKHOUSE: _encoding(_CLICKHOUSE),
                },
            ),
        ),
        limitations=dict.fromkeys(
            ("duckdb", *_ENGINES),
            (
                Limitation(
                    f"A name of more than {_NAME_BYTES} bytes is refused by name where it runs: "
                    "PostgreSQL cuts it to them as it reads it.",
                    _ENCODING,
                ),
            ),
        ),
    ),
)

FEATURES: tuple[Feature, ...] = (AGE, MXID_AGE, PG_CHAR_TO_ENCODING)
