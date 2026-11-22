# Copyright (c) 2026 Kenneth Stott
# Canary: 3f6c1d2e-8a4b-4c7e-9d10-5b2e7f9a1c44
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The refusals and stated limitations of the exact-or-refused engines, Db2 and SAP HANA (REQ-2236,
THE REFUSAL LIVES IN THE GRID; amended 2026-11-21 for Db2 and for SAP HANA).

Each engine's dialect (provisa/db2/dialect.py, provisa/saphana/dialect.py) writes a statement only
when each construct in it has a form there that gives PostgreSQL's answer. Which constructs it
refuses, why, and the error a client receives for each are these rows: the dialect finds the
construct, and raises the row's refusal (:func:`refusal`). A construct no row names is refused by
the row for a construct with no listed form (:data:`UNLISTED`), which is the dialect's rule that
what it does not list is refused. A refusal's message names the construct; where the construct
named depends on the statement (a type, a field), the row's message holds its place in braces.

The rows with no refusal are the stated limitations: behaviours that differ from PostgreSQL's within
a bound, listed here and on each engine's page."""

# Requirements: REQ-2236

from __future__ import annotations

from collections.abc import Mapping

from provisa.transpiler.polyfill.grid import Feature, Limitation, Side
from provisa.transpiler.polyfill.notation import Refusal

FAMILY = "exact or refused"

DB2 = "db2"
HANA = "saphana"
_TITLES: Mapping[str, str] = {DB2: "Db2", HANA: "SAP HANA"}

#: The SQLSTATE of every refusal here: feature_not_supported.
_SQLSTATE = "0A000"

#: The row of a construct with no listed form: the dialect's rule that what it does not list as
#: exact is refused by name.
UNLISTED = "a construct with no exact form"


def _message(construct: str, dialect: str) -> str:
    return f"{construct} has no form on {_TITLES[dialect]} that gives PostgreSQL's answer"


def _refused(name: str, summary: str, construct: str, reasons: Mapping[str, str]) -> Feature:
    """A row refusing ``construct`` on each dialect of ``reasons``, each for its reason."""
    return Feature(
        name,
        FAMILY,
        summary,
        read=Side(
            limitations={
                dialect: (Limitation(reason, Refusal(_SQLSTATE, _message(construct, dialect))),)
                for dialect, reason in reasons.items()
            }
        ),
    )


def _both(reason: str) -> dict[str, str]:
    return {DB2: reason, HANA: reason}


def _limited(name: str, summary: str, texts: Mapping[str, tuple[str, ...]]) -> Feature:
    """A row of stated limitations: no refusal, a bound the answer is given within."""
    return Feature(
        name,
        FAMILY,
        summary,
        read=Side(
            limitations={
                dialect: tuple(Limitation(text) for text in lines)
                for dialect, lines in texts.items()
            }
        ),
    )


_NULL_ARGUMENT = "It returns NULL for any NULL argument; PostgreSQL skips it."
_RESULT_TYPE = "Its result type follows its arguments; PostgreSQL's does not."
_SERVER_CLOCK = "Its clock is the server's zone, not the session's."
_NO_INTERVAL = "It has no interval type; a labeled duration keeps a date a date."
_PARAMETER_MARKERS = (
    "Each $n is written as a positional marker (?), so parameters must appear once each, in order."
)

_REFUSED: tuple[Feature, ...] = (
    _refused(
        UNLISTED,
        "Any construct the engine's form is not known to agree with PostgreSQL on.",
        "{construct}",
        _both(
            "A construct not listed as having a form that gives PostgreSQL's answer is refused by "
            "name before any SQL is written."
        ),
    ),
    _refused(
        "integer division",
        "a / b of two integers.",
        "/",
        {
            HANA: "HANA's quotient of two integers is a decimal; PostgreSQL's is the truncated "
            "integer. It is refused until the compiler supplies the operands' types."
        },
    ),
    _refused(
        "avg of an integer",
        "avg(a).",
        "avg",
        {
            DB2: "Db2's AVG of an integer column is an integer (truncated); PostgreSQL's is "
            "numeric.",
            HANA: "HANA's AVG result type and scale are not PostgreSQL's numeric.",
        },
    ),
    _refused("greatest of a NULL", "greatest(a, b).", "greatest", _both(_NULL_ARGUMENT)),
    _refused("least of a NULL", "least(a, b).", "least", _both(_NULL_ARGUMENT)),
    _refused("concat of a NULL", "concat(a, b).", "concat", _both(_NULL_ARGUMENT)),
    _refused("round's result type", "round(a, n).", "round", _both(_RESULT_TYPE)),
    _refused("power's result type", "power(a, b).", "power", _both(_RESULT_TYPE)),
    _refused("an interval", "a + INTERVAL '1 day'.", "interval", _both(_NO_INTERVAL)),
    _refused(
        "date_trunc on an exact engine",
        "date_trunc('month', d).",
        "timestamp_trunc",
        _both(_NO_INTERVAL),
    ),
    _refused(
        "current_date in the session's zone", "CURRENT_DATE.", "current_date", _both(_SERVER_CLOCK)
    ),
    _refused(
        "current_timestamp in the session's zone",
        "now(), CURRENT_TIMESTAMP.",
        "current_timestamp",
        _both(_SERVER_CLOCK),
    ),
    _refused(
        "a POSIX regular expression",
        "a ~ 'x'.",
        "regexp_like",
        {
            DB2: "Db2's REGEXP_LIKE is ICU's syntax, not PostgreSQL's.",
            HANA: "HANA's LIKE_REGEXPR is PCRE's syntax, not PostgreSQL's.",
        },
    ),
    _refused(
        "a type with no exact form",
        "A value of a type the engine holds no PostgreSQL value of.",
        "the type {type}",
        {
            DB2: "Db2 holds boolean, smallint, integer, bigint, real, double precision, numeric "
            "with a precision, varchar, char, text, date and timestamp with PostgreSQL's values. "
            "Its time has no fractional seconds; it has no timestamptz, bytea, uuid, JSON or "
            "arrays.",
            HANA: "HANA holds boolean, smallint, integer, bigint, real, double precision, numeric "
            "with a precision, varchar, text, date and timestamp with PostgreSQL's values. Its "
            "char pads with blanks; it has no time, timestamptz, bytea, uuid, JSON or arrays of "
            "PostgreSQL's.",
        },
    ),
    _refused(
        "numeric without a precision",
        "CAST(a AS numeric).",
        "numeric without a precision of at most {max}",
        {
            DB2: "Db2's default DECIMAL is DECIMAL(5,0), and its largest precision 31.",
            HANA: "HANA's DECIMAL with no precision is a decimal float, and its largest precision "
            "38.",
        },
    ),
    _refused(
        "a string type longer than the engine's longest",
        "CAST(a AS varchar(n)) past the engine's longest.",
        "{kind} longer than {max}",
        {
            DB2: "Db2's longest VARCHAR is 8168 characters.",
            HANA: "HANA's longest NVARCHAR is 5000 characters.",
        },
    ),
    _refused(
        "try_cast on an exact engine",
        "TRY_CAST(a AS t).",
        "try_cast",
        _both("A conversion that fails gives NULL there; PostgreSQL has none."),
    ),
    _refused(
        "an integer cast of a fraction",
        "CAST(a AS integer) of a value not known to be integral.",
        "a cast to an integer type of a value that may have a fraction",
        _both(
            "It truncates a fraction (CAST(2.5 AS INTEGER) is 2); PostgreSQL rounds (3) "
            "(provisa/core/sql_integral.py)."
        ),
    ),
    _refused(
        "a boolean cast of a non-boolean",
        "CAST(a AS boolean) of a value that is not a boolean.",
        "a cast to boolean of a value that is not a boolean",
        {HANA: "HANA reads another set of words as booleans than PostgreSQL."},
    ),
    _refused(
        "octet_length on an exact engine",
        "octet_length(a).",
        "octet_length",
        _both("Its byte length of a string is of its own encoding, not PostgreSQL's UTF-8."),
    ),
    _refused(
        "substring from a computed start",
        "substring(a FROM s) where s is not a literal of at least 1.",
        "substring with a start that is not a literal of at least 1",
        _both("Its substring of a start below 1 is not PostgreSQL's."),
    ),
    _refused(
        "substring of a computed length",
        "substring(a FROM 1 FOR n) where n is not a literal.",
        "substring with a length that is not a literal",
        _both("Its substring of a negative length is not PostgreSQL's error."),
    ),
    _refused(
        "strpos from a position",
        "A position search from a given position or occurrence.",
        "strpos from a position",
        _both("PostgreSQL's strpos has no start or occurrence."),
    ),
    _refused(
        "trim of other characters",
        "trim(BOTH 'x' FROM a).",
        "trim of characters other than spaces",
        _both("Its trim of a character set is not PostgreSQL's."),
    ),
    _refused(
        "extract of another field",
        "extract(f FROM d) of a field the engine numbers otherwise.",
        "extract of {field}",
        {
            DB2: "Db2 reads year, month, day, hour, minute and second with PostgreSQL's "
            "numbering; any other field is refused.",
            HANA: "HANA reads year, month, day, hour and minute with PostgreSQL's numbering; "
            "second (its fraction) and any other field are refused.",
        },
    ),
    _refused(
        "ceil to decimals",
        "ceil(a, n).",
        "ceil to decimals",
        _both("PostgreSQL's ceil takes no decimals."),
    ),
    _refused(
        "floor to decimals",
        "floor(a, n).",
        "floor to decimals",
        _both("PostgreSQL's floor takes no decimals."),
    ),
    _refused(
        "distinct on",
        "SELECT DISTINCT ON (a) ...",
        "distinct on",
        _both("It has no DISTINCT ON."),
    ),
    _refused(
        "with recursive",
        "WITH RECURSIVE ...",
        "with recursive",
        _both("Its recursive common table expressions are not written as PostgreSQL's."),
    ),
    _refused(
        "an escape of more than one character",
        "a LIKE b ESCAPE e where e is not one character.",
        "an escape that is not one character",
        _both("Its ESCAPE takes one character."),
    ),
    _refused(
        "is true or is false",
        "a IS TRUE, a IS FALSE.",
        "is true or is false",
        {HANA: "HANA has no IS TRUE or IS FALSE."},
    ),
    _refused(
        "intersect all or except all",
        "INTERSECT ALL, EXCEPT ALL.",
        "{kind} all",
        {HANA: "HANA has no INTERSECT ALL or EXCEPT ALL."},
    ),
    _refused(
        "a range frame with an offset",
        "RANGE BETWEEN n PRECEDING AND ...",
        "a range frame with an offset",
        {HANA: "HANA's RANGE frame takes no offset."},
    ),
    _refused(
        "fetch with ties",
        "FETCH FIRST n ROWS WITH TIES.",
        "fetch with ties",
        {HANA: "HANA has no WITH TIES."},
    ),
    _refused(
        "fetch percent",
        "FETCH FIRST n PERCENT ROWS ONLY.",
        "fetch percent",
        {HANA: "HANA has no FETCH PERCENT."},
    ),
    _refused(
        "offset without limit",
        "OFFSET n with no LIMIT.",
        "offset without limit",
        {HANA: "HANA takes OFFSET only after LIMIT."},
    ),
    _refused(
        "update from",
        "UPDATE t SET ... FROM u.",
        "update from",
        {HANA: "HANA's UPDATE ... FROM is not PostgreSQL's."},
    ),
    _refused(
        "delete using",
        "DELETE FROM t USING u.",
        "delete using",
        {HANA: "HANA has no DELETE ... USING."},
    ),
    _refused(
        "a named parameter",
        "A parameter by name.",
        "a named parameter",
        _both("Its driver's parameters are positional markers (?) only."),
    ),
    _refused(
        "a parameter in LIMIT or OFFSET",
        "LIMIT $1, OFFSET $1.",
        "a parameter in LIMIT or OFFSET",
        _both(
            "Its paging clause is written in another order than PostgreSQL's, so a positional "
            "marker there binds another parameter."
        ),
    ),
    _refused(
        "parameters out of order",
        "$2 before $1, or a parameter used twice.",
        "parameters not each used once in order",
        _both(_PARAMETER_MARKERS),
    ),
)

_LIMITED: tuple[Feature, ...] = (
    _limited(
        "text's bound",
        "text and an unbounded varchar.",
        {
            DB2: (
                "A cast to text, and an unbounded varchar, is VARCHAR(8168 CODEUNITS32), Db2's "
                "longest: a longer value raises.",
            ),
            HANA: (
                "text and an unbounded varchar are NVARCHAR(5000), HANA's longest: a longer value "
                "raises.",
            ),
        },
    ),
    _limited(
        "string comparison",
        "a = b, a < b of strings.",
        {
            DB2: ("String equality ignores trailing blanks: 'a' = 'a ' is true.",),
            HANA: (
                "Text sorts and compares in binary order (code points), not the locale collation.",
            ),
        },
    ),
    _limited(
        "sum of bigint",
        "sum(a) of a bigint column.",
        {HANA: ("SUM over bigint can overflow where PostgreSQL widens to numeric.",)},
    ),
    _limited(
        "timestamp precision",
        "A timestamp's fractional seconds.",
        {
            HANA: (
                "Timestamps keep 7 fractional digits where PostgreSQL keeps 6 (unverified until "
                "a live HANA run).",
            )
        },
    ),
    _limited(
        "length outside the BMP",
        "length(a) of characters outside the Basic Multilingual Plane.",
        {
            HANA: (
                "LENGTH may count a character outside the BMP differently from PostgreSQL "
                "(unverified until a live HANA run).",
            )
        },
    ),
)

#: The family's place in the grid (polyfill.GRID).
ORDER = 380
FEATURES: tuple[Feature, ...] = (*_REFUSED, *_LIMITED)

#: Each dialect's refusals, by the construct its message names.
_BY_CONSTRUCT: dict[str, dict[str, Feature]] = {}


def _construct_of(feature: Feature, dialect: str) -> str | None:
    """The construct the row's refusal on ``dialect`` names, as its message's text before the
    engine's title; None where the row refuses nothing there."""
    for limitation in feature.read.limitations.get(dialect, ()):
        if limitation.refusal is not None:
            return limitation.refusal.message.removesuffix(_message("", dialect))
    return None


def refusal(dialect: str, construct: str, **named: str) -> Refusal:
    """The refusal a client receives for ``construct`` on ``dialect``: the row's that names it,
    its braces filled from ``named``. ``construct`` is the row's construct as written, braces and
    all (``"extract of {field}"``); one no row names is refused by :data:`UNLISTED`, by name."""
    if dialect not in _BY_CONSTRUCT:
        _BY_CONSTRUCT[dialect] = {
            construct_of: feature
            for feature in _REFUSED
            if (construct_of := _construct_of(feature, dialect)) is not None
        }
    rows = _BY_CONSTRUCT[dialect]
    if construct in rows:
        row = rows[construct]
    else:  # the dialect's rule: a construct it does not list is refused by name (UNLISTED)
        row = rows["{construct}"]
        named = {"construct": construct}
    (limitation,) = (lim for lim in row.read.limitations[dialect] if lim.refusal is not None)
    assert limitation.refusal is not None
    return Refusal(limitation.refusal.sqlstate, limitation.refusal.message.format(**named))
