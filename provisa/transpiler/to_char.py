# Copyright (c) 2026 Kenneth Stott
# Canary: bcb52bd9-be2b-4aa6-a353-594bfca65671
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""PostgreSQL's ``to_char(value, format)`` written in an engine's own functions (REQ-2167,
PGWIRE'S CATALOG FUNCTIONS; REQ-2236): DuckDB, which has no to_char, and ClickHouse and Trino,
whose own formats read a template otherwise.

A date/time template is read as formatting.c reads it (:func:`tokens`): keywords longest first,
one FM or TM before a keyword and one TH, th or SP after it, double-quoted text, a backslash before
a double quote. Each keyword is then written as one expression of the engine that yields exactly
PostgreSQL's text for it (SSSS the seconds past midnight, DDD the day of the year padded to 3, a
month's name from a list of PostgreSQL's names), and the pieces are concatenated; any other
character is a string constant, so a literal ``%Y`` is the two characters it is. A keyword with no
exact form (a name in lc_time's language, TM; a zone's keyword of a value whose type is not read)
is refused by name, never answered by a guess.

A numeric format (9, 0, the decimal point, the group separator, FM) is written, on DuckDB, as the
digits PostgreSQL writes. sqlglot's own reading of TO_CHAR maps a template to strftime element by
element, wrongly for several (D, Day, WW, SSSS, a literal %) and not at all for others, so the
call is read here from the template the statement wrote."""

# Requirements: REQ-2167

from __future__ import annotations

import functools
import re
from dataclasses import dataclass

import sqlglot
import sqlglot.expressions as exp
from sqlglot.dialects.postgres import Postgres


class _CallsKept(Postgres):
    """PostgreSQL, with to_char read as the call it is (not sqlglot's TimeToStr)."""

    class Parser(Postgres.Parser):
        FUNCTIONS = {
            **Postgres.Parser.FUNCTIONS,
            "TO_CHAR": lambda args: exp.Anonymous(this="to_char", expressions=args),
        }


def _refused(element: str) -> NotImplementedError:
    return NotImplementedError(
        f"to_char: the format element {element!r} is not supported; it is not answered by a guess"
    )


# Date/time elements (PostgreSQL's formatting.c keywords), longest first, each to how it is


# formatting.c's date/time keywords (DCH_keywords). Its numeric ones are read in either case; its
# names (and the meridiem, the era, the zone) in the cases listed. FX is a keyword that writes
# nothing to to_char. A keyword is matched longest first at each position, as formatting.c's
# ordered table matches it.
_NUMERIC_ELEMENTS = (
    "Y,YYY",
    "SSSSS",
    "IYYY",
    "IDDD",
    "HH24",
    "HH12",
    "SSSS",
    "YYYY",
    "FF1",
    "FF2",
    "FF3",
    "FF4",
    "FF5",
    "FF6",
    "DDD",
    "IYY",
    "YYY",
    "HH",
    "MI",
    "SS",
    "MS",
    "US",
    "YY",
    "MM",
    "DD",
    "ID",
    "IW",
    "WW",
    "IY",
    "CC",
    "FX",
    "Q",
    "W",
    "J",
    "D",
    "Y",
    "I",
)
_NAME_ELEMENTS = (
    "A.D.",
    "a.d.",
    "B.C.",
    "b.c.",
    "A.M.",
    "a.m.",
    "P.M.",
    "p.m.",
    "MONTH",
    "Month",
    "month",
    "TZH",
    "TZM",
    "tzh",
    "tzm",
    "MON",
    "Mon",
    "mon",
    "DAY",
    "Day",
    "day",
    "AD",
    "ad",
    "BC",
    "bc",
    "AM",
    "am",
    "PM",
    "pm",
    "DY",
    "Dy",
    "dy",
    "RM",
    "rm",
    "TZ",
    "tz",
    "OF",
    "of",
)
_ELEMENTS = sorted(
    [*_NUMERIC_ELEMENTS, *(e.lower() for e in _NUMERIC_ELEMENTS), *_NAME_ELEMENTS],
    key=len,
    reverse=True,
)
#: The modifiers formatting.c reads around a keyword (DCH_suff): one prefix before it, one suffix
#: after it, at most.
_PREFIXES = ("FM", "fm", "TM", "tm")
_SUFFIXES = ("TH", "th", "SP")


@dataclass(frozen=True)
class Token:
    """One node of PostgreSQL's reading of a template: a keyword and its modifiers, or a character
    written as it is."""

    text: str
    keyword: bool = False
    fill: bool = False  # FM: no padding
    translated: bool = False  # TM: a name in lc_time's language
    suffix: str = ""  # TH, th (the number's ordinal suffix), or SP (read, and nothing to to_char)

    @property
    def written(self) -> str:
        """The keyword as the template writes it, its modifiers with it."""
        prefix = "FM" if self.fill else "TM" if self.translated else ""
        return f"{prefix}{self.text}{self.suffix}"


def tokens(template: str) -> tuple[Token, ...]:
    """``template`` as formatting.c's parse_format reads it for to_char: at each position one
    prefix (FM or TM), then the longest keyword, then one suffix (TH, th or SP); a prefix before no
    keyword is dropped. Any other character is written as it is: double quotes hold such text, a
    backslash in it quoting the next character; outside them, a backslash is itself unless it
    precedes a double quote."""
    read: list[Token] = []
    i, end = 0, len(template)
    while i < end:
        prefix = template[i : i + 2] if template[i : i + 2] in _PREFIXES else ""
        i += len(prefix)
        element = next((e for e in _ELEMENTS if template.startswith(e, i)), None)
        if element is not None:
            i += len(element)
            suffix = template[i : i + 2] if template[i : i + 2] in _SUFFIXES else ""
            i += len(suffix)
            fill, translated = prefix.upper() == "FM", prefix.upper() == "TM"
            read.append(Token(element, True, fill, translated, suffix))
            continue
        if i >= end:
            break
        if template[i] == '"':
            i += 1
            while i < end and template[i] != '"':
                if template[i] == "\\" and i + 1 < end:
                    i += 1
                read.append(Token(template[i]))
                i += 1
            i += 1  # the closing quote, where there is one
            continue
        if template[i] == "\\" and template[i + 1 : i + 2] == '"':
            i += 1
        read.append(Token(template[i]))
        i += 1
    return tuple(read)


#: The keywords every engine's own format writes as PostgreSQL's (its strftime element), and the
#: characters written as they are that no format reads as an element.
_PLAIN_STRFTIME = {"YYYY": "%Y", "MM": "%m", "DD": "%d", "HH24": "%H", "MI": "%M", "SS": "%S"}
_PLAIN_LITERALS = frozenset("-/: ")


def plain_strftime(template: str) -> str | None:
    """The strftime format of PostgreSQL's to_char ``template``, as formatting.c reads it
    (:func:`tokens`): where it is made only of YYYY, MM, DD, HH24, MI and SS (either case, with no
    modifier) and the characters ``-/: ``. None for any other, such as a literal ``%``, which
    strftime would read as an element, or SSSS, which is not SS twice."""
    written = ""
    for token in tokens(template):
        if not token.keyword:
            if token.text not in _PLAIN_LITERALS:
                return None
            written += token.text
            continue
        element = token.text.upper()
        if token.fill or token.translated or token.suffix or element not in _PLAIN_STRFTIME:
            return None
        written += _PLAIN_STRFTIME[element]
    return written


# --- Writing a template's tokens in an engine's SQL ---------------------------------------------

#: The value, in each form below: a column of this name, replaced by the call's value.
_VALUE = "__to_char_value"


@dataclass(frozen=True)
class _Engine:
    """An engine's SQL of the parts of a timestamp each keyword writes, each of the value
    ``{v}``: integers, in the engine's dialect. ``bc``: whether the engine holds a year before 1
    (a year 0 or below, as the ISO calendar counts the years before Christ)."""

    dialect: str
    year: str
    month: str
    day: str
    hour: str
    minute: str
    second: str
    micro: str  # the microseconds past the second
    doy: str
    isodow: str  # Monday 1 .. Sunday 7
    isoweek: str
    isoyear: str
    julian: str  # the days since 4714-11-24 BC, as PostgreSQL's J counts them
    text_form: str  # an integer's text, of {x}
    pad_form: str  # a non-negative integer's text padded with zeros to {w}, of {x}
    idiv_form: str  # the quotient of two non-negative integers, of {a} and {b}
    array_form: str  # the element at {i} (from 1) of an array of {items}
    bc: bool

    def text(self, x: str) -> str:
        return self.text_form.format(x=x)

    def pad(self, x: str, width: int) -> str:
        return self.pad_form.format(x=x, w=width)

    def wide(self, x: str, width: int) -> str:
        """A non-negative integer's text padded to ``width``, its every digit kept past it (an
        engine's pad cuts a text longer than the width)."""
        return f"CASE WHEN {x} >= {10**width} THEN {self.text(x)} ELSE {self.pad(x, width)} END"

    def idiv(self, a: str, b: int) -> str:
        return self.idiv_form.format(a=a, b=b)

    def adjusted(self, year: str) -> str:
        """formatting.c's ADJUST_YEAR: a year before Christ as its number (1 BC, ISO's 0, is 1)."""
        return f"(CASE WHEN {year} <= 0 THEN 1 - {year} ELSE {year} END)" if self.bc else year


_CLICKHOUSE = _Engine(
    dialect="clickhouse",
    year="toYear({v})",
    month="toMonth({v})",
    day="toDayOfMonth({v})",
    hour="toHour({v})",
    minute="toMinute({v})",
    second="toSecond({v})",
    micro="positiveModulo(toUnixTimestamp64Micro({v}), 1000000)",
    doy="toDayOfYear({v})",
    isodow="toDayOfWeek({v})",
    isoweek="toISOWeek({v})",
    isoyear="toISOYear({v})",
    julian="(dateDiff('day', toDate32('1970-01-01'), toDate32({v})) + 2440588)",
    text_form="toString({x})",
    pad_form="leftPad(toString({x}), {w}, '0')",
    idiv_form="intDiv({a}, {b})",
    array_form="[{items}][{i}]",
    bc=False,  # DateTime64 holds 1900-01-01 .. 2299-12-31
)
_TRINO = _Engine(
    dialect="trino",
    year="year({v})",
    month="month({v})",
    day="day({v})",
    hour="hour({v})",
    minute="minute({v})",
    second="second({v})",
    # The six digits past the point of the value's text at microseconds.
    micro="CAST(substr(CAST(CAST({v} AS TIMESTAMP(6)) AS VARCHAR), -6) AS BIGINT)",
    doy="day_of_year({v})",
    isodow="day_of_week({v})",
    isoweek="week({v})",
    isoyear="year_of_week({v})",
    julian="(date_diff('day', DATE '1970-01-01', CAST({v} AS DATE)) + 2440588)",
    text_form="CAST({x} AS VARCHAR)",
    pad_form="lpad(CAST({x} AS VARCHAR), {w}, '0')",
    idiv_form="(({a}) / {b})",  # of two bigints, Trino's quotient is an integer
    array_form="ARRAY[{items}][{i}]",
    bc=True,
)
#: DuckDB's, written in PostgreSQL's SQL: the DuckDB path rewrites to_char in the statement's
#: text, before it is transpiled (provisa.transpiler.transpile._duckdb_form).
_DUCKDB = _Engine(
    dialect="postgres",
    year="EXTRACT(YEAR FROM {v})",
    month="EXTRACT(MONTH FROM {v})",
    day="EXTRACT(DAY FROM {v})",
    hour="EXTRACT(HOUR FROM {v})",
    minute="EXTRACT(MINUTE FROM {v})",
    second="CAST(TRUNC(EXTRACT(SECOND FROM {v})) AS BIGINT)",
    micro="(EXTRACT(MICROSECONDS FROM {v}) % 1000000)",
    doy="EXTRACT(DOY FROM {v})",
    isodow="EXTRACT(ISODOW FROM {v})",
    isoweek="EXTRACT(WEEK FROM {v})",
    isoyear="EXTRACT(ISOYEAR FROM {v})",
    julian="((CAST({v} AS DATE) - DATE '1970-01-01') + 2440588)",
    text_form="CAST({x} AS TEXT)",
    pad_form="LPAD(CAST({x} AS TEXT), {w}, '0')",
    # DuckDB's / of two integers is a double's; its integer quotient is written so.
    idiv_form="CAST(TRUNC(({a}) / {b}) AS BIGINT)",
    array_form="(ARRAY[{items}])[{i}]",
    bc=True,
)
ENGINES = {"clickhouse": _CLICKHOUSE, "trino": _TRINO, "duckdb": _DUCKDB}

_MONTHS = (
    "January",
    "February",
    "March",
    "April",
    "May",
    "June",
    "July",
    "August",
    "September",
    "October",
    "November",
    "December",
)
_DAYS = ("Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday")
_ROMAN = ("I", "II", "III", "IV", "V", "VI", "VII", "VIII", "IX", "X", "XI", "XII")
#: The name keywords: the names (by month, or by ISO day of the week), and the width a name is
#: padded to with blanks unless FM.
_NAMES = {
    "MONTH": (_MONTHS, 9),
    "MON": (tuple(m[:3] for m in _MONTHS), 0),
    "DAY": (_DAYS, 9),
    "DY": (tuple(d[:3] for d in _DAYS), 0),
    "RM": (_ROMAN, 4),
}
#: The keywords TM translates: their names are lc_time's.
_TRANSLATED = frozenset({"MONTH", "MON", "DAY", "DY"})


def _cased(name: str, element: str) -> str:
    """A name in the case its keyword is spelled in: MONTH, Month or month."""
    if element.isupper():
        return name.upper()
    if element.islower():
        return name.lower()
    return name


def _quoted(text: str) -> str:
    return "'" + text.replace("'", "''") + "'"


def _ordinal(number: str, lower: bool) -> str:
    """formatting.c's str_numth of a non-negative integer's text: TH after 11, 12 and 13 (any
    number whose tens digit is 1), else ST, ND or RD after 1, 2 or 3."""
    th, st, nd, rd = ("th", "st", "nd", "rd") if lower else ("TH", "ST", "ND", "RD")
    return (
        f"CASE WHEN ({number}) % 100 BETWEEN 11 AND 13 THEN '{th}' "
        f"WHEN ({number}) % 10 = 1 THEN '{st}' WHEN ({number}) % 10 = 2 THEN '{nd}' "
        f"WHEN ({number}) % 10 = 3 THEN '{rd}' ELSE '{th}' END"
    )


def _number_of(token: Token, e: _Engine) -> tuple[str, str] | None:
    """A numeric keyword's text and the number its ordinal suffix reads (each of ``{v}``); None
    for a keyword that is no number."""
    element, fill = token.text.upper(), token.fill
    year, isoyear = e.adjusted(e.year), e.adjusted(e.isoyear)
    hour12 = f"(CASE WHEN {e.hour} % 12 = 0 THEN 12 ELSE {e.hour} % 12 END)"
    # The integer, the width formatting.c pads it to (0: none), whether FM drops that padding
    # (a fraction's digits are kept), and whether it may have more digits than the width.
    numbers: dict[str, tuple[str, int, bool, bool]] = {
        "HH24": (e.hour, 2, True, False),
        "HH12": (hour12, 2, True, False),
        "HH": (hour12, 2, True, False),
        "MI": (e.minute, 2, True, False),
        "SS": (e.second, 2, True, False),
        "FF1": (e.idiv(e.micro, 100000), 1, False, False),
        "FF2": (e.idiv(e.micro, 10000), 2, False, False),
        "FF3": (e.idiv(e.micro, 1000), 3, False, False),
        "MS": (e.idiv(e.micro, 1000), 3, False, False),
        "FF4": (e.idiv(e.micro, 100), 4, False, False),
        "FF5": (e.idiv(e.micro, 10), 5, False, False),
        "FF6": (e.micro, 6, False, False),
        "US": (e.micro, 6, False, False),
        "SSSS": (f"({e.hour} * 3600 + {e.minute} * 60 + {e.second})", 0, True, False),
        "SSSSS": (f"({e.hour} * 3600 + {e.minute} * 60 + {e.second})", 0, True, False),
        "YYYY": (year, 4, True, True),
        "IYYY": (isoyear, 4, True, True),
        "YYY": (f"({year} % 1000)", 3, True, False),
        "IYY": (f"({isoyear} % 1000)", 3, True, False),
        "YY": (f"({year} % 100)", 2, True, False),
        "IY": (f"({isoyear} % 100)", 2, True, False),
        "Y": (f"({year} % 10)", 0, True, False),
        "I": (f"({isoyear} % 10)", 0, True, False),
        "MM": (e.month, 2, True, False),
        "DDD": (e.doy, 3, True, False),
        "IDDD": (f"(({e.isoweek} - 1) * 7 + {e.isodow})", 3, True, False),
        "DD": (e.day, 2, True, False),
        "D": (f"({e.isodow} % 7 + 1)", 0, True, False),
        "ID": (e.isodow, 0, True, False),
        "W": (f"({e.idiv(f'{e.day} - 1', 7)} + 1)", 0, True, False),
        "WW": (f"({e.idiv(f'{e.doy} - 1', 7)} + 1)", 2, True, False),
        "IW": (e.isoweek, 2, True, False),
        "J": (e.julian, 0, True, False),
        "Q": (f"({e.idiv(f'{e.month} - 1', 3)} + 1)", 0, True, False),
    }
    if element == "Y,YYY":  # the thousands, a comma, and the rest padded to 3; FM pads it still
        text = f"{e.text(e.idiv(year, 1000))} || ',' || {e.pad(f'{year} % 1000', 3)}"
        return text, year
    if element == "CC":
        return _century(e, fill)
    if element not in numbers:
        return None
    number, width, fm_drops, wide = numbers[element]
    if width == 0 or (fill and fm_drops):
        return e.text(number), number
    return (e.wide(number, width) if wide else e.pad(number, width)), number


def _century(e: _Engine, fill: bool) -> tuple[str, str]:
    """CC: (year - 1) / 100 + 1 of a year after Christ, padded to 2; of one before, year / 100 - 1
    (formatting.c's tm_year, ISO's count: 1 BC is 0), its sign then 2 digits."""
    after = f"({e.idiv(f'{e.year} - 1', 100)} + 1)"
    written = e.text(after) if fill else e.wide(after, 2)
    if not e.bc:
        return written, after
    before = f"({e.idiv(f'-{e.year}', 100)} + 1)"  # the magnitude of year / 100 - 1, truncated
    minus = f"'-' || {e.text(before) if fill else e.wide(before, 2)}"
    return (
        f"CASE WHEN {e.year} > 0 THEN {written} ELSE {minus} END",
        f"CASE WHEN {e.year} > 0 THEN {after} ELSE {before} END",
    )


def _named(token: Token, e: _Engine, zoned: bool | None) -> str | None:
    """A name keyword's text (of ``{v}``), or a constant's; None for a keyword that is no name."""
    element = token.text
    upper = element.upper().replace(".", "")
    if upper in _NAMES:
        names, width = _NAMES[upper]
        index = e.month if upper in ("MONTH", "MON", "RM") else e.isodow
        padded = [_cased(n, element).ljust(0 if token.fill else width) for n in names]
        return e.array_form.format(items=", ".join(map(_quoted, padded)), i=index)
    if upper in ("AM", "PM"):
        am, pm = ("A.M.", "P.M.") if "." in element else ("AM", "PM")
        am, pm = (am.lower(), pm.lower()) if element.islower() else (am, pm)
        return f"CASE WHEN {e.hour} >= 12 THEN {_quoted(pm)} ELSE {_quoted(am)} END"
    if upper in ("AD", "BC"):
        ad, bc = ("A.D.", "B.C.") if "." in element else ("AD", "BC")
        ad, bc = (ad.lower(), bc.lower()) if element.islower() else (ad, bc)
        return f"CASE WHEN {e.year} <= 0 THEN {_quoted(bc)} ELSE {_quoted(ad)} END"
    if upper == "FX":  # to_date's fixed format: nothing to to_char
        return "''"
    if upper in ("TZ", "OF", "TZH", "TZM"):
        if zoned is None:
            return None  # the value's type is not read: whether it has a zone is not known
        # A timestamp has no zone (formatting.c's tzn is NULL, its offset 0); an instant is read
        # in the session's zone, UTC wherever this is called with zoned.
        if upper == "TZ":
            return _quoted(
                ("UTC" if zoned else "") if element.isupper() else ("utc" if zoned else "")
            )
        if upper == "OF":
            return "'+0'" if token.fill else "'+00'"
        return "'+00'" if upper == "TZH" else "'00'"
    return None


def _written(token: Token, e: _Engine, zoned: bool | None) -> str:
    """One keyword's text in the engine's SQL, of ``{v}``. Raises NotImplementedError naming a
    keyword with no exact form."""
    if token.translated and token.text.upper() in _TRANSLATED:
        raise _refused(token.written)  # lc_time's names, which the engine does not read
    numeric = _number_of(token, e)
    if numeric is not None:
        text, number = numeric
        if token.suffix in ("TH", "th"):
            return f"{text} || {_ordinal(number, token.suffix == 'th')}"
        return text
    named = _named(token, e, zoned)
    if named is None:
        raise _refused(token.written)
    return named  # a suffix after a name is read and writes nothing


@functools.lru_cache(maxsize=4096)
def _piece(sql: str, dialect: str) -> exp.Expr:
    return sqlglot.parse_one(sql, read=dialect)


def written(template: str, value: exp.Expr, engine: str, zoned: bool | None) -> exp.Expr:
    """PostgreSQL's to_char(``value``, ``template``) in ``engine``'s SQL (clickhouse, trino, or
    duckdb, in PostgreSQL's SQL), each token of :func:`tokens` written as the text formatting.c
    writes of it, and the pieces concatenated: a character written as it is is a string constant.
    ``zoned``: whether the value is an instant read in UTC (True), a timestamp (False), or of a
    type not read here (None: a zone's keyword is refused). Raises NotImplementedError naming a
    keyword with no exact form."""
    e = ENGINES[engine]
    if not template:
        return exp.cast(exp.null(), "TEXT")  # formatting.c answers NULL of an empty template
    pieces: list[exp.Expr] = []
    text = ""
    reads = False  # whether a piece reads the value: to_char is strict
    for token in tokens(template):
        if not token.keyword:
            text += token.text
            continue
        sql = _written(token, e, zoned)
        if "{v}" not in sql:  # a constant: its text
            text += sql[1:-1].replace("''", "'")
            continue
        if text:
            pieces.append(exp.Literal.string(text))
            text = ""
        reads = True
        pieces.append(_piece(sql.format(v=_VALUE), e.dialect).copy())
    if text or not pieces:
        pieces.append(exp.Literal.string(text))
    out = pieces[0]
    for piece in pieces[1:]:
        out = exp.DPipe(this=out, expression=piece)
    out = out.transform(
        lambda node: value.copy() if isinstance(node, exp.Column) and node.name == _VALUE else node
    )
    if not reads:  # NULL of a NULL value, though no keyword reads it
        out = exp.Case(
            ifs=[exp.If(this=exp.Is(this=value.copy(), expression=exp.Null()), true=exp.Null())],
            default=out,
        )
    return exp.Paren(this=out)


def _text(node: exp.Expr) -> exp.Expr:
    return exp.cast(node, "TEXT")


# A numeric format this translation carries: FM, then digit positions (9, 0) with group
# separators among them, then a decimal point and the fraction's digit positions.
_NUMERIC = re.compile(r"^(?P<fm>FM)?(?P<int>[90,]*)(?:(?P<dot>\.)(?P<frac>[90]*))?$")


def _is_numeric(fmt: str) -> bool:
    """Whether a format is a number's: a digit position (9 or 0) outside quoted text, other than
    the digits of HH24, HH12 and FF1..FF6."""
    unquoted = re.sub(r'"[^"]*"', "", fmt).upper()
    unquoted = re.sub(r"HH24|HH12|FF[1-6]", "", unquoted)
    return bool(re.search(r"[90]", unquoted))


def _numeric(value: exp.Expr, fmt: str) -> exp.Expr:
    match = _NUMERIC.match(fmt)
    if match is None or (match["int"] and match["int"].startswith(",")):
        rest = fmt[2:] if fmt.startswith("FM") else fmt
        element = next(
            (c for c in re.findall(r"EEEE|PL|SG|MI|RN|rn|TH|th|.", rest) if c not in "90,."),
            rest,
        )
        raise _refused(element)
    fill = match["fm"] is not None
    integer = match["int"] or ""
    fraction = match["frac"] or ""
    digits = integer.replace(",", "")
    n, d = len(digits), len(fraction)
    first_zero = digits.find("0")
    # A 0 writes its digit and every one after it; a 9 drops an insignificant zero -- the lone
    # zero of a value under one too, but only before a fraction (to_char(0, '999') is '   0').
    least = n - first_zero if first_zero >= 0 else (0 if d else 1)
    kept = fraction.rfind("0") + 1  # FM drops a fraction's trailing zeros only at its 9s

    magnitude = exp.cast(
        exp.Round(
            this=exp.Abs(this=exp.cast(value.copy(), "DECIMAL(38, 10)")),
            decimals=exp.Literal.number(d),
        ),
        f"DECIMAL(38, {d})",
    )
    written = _text(magnitude)
    whole = exp.Anonymous(
        this="split_part", expressions=[written, exp.Literal.string("."), exp.Literal.number(1)]
    )
    significant = exp.Case(
        ifs=[
            exp.If(
                this=exp.EQ(this=whole, expression=exp.Literal.string("0")),
                true=exp.Literal.string(""),
            )
        ],
        default=whole.copy(),
    )
    overflow = exp.GT(this=exp.Length(this=significant), expression=exp.Literal.number(n))
    padded = exp.Anonymous(
        this="lpad",
        expressions=[
            exp.Anonymous(
                this="lpad",
                expressions=[
                    significant.copy(),
                    exp.cast(
                        exp.Greatest(
                            this=exp.Literal.number(least),
                            expressions=[exp.Length(this=significant.copy())],
                        ),
                        "INT",
                    ),
                    exp.Literal.string("0"),
                ],
            ),
            exp.Literal.number(n),
            exp.Literal.string(" "),
        ],
    )

    def hashes_or(count: int, otherwise: exp.Expr) -> exp.Expr:
        return exp.Case(
            ifs=[
                exp.If(
                    this=overflow.copy(),
                    true=exp.Anonymous(
                        this="repeat",
                        expressions=[exp.Literal.string("#"), exp.Literal.number(count)],
                    ),
                )
            ],
            default=otherwise,
        )

    positions = hashes_or(n, padded)
    pieces: list[exp.Expr] = []
    at = 0  # digit positions written so far
    for ch in integer:
        if ch == ",":
            # Written where a digit precedes it, as PostgreSQL writes it; a blank otherwise.
            before = exp.Substring(
                this=positions.copy(), start=exp.Literal.number(at), length=exp.Literal.number(1)
            )
            pieces.append(
                exp.Case(
                    ifs=[
                        exp.If(
                            this=exp.EQ(this=before, expression=exp.Literal.string(" ")),
                            true=exp.Literal.string(" "),
                        )
                    ],
                    default=exp.Literal.string(","),
                )
            )
        else:
            at += 1
            pieces.append(
                exp.Substring(
                    this=positions.copy(),
                    start=exp.Literal.number(at),
                    length=exp.Literal.number(1),
                )
            )
    if match["dot"]:
        pieces.append(exp.Literal.string("."))
        part = exp.Anonymous(
            this="split_part",
            expressions=[written.copy(), exp.Literal.string("."), exp.Literal.number(2)],
        )
        if fill and kept < d:
            part = exp.DPipe(
                this=exp.Substring(
                    this=part, start=exp.Literal.number(1), length=exp.Literal.number(kept)
                ),
                expression=exp.Anonymous(
                    this="rtrim",
                    expressions=[
                        exp.Substring(this=part.copy(), start=exp.Literal.number(kept + 1)),
                        exp.Literal.string("0"),
                    ],
                ),
            )
        pieces.append(hashes_or(d, part))
    body: exp.Expr = pieces[0] if pieces else exp.Literal.string("")
    for p in pieces[1:]:
        body = exp.DPipe(this=body, expression=p)
    negative = exp.LT(this=value.copy(), expression=exp.Literal.number(0))
    sign = exp.Case(
        ifs=[exp.If(this=negative, true=exp.Literal.string("-"))],
        default=exp.Literal.string("" if fill else " "),
    )
    signed = exp.DPipe(this=sign, expression=exp.Anonymous(this="ltrim", expressions=[body]))
    if fill:
        return signed
    width = len(fmt) + 1  # a position for the sign
    return exp.Anonymous(
        this="lpad",
        expressions=[signed, exp.Literal.number(width), exp.Literal.string(" ")],
    )


def _zoned(value: exp.Expr) -> bool | None:
    """False for a value the statement casts to timestamp, which has no zone; None for any other,
    whose type is not read here."""
    if isinstance(value, exp.Cast) and value.to.this == exp.DataType.Type.TIMESTAMP:
        return False
    return None


def _translated(call: exp.Anonymous) -> exp.Expr:
    if len(call.expressions) != 2:
        raise NotImplementedError("to_char takes a value and a format")
    value, fmt = call.expressions
    if isinstance(fmt, exp.Null) or (isinstance(fmt, exp.Cast) and isinstance(fmt.this, exp.Null)):
        return exp.cast(exp.null(), "TEXT")  # to_char is strict: a NULL format answers NULL
    if not (isinstance(fmt, exp.Literal) and fmt.is_string):
        raise NotImplementedError(
            "to_char: a format that is not a literal is not supported; each of its elements is "
            "translated where the statement is read"
        )
    pattern = fmt.this
    if _is_numeric(pattern) and _zoned(value) is None:  # a timestamp's template is a date's
        return exp.Paren(this=_numeric(value, pattern))
    return written(pattern, value, "duckdb", _zoned(value))


def to_char_for_duckdb(pg_sql: str) -> str:  # REQ-2167
    """``pg_sql`` with each to_char call written in DuckDB's functions (see the module)."""
    if "to_char" not in pg_sql.lower():
        return pg_sql
    tree = sqlglot.parse_one(pg_sql, read=_CallsKept)

    def _call(node: exp.Expr) -> exp.Expr:
        target = node
        if (
            isinstance(node, exp.Dot)
            and isinstance(node.this, exp.Identifier)
            and node.this.name.lower() == "pg_catalog"
        ):
            target = node.expression
        if isinstance(target, exp.Anonymous) and target.name.lower() == "to_char":
            for i, arg in enumerate(target.expressions):
                target.expressions[i] = arg.transform(_call)
            return _translated(target)
        return node

    return tree.transform(_call).sql(dialect="postgres")
