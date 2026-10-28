# Copyright (c) 2026 Kenneth Stott
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""PostgreSQL's ``to_char(value, format)`` on DuckDB (REQ-2167, PGWIRE'S CATALOG FUNCTIONS).

DuckDB has no to_char. Its format is translated here, element by element, into DuckDB's own
functions: a date/time format into strftime and the date parts strftime has no element for, a
numeric format (9, 0, the decimal point, the group separator, FM) into the digits PostgreSQL
writes. An element PostgreSQL has and this translation does not carry is refused by name --
never answered by a guess. sqlglot's own reading of TO_CHAR maps a pattern to strftime element by
element, wrongly for several (D, Day, WW) and not at all for others (Mon, Month, AM, quoted
text), so the call is read here before sqlglot reads it."""

# Requirements: REQ-2167

from __future__ import annotations

import re

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
# written: a strftime element, or a function of the value. An element PostgreSQL has and this
# does not carry is listed with None and refused by name.
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
)
# Each element, as its upper-case spelling for the numeric ones (PostgreSQL reads those in either
# case), longest first.
_ELEMENTS = sorted(
    [*_NUMERIC_ELEMENTS, *(e.lower() for e in _NUMERIC_ELEMENTS), *_NAME_ELEMENTS],
    key=len,
    reverse=True,
)

# strftime's element for each date/time element it carries: (padded, FM).
_STRFTIME: dict[str, tuple[str, str | None]] = {
    "HH24": ("%H", "%-H"),
    "HH12": ("%I", "%-I"),
    "HH": ("%I", "%-I"),
    "MI": ("%M", "%-M"),
    "SS": ("%S", "%-S"),
    "MS": ("%g", None),
    "US": ("%f", None),
    "YYYY": ("%Y", "%Y"),
    "YY": ("%y", None),
    "MM": ("%m", "%-m"),
    "DD": ("%d", "%-d"),
    "DDD": ("%j", "%-j"),
    "ID": ("%u", "%u"),
    "IW": ("%V", None),
}


def _strftime(value: exp.Expr, fmt: str) -> exp.Expr:
    return exp.Anonymous(this="strftime", expressions=[value.copy(), exp.Literal.string(fmt)])


def _text(node: exp.Expr) -> exp.Expr:
    return exp.cast(node, "TEXT")


def _named(value: exp.Expr, element: str, fill: bool) -> exp.Expr:
    """A month or day name, or the meridiem: blank-padded to 9 (a full name, unless FM), in the
    case its element is spelled in."""
    upper = element.upper()
    if upper in ("MONTH", "DAY"):
        name: exp.Expr = _strftime(value, "%B" if upper == "MONTH" else "%A")
        if not fill:
            name = exp.Anonymous(
                this="rpad", expressions=[name, exp.Literal.number(9), exp.Literal.string(" ")]
            )
    elif upper in ("MON", "DY"):
        name = _strftime(value, "%b" if upper == "MON" else "%a")
    elif upper in ("AM", "PM"):
        name = _strftime(value, "%p")
    else:  # TZ
        name = _strftime(value, "%Z")
    if element.isupper() and upper not in ("AM", "PM", "TZ"):
        return exp.Upper(this=name)
    if element.islower():
        return exp.Lower(this=name)
    return name


def _part(value: exp.Expr, unit: str) -> exp.Expr:
    return exp.Extract(this=exp.var(unit), expression=value.copy())


def _datetime(value: exp.Expr, fmt: str) -> exp.Expr:
    pieces: list[exp.Expr | str] = []  # an expression, or a run of strftime format

    def strf(piece: str) -> None:
        if pieces and isinstance(pieces[-1], str):
            pieces[-1] += piece
        else:
            pieces.append(piece)

    i, fill = 0, False
    while i < len(fmt):
        if fmt[i] == '"':
            end = i + 1
            literal = ""
            while end < len(fmt) and fmt[end] != '"':
                if fmt[end] == "\\" and end + 1 < len(fmt):
                    end += 1
                literal += fmt[end]
                end += 1
            strf(literal.replace("%", "%%"))
            i = end + 1
            continue
        if fmt[i] == "\\" and i + 1 < len(fmt):
            strf(fmt[i + 1].replace("%", "%%"))
            i += 2
            continue
        prefix = fmt[i : i + 2]
        if prefix in ("FM", "fm"):
            fill = True
            i += 2
            continue
        if prefix in ("FX", "fx"):  # to_date's fixed-format option: nothing to to_char
            i += 2
            continue
        if prefix in ("TM", "tm"):
            raise _refused(prefix)
        element = next((e for e in _ELEMENTS if fmt.startswith(e, i)), None)
        if element is None:
            strf(fmt[i].replace("%", "%%"))
            i += 1
            continue
        i += len(element)
        if fmt[i : i + 2] in ("TH", "th", "SP"):
            raise _refused(element + fmt[i : i + 2])
        upper = element.upper()
        if upper in _STRFTIME:
            padded, filled = _STRFTIME[upper]
            if fill and filled is None:
                raise _refused("FM" + element)
            strf(filled if fill else padded)  # type: ignore[arg-type]
        elif element in ("MONTH", "Month", "month", "MON", "Mon", "mon", "DAY", "Day", "day"):
            pieces.append(_named(value, element, fill))
        elif element in ("DY", "Dy", "dy", "AM", "am", "PM", "pm", "TZ", "tz"):
            pieces.append(_named(value, element, fill))
        elif upper == "D":  # day of the week, Sunday 1
            pieces.append(
                _text(exp.Add(this=_part(value, "DOW"), expression=exp.Literal.number(1)))
            )
        elif upper == "Q":
            pieces.append(_text(_part(value, "QUARTER")))
        elif upper == "WW":  # week of the year, its first starting on January 1
            week = exp.cast(
                exp.Add(
                    this=exp.Floor(
                        this=exp.Div(
                            this=exp.Sub(
                                this=_part(value, "DOY"), expression=exp.Literal.number(1)
                            ),
                            expression=exp.Literal.number(7),
                        )
                    ),
                    expression=exp.Literal.number(1),
                ),
                "INT",
            )
            pieces.append(
                _text(week)
                if fill
                else exp.Anonymous(
                    this="lpad",
                    expressions=[_text(week), exp.Literal.number(2), exp.Literal.string("0")],
                )
            )
        else:
            raise _refused(("FM" if fill else "") + element)
        fill = False
    parts = [_strftime(value, p) if isinstance(p, str) else p for p in pieces]
    if not parts:
        return exp.Literal.string("")
    out = parts[0]
    for p in parts[1:]:
        out = exp.DPipe(this=out, expression=p)
    return out


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


def _translated(call: exp.Anonymous) -> exp.Expr:
    if len(call.expressions) != 2:
        raise NotImplementedError("to_char takes a value and a format")
    value, fmt = call.expressions
    if not (isinstance(fmt, exp.Literal) and fmt.is_string):
        raise NotImplementedError(
            "to_char: a format that is not a literal is not supported; each of its elements is "
            "translated where the statement is read"
        )
    pattern = fmt.this
    out = _numeric(value, pattern) if _is_numeric(pattern) else _datetime(value, pattern)
    return exp.Paren(this=out)


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
