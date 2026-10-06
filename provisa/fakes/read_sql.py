# Copyright (c) 2026 Kenneth Stott
# Canary: 6b7a9d86-b47b-4db2-93c5-0a7fcbf05305
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A faked read's expression for each kind of fake (REQ-1494), in the governed dialect.

Every expression is a keyed function of the real value, computed by the serving engine: the
uniform point ``u`` in [0, 1) is the value's digest under the platform key
(``provisa_digest(fingerprint, text)``) scaled, so one value always gives one fake and ordering,
filtering, grouping and joining work on the fakes. A relative fake reads the faked form of the
column it names. The projection that holds these expressions is :mod:`provisa.fakes.projection`.

A fake that needs measured data at read time (categories(), bool(), profile(), pattern(), after
and the like with no distance) and a stable fake are refused by name until their data reaches the
read; encrypt is refused until it is built.
"""

# Requirements: REQ-1494

from __future__ import annotations

import json
import math
import re
from itertools import groupby
from collections.abc import Callable
from dataclasses import dataclass

import sqlglot
from sqlglot import exp

from provisa.fakes.measurement import Measured
from provisa.fakes.kinds import (
    Bool,
    Bucket,
    Categories,
    Encrypt,
    FakeKind,
    Hash,
    LogNormal,
    Method,
    Normal,
    Ordered,
    Pattern,
    Percentiles,
    Poisson,
    Prefix,
    Profile,
    Sequence,
    Sql,
    Triangular,
    Truncate,
    Uniform,
    kind_name,
)

#: Methods whose fakes must stay distinct for distinct values (REQ-1494, UNIQUENESS): each carries
#: a short tag of the value's digest.
TAGGED_EMAIL = frozenset({"email", "free_email", "company_email", "safe_email", "ascii_email"})
TAGGED_IDENTIFIER = frozenset({"user_name", "ssn", "license_plate", "ean", "ean13", "isbn13"})
TAGGED_PHONE = frozenset({"phone_number", "msisdn"})


class FakeReadRefused(ValueError):
    """A column whose fake this read cannot compute, said by name."""


@dataclass(frozen=True)
class Column:
    """A faked column as the projection sees it."""

    name: str
    data_type: str
    family: str  # provisa.fakes.checks.family
    kind: FakeKind
    stable: bool
    measured: Measured | None = None  # bound at model build, for a kind that is measured


def _lit(text: str) -> str:
    return "'" + text.replace("'", "''") + "'"


def _q(name: str) -> str:
    return '"' + name.replace('"', '""') + '"'


def digest_sql(fingerprint: str, value_sql: str, salt: str = "") -> str:
    text = f"CAST({value_sql} AS VARCHAR)"
    if salt:
        text = f"({text} || {_lit(':' + salt)})"
    return f"provisa_digest({_lit(fingerprint)}, {text})"


def uniform_sql(digest: str) -> str:
    """[0, 1) from a signed 64-bit digest."""
    return f"(CAST({digest} AS DOUBLE PRECISION) / 18446744073709551616.0 + 0.5)"


# Acklam's rational approximation to the inverse standard normal CDF (relative error < 1.2e-9).
_A = (-39.69683028665376, 220.9460984245205, -275.9285104469687, 138.3577518672690,
      -30.66479806614716, 2.506628277459239)  # fmt: skip
_B = (-54.47609879822406, 161.5858368580409, -155.6989798598866, 66.80131188771972,
      -13.28068155288572)  # fmt: skip
_C = (-0.007784894002430293, -0.3223964580411365, -2.400758277161838, -2.549732539343734,
      4.374664141464968, 2.938163982698783)  # fmt: skip
_D = (0.007784695709041462, 0.3224671290700398, 2.445134137142996, 3.754408661907416)
_P_LOW = 0.02425


def _poly(coeffs: tuple[float, ...], x: str) -> str:
    out = repr(coeffs[0])
    for c in coeffs[1:]:
        out = f"(({out}) * {x} + {c!r})"
    return out


def inverse_normal_sql(u: str) -> str:
    """The standard normal quantile of ``u`` (a column, so read many times at no cost)."""
    q_mid = f"({u} - 0.5)"
    r_mid = f"({q_mid} * {q_mid})"
    central = f"({_poly(_A, r_mid)} * {q_mid} / ({_poly(_B, r_mid)} * {r_mid} + 1.0))"
    q_lo = f"SQRT(-2.0 * LN(GREATEST({u}, 1e-300)))"
    low = f"({_poly(_C, q_lo)} / ({_poly(_D, q_lo)} * {q_lo} + 1.0))"
    q_hi = f"SQRT(-2.0 * LN(GREATEST(1.0 - {u}, 1e-300)))"
    high = f"(-({_poly(_C, q_hi)}) / ({_poly(_D, q_hi)} * {q_hi} + 1.0))"
    return (
        f"(CASE WHEN {u} < {_P_LOW} THEN {low} WHEN {u} > {1 - _P_LOW!r} THEN {high} "
        f"ELSE {central} END)"
    )


def _pick(u: str, values: list[str], shares: list[float]) -> str:
    """The value whose cumulative share first exceeds ``u``."""
    total = 0.0
    whens = []
    for v, s in zip(values[:-1], shares[:-1]):
        total += s
        whens.append(f"WHEN {u} < {total!r} THEN {v}")
    return f"(CASE {' '.join(whens)} ELSE {values[-1]} END)" if whens else values[-1]


def _clamp(x: str, lo: float | None, hi: float | None) -> str:
    if lo is not None:
        x = f"GREATEST({x}, {lo!r})"
    if hi is not None:
        x = f"LEAST({x}, {hi!r})"
    return x


def _poisson_table(mean: float) -> tuple[list[str], list[float]]:
    top = int(mean + 12 * math.sqrt(mean) + 12)
    p = math.exp(-mean)
    values, shares = [], []
    for k in range(top + 1):
        values.append(str(k))
        shares.append(p)
        p *= mean / (k + 1)
    return values, shares


def _numeric_out(x: str, col: Column, temporal: bool) -> str:
    """A number (seconds since the epoch for a temporal kind) cast to the column's type."""
    if col.family == "integer":
        return f"CAST(ROUND({x}) AS {col.data_type})"
    if temporal:
        # At UTC, as _epoch reads a timestamp: a cast of the zoned value would read local time.
        stamp = f"CAST(TO_TIMESTAMP({x}) AT TIME ZONE 'UTC' AS TIMESTAMP)"
        return f"CAST({stamp} AS DATE)" if col.family == "date" else stamp
    return f"CAST({x} AS {col.data_type})"


def _epoch(x: str, family: str) -> str:
    if family in ("date", "timestamp"):
        return f"EXTRACT(EPOCH FROM CAST({x} AS TIMESTAMP))"
    return x


_SECONDS = {
    "second": 1.0,
    "minute": 60.0,
    "hour": 3600.0,
    "day": 86400.0,
    "week": 7 * 86400.0,
    "month": 30 * 86400.0,
    "year": 365.25 * 86400.0,
}


def _tag(digest: str) -> str:
    return f"provisa_digest_tag({digest})"


def expression(
    col: Column,
    *,
    real: str,
    u: str,
    digest: str,
    faked: Callable[[str], str],
    family_of: Callable[[str], str],
) -> str:
    """The faked value of ``col``. ``real`` and ``u`` are SQL for its real value and uniform point;
    ``digest`` SQL for its digest; ``faked(name)`` the faked form of another column, and
    ``family_of(name)`` that column's family."""
    if col.stable:
        raise FakeReadRefused(
            f"{col.name}: a stable fake is not yet computed on a faked read (REQ-1494)"
        )
    k = col.kind
    guarded = True
    if isinstance(k, Method):
        args = json.dumps(dict(k.args), sort_keys=True, separators=(",", ":"))
        value = f"provisa_fake_method({_lit(k.name)}, {_lit(args)}, {digest})"
        if k.name in TAGGED_EMAIL:
            value = f"REPLACE({value}, '@', '.' || {_tag(digest)} || '@')"
        elif k.name in TAGGED_IDENTIFIER:
            value = f"({value} || '-' || {_tag(digest)})"
        elif k.name in TAGGED_PHONE:
            value = f"({value} || ' x' || {_tag(digest)})"
        out = value if col.family == "text" else f"CAST({value} AS {col.data_type})"
    elif isinstance(k, Bool):
        share = _measured(col).true_share if k.share is None else k.share
        out = f"({u} < {share!r})"
    elif isinstance(k, Categories):
        if k.values is None:
            out = _pick_measured(u, _measured(col).values, col)
        else:
            literals = [_category_literal(v, col) for v in k.values]
            shares = list(k.shares) if k.shares else [1 / len(literals)] * len(literals)
            out = _pick(u, literals, shares)
    elif isinstance(k, Uniform):
        out = _numeric_out(f"({k.min!r} + {u} * {k.max - k.min!r})", col, k.temporal)
    elif isinstance(k, Triangular):
        span = k.max - k.min
        c = (k.mode - k.min) / span if span else 0.0
        x = (
            f"(CASE WHEN {u} < {c!r} THEN {k.min!r} + SQRT({u} * {span * (k.mode - k.min)!r}) "
            f"ELSE {k.max!r} - SQRT((1.0 - {u}) * {span * (k.max - k.mode)!r}) END)"
        )
        out = _numeric_out(x, col, k.temporal)
    elif isinstance(k, Normal):
        x = _clamp(f"({k.mean!r} + {k.sd!r} * {inverse_normal_sql(u)})", k.min, k.max)
        out = _numeric_out(x, col, k.temporal)
    elif isinstance(k, LogNormal):
        origin = k.min if k.temporal and k.min is not None else 0.0
        x = f"({origin!r} + EXP({k.mu!r} + {k.sigma!r} * {inverse_normal_sql(u)}))"
        out = _numeric_out(_clamp(x, k.min, k.max), col, k.temporal)
    elif isinstance(k, Percentiles):
        out = _numeric_out(_piecewise(u, k.points), col, k.temporal)
    elif isinstance(k, Poisson):
        values, shares = _poisson_table(k.mean)
        out = f"CAST({_pick(u, values, shares)} AS {col.data_type})"
    elif isinstance(k, Bucket):
        if k.width is not None:
            out = f"CAST(FLOOR({real} / {k.width!r}) * {k.width!r} AS {col.data_type})"
        else:
            assert k.edges is not None  # a bucket names a width or its edges
            whens = " ".join(
                f"WHEN {real} < {hi!r} THEN {lo!r}" for lo, hi in zip(k.edges, k.edges[1:])
            )
            out = f"CAST(CASE WHEN {real} < {k.edges[0]!r} THEN {k.edges[0]!r} {whens} ELSE {k.edges[-1]!r} END AS {col.data_type})"
    elif isinstance(k, Truncate):
        out = f"CAST(DATE_TRUNC({_lit(k.unit)}, {real}) AS {col.data_type})"
    elif isinstance(k, Prefix):
        out = f"SUBSTRING({real}, 1, {k.length})"
    elif isinstance(k, Hash):
        out = _tag(digest) if col.family == "text" else f"CAST({digest} AS {col.data_type})"
    elif isinstance(k, Ordered):
        named = faked(k.column)
        if k.distance is None:
            # The measured difference (this column less the named one), signed as measured.
            amount = _piecewise(u, _measured(col).points)
            sign = "+"
        else:
            scale = _SECONDS[k.distance.unit] if k.distance.unit else 1.0
            low = k.distance.low * scale
            amount = (
                repr(low)
                if k.distance.high is None
                else f"({low!r} + {u} * {(k.distance.high - k.distance.low) * scale!r})"
            )
            sign = "+" if k.kind in ("after", "greater_than") else "-"
        x = f"({_epoch(named, family_of(k.column))} {sign} {amount})"
        out = _numeric_out(x, col, temporal=k.kind in ("after", "before"))
        guarded = False  # NULL exactly when the column it follows is
    elif isinstance(k, Sql) and not k.group:
        tree = sqlglot.parse_one(k.expression, read="postgres")
        for c in list(tree.find_all(exp.Column)):
            c.replace(sqlglot.parse_one(faked(c.name), read="postgres"))
        out = f"CAST({tree.sql(dialect='postgres')} AS {col.data_type})"
        guarded = False  # NULL as the expression makes it
    elif isinstance(k, Profile):
        m = _measured(col)
        if m.values is not None:
            out = _pick_measured(u, m.values, col)
        else:
            out = _numeric_out(_piecewise(u, m.points), col, col.family in ("date", "timestamp"))
    elif isinstance(k, Pattern):
        shapes = _measured(col).shapes
        assert shapes is not None  # a pattern's measurement holds its shapes, or a refusal
        filled = [_filled_shape(shape, digest) for shape, _ in shapes]
        out = _pick(u, filled, [share for _, share in shapes])
        if col.family != "text":
            out = f"CAST({out} AS {col.data_type})"
    elif isinstance(k, Sequence) or (isinstance(k, Sql) and k.group):
        # Refused when declared as a fake (provisa.fakes.kinds.RULE_ONLY); a fake never holds one.
        raise FakeReadRefused(f"{col.name}: {kind_name(k)}() is a synthetic rule, not a fake")
    elif isinstance(k, Encrypt):
        raise FakeReadRefused(
            f"{col.name}: encrypt() is not yet computed on a faked read (REQ-1494)"
        )
    else:
        raise FakeReadRefused(f"{col.name}: {kind_name(k)}() has no faked read")
    if not guarded:
        return out
    return f"(CASE WHEN {real} IS NULL THEN NULL ELSE {out} END)"


_FAKED = re.compile(r'"__fake__((?:[^"]|"")+)"')


def reads_fakes(sql: str) -> bool:
    """Whether a governed statement reads faked columns (its faked projections name them)."""
    return _FAKED.search(sql) is not None


def require_fake_engine(sql: str, engine: str, computes_fakes: bool) -> None:
    """Refuse, naming the faked columns, a statement that reads fakes on an engine without the fake
    functions (REQ-1494: DuckDB and Trino hold them; every other engine refuses by name)."""
    if computes_fakes:
        return
    names = sorted({m.replace('""', '"') for m in _FAKED.findall(sql)})
    if names:
        raise FakeReadRefused(
            f"{', '.join(names)}: faked columns are computed by the engine, and the {engine} "
            f"engine does not compute fakes"
        )


def _measured(col: Column) -> Measured:
    """The column's measurement, bound at model build (provisa.fakes.measured)."""
    m = col.measured
    if m is None:
        raise FakeReadRefused(
            f"{col.name}: {kind_name(col.kind)}() computes from measured values, and the model's "
            f"measurement of it has not completed"
        )
    if m.refused is not None:
        raise FakeReadRefused(m.refused)
    return m


def _piecewise(u: str, points: tuple[tuple[float, float], ...] | None) -> str:
    """The value at quantile ``u`` of points (quantile, value), joined linearly."""
    assert points, "a measurement or declaration of points holds at least one"
    whens = []
    for (q0, v0), (q1, v1) in zip(points, points[1:]):
        slope = (v1 - v0) / (q1 - q0) if q1 > q0 else 0.0
        whens.append(f"WHEN {u} < {q1!r} THEN {v0!r} + ({u} - {q0!r}) * {slope!r}")
    return f"(CASE WHEN {u} < {points[0][0]!r} THEN {points[0][1]!r} {' '.join(whens)} ELSE {points[-1][1]!r} END)"


def _pick_measured(u: str, values: tuple[tuple[str, float], ...] | None, col: Column) -> str:
    """A measured value at its measured share. Measured values are text, cast to the column."""
    assert values, "a measurement of values holds at least one, or a refusal"
    if col.family == "text":
        literals = [_lit(v) for v, _ in values]
    elif col.family == "boolean":
        literals = [_category_literal(v, col) for v, _ in values]
    else:
        literals = [f"CAST({_lit(v)} AS {col.data_type})" for v, _ in values]
    return _pick(u, literals, [s for _, s in values])


_UPPER = "ABCDEFGHIJKLMNOPQRSTUVWXYZ"


def _filled_shape(shape: str, digest: str) -> str:
    """A value of ``shape`` (A an upper-case letter, a a lower-case one, 9 a digit, every other
    character kept), its letters and digits drawn from the digest by the bothify method. The
    method sees only its own placeholders, so no character of the shape is read as one."""
    template = "".join("#" if ch == "9" else "?" for ch in shape if ch in "Aa9")
    if not template:
        return _lit(shape)
    args = json.dumps({"letters": _UPPER, "text": template}, sort_keys=True, separators=(",", ":"))
    drawn = f"provisa_fake_method('bothify', {_lit(args)}, {digest})"
    parts: list[str] = []
    at = 1  # position in the drawn text, from 1

    def run_of(ch: str) -> str:
        return "kept" if ch not in "Aa9" else "lower" if ch == "a" else "drawn"

    for run, chars in groupby(shape, key=run_of):
        text = "".join(chars)
        if run == "kept":
            parts.append(_lit(text))
            continue
        piece = f"SUBSTRING({drawn}, {at}, {len(text)})"
        parts.append(f"LOWER({piece})" if run == "lower" else piece)
        at += len(text)
    return "(" + " || ".join(parts) + ")"


def _category_literal(value: str, col: Column) -> str:
    if col.family in ("integer", "numeric"):
        return f"CAST({value} AS {col.data_type})"
    if col.family == "boolean":
        return "TRUE" if value.lower() == "true" else "FALSE"
    if col.family in ("date", "timestamp"):
        return f"CAST({_lit(value)} AS {col.data_type})"
    return _lit(value)


__all__ = ["Column", "FakeReadRefused", "digest_sql", "expression", "uniform_sql", "_q"]
