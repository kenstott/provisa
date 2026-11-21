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
import dataclasses
import re
from itertools import groupby
from collections.abc import Callable
from collections.abc import Sequence as SequenceOf
from dataclasses import dataclass

import sqlglot
from sqlglot import exp

from provisa.fakes.digest import definition_hash
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
    stable_version: int | None = None  # the portable definition version a stable method reads


def definition_of(col: Column) -> int:
    """The definition hash of the column's fake (provisa.fakes.digest.definition_hash): a method
    by its name and arguments, any other kind by its name and declared fields; with the pinned
    portable definition version when stable, and the column's type. REQ-1494: every fake is
    seeded by the value's digest combined with it."""
    k = col.kind
    version = col.stable_version if col.stable else None
    if isinstance(k, Method):
        return definition_hash(k.name, dict(k.args), version, col.data_type)
    return definition_hash(kind_name(k), dataclasses.asdict(k), version, col.data_type)


#: The largest value of each integer type narrower than BIGINT: a remainder by it fits the type.
_INTEGER_BOUND = {
    "tinyint": 127,
    "smallint": 32767,
    "int2": 32767,
    "integer": 2147483647,
    "int": 2147483647,
    "int4": 2147483647,
}


def _num(value: float | int) -> exp.Expr:  # pyright: ignore[reportPrivateImportUsage]
    """A numeric literal as the parser reads one: a negative number is a negated literal."""
    text = repr(value)
    if text.startswith("-"):
        return exp.Neg(this=exp.Literal.number(text[1:]))
    return exp.Literal.number(text)


def _lit(text: str) -> exp.Literal:
    return exp.Literal.string(text)


def _q(name: str) -> str:
    return '"' + name.replace('"', '""') + '"'


def column(name: str) -> exp.Column:
    """A reference to the column ``name`` of the level being read (quoted, unqualified)."""
    return exp.Column(this=exp.to_identifier(name, quoted=True))


def _cast(value: exp.Expr, data_type: str) -> exp.Cast:  # pyright: ignore[reportPrivateImportUsage]
    return exp.Cast(this=value, to=exp.DataType.build(data_type, dialect="postgres"))


def _f(name: str, *args: exp.Expr) -> exp.Expr:  # pyright: ignore[reportPrivateImportUsage]
    return exp.func(name, *args, dialect="postgres")


def _div(this: exp.Expr, by: exp.Expr) -> exp.Div:  # pyright: ignore[reportPrivateImportUsage]
    """``this / by`` as PostgreSQL divides: integer by integer is integer division."""
    return exp.Div(this=this, expression=by, typed=True, safe=False)


def _p(value: exp.Expr) -> exp.Paren:  # pyright: ignore[reportPrivateImportUsage]
    return exp.Paren(this=value)


def _case(
    whens: SequenceOf[tuple[exp.Expr, exp.Expr]],  # pyright: ignore[reportPrivateImportUsage]
    default: exp.Expr | None,  # pyright: ignore[reportPrivateImportUsage]
) -> exp.Case:
    return exp.Case(ifs=[exp.If(this=c, true=v) for c, v in whens], default=default)


def seed(digest: exp.Expr, def_hash: int) -> exp.Expr:  # pyright: ignore[reportPrivateImportUsage]
    """A value's seed: its digest mixed with the definition hash (digest.seed)."""
    return _f("provisa_seed", digest, _bigint(def_hash))


def _bigint(n: int) -> exp.Cast:
    # As text cast: the least BIGINT has no literal (its magnitude is out of range).
    return _cast(_lit(str(n)), "BIGINT")


def digest(fingerprint: str, value: exp.Expr, salt: str = "") -> exp.Expr:  # pyright: ignore[reportPrivateImportUsage]
    """The keyed digest of ``value`` under the platform key's ``fingerprint``."""
    text: exp.Expr = _cast(value, "VARCHAR")  # pyright: ignore[reportPrivateImportUsage]
    if salt:
        text = _p(exp.DPipe(this=text, expression=_lit(":" + salt)))
    return _f("provisa_digest", _lit(fingerprint), text)


def uniform(value: exp.Expr) -> exp.Expr:  # pyright: ignore[reportPrivateImportUsage]
    """[0, 1) from a signed 64-bit digest."""
    scaled = _div(_cast(value, "DOUBLE PRECISION"), exp.Literal.number("18446744073709551616.0"))
    return _p(exp.Add(this=scaled, expression=exp.Literal.number("0.5")))


# Acklam's rational approximation to the inverse standard normal CDF (relative error < 1.2e-9).
_A = (-39.69683028665376, 220.9460984245205, -275.9285104469687, 138.3577518672690,
      -30.66479806614716, 2.506628277459239)  # fmt: skip
_B = (-54.47609879822406, 161.5858368580409, -155.6989798598866, 66.80131188771972,
      -13.28068155288572)  # fmt: skip
_C = (-0.007784894002430293, -0.3223964580411365, -2.400758277161838, -2.549732539343734,
      4.374664141464968, 2.938163982698783)  # fmt: skip
_D = (0.007784695709041462, 0.3224671290700398, 2.445134137142996, 3.754408661907416)
_P_LOW = 0.02425


def _poly(coeffs: tuple[float, ...], x: exp.Expr) -> exp.Expr:  # pyright: ignore[reportPrivateImportUsage]
    out = _num(coeffs[0])
    for c in coeffs[1:]:
        out = _p(exp.Add(this=exp.Mul(this=_p(out), expression=x.copy()), expression=_num(c)))
    return out


def _ratio(top: exp.Expr, bottom: exp.Expr, x: exp.Expr, times: exp.Expr | None = None) -> exp.Expr:  # pyright: ignore[reportPrivateImportUsage]
    """``top [* times] / (bottom * x + 1.0)``."""
    numerator = top if times is None else exp.Mul(this=top, expression=times.copy())
    denominator = _p(
        exp.Add(
            this=exp.Mul(this=bottom, expression=x.copy()), expression=exp.Literal.number("1.0")
        )
    )
    return _div(numerator, denominator)


def inverse_normal(u: exp.Expr) -> exp.Expr:  # pyright: ignore[reportPrivateImportUsage]
    """The standard normal quantile of ``u`` (a column, so read many times at no cost)."""
    q_mid = _p(exp.Sub(this=u.copy(), expression=exp.Literal.number("0.5")))
    r_mid = _p(exp.Mul(this=q_mid.copy(), expression=q_mid.copy()))
    central = _p(_ratio(_poly(_A, r_mid), _poly(_B, r_mid), r_mid, times=q_mid))

    def tail(of: exp.Expr) -> exp.Expr:  # pyright: ignore[reportPrivateImportUsage]
        floor = _f("GREATEST", of, exp.Literal.number("1e-300"))
        return _f(
            "SQRT",
            exp.Mul(this=exp.Neg(this=exp.Literal.number("2.0")), expression=_f("LN", floor)),
        )

    q_lo = tail(u.copy())
    low = _p(_ratio(_poly(_C, q_lo), _poly(_D, q_lo), q_lo))
    q_hi = tail(exp.Sub(this=exp.Literal.number("1.0"), expression=u.copy()))
    high = _p(_ratio(exp.Neg(this=_p(_poly(_C, q_hi))), _poly(_D, q_hi), q_hi))
    return _p(
        _case(
            [
                (exp.LT(this=u.copy(), expression=_num(_P_LOW)), low),
                (exp.GT(this=u.copy(), expression=_num(1 - _P_LOW)), high),
            ],
            central,
        )
    )


def inverse_normal_sql(u: str) -> str:
    """:func:`inverse_normal` as text, for synthetic generation, which builds its statements as
    text (provisa.synthetic.generate)."""
    return inverse_normal(sqlglot.parse_one(u, read="postgres")).sql(dialect="postgres")


def _pick(u: exp.Expr, values: list[exp.Expr], shares: list[float]) -> exp.Expr:  # pyright: ignore[reportPrivateImportUsage]
    """The value whose cumulative share first exceeds ``u``."""
    total = 0.0
    whens = []
    for v, s in zip(values[:-1], shares[:-1]):
        total += s
        whens.append((exp.LT(this=u.copy(), expression=_num(total)), v))
    return _p(_case(whens, values[-1])) if whens else values[-1]


def _clamp(x: exp.Expr, lo: float | None, hi: float | None) -> exp.Expr:  # pyright: ignore[reportPrivateImportUsage]
    if lo is not None:
        x = _f("GREATEST", x, _num(lo))
    if hi is not None:
        x = _f("LEAST", x, _num(hi))
    return x


def _poisson_table(mean: float) -> tuple[list[exp.Expr], list[float]]:  # pyright: ignore[reportPrivateImportUsage]
    top = int(mean + 12 * math.sqrt(mean) + 12)
    p = math.exp(-mean)
    values: list[exp.Expr] = []  # pyright: ignore[reportPrivateImportUsage]
    shares: list[float] = []
    for k in range(top + 1):
        values.append(exp.Literal.number(str(k)))
        shares.append(p)
        p *= mean / (k + 1)
    return values, shares


def _numeric_out(x: exp.Expr, col: Column, temporal: bool) -> exp.Expr:  # pyright: ignore[reportPrivateImportUsage]
    """A number (seconds since the epoch for a temporal kind) cast to the column's type."""
    if col.family == "integer":
        return _cast(_f("ROUND", x), col.data_type)
    if temporal:
        # At UTC, as _epoch reads a timestamp: a cast of the zoned value would read local time.
        zoned = exp.AtTimeZone(this=_f("TO_TIMESTAMP", x), zone=_lit("UTC"))
        stamp = _cast(zoned, "TIMESTAMP")
        return _cast(stamp, "DATE") if col.family == "date" else stamp
    return _cast(x, col.data_type)


def _epoch(x: exp.Expr, family: str) -> exp.Expr:  # pyright: ignore[reportPrivateImportUsage]
    if family in ("date", "timestamp"):
        return exp.Extract(this=exp.var("EPOCH"), expression=_cast(x, "TIMESTAMP"))
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


def _tag(value: exp.Expr) -> exp.Expr:  # pyright: ignore[reportPrivateImportUsage]
    return _f("provisa_digest_tag", value)


def _concat(*parts: exp.Expr) -> exp.Expr:  # pyright: ignore[reportPrivateImportUsage]
    """``a || b || ...``, left to right as the parser reads it."""
    out = parts[0]
    for part in parts[1:]:
        out = exp.DPipe(this=out, expression=part)
    return out


def expression(
    col: Column,
    *,
    real: exp.Expr,  # pyright: ignore[reportPrivateImportUsage]
    u: exp.Expr,  # pyright: ignore[reportPrivateImportUsage]
    digest: exp.Expr,  # pyright: ignore[reportPrivateImportUsage]
    faked: Callable[[str], exp.Expr],  # pyright: ignore[reportPrivateImportUsage]
    family_of: Callable[[str], str],
) -> exp.Expr:  # pyright: ignore[reportPrivateImportUsage]  # REQ-1494, REQ-913
    """The faked value of ``col``, built on the AST. ``real`` and ``u`` are its real value and
    uniform point; ``digest`` its digest; ``faked(name)`` the faked form of another column, and
    ``family_of(name)`` that column's family. Each is copied in where it is read."""
    k = col.kind
    guarded = True
    out: exp.Expr  # pyright: ignore[reportPrivateImportUsage]
    if isinstance(k, Method):
        if col.stable:
            # The portable definition, the same on every engine (provisa.fakes.portable).
            if col.stable_version is None:
                raise FakeReadRefused(
                    f"{col.name}: a stable fake names the portable definition version it is "
                    f"computed by, and none is recorded"
                )
            value = _f(
                "provisa_stable_fake",
                _lit(k.name),
                exp.Literal.number(str(col.stable_version)),
                digest.copy(),
                _bigint(definition_of(col)),
            )
        else:
            args = json.dumps(dict(k.args), sort_keys=True, separators=(",", ":"))
            value = _f(
                "provisa_fake_method",
                _lit(k.name),
                _lit(args),
                digest.copy(),
                _bigint(definition_of(col)),
            )
        if k.name in TAGGED_EMAIL:
            value = _f(
                "REPLACE", value, _lit("@"), _concat(_lit("."), _tag(digest.copy()), _lit("@"))
            )
        elif k.name in TAGGED_IDENTIFIER:
            value = _p(_concat(value, _lit("-"), _tag(digest.copy())))
        elif k.name in TAGGED_PHONE:
            value = _p(_concat(value, _lit(" x"), _tag(digest.copy())))
        out = value if col.family == "text" else _cast(value, col.data_type)
    elif isinstance(k, Bool):
        share = _measured(col).true_share if k.share is None else k.share
        assert share is not None  # a bool's measurement holds its share, or a refusal
        out = _p(exp.LT(this=u.copy(), expression=_num(share)))
    elif isinstance(k, Categories):
        if k.values is None:
            out = _pick_measured(u, _measured(col).values, col)
        else:
            literals = [_category_literal(v, col) for v in k.values]
            shares = list(k.shares) if k.shares else [1 / len(literals)] * len(literals)
            out = _pick(u, literals, shares)
    elif isinstance(k, Uniform):
        x = _p(
            exp.Add(
                this=_num(k.min), expression=exp.Mul(this=u.copy(), expression=_num(k.max - k.min))
            )
        )
        out = _numeric_out(x, col, k.temporal)
    elif isinstance(k, Triangular):
        span = k.max - k.min
        c = (k.mode - k.min) / span if span else 0.0
        rising = exp.Add(
            this=_num(k.min),
            expression=_f("SQRT", exp.Mul(this=u.copy(), expression=_num(span * (k.mode - k.min)))),
        )
        falling = exp.Sub(
            this=_num(k.max),
            expression=_f(
                "SQRT",
                exp.Mul(
                    this=_p(exp.Sub(this=exp.Literal.number("1.0"), expression=u.copy())),
                    expression=_num(span * (k.max - k.mode)),
                ),
            ),
        )
        x = _p(_case([(exp.LT(this=u.copy(), expression=_num(c)), rising)], falling))
        out = _numeric_out(x, col, k.temporal)
    elif isinstance(k, Normal):
        drawn = exp.Add(
            this=_num(k.mean), expression=exp.Mul(this=_num(k.sd), expression=inverse_normal(u))
        )
        out = _numeric_out(_clamp(_p(drawn), k.min, k.max), col, k.temporal)
    elif isinstance(k, LogNormal):
        origin = k.min if k.temporal and k.min is not None else 0.0
        power = exp.Add(
            this=_num(k.mu), expression=exp.Mul(this=_num(k.sigma), expression=inverse_normal(u))
        )
        x = _p(exp.Add(this=_num(origin), expression=_f("EXP", power)))
        out = _numeric_out(_clamp(x, k.min, k.max), col, k.temporal)
    elif isinstance(k, Percentiles):
        out = _numeric_out(_piecewise(u, k.points), col, k.temporal)
    elif isinstance(k, Poisson):
        values, shares = _poisson_table(k.mean)
        out = _cast(_pick(u, values, shares), col.data_type)
    elif isinstance(k, Bucket):
        if k.width is not None:
            floored = _f("FLOOR", _div(real.copy(), _num(k.width)))
            out = _cast(exp.Mul(this=floored, expression=_num(k.width)), col.data_type)
        else:
            assert k.edges is not None  # a bucket names a width or its edges
            whens = [(exp.LT(this=real.copy(), expression=_num(k.edges[0])), _num(k.edges[0]))]
            whens += [
                (exp.LT(this=real.copy(), expression=_num(hi)), _num(lo))
                for lo, hi in zip(k.edges, k.edges[1:])
            ]
            out = _cast(_case(whens, _num(k.edges[-1])), col.data_type)
    elif isinstance(k, Truncate):
        out = _cast(_f("DATE_TRUNC", _lit(k.unit), real.copy()), col.data_type)
    elif isinstance(k, Prefix):
        out = _f(
            "SUBSTRING", real.copy(), exp.Literal.number("1"), exp.Literal.number(str(k.length))
        )
    elif isinstance(k, Hash):
        # The keyed digest mixed with the definition, as every fake is seeded (REQ-1494): one
        # value still gives one hash, and joined columns of one definition still match.
        hashed = seed(digest.copy(), definition_of(col))
        if col.family == "text":
            out = _tag(hashed)
        else:
            # The seed is 64-bit; a narrower integer takes it within its own range (#272), the
            # same on every engine, so one value still gives one hash in either column of a join.
            bound = _INTEGER_BOUND.get(col.data_type.lower().split("(")[0].strip())
            narrowed = (
                hashed
                if bound is None
                else _p(exp.Mod(this=hashed, expression=exp.Literal.number(str(bound))))
            )
            out = _cast(narrowed, col.data_type)
    elif isinstance(k, Ordered):
        named = faked(k.column)
        amount: exp.Expr  # pyright: ignore[reportPrivateImportUsage]
        if k.distance is None:
            # The measured difference (this column less the named one), signed as measured.
            amount = _piecewise(u, _measured(col).points)
            add = True
        else:
            scale = _SECONDS[k.distance.unit] if k.distance.unit else 1.0
            low = k.distance.low * scale
            amount = (
                _num(low)
                if k.distance.high is None
                else _p(
                    exp.Add(
                        this=_num(low),
                        expression=exp.Mul(
                            this=u.copy(),
                            expression=_num((k.distance.high - k.distance.low) * scale),
                        ),
                    )
                )
            )
            add = k.kind in ("after", "greater_than")
        step = exp.Add if add else exp.Sub
        x = _p(step(this=_epoch(named, family_of(k.column)), expression=amount))
        out = _numeric_out(x, col, temporal=k.kind in ("after", "before"))
        guarded = False  # NULL exactly when the column it follows is
    elif isinstance(k, Sql) and not k.group:
        # The administrator's declared expression, parsed once; each column it names is read
        # as that column's faked form.
        tree = sqlglot.parse_one(k.expression, read="postgres")
        for c in list(tree.find_all(exp.Column)):
            c.replace(faked(c.name))
        out = _cast(tree, col.data_type)
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
        filled = [_filled_shape(shape, digest, definition_of(col)) for shape, _ in shapes]
        out = _pick(u, filled, [share for _, share in shapes])
        if col.family != "text":
            out = _cast(out, col.data_type)
    elif isinstance(k, Sequence) or (isinstance(k, Sql) and k.group):
        # Refused when declared as a fake (provisa.fakes.kinds.RULE_ONLY); a fake never holds one.
        raise FakeReadRefused(f"{col.name}: {kind_name(k)}() is a synthetic rule, not a fake")
    elif isinstance(k, Encrypt):
        raise FakeReadRefused(f"{col.name}: encrypt() is not yet computed on a faked read")
    else:
        raise FakeReadRefused(f"{col.name}: {kind_name(k)}() has no faked read")
    if not guarded:
        return out
    null = exp.Is(this=real.copy(), expression=exp.Null())
    return _p(_case([(null, exp.Null())], out))


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


def _piecewise(u: exp.Expr, points: tuple[tuple[float, float], ...] | None) -> exp.Expr:  # pyright: ignore[reportPrivateImportUsage]
    """The value at quantile ``u`` of points (quantile, value), joined linearly."""
    assert points, "a measurement or declaration of points holds at least one"
    whens = [(exp.LT(this=u.copy(), expression=_num(points[0][0])), _num(points[0][1]))]
    for (q0, v0), (q1, v1) in zip(points, points[1:]):
        slope = (v1 - v0) / (q1 - q0) if q1 > q0 else 0.0
        offset = exp.Mul(
            this=_p(exp.Sub(this=u.copy(), expression=_num(q0))), expression=_num(slope)
        )
        whens.append(
            (exp.LT(this=u.copy(), expression=_num(q1)), exp.Add(this=_num(v0), expression=offset))
        )
    return _p(_case(whens, _num(points[-1][1])))


def _pick_measured(
    u: exp.Expr,  # pyright: ignore[reportPrivateImportUsage]
    values: tuple[tuple[str, float], ...] | None,
    col: Column,
) -> exp.Expr:  # pyright: ignore[reportPrivateImportUsage]
    """A measured value at its measured share. Measured values are text, cast to the column."""
    assert values, "a measurement of values holds at least one, or a refusal"
    literals: list[exp.Expr]  # pyright: ignore[reportPrivateImportUsage]
    if col.family == "text":
        literals = [_lit(v) for v, _ in values]
    elif col.family == "boolean":
        literals = [_category_literal(v, col) for v, _ in values]
    else:
        literals = [_cast(_lit(v), col.data_type) for v, _ in values]
    return _pick(u, literals, [s for _, s in values])


_UPPER = "ABCDEFGHIJKLMNOPQRSTUVWXYZ"


def _filled_shape(shape: str, value_digest: exp.Expr, def_hash: int) -> exp.Expr:  # pyright: ignore[reportPrivateImportUsage]
    """A value of ``shape`` (A an upper-case letter, a a lower-case one, 9 a digit, every other
    character kept), its letters and digits drawn from the digest by the bothify method. The
    method sees only its own placeholders, so no character of the shape is read as one."""
    template = "".join("#" if ch == "9" else "?" for ch in shape if ch in "Aa9")
    if not template:
        return _lit(shape)
    call = {"letters": _UPPER, "text": template}
    args = json.dumps(call, sort_keys=True, separators=(",", ":"))

    def drawn() -> exp.Expr:  # pyright: ignore[reportPrivateImportUsage]
        return _f(
            "provisa_fake_method",
            _lit("bothify"),
            _lit(args),
            value_digest.copy(),
            _bigint(def_hash),
        )

    parts: list[exp.Expr] = []  # pyright: ignore[reportPrivateImportUsage]
    at = 1  # position in the drawn text, from 1

    def run_of(ch: str) -> str:
        return "kept" if ch not in "Aa9" else "lower" if ch == "a" else "drawn"

    for run, chars in groupby(shape, key=run_of):
        text = "".join(chars)
        if run == "kept":
            parts.append(_lit(text))
            continue
        piece = _f(
            "SUBSTRING", drawn(), exp.Literal.number(str(at)), exp.Literal.number(str(len(text)))
        )
        parts.append(_f("LOWER", piece) if run == "lower" else piece)
        at += len(text)
    return _p(_concat(*parts))


def _category_literal(value: str, col: Column) -> exp.Expr:  # pyright: ignore[reportPrivateImportUsage]
    if col.family in ("integer", "numeric"):
        # A declared category of a numeric column is a number as written in the model.
        return _cast(_number_text(value), col.data_type)
    if col.family == "boolean":
        return exp.Boolean(this=value.lower() == "true")
    if col.family in ("date", "timestamp"):
        return _cast(_lit(value), col.data_type)
    return _lit(value)


def _number_text(value: str) -> exp.Expr:  # pyright: ignore[reportPrivateImportUsage]
    text = value.strip()
    if text.startswith("-"):
        return exp.Neg(this=exp.Literal.number(text[1:].strip()))
    return exp.Literal.number(text)


__all__ = ["Column", "FakeReadRefused", "column", "digest", "expression", "uniform", "_q"]
