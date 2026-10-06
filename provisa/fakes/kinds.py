# Copyright (c) 2026 Kenneth Stott
# Canary: d81c343b-9b39-4273-8089-db0c1cd2e74e
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A column's kind of fake, as the operator writes it, and the checks it passes on its own
(REQ-1494).

A declaration is one call: ``categories((shoes, bra), (.4, .6))``, ``bool(.8)``,
``normal(mean=50, sd=10, min=0)``, ``after(created_at, 1 to 10 days)``, ``sql(quantity * price)``,
``email()``. :func:`parse` turns it into one of the kinds below, refusing by name anything that
cannot describe values: an unknown kind, arguments the kind does not take, shares that do not sum
to one, a distribution whose points are out of order. Checks that need the rest of the table --
the columns a relative fake names, cycles, the column's type -- are in :mod:`provisa.fakes.checks`.

Any name that is not one of Provisa's own kinds names a fake method (:mod:`provisa.fakes.methods`)
called with the given arguments.
"""

# Requirements: REQ-1494

from __future__ import annotations

import math
import re
from dataclasses import dataclass
from typing import Any, Union


class FakeRefused(ValueError):
    """A declaration that cannot be used, said by name."""


# -- the kinds -----------------------------------------------------------------------------------


@dataclass(frozen=True)
class Categories:
    """A value from a list: ``categories()`` the measured values and shares, ``categories((v..))``
    the named values at measured shares, ``categories((v..), (s..))`` both named."""

    values: tuple[str, ...] | None = None
    shares: tuple[float, ...] | None = None


@dataclass(frozen=True)
class Bool:
    """True in ``share`` of rows; ``share`` None takes the measured share."""

    share: float | None = None


@dataclass(frozen=True)
class Percentiles:
    """Points of the distribution (``min``, ``p5`` .. ``p95``, ``max``), joined piecewise-linearly."""

    points: tuple[tuple[float, float], ...]  # (quantile in [0, 1], value), quantiles ascending


@dataclass(frozen=True)
class Normal:
    mean: float
    sd: float
    min: float | None = None
    max: float | None = None


@dataclass(frozen=True)
class LogNormal:
    mu: float
    sigma: float
    min: float | None = None
    max: float | None = None


@dataclass(frozen=True)
class Uniform:
    min: float
    max: float


@dataclass(frozen=True)
class Triangular:
    min: float
    mode: float
    max: float


@dataclass(frozen=True)
class Poisson:
    mean: float


@dataclass(frozen=True)
class Profile:
    """The column's profiled distribution; ``run`` pins one run of it, else the latest is read."""

    run: str | None = None


@dataclass(frozen=True)
class Bucket:
    """The range a value falls in: a fixed ``width``, or between named ``edges``."""

    width: float | None = None
    edges: tuple[float, ...] | None = None


@dataclass(frozen=True)
class Truncate:
    unit: str


@dataclass(frozen=True)
class Prefix:
    length: int


@dataclass(frozen=True)
class Hash:
    pass


@dataclass(frozen=True)
class Encrypt:
    pass


@dataclass(frozen=True)
class Distance:
    """How far a relative fake lies from the column it names: ``low`` exactly when ``high`` is
    None, else drawn evenly between them; ``unit`` an interval unit for dates and times, None for
    numbers."""

    low: float
    high: float | None
    unit: str | None


@dataclass(frozen=True)
class Ordered:
    """``after``/``before`` (dates and times) and ``greater_than``/``less_than`` (numbers): the
    named column's value moved by ``distance``, or by the measured difference when None."""

    kind: str  # after | before | greater_than | less_than
    column: str
    distance: Distance | None = None


@dataclass(frozen=True)
class Sql:
    """A value derived from other columns of the same row (``group`` False), or across rows --
    windows over the table and aggregates over a parent's children (``group`` True).

    A ``sql_group`` fake carries the row fake ``fake`` that draws the column's value: a faked read
    shows the column through it, and a synthetic dataset draws it first, the expression naming it
    ``self`` (REQ-1494, THE FAKE PARAMETER OF SQL_GROUP AND SEQUENCE)."""

    expression: str
    group: bool = False
    fake: FakeKind | None = None


@dataclass(frozen=True)
class Sequence:
    """Each ``entity``'s rows, in ``order``, take ``states`` in turn and never go back; an entity
    with fewer rows stops part-way, and has no more rows than there are states. ``fake`` draws the
    value a faked read shows; ``categories(states)`` at even shares unless declared (REQ-1494, THE
    SEQUENCE FAKE)."""

    states: tuple[str, ...]
    entity: str
    order: str
    fake: FakeKind


@dataclass(frozen=True)
class Method:
    """A named fake method and the arguments it is called with."""

    name: str
    args: tuple[tuple[str, Any], ...] = ()


FakeKind = Union[
    Categories,
    Bool,
    Percentiles,
    Normal,
    LogNormal,
    Uniform,
    Triangular,
    Poisson,
    Profile,
    Bucket,
    Truncate,
    Prefix,
    Hash,
    Encrypt,
    Ordered,
    Sql,
    Sequence,
    Method,
]

#: Kinds computed from the value alone or from nothing -- a row's independent values.
INDEPENDENT = (
    Categories,
    Bool,
    Percentiles,
    Normal,
    LogNormal,
    Uniform,
    Triangular,
    Poisson,
    Profile,
    Bucket,
    Truncate,
    Prefix,
    Hash,
    Encrypt,
    Method,
)


def kind_name(kind: FakeKind) -> str:
    """The name a declaration gives ``kind``."""
    if isinstance(kind, Method):
        return kind.name
    if isinstance(kind, Ordered):
        return kind.kind
    if isinstance(kind, Sql):
        return "sql_group" if kind.group else "sql"
    return type(kind).__name__.lower()


def references(kind: FakeKind) -> frozenset[str]:
    """The columns of the same table a fake names; empty for an independent one. A ``sql`` or
    ``sql_group`` expression is read for its references by :mod:`provisa.fakes.sql_subset`."""
    if isinstance(kind, Ordered):
        return frozenset({kind.column})
    return frozenset()


# -- parsing -------------------------------------------------------------------------------------

_TOKEN = re.compile(
    r"""\s*(?:
        (?P<num>[-+]?(?:\d+\.?\d*|\.\d+)(?:[eE][-+]?\d+)?)
      | (?P<str>'(?:[^']|'')*'|"(?:[^"]|"")*")
      | (?P<ident>[A-Za-z_][A-Za-z0-9_.]*)
      | (?P<punct>[(),=])
    )""",
    re.VERBOSE,
)

_TEMPORAL_UNITS = {
    "second": "second",
    "seconds": "second",
    "minute": "minute",
    "minutes": "minute",
    "hour": "hour",
    "hours": "hour",
    "day": "day",
    "days": "day",
    "week": "week",
    "weeks": "week",
    "month": "month",
    "months": "month",
    "year": "year",
    "years": "year",
}
TRUNCATE_UNITS = ("year", "quarter", "month", "week", "day", "hour", "minute", "second")

# The percentile names a ``percentiles`` fake may give, and the quantile each stands for.
_QUANTILES = {"min": 0.0, "p5": 0.05, "p25": 0.25, "p50": 0.5, "p75": 0.75, "p95": 0.95, "max": 1.0}


@dataclass(frozen=True)
class _Word:
    """A bare word: a value of a list, a column, a unit, or ``to``."""

    text: str


@dataclass(frozen=True)
class _Seq:
    """Atoms written one after another: ``3 days``, ``1 to 10 days``."""

    atoms: tuple[Any, ...]


def _tokens(text: str) -> list[tuple[str, Any]]:
    out: list[tuple[str, Any]] = []
    pos = 0
    text = text.rstrip()
    while pos < len(text):
        m = _TOKEN.match(text, pos)
        if m is None or m.end() == pos:
            raise FakeRefused(f"cannot read {text[pos:]!r}")
        pos = m.end()
        if m.group("num") is not None:
            out.append(("value", float(m.group("num"))))
        elif m.group("str") is not None:
            s = m.group("str")
            out.append(("value", s[1:-1].replace(s[0] * 2, s[0])))
        elif m.group("ident") is not None:
            out.append(("value", _Word(m.group("ident"))))
        else:
            out.append(("punct", m.group("punct")))
    return out


class _Parser:
    def __init__(self, tokens: list[tuple[str, Any]]) -> None:
        self.tokens = tokens
        self.pos = 0

    def peek(self) -> tuple[str, Any] | None:
        return self.tokens[self.pos] if self.pos < len(self.tokens) else None

    def take(self, punct: str) -> None:
        tok = self.peek()
        if tok != ("punct", punct):
            raise FakeRefused(f"expected {punct!r}")
        self.pos += 1

    def args(self) -> tuple[list[Any], dict[str, Any]]:
        """``( arg, arg, name=arg )`` -- positional before named."""
        self.take("(")
        pos: list[Any] = []
        named: dict[str, Any] = {}
        if self.peek() == ("punct", ")"):
            self.pos += 1
            return pos, named
        while True:
            tok = self.peek()
            nxt = self.tokens[self.pos + 1] if self.pos + 1 < len(self.tokens) else None
            if tok is not None and isinstance(tok[1], _Word) and nxt == ("punct", "="):
                self.pos += 2
                name = tok[1].text
                if name in named:
                    raise FakeRefused(f"names argument {name!r} twice")
                named[name] = self.arg()
            elif named:
                raise FakeRefused("gives a positional argument after a named one")
            else:
                pos.append(self.arg())
            tok = self.peek()
            if tok == ("punct", ","):
                self.pos += 1
                continue
            self.take(")")
            return pos, named

    def arg(self) -> Any:
        """One argument: a tuple ``(a, b)``, or atoms written one after another."""
        if self.peek() == ("punct", "("):
            items, named = self.args()
            if named:
                raise FakeRefused("a list holds values, not named arguments")
            return tuple(items)
        atoms = []
        while (tok := self.peek()) is not None and tok[0] == "value":
            atoms.append(tok[1])
            self.pos += 1
        if not atoms:
            raise FakeRefused("expected a value")
        return atoms[0] if len(atoms) == 1 else _Seq(tuple(atoms))


def parse(declaration: str) -> FakeKind:
    """The kind a declaration names, refusing by name one that cannot describe values."""
    text = declaration.strip()
    m = re.match(r"([A-Za-z_][A-Za-z0-9_]*)\s*\(", text)
    if m is None or not text.endswith(")"):
        raise FakeRefused(f"{declaration!r} is not a fake kind: write it as kind(arguments)")
    name = m.group(1)
    if name == "sql":
        expression = text[m.end() : -1].strip()
        if not expression:
            raise FakeRefused("sql() needs an expression")
        return Sql(expression)
    if name in ("sql_group", "sequence"):
        try:
            return _with_fake(name, text[m.end() : -1])
        except FakeRefused as exc:
            raise FakeRefused(f"{name}(): {exc}") from exc
    try:
        parser = _Parser(_tokens(text[m.end() - 1 :]))
        pos, named = parser.args()
        if parser.peek() is not None:
            raise FakeRefused("has text after its closing parenthesis")
    except FakeRefused as exc:
        raise FakeRefused(f"{declaration!r}: {exc}") from exc
    builder = _BUILDERS.get(name)
    if builder is None:
        return Method(name, _method_args(name, pos, named))
    try:
        return builder(pos, named)
    except FakeRefused as exc:
        raise FakeRefused(f"{name}(): {exc}") from exc


# -- the kinds' arguments ------------------------------------------------------------------------


def _number(v: Any, what: str) -> float:
    if isinstance(v, float):
        return v
    raise FakeRefused(f"{what} must be a number, got {_shown(v)}")


def _text(v: Any) -> str:
    if isinstance(v, _Word):
        return v.text
    if isinstance(v, float):
        return f"{int(v)}" if v.is_integer() else f"{v}"
    if isinstance(v, str):
        return v
    raise FakeRefused(f"expected a value, got {_shown(v)}")


def _shown(v: Any) -> str:
    if isinstance(v, _Word):
        return v.text
    if isinstance(v, _Seq):
        return " ".join(_shown(a) for a in v.atoms)
    if isinstance(v, tuple):
        return "(" + ", ".join(_shown(a) for a in v) + ")"
    return repr(v)


def _expect(
    pos: list[Any], named: dict[str, Any], positional: int, names: tuple[str, ...] = ()
) -> None:
    if len(pos) > positional:
        raise FakeRefused(f"takes at most {positional} positional argument(s), got {len(pos)}")
    unknown = sorted(set(named) - set(names))
    if unknown:
        raise FakeRefused(f"takes no argument {unknown[0]!r}")


def _categories(pos: list[Any], named: dict[str, Any]) -> Categories:
    _expect(pos, named, 2)
    if not pos:
        return Categories()
    values = pos[0] if isinstance(pos[0], tuple) else (pos[0],)
    if not values:
        raise FakeRefused("names no values")
    texts = tuple(_text(v) for v in values)
    if len(set(texts)) != len(texts):
        raise FakeRefused("names a value twice")
    if len(pos) == 1:
        return Categories(texts)
    raw = pos[1] if isinstance(pos[1], tuple) else (pos[1],)
    shares = tuple(_number(s, "a share") for s in raw)
    check_shares(texts, shares)
    return Categories(texts, shares)


def check_shares(values: tuple[str, ...], shares: tuple[float, ...]) -> None:
    """Stated shares must be as many as the values, each in [0, 1], summing to 1 (REQ-1494,
    CATEGORY WEIGHTS)."""
    if len(shares) != len(values):
        raise FakeRefused(f"states {len(shares)} shares for {len(values)} values")
    if any(not 0.0 <= s <= 1.0 for s in shares):
        raise FakeRefused("states shares outside [0, 1]")
    if not math.isclose(sum(shares), 1.0, abs_tol=1e-9):
        raise FakeRefused(f"states shares summing to {sum(shares):g}, not 1")


def _bool(pos: list[Any], named: dict[str, Any]) -> Bool:
    _expect(pos, named, 1)
    if not pos:
        return Bool()
    share = _number(pos[0], "the share of true")
    if not 0.0 <= share <= 1.0:
        raise FakeRefused(f"the share of true {share:g} is outside [0, 1]")
    return Bool(share)


def _bounds(named: dict[str, Any]) -> tuple[float | None, float | None]:
    lo = _number(named["min"], "min") if "min" in named else None
    hi = _number(named["max"], "max") if "max" in named else None
    if lo is not None and hi is not None and not lo < hi:
        raise FakeRefused(f"min {lo:g} is not below max {hi:g}")
    return lo, hi


def _percentiles(pos: list[Any], named: dict[str, Any]) -> Percentiles:
    _expect(pos, named, 0, tuple(_QUANTILES))
    if len(named) < 3:
        raise FakeRefused("needs three or more of min, p5, p25, p50, p75, p95 and max")
    points = sorted((_QUANTILES[k], _number(v, k)) for k, v in named.items())
    for (q0, v0), (q1, v1) in zip(points, points[1:]):
        if v1 < v0:
            names = {q: k for k, q in _QUANTILES.items()}
            raise FakeRefused(f"{names[q1]} {v1:g} is below {names[q0]} {v0:g}")
    return Percentiles(tuple(points))


def _normal(pos: list[Any], named: dict[str, Any]) -> Normal:
    _expect(pos, named, 0, ("mean", "sd", "min", "max"))
    for k in ("mean", "sd"):
        if k not in named:
            raise FakeRefused(f"needs {k}")
    sd = _number(named["sd"], "sd")
    if not sd > 0:
        raise FakeRefused(f"the standard deviation {sd:g} is not above zero")
    lo, hi = _bounds(named)
    return Normal(_number(named["mean"], "mean"), sd, lo, hi)


_Z95 = 1.6448536269514722  # the standard normal's 95th percentile


def _lognormal(pos: list[Any], named: dict[str, Any]) -> LogNormal:
    _expect(pos, named, 0, ("median", "p95", "mu", "sigma", "min", "max"))
    lo, hi = _bounds(named)
    if {"median", "p95"} <= set(named) and not {"mu", "sigma"} & set(named):
        median, p95 = _number(named["median"], "median"), _number(named["p95"], "p95")
        if not 0 < median < p95:
            raise FakeRefused(f"needs 0 < median < p95, got median {median:g}, p95 {p95:g}")
        return LogNormal(math.log(median), (math.log(p95) - math.log(median)) / _Z95, lo, hi)
    if {"mu", "sigma"} <= set(named) and not {"median", "p95"} & set(named):
        sigma = _number(named["sigma"], "sigma")
        if not sigma > 0:
            raise FakeRefused(f"sigma {sigma:g} is not above zero")
        return LogNormal(_number(named["mu"], "mu"), sigma, lo, hi)
    raise FakeRefused("needs median and p95, or mu and sigma")


def _uniform(pos: list[Any], named: dict[str, Any]) -> Uniform:
    _expect(pos, named, 0, ("min", "max"))
    if not {"min", "max"} <= set(named):
        raise FakeRefused("needs min and max")
    lo, hi = _bounds(named)
    assert lo is not None and hi is not None
    return Uniform(lo, hi)


def _triangular(pos: list[Any], named: dict[str, Any]) -> Triangular:
    _expect(pos, named, 0, ("min", "mode", "max"))
    if not {"min", "mode", "max"} <= set(named):
        raise FakeRefused("needs min, mode and max")
    lo, hi = _bounds(named)
    mode = _number(named["mode"], "mode")
    assert lo is not None and hi is not None
    if not lo <= mode <= hi:
        raise FakeRefused(f"mode {mode:g} is outside min {lo:g} and max {hi:g}")
    return Triangular(lo, mode, hi)


def _poisson(pos: list[Any], named: dict[str, Any]) -> Poisson:
    _expect(pos, named, 0, ("mean",))
    if "mean" not in named:
        raise FakeRefused("needs mean")
    mean = _number(named["mean"], "mean")
    if not mean > 0:
        raise FakeRefused(f"mean {mean:g} is not above zero")
    return Poisson(mean)


def _profile(pos: list[Any], named: dict[str, Any]) -> Profile:
    _expect(pos, named, 0, ("run",))
    return Profile(_text(named["run"]) if "run" in named else None)


def _bucket(pos: list[Any], named: dict[str, Any]) -> Bucket:
    _expect(pos, named, 1)
    if len(pos) != 1:
        raise FakeRefused("needs a width or a list of edges")
    if isinstance(pos[0], tuple):
        edges = tuple(_number(e, "an edge") for e in pos[0])
        if len(edges) < 2:
            raise FakeRefused("needs two or more edges")
        if any(b <= a for a, b in zip(edges, edges[1:])):
            raise FakeRefused("its edges are not in ascending order")
        return Bucket(edges=edges)
    width = _number(pos[0], "the width")
    if not width > 0:
        raise FakeRefused(f"the width {width:g} is not above zero")
    return Bucket(width=width)


def _truncate(pos: list[Any], named: dict[str, Any]) -> Truncate:
    _expect(pos, named, 1)
    unit = _text(pos[0]).lower() if pos else ""
    if unit not in TRUNCATE_UNITS:
        raise FakeRefused(f"needs a unit, one of {', '.join(TRUNCATE_UNITS)}")
    return Truncate(unit)


def _prefix(pos: list[Any], named: dict[str, Any]) -> Prefix:
    _expect(pos, named, 1)
    n = _number(pos[0], "the length") if pos else 0.0
    if not (n.is_integer() and n >= 1):
        raise FakeRefused("needs a whole length of one or more")
    return Prefix(int(n))


def _nothing(kind: type) -> Any:
    def build(pos: list[Any], named: dict[str, Any]) -> Any:
        _expect(pos, named, 0)
        return kind()

    return build


def _distance(v: Any, temporal: bool) -> Distance:
    atoms = v.atoms if isinstance(v, _Seq) else (v,)
    unit: str | None = None
    if temporal:
        if not (atoms and isinstance(atoms[-1], _Word)):
            raise FakeRefused(f"the distance {_shown(v)} needs a unit such as days")
        unit = _TEMPORAL_UNITS.get(atoms[-1].text.lower())
        if unit is None:
            raise FakeRefused(f"{atoms[-1].text!r} is not a unit of time")
        atoms = atoms[:-1]
    if len(atoms) == 1:
        low, high = _number(atoms[0], "the distance"), None
    elif len(atoms) == 3 and isinstance(atoms[1], _Word) and atoms[1].text.lower() == "to":
        low, high = _number(atoms[0], "the distance"), _number(atoms[2], "the distance")
        if high < low:
            raise FakeRefused(f"the range {low:g} to {high:g} runs backwards")
    else:
        raise FakeRefused(f"cannot read the distance {_shown(v)}")
    if low < 0:
        raise FakeRefused(f"the distance {low:g} is below zero")
    return Distance(low, high, unit)


def _ordered(kind: str, temporal: bool) -> Any:
    def build(pos: list[Any], named: dict[str, Any]) -> Ordered:
        _expect(pos, named, 2)
        if not pos or not isinstance(pos[0], _Word):
            raise FakeRefused("needs the column it follows")
        distance = _distance(pos[1], temporal) if len(pos) == 2 else None
        return Ordered(kind, pos[0].text, distance)

    return build


_BUILDERS = {
    "categories": _categories,
    "bool": _bool,
    "percentiles": _percentiles,
    "normal": _normal,
    "lognormal": _lognormal,
    "uniform": _uniform,
    "triangular": _triangular,
    "poisson": _poisson,
    "profile": _profile,
    "bucket": _bucket,
    "truncate": _truncate,
    "prefix": _prefix,
    "hash": _nothing(Hash),
    "encrypt": _nothing(Encrypt),
    "after": _ordered("after", True),
    "before": _ordered("before", True),
    "greater_than": _ordered("greater_than", False),
    "less_than": _ordered("less_than", False),
}

#: Provisa's own kinds, by the name a declaration gives them.
OWN_KINDS = frozenset(_BUILDERS) | {"sql", "sql_group", "sequence"}


def _split(inner: str) -> list[str]:
    """``inner`` split at its top-level commas -- none inside parentheses or quotes."""
    parts: list[str] = []
    depth, quote, start = 0, "", 0
    for i, ch in enumerate(inner):
        if quote:
            if ch == quote:
                quote = ""
        elif ch in "'\"":
            quote = ch
        elif ch == "(":
            depth += 1
        elif ch == ")":
            depth -= 1
        elif ch == "," and depth == 0:
            parts.append(inner[start:i].strip())
            start = i + 1
    parts.append(inner[start:].strip())
    return parts


_FAKE_PARAM = re.compile(r"fake\s*=\s*(.*)\Z", re.DOTALL)


def _row_fake(text: str) -> FakeKind:
    """The ``fake=`` parameter: a row fake, never a cross-row one."""
    kind = parse(text)
    if isinstance(kind, Sequence) or (isinstance(kind, Sql) and kind.group):
        raise FakeRefused(f"its fake= {text!r} is itself a cross-row fake; name a row fake")
    return kind


def _with_fake(name: str, inner: str) -> FakeKind:
    parts = [p for p in _split(inner)]
    fakes = [m.group(1) for p in parts if (m := _FAKE_PARAM.match(p))]
    rest = [p for p in parts if not _FAKE_PARAM.match(p)]
    if len(fakes) > 1:
        raise FakeRefused("names fake= twice")
    fake = _row_fake(fakes[0]) if fakes else None
    if name == "sql_group":
        if len(rest) != 1 or not rest[0]:
            raise FakeRefused("needs one expression")
        if fake is None:
            raise FakeRefused("needs fake=<row fake>, the fake that draws the column's value")
        return Sql(rest[0], group=True, fake=fake)
    if len(rest) != 3:
        raise FakeRefused("needs (states), the entity column and the order column")
    parser = _Parser(_tokens("(" + ", ".join(rest) + ")"))
    pos, _named = parser.args()
    states_arg, entity, order = pos
    states = tuple(
        _text(v) for v in (states_arg if isinstance(states_arg, tuple) else (states_arg,))
    )
    if not states:
        raise FakeRefused("names no states")
    if len(set(states)) != len(states):
        raise FakeRefused("names a state twice")
    if not isinstance(entity, _Word) or not isinstance(order, _Word):
        raise FakeRefused("names its entity and order columns by name")
    if fake is None:
        fake = Categories(states, tuple(1 / len(states) for _ in states))
    return Sequence(states, entity.text, order.text, fake)


def _json_value(v: Any) -> Any:
    if isinstance(v, _Word):
        return {"true": True, "false": False, "null": None}.get(v.text.lower(), v.text)
    if isinstance(v, float):
        return int(v) if v.is_integer() else v
    if isinstance(v, tuple):
        return tuple(_json_value(a) for a in v)
    if isinstance(v, str):
        return v
    raise FakeRefused(f"cannot pass {_shown(v)} to a fake method")


def _method_args(name: str, pos: list[Any], named: dict[str, Any]) -> tuple[tuple[str, Any], ...]:
    if pos:
        raise FakeRefused(f"{name}() takes named arguments only, as {name}(name=value)")
    return tuple(sorted((k, _json_value(v)) for k, v in named.items()))
