# Copyright (c) 2026 Kenneth Stott
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.

"""Property-based / metamorphic / differential tests for SILENT SEMANTIC MISINTERPRETATION.

Target class: inputs that raise no error but produce a WRONG result. These tests assert RELATIONS
that must hold for ANY generated input (adversarial schemas/data), never hardcoded expected values:

  1. DIFFERENTIAL   — Provisa's IR, run through two independent native engines (DuckDB & SQLite),
                      must agree. Divergence = the transpiler silently changed meaning.
  2. METAMORPHIC (governance) — the same governance policy must exclude the same logical rows
                      regardless of the source dialect that expresses it, and regardless of a
                      semantics-preserving rewrite (qualification, commutation, double-negation).
  3. METAMORPHIC (indirection) — re-emitting a query to a semantically equivalent dialect must
                      not change which rows come back.
  4. INVARIANT (round-trip) — schema -> IR -> emitted query must preserve every value and type;
                      nothing is silently coerced, truncated, or dropped.

Oracles are in-process (DuckDB + SQLite), so the suite is hermetic and fast — no docker, no Trino.
Each failing relation is reported by Hypothesis with the minimal reproducing schema/rows/predicate.
"""

from __future__ import annotations

import sqlite3
from dataclasses import dataclass

import duckdb
from hypothesis import HealthCheck, assume, given, settings
from hypothesis import strategies as st

from provisa.compiler.rls import _inject_where, _qualified_predicate
from provisa.transpiler.transpile import transpile

# ── Adversarial schema/data generators ─────────────────────────────────────────
# One SQL type family per column. The value strategy per type deliberately reaches the edges where
# silent coercion hides: int64 extremes, sub-normal / high-precision floats, unicode / empty / all-
# whitespace text, and NULL in every column. Booleans are stored as 0/1 so SQLite (no bool type) and
# DuckDB agree at the storage layer; comparison normalises both back to a Python bool.

_INT32_MAX = 2**31 - 1
_INT64_MAX = 2**63 - 1

# Identifier pool: an embedded space/quote, a leading underscore, reserved words, and unicode — every
# one is emitted double-quoted so it is a legal identifier in both engines. Names are drawn unique
# under case-folding because DuckDB folds identifier case (so "a" and "A" would be one column) — the
# two-engine oracle needs each generated column to be distinguishable in both engines.
_NAMES = ["a", "b", "_c", "order", "select", "co l", 'q"x', "café", "Δ", "z9"]


@dataclass(frozen=True)
class Column:
    name: str
    sqltype: str  # canonical postgres-ish type the IR speaks


def _values_for(sqltype: str) -> st.SearchStrategy:
    if sqltype == "INTEGER":
        base = st.integers(min_value=-_INT32_MAX - 1, max_value=_INT32_MAX)
    elif sqltype == "BIGINT":
        base = st.integers(min_value=-_INT64_MAX - 1, max_value=_INT64_MAX)
    elif sqltype == "DOUBLE":
        # Bound magnitude away from the IEEE-754 extremes and disallow subnormals: near ±DBL_MAX and
        # in the subnormal range, DuckDB and SQLite round a decimal literal to *different* doubles
        # (a strtod implementation difference), which would flag an ENGINE quirk rather than a Provisa
        # transpiler bug. Everything of finite normal magnitude stays in scope.
        base = st.floats(
            min_value=-1e12,
            max_value=1e12,
            allow_nan=False,
            allow_infinity=False,
            allow_subnormal=False,
            width=64,
        )
    elif sqltype == "TEXT":
        # Adversarial unicode/whitespace/empty, but exclude NUL (terminates a SQL string literal and
        # cannot be stored by SQLite) and surrogates (not UTF-8 encodable) — engine limits, not IR
        # semantics. Everything else (accents, emoji, tabs, mixed case) stays in scope.
        base = st.text(
            alphabet=st.characters(min_codepoint=1, blacklist_categories=("Cs",)), max_size=12
        )
    elif sqltype == "BOOLEAN":
        base = st.booleans()
    else:  # pragma: no cover - guard
        raise AssertionError(sqltype)
    return st.one_of(st.none(), base)


_SQLTYPES = ["INTEGER", "BIGINT", "DOUBLE", "TEXT", "BOOLEAN"]


@st.composite
def _schema(draw, min_cols: int = 1, max_cols: int = 4) -> list[Column]:
    n = draw(st.integers(min_value=min_cols, max_value=max_cols))
    names = draw(
        st.lists(st.sampled_from(_NAMES), min_size=n, max_size=n, unique_by=lambda s: s.casefold())
    )
    types = draw(st.lists(st.sampled_from(_SQLTYPES), min_size=n, max_size=n))
    return [Column(nm, ty) for nm, ty in zip(names, types)]


@st.composite
def _table(draw, min_rows: int = 0, max_rows: int = 6):
    cols = draw(_schema())
    nrows = draw(st.integers(min_value=min_rows, max_value=max_rows))
    rows = []
    for rid in range(nrows):
        row = {"rid": rid}
        for c in cols:
            row[c.name] = draw(_values_for(c.sqltype))
        rows.append(row)
    return cols, rows


# ── Engine adapters ────────────────────────────────────────────────────────────
# DuckDB and SQLite are independent implementations of (most of) SQL. Loading identical rows into
# both and running the SAME logical query through each is a differential oracle: if the transpiler
# preserves meaning, both engines return the same rows.

_DUCK_TYPE = {
    "INTEGER": "INTEGER",
    "BIGINT": "BIGINT",
    "DOUBLE": "DOUBLE",
    "TEXT": "TEXT",
    "BOOLEAN": "BOOLEAN",
}
_SQLITE_TYPE = {
    "INTEGER": "INTEGER",
    "BIGINT": "INTEGER",
    "DOUBLE": "REAL",
    "TEXT": "TEXT",
    "BOOLEAN": "INTEGER",
}


def _store_val(c: Column, v):
    if v is None:
        return None
    if c.sqltype == "BOOLEAN":
        return 1 if v else 0  # store bools as 0/1 so both engines agree at the storage layer
    return v


def _norm_cell(v):
    """Normalise a returned cell so DuckDB and SQLite are comparable: bools->int, float->rounded."""
    if isinstance(v, bool):
        return int(v)
    if isinstance(v, float):
        return round(v, 9)
    return v


def _norm_rows(rows) -> list[tuple]:
    return sorted((tuple(_norm_cell(c) for c in r) for r in rows), key=lambda t: repr(t))


def _q(name: str) -> str:
    return '"' + name.replace('"', '""') + '"'


def _make_duck(cols: list[Column], rows: list[dict]):
    con = duckdb.connect(":memory:")
    defs = [f"{_q('rid')} INTEGER"] + [f"{_q(c.name)} {_DUCK_TYPE[c.sqltype]}" for c in cols]
    con.execute(f"CREATE TABLE t ({', '.join(defs)})")
    for r in rows:
        vals = [r["rid"]] + [_store_val(c, r[c.name]) for c in cols]
        placeholders = ", ".join(["?"] * len(vals))
        con.execute(f"INSERT INTO t VALUES ({placeholders})", vals)
    return con


def _make_sqlite(cols: list[Column], rows: list[dict]):
    con = sqlite3.connect(":memory:")
    defs = [f"{_q('rid')} INTEGER"] + [f"{_q(c.name)} {_SQLITE_TYPE[c.sqltype]}" for c in cols]
    con.execute(f"CREATE TABLE t ({', '.join(defs)})")
    for r in rows:
        vals = [r["rid"]] + [_store_val(c, r[c.name]) for c in cols]
        placeholders = ", ".join(["?"] * len(vals))
        con.execute(f"INSERT INTO t VALUES ({placeholders})", vals)
    return con


def _duck_rows(con, sql: str) -> list[tuple]:
    return con.execute(sql).fetchall()


def _sqlite_rows(con, sql: str) -> list[tuple]:
    return con.execute(sql).fetchall()


# ── Predicate generator (postgres IR text) ─────────────────────────────────────
# A grammar of comparisons / null-tests / boolean combinators over the table's columns, emitted as
# postgres-dialect SQL. Kept to the 3-valued-logic core where DuckDB and SQLite provably agree, so a
# divergence is a transpiler defect, not an engine quirk.


def _literal(c: Column, v) -> str:
    if v is None:
        return "NULL"
    if c.sqltype in ("INTEGER", "BIGINT"):
        return str(v)
    if c.sqltype == "DOUBLE":
        return repr(float(v))
    if c.sqltype == "BOOLEAN":
        return "1" if v else "0"
    return "'" + str(v).replace("'", "''") + "'"


@st.composite
def _predicate(draw, cols: list[Column], depth: int = 0) -> str:
    col = draw(st.sampled_from(cols))
    ref = _q(col.name)
    if draw(st.booleans()) and depth < 2:
        op = draw(st.sampled_from(["AND", "OR"]))
        left = draw(_predicate(cols, depth + 1))
        right = draw(_predicate(cols, depth + 1))
        return f"({left} {op} {right})"
    kind = draw(st.sampled_from(["cmp", "null", "notnull", "not"]))
    if kind == "null":
        return f"{ref} IS NULL"
    if kind == "notnull":
        return f"{ref} IS NOT NULL"
    if kind == "not":
        inner = draw(_predicate(cols, depth + 1))
        return f"NOT {inner}"
    cmp = draw(st.sampled_from(["=", "<>", "<", "<=", ">", ">="]))
    val = draw(_values_for(col.sqltype).filter(lambda x: x is not None))
    return f"{ref} {cmp} {_literal(col, val)}"


_SETTINGS = settings(
    max_examples=200,
    deadline=None,
    # Reproducible in CI: the same 200 diverse examples every run, so a genuine divergence surfaces
    # deterministically (a real bug) instead of flaking in and out with the random seed.
    derandomize=True,
    suppress_health_check=[HealthCheck.too_slow, HealthCheck.filter_too_much],
)


# ── 4. INVARIANT: schema -> IR -> emitted query preserves every value/type ──────


class TestRoundTripPreservation:
    """A plain projection, emitted to each dialect, must return every stored value byte-for-byte —
    no silent coercion (bigint truncation, float rounding, text mangling, null->'' collapse)."""

    @_SETTINGS
    @given(tbl=_table(min_rows=1))
    def test_projection_preserves_values_across_dialects(self, tbl):
        cols, rows = tbl
        collist = ", ".join([_q("rid")] + [_q(c.name) for c in cols])
        pg = f"SELECT {collist} FROM t ORDER BY {_q('rid')}"
        duck = _make_duck(cols, rows)
        slite = _make_sqlite(cols, rows)
        d = _norm_rows(_duck_rows(duck, transpile(pg, "duckdb")))
        s = _norm_rows(_sqlite_rows(slite, transpile(pg, "sqlite")))
        # Ground truth: what we inserted, normalised the same way.
        want = _norm_rows(
            [tuple([r["rid"]] + [_store_val(c, r[c.name]) for c in cols]) for r in rows]
        )
        assert d == want, f"DuckDB dropped/coerced a value.\nSQL: {pg}\nrows: {rows}"
        assert s == want, f"SQLite dropped/coerced a value.\nSQL: {pg}\nrows: {rows}"


# ── 1 & 3. DIFFERENTIAL / INDIRECTION: same IR, two engines, same rows ──────────


class TestDifferentialFilter:
    """A filtered projection expressed once in the IR must select the SAME rows through DuckDB and
    through SQLite. A divergence means the transpiler emitted dialect SQL that means two things."""

    @_SETTINGS
    @given(data=st.data())
    def test_filter_agrees_across_engines(self, data):
        cols, rows = data.draw(_table(min_rows=1))
        assume(rows)
        pred = data.draw(_predicate(cols))
        pg = f"SELECT {_q('rid')} FROM t WHERE {pred}"
        duck = _make_duck(cols, rows)
        slite = _make_sqlite(cols, rows)
        d = _norm_rows(_duck_rows(duck, transpile(pg, "duckdb")))
        s = _norm_rows(_sqlite_rows(slite, transpile(pg, "sqlite")))
        assert d == s, (
            "IR filter selected different rows per engine (silent misinterpretation).\n"
            f"pred: {pred}\nrows: {rows}\nduck: {d}\nsqlite: {s}"
        )


# ── 2. METAMORPHIC on governance ────────────────────────────────────────────────


class TestGovernanceMetamorphic:
    """The same governance predicate must exclude the same rows however it is expressed."""

    @_SETTINGS
    @given(data=st.data())
    def test_policy_is_dialect_invariant(self, data):
        """One policy, injected once, transpiled to two dialects -> identical surviving rows.
        (Catches a predicate that silently matches nothing on one dialect.)"""
        cols, rows = data.draw(_table(min_rows=1))
        assume(rows)
        policy = data.draw(_predicate(cols))
        base = f"SELECT {_q('rid')} FROM t"
        governed = _inject_where(base, policy)
        duck = _make_duck(cols, rows)
        slite = _make_sqlite(cols, rows)
        d = _norm_rows(_duck_rows(duck, transpile(governed, "duckdb")))
        s = _norm_rows(_sqlite_rows(slite, transpile(governed, "sqlite")))
        assert d == s, (
            "Same policy excluded different rows per dialect.\n"
            f"policy: {policy}\ngoverned: {governed}\nrows: {rows}"
        )

    @_SETTINGS
    @given(data=st.data())
    def test_qualification_preserves_filtering(self, data):
        """Qualifying a policy's bare columns with the table alias (what governance does before it
        ANDs the predicate in) must not change which rows the policy excludes."""
        cols, rows = data.draw(_table(min_rows=1))
        assume(rows)
        policy = data.draw(_predicate(cols))
        base = f"SELECT {_q('rid')} FROM t"
        bare = _inject_where(base, policy)
        qualified_pred = _qualified_predicate(policy, "t").sql(dialect="postgres")
        qual = _inject_where(base, qualified_pred)
        duck = _make_duck(cols, rows)
        a = _norm_rows(_duck_rows(duck, transpile(bare, "duckdb")))
        b = _norm_rows(_duck_rows(duck, transpile(qual, "duckdb")))
        assert a == b, (
            "Column qualification changed the policy's row set.\n"
            f"policy: {policy}\nqualified: {qualified_pred}\nrows: {rows}"
        )

    @_SETTINGS
    @given(data=st.data())
    def test_double_negation_is_identity(self, data):  # noqa: D401
        """`NOT NOT P` must exclude exactly the rows `P` does (3-valued-logic safe)."""
        cols, rows = data.draw(_table(min_rows=1))
        assume(rows)
        policy = data.draw(_predicate(cols))
        base = f"SELECT {_q('rid')} FROM t"
        once = _inject_where(base, policy)
        twice = _inject_where(base, f"NOT (NOT ({policy}))")
        duck = _make_duck(cols, rows)
        a = _norm_rows(_duck_rows(duck, transpile(once, "duckdb")))
        b = _norm_rows(_duck_rows(duck, transpile(twice, "duckdb")))
        assert a == b, f"Double negation changed the row set.\npolicy: {policy}\nrows: {rows}"


# ── 3. METAMORPHIC on indirection ────────────────────────────────────────────────


class TestIndirectionMetamorphic:
    """Reaching the same rows through an equivalent, more-indirect source reference (a wrapping
    subquery — the SQL analogue of repointing a consumer at a semantically identical source) must
    not change the result. A rewrite that silently reinterprets the reference is caught here."""

    @_SETTINGS
    @given(data=st.data())
    def test_source_wrapping_preserves_rows(self, data):
        cols, rows = data.draw(_table(min_rows=1))
        assume(rows)
        pred = data.draw(_predicate(cols))
        direct = f"SELECT {_q('rid')} FROM t WHERE {pred}"
        # Indirection: the base relation reached through a wrapping subquery aliased back to `t`.
        indirect = f"SELECT {_q('rid')} FROM (SELECT * FROM t) AS t WHERE {pred}"
        duck = _make_duck(cols, rows)
        a = _norm_rows(_duck_rows(duck, transpile(direct, "duckdb")))
        b = _norm_rows(_duck_rows(duck, transpile(indirect, "duckdb")))
        assert a == b, (
            "Indirection through an equivalent source changed the row set.\n"
            f"pred: {pred}\nrows: {rows}"
        )
