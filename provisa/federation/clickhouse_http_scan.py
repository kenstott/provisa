# Copyright (c) 2026 Kenneth Stott
# Canary: 3f9d2c71-8a4e-4b06-9e5d-c1a7b2e8f403
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""DuckDB reads a live ClickHouse table over ClickHouse's HTTP interface (REQ-899).

DuckDB has no ClickHouse extension. It does have core ``httpfs`` + ``read_parquet``, and ClickHouse's
HTTP interface answers any ``SELECT ... FORMAT Parquet`` as a Parquet document. So each reference to
a ClickHouse-sourced table in an engine statement is replaced, at query time, by
``read_parquet('<http://host:port/?query=...>')`` whose ClickHouse query:

* projects only the columns the statement uses (all of them for ``*`` or a whole-row reference);
* carries every pushable literal predicate on that table — the top-level ``AND`` conjuncts of the
  table's own SELECT's WHERE and of its INNER/LEFT join's ON, classified by the same
  ``_literal_predicate_target`` REQ-1880 uses (equality, ``IN``, ``BETWEEN``, ranges), with bound
  ``$N`` parameters inlined from the statement's parameters.

Governance: the rewrite runs on the governed engine statement (RLS/masking already applied) and
leaves every outer predicate in place — a pushed predicate is a copy, so the read can only narrow,
never widen, what the governed statement reads.

Type mapping (ClickHouse's Parquet writer, verified against 24.3 and current): Nullable,
LowCardinality, DateTime64, Decimal(P<=38), Array/Map/Tuple of those export natively. UUID has no
Parquet mapping (ClickHouse raises), IPv4/Date/DateTime/Enum/FixedString export as raw integers or
bytes, Int128/UInt128/Int256/UInt256/Decimal(P>38) as bytes or a lossy DOUBLE — each is converted
in the ClickHouse projection (and cast back on the DuckDB side where DuckDB has the type). A type
with no verified mapping raises; nothing is guessed.

Not range-streamable: ClickHouse computes the result per request, so DuckDB must download the whole
document (``force_download``) before reading it — an unpredicated scan downloads the whole table.
There is no row cap. The request deadline bounds it instead: DuckDB's ``interrupt`` cannot abort an
in-flight download, so the remaining budget is sent as ClickHouse's own ``max_execution_time``
(one second past the Provisa deadline, so the Provisa deadline fires first and the request fails with
its TimeoutError when ClickHouse aborts the stream).

Leaf module: sqlglot + the compiler's predicate classifier only; the runtime owns every connection.
"""

# Requirements: REQ-899

from __future__ import annotations
from provisa.compiler.sql_literals import sql_literal

import datetime
import decimal
import math
import re
import uuid
from collections.abc import Mapping
from dataclasses import dataclass
from urllib.parse import quote, urlencode

import sqlglot
import sqlglot.expressions as exp

from provisa.compiler.sql_rewrite import _literal_predicate_target, _split_and


# DuckDB's widest DECIMAL precision; a ClickHouse Decimal past it exports as a lossy DOUBLE.
_MAX_DECIMAL_PRECISION = 38


class ClickHouseScanError(ValueError):
    """A ClickHouse relation the engine statement reads cannot be expressed as a live HTTP read."""


@dataclass(frozen=True)
class ClickHouseRelation:
    """One registered ClickHouse table, as the DuckDB runtime exposes it."""

    base_url: str  # scheme://host:port — also the DuckDB http secret's SCOPE
    database: str
    table: str
    columns: tuple[tuple[str, str], ...]  # (name, ClickHouse type), table order


# ClickHouse settings every read carries: String exports as Parquet UTF8 (VARCHAR), not BLOB, on
# every server version.
_READ_SETTINGS = {"output_format_parquet_string_as_string": "1"}


def _ch_ident(name: str) -> str:
    return '"' + name.replace("\\", "\\\\").replace('"', '\\"') + '"'


def _ch_string(value: str) -> str:
    # ClickHouse string literals interpret backslash escapes; DuckDB's do not.
    return "'" + value.replace("\\", "\\\\").replace("'", "\\'") + "'"


def _duck_ident(name: str) -> str:
    return '"' + name.replace('"', '""') + '"'


def secret_ddl(source_id: str, base_url: str, user: str, password: str) -> str:
    """The DuckDB http secret that authenticates every read under ``base_url`` — credentials ride
    in ClickHouse's X-ClickHouse-User/X-ClickHouse-Key headers, never in a URL."""

    def lit(v: str) -> str:
        return sql_literal(v, "duckdb")

    return (
        f'CREATE OR REPLACE SECRET "_ch_{source_id}" (TYPE http, EXTRA_HTTP_HEADERS MAP '
        f"{{'X-ClickHouse-User': {lit(user)}, 'X-ClickHouse-Key': {lit(password)}}}, "
        f"SCOPE {lit(base_url + '/')})"
    )


def read_url(base_url: str, ch_sql: str, *, deadline_s: float | None) -> str:
    """The HTTP URL answering ``ch_sql`` as Parquet. A fresh query_id per read keeps every URL
    distinct: DuckDB's global external file cache keys on the URL, and ClickHouse's answer to the
    same query changes with the data (a reused URL served a stale cached footer — "Invalid data")."""
    params: dict[str, str] = {"query_id": f"provisa-duckdb-{uuid.uuid4().hex}", **_READ_SETTINGS}
    if deadline_s is not None:
        params["max_execution_time"] = str(max(1, math.ceil(deadline_s)) + 1)
    params["query"] = ch_sql
    return f"{base_url}/?{urlencode(params, quote_via=quote, safe='')}"


def columns_query(database: str, table: str) -> str:
    """ClickHouse query listing a table's (name, type) in table order."""
    return (
        "SELECT name, type FROM system.columns "
        f"WHERE database = {_ch_string(database)} AND table = {_ch_string(table)} "
        "ORDER BY position FORMAT Parquet"
    )


# -- type mapping ---------------------------------------------------------------------------------

_WRAPPER = re.compile(r"^(?:Nullable|LowCardinality)\((.*)\)$", re.S)
_NATIVE = frozenset(
    {
        "Int8",
        "Int16",
        "Int32",
        "Int64",
        "UInt8",
        "UInt16",
        "UInt32",
        "UInt64",
        "Float32",
        "Float64",
        "String",
        "Bool",
        "Date32",
        "DateTime64",
        "Decimal32",
        "Decimal64",
        "Decimal128",
    }
)
_TO_STRING = frozenset({"IPv4", "IPv6", "FixedString", "Enum8", "Enum16"})
# Inside a Map/Tuple a conversion cannot be spliced in per element; such a column raises.
_NEEDS_CONVERSION = re.compile(
    r"\b(?:UUID|IPv4|IPv6|FixedString|Enum8|Enum16|Date|DateTime|Int128|UInt128|Int256|UInt256|"
    r"Decimal256)\b|\bDecimal\(\s*(?:39|[4-7]\d)\b"
)


def _unwrap(ch_type: str) -> str:
    t = ch_type.strip()
    while (m := _WRAPPER.match(t)) is not None:
        t = m.group(1).strip()
    return t


def _decimal_precision(base: str) -> int | None:
    m = re.match(r"^Decimal\(\s*(\d+)", base)
    return int(m.group(1)) if m else None


def _export(expr: str, ch_type: str, column: str, depth: int = 0) -> tuple[str, str | None]:
    """(ClickHouse projection expression, DuckDB cast type or None) exporting ``expr`` of
    ``ch_type`` so DuckDB reads the value, not its Parquet storage bytes."""
    base = _unwrap(ch_type)
    if base.startswith("Array(") and base.endswith(")"):
        var = f"_v{depth}"
        inner, cast = _export(var, base[len("Array(") : -1], column, depth + 1)
        ch = expr if inner == var else f"arrayMap({var} -> {inner}, {expr})"
        return ch, (f"{cast}[]" if cast else None)
    if base.startswith(("Map(", "Tuple(", "Nested(")):
        if _NEEDS_CONVERSION.search(base):
            raise ClickHouseScanError(
                f"ClickHouse column {column!r} of type {ch_type} nests a type with no native "
                "Parquet mapping inside a Map/Tuple; it cannot be read live over HTTP"
            )
        return expr, None
    name = base.split("(", 1)[0].strip()
    if name == "UUID":
        return f"toString({expr})", "UUID"
    if name in _TO_STRING:
        return f"toString({expr})", None
    if name == "Date":
        return f"toDate32({expr})", None
    if name == "DateTime":
        return f"toDateTime64({expr}, 0)", None
    if name == "Int128":
        return f"toString({expr})", "HUGEINT"
    if name == "UInt128":
        return f"toString({expr})", "UHUGEINT"
    if name in ("Int256", "UInt256", "Decimal256"):
        return f"toString({expr})", None  # wider than any DuckDB numeric: exact text
    if name == "Decimal":
        precision = _decimal_precision(base)
        if precision is not None and precision > _MAX_DECIMAL_PRECISION:
            return f"toString({expr})", None  # ClickHouse would export a lossy DOUBLE
        return expr, None
    if name in _NATIVE:
        return expr, None
    raise ClickHouseScanError(
        f"ClickHouse column {column!r} has type {ch_type}, which has no verified Parquet mapping "
        "for a live HTTP read"
    )


# -- predicate pushdown ---------------------------------------------------------------------------

_NUMERIC = re.compile(r"^(?:U?Int\d+|Float\d+|Decimal\d*)$")
_STRINGISH = frozenset({"String", "FixedString", "Enum8", "Enum16", "IPv4", "IPv6"})
_DATE = frozenset({"Date", "Date32"})


class _Unset:
    """No plain constant: a column, an expression, or an unbound placeholder."""


_UNSET = _Unset()
# A bound parameter or literal's Python value (params arrive from the driver as these types).
_Value = str | int | float | decimal.Decimal | datetime.date | bool | None


def _param_value(node: exp.Expr, params: list | None) -> _Value | _Unset:
    """The bound value of a numbered ``$N`` placeholder, or ``_UNSET`` when it has none."""
    raw = node.this if isinstance(node, (exp.Placeholder, exp.Parameter)) else None
    if isinstance(raw, exp.Expr):
        raw = raw.name
    if raw is None or not str(raw).isdigit() or not params:
        return _UNSET
    index = int(str(raw)) - 1
    return params[index] if 0 <= index < len(params) else _UNSET


def _operand(node: exp.Expr, params: list | None) -> _Value | _Unset:
    """The Python value of a literal-side operand (``_UNSET`` if not a plain constant)."""
    if isinstance(node, (exp.Placeholder, exp.Parameter)):
        return _param_value(node, params)
    if isinstance(node, exp.Neg) and isinstance(node.this, exp.Literal) and node.this.is_number:
        return decimal.Decimal("-" + node.this.name)
    if isinstance(node, exp.Literal):
        return node.name if node.is_string else decimal.Decimal(node.name)
    return _UNSET


def _render_value(value: _Value | _Unset, base: str) -> str | None:
    """``value`` as a ClickHouse literal comparable to a column of base type ``base`` with the
    same outcome DuckDB's comparison has, or None when that equivalence is not guaranteed (the
    predicate then simply isn't pushed — the outer statement still applies it)."""
    name = base.split("(", 1)[0].strip()
    if isinstance(value, bool):
        return None
    if _NUMERIC.match(name) or name == "Decimal":
        if isinstance(value, int):
            if name.startswith("Float") and abs(value) > 2**53:
                return None
            return str(value)
        if isinstance(value, float):
            value = decimal.Decimal(repr(value)) if math.isfinite(value) else None
        if not isinstance(value, decimal.Decimal) or not value.is_finite():
            return None
        if value == value.to_integral_value():
            return str(int(value))
        if name == "Float32":
            return None  # DuckDB and ClickHouse round a fractional literal differently at FLOAT
        text = format(value, "f")
        if name.startswith("Float"):
            return text
        scale = len(text.split(".", 1)[1])
        return (
            None if scale > _MAX_DECIMAL_PRECISION else f"toDecimal128({_ch_string(text)}, {scale})"
        )
    if name in _STRINGISH:
        return _ch_string(value) if isinstance(value, str) else None
    if name in _DATE:
        if isinstance(value, datetime.datetime):
            return None
        if isinstance(value, datetime.date):
            return _ch_string(value.isoformat())
        return _ch_string(value) if isinstance(value, str) else None
    return None  # DateTime/DateTime64 (time-zone semantics differ), UUID, Bool, nested: not pushed


def _pushable_sql(
    conjunct: exp.Expr, column_expr: str, base: str, params: list | None
) -> str | None:
    """The ClickHouse text of a classified literal conjunct over ``column_expr``, or None."""

    def val(node: exp.Expr | None) -> str | None:
        return None if node is None else _render_value(_operand(node, params), base)

    ops = {exp.EQ: "=", exp.GT: ">", exp.GTE: ">=", exp.LT: "<", exp.LTE: "<="}
    for kind, op in ops.items():
        if isinstance(conjunct, kind) and isinstance(conjunct, exp.Binary):
            flipped = {">": "<", ">=": "<=", "<": ">", "<=": ">=", "=": "="}
            if isinstance(conjunct.left, exp.Column):
                rhs = val(conjunct.right)
                return None if rhs is None else f"{column_expr} {op} {rhs}"
            lhs = val(conjunct.left)
            return None if lhs is None else f"{column_expr} {flipped[op]} {lhs}"
    if isinstance(conjunct, exp.In):
        values = [val(e) for e in conjunct.args.get("expressions") or []]
        if not values or any(v is None for v in values):
            return None
        return f"{column_expr} IN ({', '.join(v for v in values if v is not None)})"
    if isinstance(conjunct, exp.Between):
        low, high = val(conjunct.args.get("low")), val(conjunct.args.get("high"))
        if low is None or high is None:
            return None
        return f"{column_expr} BETWEEN {low} AND {high}"
    return None


def _predicate_column_expr(name: str, ch_type: str) -> str | None:
    """The ClickHouse expression a predicate on ``name`` compares — the SAME expression the
    projection exports for a to-string type (so DuckDB and ClickHouse compare identical values),
    the raw column otherwise; None for a type whose comparison is not pushed."""
    base = _unwrap(ch_type)
    kind = base.split("(", 1)[0].strip()
    if kind in ("IPv4", "IPv6", "FixedString", "Enum8", "Enum16"):
        return f"toString({_ch_ident(name)})"
    if kind == "Decimal" and (_decimal_precision(base) or 0) > _MAX_DECIMAL_PRECISION:
        return None
    if kind in ("Int128", "UInt128", "Int256", "UInt256", "Decimal256"):
        return None
    return _ch_ident(name)


# -- the rewrite ----------------------------------------------------------------------------------


def _own_select(tbl: exp.Table) -> exp.Select | None:
    parent = tbl.parent
    if isinstance(parent, (exp.From, exp.Join)) and isinstance(parent.parent, exp.Select):
        return parent.parent
    return None


def _is_write_target(tbl: exp.Table) -> bool:
    node: exp.Expr | None = tbl
    while node is not None and not isinstance(node, (exp.Insert, exp.Update, exp.Delete)):
        if isinstance(node, exp.Select):
            return False
        node = node.parent
    return node is not None


def _needed_columns(
    tree: exp.Expr, tbl: exp.Table, select: exp.Select | None, rel: ClickHouseRelation
) -> list[str]:
    alias = tbl.alias_or_name.lower()
    by_lower = {name.lower(): name for name, _ in rel.columns}
    if select is not None:
        for proj in select.expressions:
            star = proj if isinstance(proj, exp.Star) else None
            if isinstance(proj, exp.Column) and isinstance(proj.this, exp.Star):
                if not proj.table or proj.table.lower() == alias:
                    star = proj.this
            if star is not None:
                return [name for name, _ in rel.columns]
    wanted: set[str] = set()
    for col in tree.find_all(exp.Column):
        if isinstance(col.this, exp.Star):
            continue
        qualifier = col.table.lower()
        if qualifier and qualifier != alias:
            continue
        name = col.name.lower()
        if not qualifier and name == alias and name not in by_lower:
            return [n for n, _ in rel.columns]  # DuckDB whole-row reference: SELECT t0 FROM ...
        if name in by_lower:
            wanted.add(by_lower[name])
    for join in tree.find_all(exp.Join):
        for ident in join.args.get("using") or []:
            if ident.name.lower() in by_lower:
                wanted.add(by_lower[ident.name.lower()])
    if not wanted:  # e.g. count(*): Parquet needs at least one column
        return [rel.columns[0][0]]
    return [name for name, _ in rel.columns if name in wanted]


def _candidate_conjuncts(tbl: exp.Table, select: exp.Select | None) -> list[exp.Expr]:
    """Top-level AND conjuncts that filter this table's rows wherever they appear: its SELECT's
    WHERE (every classified predicate is null-rejecting, so even a null-supplying side is safe), an
    INNER join's ON anywhere in the SELECT, and its own LEFT join's ON (it is the null-supplying
    side there, so ON narrows only its own rows)."""
    if select is None:
        return []
    out: list[exp.Expr] = []
    where = select.args.get("where")
    if where is not None:
        out.extend(_split_and(where.this))
    for join in select.args.get("joins") or []:
        on = join.args.get("on")
        if on is None:
            continue
        side = (join.side or "").upper()
        kind = (join.kind or "").upper()
        inner = not side and kind in ("", "INNER")
        own_left = side == "LEFT" and kind in ("", "OUTER") and join.this is tbl
        if inner or own_left:
            out.extend(_split_and(on))
    single = not (select.args.get("joins") or [])
    if single:  # a single-table SELECT may leave its columns unqualified
        qualified = []
        for c in out:
            c = c.copy()
            for col in c.find_all(exp.Column):
                if not col.table:
                    col.set("table", exp.to_identifier(tbl.alias_or_name))
            qualified.append(c)
        out = qualified
    return out


def _pushed_predicates(
    tbl: exp.Table, select: exp.Select | None, rel: ClickHouseRelation, params: list | None
) -> list[str]:
    alias = tbl.alias_or_name.lower()
    types = {name.lower(): (name, t) for name, t in rel.columns}
    pushed: list[str] = []
    for conjunct in _candidate_conjuncts(tbl, select):
        target = _literal_predicate_target(conjunct, allow_params=True)
        if target is None or target[0].lower() != alias or target[1].lower() not in types:
            continue
        name, ch_type = types[target[1].lower()]
        column_expr = _predicate_column_expr(name, ch_type)
        if column_expr is None:
            continue
        rendered = _pushable_sql(conjunct, column_expr, _unwrap(ch_type), params)
        if rendered is not None and rendered not in pushed:
            pushed.append(rendered)
    return pushed


def clickhouse_query(
    rel: ClickHouseRelation, columns: list[str], predicates: list[str], *, describe: bool
) -> tuple[str, list[tuple[str, str | None]]]:
    """(ClickHouse SELECT ... FORMAT Parquet, [(column, DuckDB cast or None)])."""
    types = dict(rel.columns)
    exprs: list[str] = []
    casts: list[tuple[str, str | None]] = []
    for name in columns:
        ch_expr, cast = _export(_ch_ident(name), types[name], name)
        exprs.append(ch_expr if ch_expr == _ch_ident(name) else f"{ch_expr} AS {_ch_ident(name)}")
        casts.append((name, cast))
    sql = (
        f"SELECT {', '.join(exprs)} FROM {_ch_ident(rel.database)}.{_ch_ident(rel.table)}"
        + (f" WHERE {' AND '.join(predicates)}" if predicates else "")
        + (" LIMIT 0" if describe else "")
        + " FORMAT Parquet"
    )
    return sql, casts


def _replacement(url: str, casts: list[tuple[str, str | None]]) -> str:
    scan = f"read_parquet('{url}')"
    if not any(cast for _, cast in casts):
        return scan
    cols = ", ".join(
        f"CAST({_duck_ident(n)} AS {cast}) AS {_duck_ident(n)}" if cast else _duck_ident(n)
        for n, cast in casts
    )
    return f"(SELECT {cols} FROM {scan})"


def rewrite(
    sql: str,
    params: list | None,
    relations: Mapping[tuple[str, str, str], ClickHouseRelation],
    *,
    deadline_s: float | None,
    describe: bool = False,
) -> str | None:
    """``sql`` (DuckDB dialect) with every registered ClickHouse relation replaced by its live
    HTTP read, or None when the statement names none. ``relations`` is keyed by the lowercased
    (catalog, schema, table) physical name. ``describe`` reads only the shape (``LIMIT 0``)."""
    lowered = sql.lower()
    if not any(rel.table.lower() in lowered for rel in relations.values()):
        return None
    tree = sqlglot.parse_one(sql, read="duckdb")
    hits = [
        (tbl, relations[key])
        for tbl in list(tree.find_all(exp.Table))
        if (key := (tbl.catalog.lower(), tbl.db.lower(), tbl.name.lower())) in relations
    ]
    if not hits:
        return None
    for tbl, rel in hits:
        if _is_write_target(tbl):
            raise ClickHouseScanError(
                f"{rel.database}.{rel.table} is read live over ClickHouse's HTTP interface; the "
                "DuckDB engine does not write to it"
            )
    # Plan every read against the unmodified tree, then substitute.
    planned = []
    for tbl, rel in hits:
        select = _own_select(tbl)
        columns = _needed_columns(tree, tbl, select, rel)
        predicates = _pushed_predicates(tbl, select, rel, params)
        ch_sql, casts = clickhouse_query(rel, columns, predicates, describe=describe)
        planned.append(
            (tbl, _replacement(read_url(rel.base_url, ch_sql, deadline_s=deadline_s), casts))
        )
    for tbl, replacement in planned:
        alias = tbl.alias_or_name
        if not tbl.alias:
            # An unaliased reference may qualify columns with its catalog/schema; the replacement
            # is aliased by the bare table name, so those qualifiers go.
            for col in tree.find_all(exp.Column):
                if col.table.lower() == tbl.name.lower() and (
                    col.args.get("db") or col.args.get("catalog")
                ):
                    col.set("db", None)
                    col.set("catalog", None)
        new = (
            sqlglot.parse_one(f"SELECT 1 FROM {replacement} AS {_duck_ident(alias)}", read="duckdb")
            .args["from_"]
            .this
        )
        tbl.replace(new)
    return tree.sql(dialect="duckdb")
