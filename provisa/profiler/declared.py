# Copyright (c) 2026 Kenneth Stott
# Canary: 5c1e9a47-2d83-4b6f-9e05-7a3f1b8d6c24
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Declared profiles (REQ-1942): the facts a profile run measures, written by hand.

A declared profile is stored as a profile run is -- one row set in each result relation of the
table, keyed by its own run id -- its ``runs`` row marked ``sample_method = 'declared'``, so
generation reads a measured and a declared profile by the one path
(:func:`provisa.synthetic.run.read_profile`). Nothing that compares measured runs (drift, a fake
measured from the latest run) ever reads a declared one: :func:`measured` is their filter.

The facts, by the column as the org admin reads it: the table's row count; each column's null
share, distinct count, range or distribution (101 quantiles) and, for a category, its values and
weights, or its shapes; each one-to-many relationship's fan-out, as a range or 101 quantiles;
optionally the dependence between columns, as a profile run's own correlation, dependency and
joint-count rows. A column a declared profile leaves out takes its fake or its synthetic rule --
a column a relationship generates, its relationship's -- and one with none is refused by name; a
key declares its range or shapes, which its keys are numbered or shaped from. :func:`as_declared` turns a run's results back into this document, so a profile run can
be copied into a declared profile and changed (a what-if) without touching its source.
"""

# Requirements: REQ-1942, REQ-1934

from __future__ import annotations

import uuid
from datetime import UTC, datetime
from typing import Any

import sqlalchemy as sa

from provisa.profiler.schema import field_names

#: The ``sample_method`` of a declared profile's ``runs`` row: no rows were read.
DECLARED = "declared"

#: The dependence kinds a declared profile may carry, as a run writes them.
DEPENDENCE_KINDS: tuple[str, ...] = ("correlations", "dependencies", "joint_counts")

_RUN_KEY = ("run_id", "run_time")


class DeclaredProfileRefused(ValueError):
    """A declared profile that cannot be stored, said naming what is wrong."""


def measured(runs: sa.Table) -> Any:
    """The filter keeping a table's measured runs, leaving its declared profiles out."""
    return runs.c.sample_method.is_distinct_from(DECLARED)


def _share(value: Any, where: str) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)) or not 0 <= value <= 1:
        raise DeclaredProfileRefused(f"{where} must be a share from 0 to 1, not {value!r}")
    return float(value)


def _count(value: Any, where: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        raise DeclaredProfileRefused(f"{where} must be a whole number from 0, not {value!r}")
    return value


def _weights(items: Any, key: str, where: str) -> list[tuple[Any, float]]:
    if not isinstance(items, list) or not items:
        raise DeclaredProfileRefused(f"{where} must be a non-empty list of {{{key}, weight}}")
    out = []
    for item in items:
        if not isinstance(item, dict) or set(item) != {key, "weight"}:
            raise DeclaredProfileRefused(f"{where}: each entry is {{{key}, weight}}, not {item!r}")
        w = item["weight"]
        if isinstance(w, bool) or not isinstance(w, (int, float)) or w < 0:
            raise DeclaredProfileRefused(f"{where}: weight {w!r} of {item[key]!r} is not >= 0")
        out.append((item[key], float(w)))
    if sum(w for _, w in out) <= 0:
        raise DeclaredProfileRefused(f"{where}: the weights leave nothing to draw")
    return out


def _point(value: Any, family: str, where: str) -> float:
    """A range bound as the quantile sketch holds it: a number, or epoch seconds."""
    if family == "numeric":
        if isinstance(value, bool) or not isinstance(value, (int, float)):
            raise DeclaredProfileRefused(f"{where} must be a number, not {value!r}")
        return float(value)
    if family == "temporal":
        if not isinstance(value, str):
            raise DeclaredProfileRefused(f"{where} must be an ISO date or timestamp")
        try:
            moment = datetime.fromisoformat(value)
        except ValueError as exc:
            raise DeclaredProfileRefused(f"{where}: {exc}") from exc
        if moment.tzinfo is None:
            moment = moment.replace(tzinfo=UTC)
        return moment.timestamp()
    raise DeclaredProfileRefused(f"{where}: a {family} column has no range; give its values")


def _sketch(fact: dict, family: str, where: str) -> tuple[float, ...] | None:
    """The 101-point quantile sketch a range or a distribution declares, or None."""
    from provisa.profiler.statement import QUANTILE_POINTS

    if "range" in fact and "quantiles" in fact:
        raise DeclaredProfileRefused(f"{where}: give a range or quantiles, not both")
    if "range" in fact:
        bounds = fact["range"]
        if not isinstance(bounds, dict) or set(bounds) != {"min", "max"}:
            raise DeclaredProfileRefused(f"{where}.range is {{min, max}}")
        lo = _point(bounds["min"], family, f"{where}.range.min")
        hi = _point(bounds["max"], family, f"{where}.range.max")
        if hi < lo:
            raise DeclaredProfileRefused(f"{where}.range: max is below min")
        return tuple(lo + q * (hi - lo) for q in QUANTILE_POINTS)
    if "quantiles" in fact:
        values = fact["quantiles"]
        if (
            not isinstance(values, list)
            or len(values) != len(QUANTILE_POINTS)
            or any(isinstance(v, bool) or not isinstance(v, (int, float)) for v in values)
        ):
            raise DeclaredProfileRefused(
                f"{where}.quantiles must be {len(QUANTILE_POINTS)} numbers, q = 0 to 1 by 0.01"
            )
        if any(b < a for a, b in zip(values, values[1:])):
            raise DeclaredProfileRefused(f"{where}.quantiles must never decrease")
        return tuple(float(v) for v in values)
    return None


def _rows_of(weighted: list[tuple[Any, float]], total: int) -> list[tuple[Any, int]]:
    """``weighted`` as row counts over ``total`` rows, heaviest first."""
    whole = sum(w for _, w in weighted)
    ranked = sorted(weighted, key=lambda vw: -vw[1])
    return [(v, round(w / whole * total)) for v, w in ranked]


_COLUMN_FACTS = frozenset(
    {"nullShare", "distinctCount", "range", "quantiles", "values", "shapes", "integerOnly"}
)


def _column_rows(spec: Any, fact: Any, rows: int, key: dict) -> dict[str, list[dict]]:
    """One declared column's rows in the result relations."""
    where = f"column {spec.name!r}"
    if not isinstance(fact, dict) or not fact.keys() <= _COLUMN_FACTS:
        raise DeclaredProfileRefused(
            f"{where}: its facts are {sorted(_COLUMN_FACTS)}, not {sorted(fact)}"
            if isinstance(fact, dict)
            else f"{where}: its facts are an object"
        )
    if "nullShare" not in fact:
        raise DeclaredProfileRefused(f"{where} declares no nullShare")
    null_share = _share(fact["nullShare"], f"{where}.nullShare")
    nulls = round(null_share * rows)
    held = rows - nulls
    values = _weights(fact["values"], "value", f"{where}.values") if "values" in fact else None
    if values is not None and ("range" in fact or "quantiles" in fact):
        raise DeclaredProfileRefused(f"{where}: a category gives its values, not a range")
    sketch = _sketch(fact, spec.family, where)
    shapes = _weights(fact["shapes"], "shape", f"{where}.shapes") if "shapes" in fact else None
    if values is None and sketch is None and shapes is None:
        raise DeclaredProfileRefused(
            f"{where} declares no values, range, quantiles or shapes to generate it from"
        )
    if values is not None:
        distinct = len({v for v, _ in values if v is not None})
        if "distinctCount" in fact and fact["distinctCount"] != distinct:
            raise DeclaredProfileRefused(
                f"{where}: distinctCount {fact['distinctCount']!r} is not its {distinct} values"
            )
    elif "distinctCount" not in fact:
        raise DeclaredProfileRefused(f"{where} declares no distinctCount")
    else:
        distinct = _count(fact["distinctCount"], f"{where}.distinctCount")
    integer_only = fact.get("integerOnly", spec.family == "numeric" and _integer(spec.data_type))
    if not isinstance(integer_only, bool):
        raise DeclaredProfileRefused(f"{where}.integerOnly must be true or false")
    out: dict[str, list[dict]] = {k: [] for k in ("columns", "quantiles", "top_values", "shapes")}
    out["columns"].append(
        {
            **key,
            "column_name": spec.name,
            "physical_column": spec.physical,
            "data_type": spec.data_type,
            "family": spec.family,
            "row_count": rows,
            "null_count": nulls,
            "null_share": null_share,
            "distinct_count": distinct,
            "distinct_ratio": distinct / rows if rows else None,
            "min_value": None if sketch is None else _render(sketch[0], spec.family),
            "max_value": None if sketch is None else _render(sketch[-1], spec.family),
            "mean": None,
            "stddev": None,
            "variance": None,
            "skewness": None,
            "kurtosis": None,
            "log_mean": None,
            "log_variance": None,
            "integer_only": integer_only,
            "zero_share": None,
            "length_min": None,
            "length_max": None,
        }
    )
    if sketch is not None:
        from provisa.profiler.statement import QUANTILE_POINTS

        out["quantiles"] = [
            {**key, "column_name": spec.name, "measure": "value", "q": q, "value": v}
            for q, v in zip(QUANTILE_POINTS, sketch)
        ]
    if values is not None:
        out["top_values"] = [
            {
                **key,
                "column_name": spec.name,
                "kind": "frequency",
                "rank": r,
                "value": None if v is None else str(v),
                "row_count": n,
            }
            for r, (v, n) in enumerate(_rows_of(values, held), 1)
        ]
    if shapes is not None:
        out["shapes"] = [
            {**key, "column_name": spec.name, "rank": r, "shape": str(s), "row_count": n}
            for r, (s, n) in enumerate(_rows_of(shapes, held), 1)
        ]
    return out


def _integer(data_type: str | None) -> bool:
    base = (data_type or "").lower().split("(")[0].strip()
    return base in ("smallint", "integer", "int", "bigint", "int2", "int4", "int8", "tinyint")


def _render(value: float, family: str) -> str:
    if family == "temporal":
        return datetime.fromtimestamp(value, UTC).isoformat()
    return repr(value)


def _fanout_rows(spec: Any, fact: Any, parents: int, key: dict) -> dict[str, list[dict]]:
    where = f"relationship {spec.relationship!r}"
    if not isinstance(fact, dict) or not fact.keys() <= {"range", "quantiles"}:
        raise DeclaredProfileRefused(f"{where}: its fan-out is a range or quantiles")
    sketch = _sketch(fact, "numeric", where)
    if sketch is None:
        raise DeclaredProfileRefused(f"{where} declares no range or quantiles of children")
    if sketch[0] < 0:
        raise DeclaredProfileRefused(f"{where}: a parent has no fewer than 0 children")
    from provisa.profiler.statement import QUANTILE_POINTS

    mean = sum(sketch) / len(sketch)
    childless = sum(1 for v in sketch if v < 0.5) / len(sketch)
    return {
        "fanout_runs": [
            {
                **key,
                "relationship": spec.relationship,
                "child_table": spec.child_table,
                "parents": parents,
                "mean": mean,
                "max": round(sketch[-1]),
                "childless_share": childless,
            }
        ],
        "fanout": [
            {**key, "relationship": spec.relationship, "q": q, "value": v}
            for q, v in zip(QUANTILE_POINTS, sketch)
        ],
    }


def _dependence_rows(dependence: Any, key: dict) -> dict[str, list[dict]]:
    if not isinstance(dependence, dict) or not dependence.keys() <= set(DEPENDENCE_KINDS):
        raise DeclaredProfileRefused(f"dependence holds {list(DEPENDENCE_KINDS)}")
    out = {}
    for kind, rows in dependence.items():
        names = set(field_names(kind)) - set(_RUN_KEY)
        if not isinstance(rows, list) or any(
            not isinstance(r, dict) or set(r) != names for r in rows
        ):
            raise DeclaredProfileRefused(
                f"dependence.{kind}: each row holds exactly {sorted(names)}"
            )
        out[kind] = [{**key, **r} for r in rows]
    return out


def results(
    target: Any, doc: Any, covered: frozenset[str], run_id: str, run_time: datetime
) -> dict[str, list[dict]]:
    """The result rows of declared profile ``doc`` of ``target``
    (:class:`provisa.profiler.run.Target`). ``covered``: the columns, as published, that generate
    without a fact -- each declaring a fake or a synthetic rule, or a relationship's child
    column."""
    if not isinstance(doc, dict) or not doc.keys() <= {
        "rowCount",
        "columns",
        "fanouts",
        "dependence",
    }:
        raise DeclaredProfileRefused(
            "a declared profile holds rowCount, columns, and optionally fanouts and dependence"
        )
    if "rowCount" not in doc:
        raise DeclaredProfileRefused("a declared profile declares the table's rowCount")
    rows = _count(doc["rowCount"], "rowCount")
    columns = doc.get("columns", {})
    if not isinstance(columns, dict):
        raise DeclaredProfileRefused("columns is an object, by column")
    specs = {c.name: c for c in target.columns}
    unknown = sorted(set(columns) - set(specs))
    if unknown:
        raise DeclaredProfileRefused(
            f"{target.table_name!r} has no column " + ", ".join(repr(u) for u in unknown)
        )
    bare = sorted(set(specs) - set(columns) - covered)
    if bare:
        raise DeclaredProfileRefused(
            "these columns have no profile fact, fake or synthetic rule to generate them from: "
            + ", ".join(f"{target.table_name}.{c}" for c in bare)
        )
    key = {"run_id": run_id, "run_time": run_time}
    out: dict[str, list[dict]] = {}

    def add(part: dict[str, list[dict]]) -> None:
        for kind, kind_rows in part.items():
            out.setdefault(kind, []).extend(kind_rows)

    for name, fact in columns.items():
        add(_column_rows(specs[name], fact, rows, key))
    fanouts = doc.get("fanouts", {})
    if not isinstance(fanouts, dict):
        raise DeclaredProfileRefused("fanouts is an object, by relationship")
    rels = {f.relationship: f for f in target.fanouts}
    unknown = sorted(set(fanouts) - set(rels))
    if unknown:
        raise DeclaredProfileRefused(
            f"{target.table_name!r} has no one-to-many relationship "
            + ", ".join(repr(u) for u in unknown)
        )
    for name, fact in fanouts.items():
        add(_fanout_rows(rels[name], fact, rows, key))
    if "dependence" in doc:
        add(_dependence_rows(doc["dependence"], key))
    out["runs"] = [
        {
            **key,
            "region": None,
            "profiled_table": target.table_name,
            "row_count": rows,
            "sampled": False,
            "sample_method": DECLARED,
            "target_fraction": 1.0,
            "sample_fraction": 1.0,
            "sample_attempts": None,
            "profiled_rows": rows,
            "previous_run_id": None,
            "duplicate_rows": None,
            "duplicate_share": None,
            "key_duplicates": None,
            "freshness_seconds": None,
            "window_runs": None,
            "dependence_method": None,
            "dependence_fraction": None,
            "dependence_rows": None,
            "network_rows": None,
            "dependence_attempts": None,
            "duration_ms": 0,
            "status": "succeeded",
            "error": None,
        }
    ]
    for kind, kind_rows in out.items():
        names = field_names(kind)
        for r in kind_rows:
            assert tuple(r) == names, f"{kind} row drifted from the shipped schema"
    return out


async def covered_columns(conn: Any, target: Any) -> frozenset[str]:
    """``target``'s columns, as published, that generate without a profile fact: each declaring a
    fake or a synthetic rule, or a child column of a relationship to a parent. A key is numbered
    from its declared minimum, or shaped by its declared shapes, so it needs its facts."""
    from provisa.core.schema_org import table_columns as tc

    result = await conn.execute_core(
        sa.select(tc.c.column_name).where(
            tc.c.table_id == target.table_id,
            sa.or_(
                tc.c.fake.is_not(None),
                tc.c.synthetic_rule.is_not(None),
            ),
        )
    )
    physical = {r[0] for r in result.fetchall()}
    children = {p.child_key for p in target.parents}  # as published
    return frozenset(c.name for c in target.columns if c.physical in physical or c.name in children)


async def declare(state: Any, table_id: int, table_name: str, doc: Any) -> str:
    """Store ``doc`` as a declared profile of the table, in the environment ``state.model_db`` is
    scoped to; its run id."""
    from provisa.profiler.run import resolve_target, write_results

    target = resolve_target(state, table_id, table_name, {})
    run_id = f"{DECLARED}-{uuid.uuid4().hex}"
    async with state.model_db.acquire() as conn:
        covered = await covered_columns(conn, target)
        rows = results(target, doc, covered, run_id, datetime.now(UTC))
        await write_results(conn, table_name, table_id, rows)
    return run_id


def as_declared(run: dict[str, list[dict]]) -> dict[str, Any]:
    """A run's results -- measured or declared, by kind -- as a declared profile document, to be
    changed and stored as a declared profile of its own."""
    from provisa.profiler.statement import QUANTILE_POINTS

    (head,) = run["runs"]
    if not head["profiled_rows"]:
        raise DeclaredProfileRefused(
            f"run {head['run_id']} read no rows, so it holds no shares to copy"
        )
    rows = int(head["row_count"])
    sketches: dict[str, list[tuple[float, float]]] = {}
    for q in run.get("quantiles", []):
        if q["measure"] == "value":
            sketches.setdefault(q["column_name"], []).append((q["q"], q["value"]))
    frequencies: dict[str, list[dict]] = {}
    for v in run.get("top_values", []):
        if v["kind"] == "frequency":
            frequencies.setdefault(v["column_name"], []).append(
                {"value": v["value"], "weight": v["row_count"]}
            )
    shapes: dict[str, list[dict]] = {}
    for s in run.get("shapes", []):
        shapes.setdefault(s["column_name"], []).append(
            {"shape": s["shape"], "weight": s["row_count"]}
        )
    columns: dict[str, dict[str, Any]] = {}
    for c in run.get("columns", []):
        name = c["column_name"]
        if c["null_share"] is None:
            # Shown to this viewer by its shape only (provisa.profiler.governance.safe_run): the
            # copy holds no fact of it, so it generates by its fake or rule, or is refused.
            continue
        fact: dict[str, Any] = {
            "nullShare": c["null_share"],  # profiled_rows > 0, so every share is measured
            "integerOnly": bool(c["integer_only"]),
        }
        sketch = sorted(sketches.get(name, []))
        if name in frequencies:
            fact["values"] = frequencies[name]
        else:
            fact["distinctCount"] = int(c["distinct_count"])
            if len(sketch) == len(QUANTILE_POINTS):
                fact["quantiles"] = [v for _, v in sketch]
        if name in shapes and "values" not in fact:
            fact["shapes"] = shapes[name]
        columns[name] = fact
    fan_q: dict[str, list[tuple[float, float]]] = {}
    for f in run.get("fanout", []):
        fan_q.setdefault(f["relationship"], []).append((f["q"], f["value"]))
    fanouts = {
        rel: {"quantiles": [v for _, v in sorted(points)]}
        for rel, points in fan_q.items()
        if len(points) == len(QUANTILE_POINTS)
    }
    doc: dict[str, Any] = {"rowCount": rows, "columns": columns}
    if fanouts:
        doc["fanouts"] = fanouts
    dependence = {
        kind: [{k: v for k, v in r.items() if k not in _RUN_KEY} for r in run[kind]]
        for kind in DEPENDENCE_KINDS
        if run.get(kind)
    }
    if dependence:
        doc["dependence"] = dependence
    return doc
