# Copyright (c) 2026 Kenneth Stott
# Canary: 8b2e5c71-0f39-4a66-9d14-c3e7a0b5f892
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""From a dataset's (table, profile run) pairs to the plans that generate its tables (REQ-1939).

Pure: the profile runs and the environment's registered tables and relationships are read by the
caller (``provisa.synthetic.run``). The maintainer's ruling (2026-10-06) is that generation uses
each profile as recorded, for every column: vocabulary, most frequent values and sketches alike;
the environment's own masks govern who sees the result.

A column's declared kind of fake (REQ-1494) decides its values: the categories fake is the only way
a real value reaches a cell, and the bool fake draws true at a stated or measured share. A column
with no fake is generated from its own profile, never a recorded value, and the report names it as
undeclared.
"""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass, field, replace

from provisa.fakes.kinds import Bool, Categories, FakeKind, kind_name
from provisa.synthetic.generate import ColumnPlan, ForeignKey, TablePlan

# A column whose distinct values are at least this share of its rows grows its vocabulary with the
# rows; one below it keeps its profiled vocabulary at any scale (REQ-1939, PAIRS, SCALE, CLOSURE).
GROWING_DISTINCT_RATIO = 0.5

_SQL_TYPES = {
    "smallint": "SMALLINT",
    "integer": "INTEGER",
    "bigint": "BIGINT",
    "float": "DOUBLE",
    "double": "DOUBLE",
    "real": "DOUBLE",
    "numeric": "DECIMAL(38, 10)",
    "decimal": "DECIMAL(38, 10)",
    "text": "VARCHAR",
    "varchar": "VARCHAR",
    "uuid": "VARCHAR",
    "date": "DATE",
    "timestamp": "TIMESTAMP",
    "timestamptz": "TIMESTAMP",
    "boolean": "BOOLEAN",
}


class DatasetRefused(ValueError):
    """A dataset that cannot be generated, said naming what is missing."""


#: A column's recorded values and their counts, nulls included: ``(value, count)`` pairs.
Counts = list[tuple[str | None, int]]


@dataclass(frozen=True)
class ProfiledColumn:
    physical: str
    family: str
    null_count: int
    distinct_count: int
    distinct_ratio: float | None
    integer_only: bool | None
    min_value: str | None
    sketch: tuple[float, ...] | None
    top: tuple[tuple[str | None, int], ...]
    frequencies: tuple[tuple[str | None, int], ...]
    shapes: tuple[tuple[str, int], ...]


@dataclass(frozen=True)
class ProfiledFanout:
    child_table: str  # domain.table as the profile names it
    parents: int
    sketch: tuple[float, ...]


@dataclass(frozen=True)
class ProfiledTable:
    run_id: str
    row_count: int  # the table's rows
    profiled_rows: int  # the rows the run read (fewer under a sample): shares are over these
    columns: dict[str, ProfiledColumn]  # by physical column
    fanouts: tuple[ProfiledFanout, ...]


@dataclass(frozen=True)
class DatasetTable:
    table_id: int
    name: str  # registered table name
    pgwire_name: str  # domain.table as a profile of it names it
    columns: tuple[tuple[str, str, bool], ...]  # (name, IR type, is primary key)
    scale: float
    profile: ProfiledTable
    pii: frozenset[str] = frozenset()  # its columns tagged pii
    fakes: dict[str, FakeKind] = field(default_factory=dict)  # each faked column's kind (REQ-1494)


@dataclass(frozen=True)
class Edge:
    """A declared relationship: ``child.child_column`` refers to ``parent.parent_column``."""

    parent_id: int
    parent_column: str
    child_id: int
    child_column: str


@dataclass
class PlannedTable:
    table: DatasetTable
    plan: TablePlan
    undeclared: list[str] = field(default_factory=list)
    unprofiled: list[str] = field(default_factory=list)


def edges_of(relationships: list[dict]) -> list[Edge]:
    """The parent/child edges of the model's direct relationships (junctions and computed edges
    have no child key column to generate)."""
    edges = []
    for r in relationships:
        if r.get("target_table_id") is None or r.get("via_table_id") is not None:
            continue
        if r["cardinality"] == "many-to-one":
            edges.append(
                Edge(
                    r["target_table_id"],
                    r["target_column"],
                    r["source_table_id"],
                    r["source_column"],
                )
            )
        elif r["cardinality"] == "one-to-many":
            edges.append(
                Edge(
                    r["source_table_id"],
                    r["source_column"],
                    r["target_table_id"],
                    r["target_column"],
                )
            )
    return edges


def check_closure(tables: dict[int, str], all_tables: dict[int, str], edges: list[Edge]) -> None:
    """Refuse a dataset naming a table without the parent of one of its relationships."""
    for e in edges:
        if e.child_id in tables and e.parent_id not in tables:
            raise DatasetRefused(
                f"table {tables[e.child_id]!r} refers to {all_tables[e.parent_id]!r} "
                f"({e.child_column} -> {e.parent_column}), which the dataset does not name; "
                f"add it so its keys exist to refer to"
            )


def sql_type(ir_type: str, table: str, column: str) -> str:
    base = (ir_type or "").lower().split("(")[0].strip()
    if base not in _SQL_TYPES:
        raise DatasetRefused(
            f"column {table}.{column} has type {ir_type!r}, which a synthetic dataset cannot generate"
        )
    return _SQL_TYPES[base]


def _shares(
    pairs: tuple[tuple[str | None, int], ...], total: int
) -> tuple[tuple[str | None, float], ...]:
    return tuple((v, n / total) for v, n in pairs) if total else ()


def _key_column(
    table: DatasetTable, name: str, ir_type: str, prof: ProfiledColumn | None
) -> ColumnPlan:
    typ = sql_type(ir_type, table.name, name)
    if typ in ("SMALLINT", "INTEGER", "BIGINT", "DECIMAL(38, 10)", "DOUBLE"):
        if prof is None or prof.min_value is None:
            raise DatasetRefused(
                f"key column {table.name}.{name} has no profiled minimum to number keys from"
            )
        return ColumnPlan(name, typ, "numeric", key=True, key_offset=int(float(prof.min_value)))
    if prof is None or not prof.shapes:
        raise DatasetRefused(
            f"key column {table.name}.{name} has no profiled value shape to generate keys from"
        )
    return ColumnPlan(name, typ, "text", key=True, key_shape=prof.shapes[0][0])


def check_pii(tables: list[DatasetTable]) -> None:
    """Refuse generating any column tagged pii with no declared fake kind, naming every one: its
    values cannot be generated from anything but its fake (REQ-1939, A COLUMN'S FAKE SETTINGS
    DECIDE ITS VALUES)."""
    undeclared = sorted(f"{t.name}.{c}" for t in tables for c in t.pii if c not in t.fakes)
    if undeclared:
        raise DatasetRefused(
            "these columns are tagged pii and declare no kind of fake, so their values cannot be "
            "generated: " + ", ".join(undeclared) + "; declare a fake kind for each"
        )


def plan_tables(
    tables: list[DatasetTable],
    edges: list[Edge],
    *,
    seed: int,
    names: dict[int, str],
    count_rows: Callable[[TablePlan], int],
    measure: Callable[[DatasetTable, str], Counts],
) -> list[PlannedTable]:
    """The generation plan of every table of the dataset, parents before children.

    A table generated by a relationship has as many rows as its parents' children: the
    children-per-parent distribution is kept at any scale, so its own scale does not apply.
    ``count_rows`` counts what such a table's statement yields (``generate.children_count_sql``
    run by the engine), so a table referring to it refers only to keys that exist. ``measure``
    reads a column's values and counts from the table itself, one GROUP BY as the org admin through
    the governed pipeline, where a fake needs them and the profile has no full frequency table
    (REQ-1494, MEASURED FROM THE PROFILE, ELSE FROM THE TABLE)."""
    by_id = {t.table_id: t for t in tables}
    # ``names``: every registered table, so a missing parent is refused by its name.
    check_closure({t.table_id: t.name for t in tables}, names, edges)
    check_pii(tables)
    ordered = _parents_first(tables, edges)
    rows: dict[int, int] = {}
    keys: dict[tuple[int, str], ColumnPlan] = {}
    out = []
    for t in ordered:
        prof = t.profile
        own_edges = [e for e in edges if e.child_id == t.table_id and e.parent_id in by_id]
        driving = _driving_edge(t, own_edges, by_id)
        planned = PlannedTable(table=t, plan=TablePlan(t.name, 0, ()))
        cols: list[ColumnPlan] = []
        for name, ir_type, is_pk in t.columns:
            p = prof.columns.get(name)
            if name not in t.fakes:
                planned.undeclared.append(name)
            fk_edge = next((e for e in own_edges if e.child_column == name), None)
            fake = _fake_of(t, name, ir_type, is_pk or fk_edge is not None)
            if isinstance(fake, Categories):
                col = _categories_column(t, name, ir_type, fake, p, prof.profiled_rows, measure)
            elif isinstance(fake, Bool):
                col = _bool_column(t, name, ir_type, fake, p, prof.profiled_rows, measure)
            elif is_pk:
                col = _key_column(t, name, ir_type, p)
                keys[(t.table_id, name)] = col
            elif fk_edge is not None:
                parent_key = keys.get((fk_edge.parent_id, fk_edge.parent_column))
                if parent_key is None:
                    raise DatasetRefused(
                        f"{t.name}.{name} refers to {by_id[fk_edge.parent_id].name}."
                        f"{fk_edge.parent_column}, which is not that table's primary key"
                    )
                col = ColumnPlan(
                    name,
                    sql_type(ir_type, t.name, name),
                    parent_key.family,
                    null_share=_null_share(p, prof.profiled_rows),
                    foreign_key=ForeignKey(
                        by_id[fk_edge.parent_id].name, rows[fk_edge.parent_id], parent_key
                    ),
                    driving=fk_edge is driving,
                )
                if col.driving:
                    col = replace(col, null_share=0.0)  # every generated child has its parent
            elif p is None:
                planned.unprofiled.append(name)
                col = ColumnPlan(name, sql_type(ir_type, t.name, name), "other", null_share=1.0)
            else:
                col = _value_column(t, name, ir_type, p, prof.profiled_rows)
            cols.append(col)
        if driving is not None:
            parent = by_id[driving.parent_id]
            fan = _fanout(parent, t)
            hot = _hot_counts(t, driving.child_column, fan, parent.scale)
            plan = TablePlan(
                t.name,
                0,
                tuple(cols),
                fanout=(rows[parent.table_id], fan.sketch, hot),
                seed=seed,
            )
            rows[t.table_id] = count_rows(plan)
        else:
            n = round(prof.row_count * t.scale)
            plan = TablePlan(t.name, n, tuple(cols), seed=seed)
            rows[t.table_id] = n
        planned.plan = plan
        out.append(planned)
    return out


def _null_share(p: ProfiledColumn | None, rows: int) -> float:
    return p.null_count / rows if p is not None and rows else 0.0


def _value_column(
    t: DatasetTable, name: str, ir_type: str, p: ProfiledColumn, rows: int
) -> ColumnPlan:
    """A column with no fake kind: a category -- its whole vocabulary recorded, being no larger
    than the profiler's low-cardinality limit -- takes its real values at their profiled
    frequencies; any other is generated from its shapes and lengths or its distribution, never
    from its recorded most frequent values."""
    typ = sql_type(ir_type, t.name, name)
    grows = p.distinct_ratio is not None and p.distinct_ratio >= GROWING_DISTINCT_RATIO
    if p.frequencies:
        # No fake: the same number of distinct values with the same shares, all generated.
        counts = [n for v, n in p.frequencies if v is not None]
        total = sum(counts)
        return ColumnPlan(
            name,
            typ,
            p.family,
            null_share=p.null_count / rows if rows else 0.0,
            sketch=p.sketch,
            shapes=_shares(p.shapes, sum(n for _, n in p.shapes)),  # type: ignore[arg-type]
            integer_only=bool(p.integer_only),
            pool=len(counts),
            pool_shares=tuple(n / total for n in counts) if total else (),
        )
    return ColumnPlan(
        name,
        typ,
        p.family,
        null_share=p.null_count / rows if rows else 0.0,
        sketch=p.sketch,
        shapes=_shares(p.shapes, sum(n for _, n in p.shapes)),  # type: ignore[arg-type]
        integer_only=bool(p.integer_only),
        pool=None if grows else max(p.distinct_count, 1),
    )


def _fake_of(t: DatasetTable, name: str, ir_type: str, keyed: bool) -> FakeKind | None:
    """The fake deciding a column's values: its declared one, else bool() for a boolean (REQ-1939,
    BOOLEANS). A key's values are the key's, never a fake's."""
    if keyed:
        return None
    fake = t.fakes.get(name)
    if fake is not None and not isinstance(fake, (Categories, Bool)):
        raise DatasetRefused(
            f"{t.name}.{name} declares {kind_name(fake)}(), which synthetic generation "
            f"does not compute yet"
        )
    if fake is None and _is_boolean(ir_type, t.profile.columns.get(name)):
        return Bool()
    return fake


def to_measure(tables: list[DatasetTable], edges: list[Edge]) -> list[tuple[DatasetTable, str]]:
    """The columns whose fake takes values or shares the profile has no full frequency table of:
    each is read from its table before planning (REQ-1494, MEASURED FROM THE PROFILE, ELSE FROM
    THE TABLE)."""
    ids = {t.table_id for t in tables}
    out = []
    for t in tables:
        fk = {e.child_column for e in edges if e.child_id == t.table_id and e.parent_id in ids}
        for name, ir_type, is_pk in t.columns:
            fake = _fake_of(t, name, ir_type, is_pk or name in fk)
            measured = (isinstance(fake, Categories) and fake.shares is None) or (
                isinstance(fake, Bool) and fake.share is None
            )
            p = t.profile.columns.get(name)
            if measured and (p is None or not p.frequencies):
                out.append((t, name))
    return out


def _is_boolean(ir_type: str, p: ProfiledColumn | None) -> bool:
    return (p is not None and p.family == "boolean") or (ir_type or "").lower() in (
        "boolean",
        "bool",
    )


def _measured(
    t: DatasetTable,
    name: str,
    p: ProfiledColumn | None,
    rows: int,
    measure: Callable[[DatasetTable, str], Counts],
) -> tuple[dict[str, int], float]:
    """The column's recorded values with their counts, and its null share: from its latest profile
    run's full frequency table, else from one GROUP BY on the table (REQ-1494, MEASURED FROM THE
    PROFILE, ELSE FROM THE TABLE)."""
    if p is not None and p.frequencies:
        return {v: n for v, n in p.frequencies if v is not None}, _null_share(p, rows)
    counts = measure(t, name)
    total = sum(n for _, n in counts)
    nulls = sum(n for v, n in counts if v is None)
    return {v: n for v, n in counts if v is not None}, nulls / total if total else 0.0


def _categories_column(
    t: DatasetTable,
    name: str,
    ir_type: str,
    fake: Categories,
    p: ProfiledColumn | None,
    rows: int,
    measure: Callable[[DatasetTable, str], Counts],
) -> ColumnPlan:
    """A column declaring the categories fake, in whichever of its three forms."""
    typ = sql_type(ir_type, t.name, name)
    family = p.family if p is not None else _family_of_type(typ)
    if fake.values is not None and not fake.values:
        raise DatasetRefused(f"{t.name}.{name} declares the categories fake with an empty list")
    if fake.shares is not None:
        assert fake.values is not None  # stated shares are of stated values, checked when declared
        null_share = _null_share(p, rows)
        return _drawn(name, typ, family, list(zip(fake.values, fake.shares)), null_share)
    recorded, null_share = _measured(t, name, p, rows, measure)
    non_null = sum(recorded.values())
    if fake.values is None:
        if not recorded:
            raise DatasetRefused(
                f"{t.name}.{name} declares categories() but its table holds no value to take "
                f"them from; declare the values"
            )
        return _drawn(
            name, typ, family, [(v, n / non_null) for v, n in recorded.items()], null_share
        )
    # The operator's values at their measured shares; a value not measured shares what remains
    # evenly (REQ-1494, CATEGORIES FROM THE PROFILE).
    shares = {v: recorded[v] / non_null for v in fake.values if v in recorded}
    unrecorded = [v for v in fake.values if v not in shares]
    remainder = max(1.0 - sum(shares.values()), 0.0)
    for v in unrecorded:
        shares[v] = remainder / len(unrecorded)
    covered = sum(shares.values())
    if covered <= 0:
        raise DatasetRefused(
            f"{t.name}.{name}: the categories fake's values leave no share to draw them by"
        )
    return _drawn(name, typ, family, [(v, shares[v] / covered) for v in fake.values], null_share)


def _bool_column(
    t: DatasetTable,
    name: str,
    ir_type: str,
    fake: Bool,
    p: ProfiledColumn | None,
    rows: int,
    measure: Callable[[DatasetTable, str], Counts],
) -> ColumnPlan:
    """True in the stated share of rows, else in the measured share -- from the profile, else the
    table, else half on an empty table (REQ-1494, THE BOOL FAKE)."""
    typ = sql_type(ir_type, t.name, name)
    if fake.share is not None:
        if not 0.0 <= fake.share <= 1.0:
            raise DatasetRefused(
                f"{t.name}.{name}: the bool fake's share {fake.share!r} is outside [0, 1]"
            )
        null_share = _null_share(p, rows)
        true_share = fake.share
    else:
        recorded, null_share = _measured(t, name, p, rows, measure)
        trues = sum(n for v, n in recorded.items() if v.lower() in ("true", "t"))
        non_null = sum(recorded.values())
        # REQ-1494, THE BOOL FAKE: an empty table measures no share, so true and false are even.
        true_share = trues / non_null if non_null else 0.5
    return _drawn(
        name, typ, "boolean", [("true", true_share), ("false", 1 - true_share)], null_share
    )


def _drawn(
    name: str, typ: str, family: str, shares: list[tuple[str, float]], null_share: float
) -> ColumnPlan:
    return ColumnPlan(
        name,
        typ,
        family,
        frequencies=tuple((v, sh * (1 - null_share)) for v, sh in shares) + ((None, null_share),),
    )


def _family_of_type(typ: str) -> str:
    if typ in ("SMALLINT", "INTEGER", "BIGINT", "DOUBLE", "DECIMAL(38, 10)"):
        return "numeric"
    if typ in ("DATE", "TIMESTAMP"):
        return "temporal"
    if typ == "BOOLEAN":
        return "boolean"
    return "text"


def _fanout(parent: DatasetTable, child: DatasetTable) -> ProfiledFanout:
    found = [f for f in parent.profile.fanouts if f.child_table == child.pgwire_name]
    if not found:
        raise DatasetRefused(
            f"the profile run {parent.profile.run_id} of {parent.name!r} records no children per "
            f"parent for {child.name!r}; run its profile again with the relationship declared"
        )
    if len(found) > 1:
        raise DatasetRefused(
            f"{parent.name!r} has {len(found)} relationships to {child.name!r}; the dataset cannot "
            f"tell which one its rows are generated by"
        )
    return found[0]


def _driving_edge(t: DatasetTable, own: list[Edge], by_id: dict[int, DatasetTable]) -> Edge | None:
    """The relationship ``t``'s rows are generated by: its first parent with a profiled fan-out."""
    for e in sorted(own, key=lambda e: (by_id[e.parent_id].name, e.child_column)):
        if any(f.child_table == t.pgwire_name for f in by_id[e.parent_id].profile.fanouts):
            return e
    return None


def _hot_counts(
    child: DatasetTable, column: str, fan: ProfiledFanout, parent_scale: float
) -> tuple[int, ...]:
    """The child counts of the relationship's hot parents, most first: the most frequent keys of
    the child's referring column whose count is beyond the fan-out sketch's 99th percentile --
    what the sketch draws only approximately -- each scaled with the parents."""
    prof = child.profile
    p = prof.columns.get(column)
    if p is None:
        return ()
    # The child's counts are of the rows its run read; a sample is scaled up to the whole table.
    up = prof.row_count / prof.profiled_rows if prof.profiled_rows else 0.0
    beyond = fan.sketch[99]
    return tuple(
        round(n * up * parent_scale) for v, n in p.top if v is not None and n * up > beyond
    )


def _parents_first(tables: list[DatasetTable], edges: list[Edge]) -> list[DatasetTable]:
    ids = {t.table_id for t in tables}
    parents = {
        t.table_id: {
            e.parent_id
            for e in edges
            if e.child_id == t.table_id and e.parent_id in ids and e.parent_id != t.table_id
        }
        for t in tables
    }
    done: list[DatasetTable] = []
    placed: set[int] = set()
    remaining = sorted(tables, key=lambda t: t.name)
    while remaining:
        ready = [t for t in remaining if parents[t.table_id] <= placed]
        if not ready:
            raise DatasetRefused(
                "the dataset's relationships form a cycle: " + ", ".join(t.name for t in remaining)
            )
        for t in ready:
            done.append(t)
            placed.add(t.table_id)
        remaining = [t for t in remaining if t.table_id not in placed]
    return done
