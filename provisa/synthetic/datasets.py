# Copyright (c) 2026 Kenneth Stott
# Canary: 1c6e9a34-7b52-4d08-8f3a-b5d2e0c71f96
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""An environment's synthetic datasets as the control plane holds them (REQ-1939, REQ-1487).

A dataset names (table, profile run) pairs, a scale and a seed. It belongs to the environment whose
schema holds it; prod holds none. Once generated, each of its tables is read, in this environment
only, from its copy in the dataset's store schema in place of the table's binding.
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Any

from sqlalchemy import delete, insert, select, update

from provisa.core.schema_org import synthetic_dataset_tables as sdt
from provisa.core.schema_org import synthetic_datasets as sd

DATASET_NAME = re.compile(r"[a-z][a-z0-9_]{1,31}")


class SyntheticJoinRefused(ValueError):
    """A statement reads a synthetic table beside a table reading real data (REQ-1487)."""


@dataclass(frozen=True)
class DatasetTableRow:
    table_id: int
    profile_env: str
    run_id: str
    scale: float | None


@dataclass(frozen=True)
class FanoutCondition:
    """A relationship's conditional child count (REQ-1939, CONDITIONAL FAN-OUT): ``condition``
    over the parent row, in the sql subset of REQ-1494, and the children a parent meeting it is
    generated with -- ``{"fixed": n}``, ``{"low": a, "high": b}`` drawn evenly, or
    ``{"measured": true}``, the fan-out of the real parents meeting it."""

    relationship: str
    condition: str
    count: dict


def check_condition(c: FanoutCondition) -> None:
    from provisa.fakes.sql_subset import read

    try:
        read(c.condition, group=False)
    except ValueError as exc:
        raise ValueError(f"relationship {c.relationship!r}'s condition: {exc}") from exc
    count = c.count
    if set(count) == {"fixed"}:
        n = count["fixed"]
        if not isinstance(n, int) or isinstance(n, bool) or n < 0:
            raise ValueError(f"condition {c.condition!r}: a fixed count is a whole number from 0")
    elif set(count) == {"low", "high"}:
        lo, hi = count["low"], count["high"]
        if not all(isinstance(x, int) and not isinstance(x, bool) for x in (lo, hi)) or not (
            0 <= lo <= hi
        ):
            raise ValueError(
                f"condition {c.condition!r}: a range is two whole numbers, low from 0 to high"
            )
    elif count != {"measured": True}:
        raise ValueError(
            f"condition {c.condition!r}: the count is fixed, a low-to-high range, or measured"
        )


@dataclass(frozen=True)
class DatasetRow:
    id: str
    seed: int
    scale: float
    status: str
    store_schema: str
    error: str | None
    generated_at: Any
    tables: tuple[DatasetTableRow, ...]
    fanout_conditions: tuple[FanoutCondition, ...] = ()
    assertions: tuple[str, ...] = ()
    private_epsilon: float | None = None  # REQ-1939, DIFFERENTIAL PRIVACY
    # REQ-1939, NOT TOO CLOSE TO A REAL ROW (maintainer ruling Z2): None when not checked
    closeness_threshold: float | None = None
    closeness_draws: int | None = None


def check_name(name: str) -> None:
    if not DATASET_NAME.fullmatch(name) or "__" in name:
        raise ValueError(
            f"synthetic dataset name {name!r}: lower-case letters, digits and single underscores, "
            f"starting with a letter, 2 to 32 characters"
        )


def check_scale(scale: float, what: str) -> None:
    if not scale > 0:
        raise ValueError(f"{what}: the scale must be above 0, got {scale!r}")


async def generated_tables(conn: Any) -> dict[int, tuple[str, str]]:
    """``{table_id: (dataset, store schema)}`` for every table read from a synthetic store in the
    environment ``conn`` is scoped to: each table of a generated dataset of its own (REQ-1939),
    and each generated table of a source bound to a synthetic store (REQ-1942) -- by generating
    its whole model here, or copied from the environment it was created from, whose generated
    data it then reads."""
    from provisa.core.env_classes import BINDING_COLUMN, SYNTHETIC
    from provisa.core.schema_org import registered_tables as rt
    from provisa.core.schema_org import sources
    from provisa.synthetic.env_model import DATASET_ID

    out: dict[int, tuple[str, str]] = {}
    bound = await conn.execute_core(
        select(sources.c.id, sources.c.synthetic).where(sources.c[BINDING_COLUMN] == SYNTHETIC)
    )
    held = {sid: where for sid, where in bound.fetchall()}
    if held:
        tables = await conn.execute_core(
            select(rt.c.id, rt.c.source_id, rt.c.schema_name, rt.c.table_name).where(
                rt.c.source_id.in_(sorted(held))
            )
        )
        for tid, sid, schema_name, table_name in tables.fetchall():
            if [schema_name, table_name] in held[sid]["tables"]:
                out[tid] = (DATASET_ID, held[sid]["schema"])
    result = await conn.execute_core(
        select(sdt.c.table_id, sd.c.id, sd.c.store_schema)
        .select_from(sdt.join(sd, sd.c.id == sdt.c.dataset_id))
        .where(sd.c.status == "generated", sd.c.id != DATASET_ID)
    )
    out.update({tid: (ds, schema) for tid, ds, schema in result.fetchall()})
    return out


async def synthetic_sources(conn: Any) -> dict[str, dict]:
    """``{source id: its synthetic binding}`` for each source bound to a synthetic store in the
    environment ``conn`` is scoped to (REQ-1942): {schema, tables, model_type}."""
    from provisa.core.env_classes import BINDING_COLUMN, SYNTHETIC
    from provisa.core.schema_org import sources

    bound = await conn.execute_core(
        select(sources.c.id, sources.c.synthetic).where(sources.c[BINDING_COLUMN] == SYNTHETIC)
    )
    return {sid: where for sid, where in bound.fetchall()}


def as_generated(tables: list[dict], bound: dict[str, dict]) -> list[dict]:
    """``tables`` (fetch_tables rows) as an environment reads them where ``bound`` -- source id ->
    its synthetic binding -- binds their source to a synthetic store (REQ-1942). A generated
    table is an ordinary table there: a required parameter generated with it is a column of it,
    a filter like any other, of the type it was generated as; any other argument of the API it
    was read from is no column of it. The model's own rows are untouched: this is how the
    environment reads them, never what it stores."""
    out = []
    for t in tables:
        held = bound.get(t["source_id"])
        if held is None or [t["schema_name"], t["table_name"]] not in held["tables"]:
            out.append(t)
            continue
        typed = held["parameters"].get(f"{t['schema_name']}.{t['table_name']}", {})
        columns = []
        for c in t["columns"]:
            if c["native_filter_type"] is None:
                columns.append(c)
            elif c["column_name"] in typed:
                columns.append(
                    {**c, "native_filter_type": None, "data_type": typed[c["column_name"]]}
                )
        out.append({**t, "columns": columns})
    return out


class TableNotAvailable(ValueError):
    """A read of a table a Test (synthetic) environment holds no generated copy of (REQ-1942)."""


def refuse_unavailable(routes: Any, table_ids: list[int]) -> None:
    """Refuse a statement reading a table that is not available in a Test (synthetic)
    environment, saying why and that declaring a profile generates it (REQ-1942)."""
    for table_id in table_ids:
        reason = routes.unavailable.get(table_id)
        if reason is not None:
            raise TableNotAvailable(reason)


def refuse_mixed_data(routes: Any, table_ids: list[int]) -> None:
    """Refuse a statement reading a synthetic table beside one reading real data, naming both:
    their keys share nothing, so the result would look plausible and mean nothing (REQ-1487).
    The admin's own tables (the meta domain) read neither and are not counted."""
    synthetic = routes.synthetic
    if not synthetic:
        return
    sources = {**routes.unfloored, **{tid: src for tid, (src, _) in routes.floored.items()}}
    fake = [t for t in table_ids if t in synthetic]
    real = [t for t in table_ids if t not in synthetic and sources.get(t) != "provisa-admin"]
    if fake and real:
        raise SyntheticJoinRefused(
            f"this statement reads table {fake[0]} from synthetic dataset {synthetic[fake[0]]!r} "
            f"and table {real[0]}, which reads real data; their keys share nothing, so the two "
            f"cannot be read together"
        )


async def list_datasets(conn: Any) -> list[DatasetRow]:
    rows = (await conn.execute_core(select(sd).order_by(sd.c.id))).fetchall()
    tables = (await conn.execute_core(select(sdt).order_by(sdt.c.table_id))).fetchall()
    by: dict[str, list[DatasetTableRow]] = {}
    for t in tables:
        m = t._mapping
        by.setdefault(m["dataset_id"], []).append(
            DatasetTableRow(m["table_id"], m["profile_env"], m["run_id"], m["scale"])
        )
    return [
        DatasetRow(
            id=r._mapping["id"],
            seed=r._mapping["seed"],
            scale=r._mapping["scale"],
            status=r._mapping["status"],
            store_schema=r._mapping["store_schema"],
            error=r._mapping["error"],
            generated_at=r._mapping["generated_at"],
            tables=tuple(by.get(r._mapping["id"], [])),
            fanout_conditions=tuple(
                FanoutCondition(c["relationship"], c["condition"], c["count"])
                for c in _json(r._mapping["fanout_conditions"])
            ),
            assertions=tuple(_json(r._mapping["assertions"])),
            private_epsilon=r._mapping["private_epsilon"],
            closeness_threshold=r._mapping["closeness_threshold"],
            closeness_draws=r._mapping["closeness_draws"],
        )
        for r in rows
    ]


def _json(value: Any) -> list:
    import json

    return json.loads(value) if isinstance(value, str) else list(value)


async def get_dataset(conn: Any, dataset_id: str) -> DatasetRow:
    for row in await list_datasets(conn):
        if row.id == dataset_id:
            return row
    raise LookupError(f"synthetic dataset {dataset_id!r} does not exist in this environment")


async def define(
    conn: Any,
    *,
    dataset_id: str,
    seed: int,
    scale: float,
    store_schema: str,
    tables: list[DatasetTableRow],
    fanout_conditions: list[FanoutCondition] | None = None,
    assertions: list[str] | None = None,
    private_epsilon: float | None = None,
    closeness_threshold: float | None = None,
    closeness_draws: int | None = None,
) -> None:
    """Define (or redefine) a dataset; it is generated by :func:`provisa.synthetic.run.generate`."""
    check_name(dataset_id)
    check_scale(scale, f"synthetic dataset {dataset_id!r}")
    if not tables:
        raise ValueError(f"synthetic dataset {dataset_id!r} names no table")
    for t in tables:
        if t.scale is not None:
            check_scale(t.scale, f"synthetic dataset {dataset_id!r} table {t.table_id}")
    for c in fanout_conditions or []:
        check_condition(c)
    if private_epsilon is not None and not private_epsilon > 0:
        raise ValueError(
            f"synthetic dataset {dataset_id!r}: a private dataset's ε must be above 0, got "
            f"{private_epsilon!r}"
        )
    if (closeness_threshold is None) != (closeness_draws is None):
        raise ValueError(
            f"synthetic dataset {dataset_id!r}: the closeness check takes both a threshold and a "
            f"number of draws, or neither"
        )
    if closeness_threshold is not None and not closeness_threshold > 0:
        raise ValueError(
            f"synthetic dataset {dataset_id!r}: the closeness threshold must be above 0, got "
            f"{closeness_threshold!r}"
        )
    if closeness_threshold is not None and private_epsilon is not None:
        raise ValueError(
            f"synthetic dataset {dataset_id!r}: a private dataset's rows are not compared with "
            f"real rows -- the comparison reads them outside its ε; declare ε or closeness, "
            f"not both"
        )
    if closeness_draws is not None and closeness_draws < 1:
        raise ValueError(
            f"synthetic dataset {dataset_id!r}: a row takes at least one draw, got {closeness_draws!r}"
        )
    for a in assertions or []:
        if not a.strip():
            raise ValueError(
                f"synthetic dataset {dataset_id!r}: an assertion is an empty statement"
            )
    taken = (
        await conn.execute_core(
            select(sdt.c.table_id, sdt.c.dataset_id).where(
                sdt.c.table_id.in_([t.table_id for t in tables]), sdt.c.dataset_id != dataset_id
            )
        )
    ).fetchall()
    if taken:
        tid, other = taken[0]
        raise ValueError(
            f"table {tid} already reads synthetic dataset {other!r}; a table reads one dataset"
        )
    async with conn.transaction():
        await conn.execute_core(delete(sdt).where(sdt.c.dataset_id == dataset_id))
        await conn.execute_core(delete(sd).where(sd.c.id == dataset_id))
        await conn.execute_core(
            insert(sd).values(
                id=dataset_id,
                seed=seed,
                scale=scale,
                status="defined",
                store_schema=store_schema,
                fanout_conditions=[
                    {"relationship": c.relationship, "condition": c.condition, "count": c.count}
                    for c in fanout_conditions or []
                ],
                assertions=list(assertions or []),
                private_epsilon=private_epsilon,
                closeness_threshold=closeness_threshold,
                closeness_draws=closeness_draws,
            )
        )
        for t in tables:
            await conn.execute_core(
                insert(sdt).values(
                    dataset_id=dataset_id,
                    table_id=t.table_id,
                    profile_env=t.profile_env,
                    run_id=t.run_id,
                    scale=t.scale,
                )
            )


async def set_status(conn: Any, dataset_id: str, status: str, **fields: Any) -> None:
    await conn.execute_core(update(sd).where(sd.c.id == dataset_id).values(status=status, **fields))


async def drop(conn: Any, dataset_id: str) -> None:
    await conn.execute_core(delete(sd).where(sd.c.id == dataset_id))
