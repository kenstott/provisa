# Copyright (c) 2026 Kenneth Stott
# Canary: f6707ac9-87c5-49ad-9fd8-67eee5e40111
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Adequacy checks on a sensitive column's fake and synthetic rule (REQ-2091).

A fake or a rule written by hand can fall short of what it is meant to do. Provisa runs three
checks on every sensitive column and keeps each result with its run time; the sensitive_coverage
meta view reads the latest, and counts a column with a failed check as inadequately covered.

- ``leak_rate``: over a sample of the column's real values, the share whose fake equals the real
  value, and, for a fake that keeps part of the value (truncate, prefix, bucket), the share sharing
  that part. Failed above ``fakes.leak_rate_threshold``.
- ``kind_mismatch``: the fake's kind against what the column's latest profile run says it plausibly
  holds -- a fake of an identifying kind (an email address, a city, a phone number) other than
  the one the profile proposes. Failed on a mismatch.
- ``copies_real``: a fake or synthetic rule that names a sensitive column with no fake, which it
  then reads real. Failed when it does.

Only Provisa reads the real values, as the profiler does; a result is a number and a flag, never a
value.
"""

# Requirements: REQ-2091

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from provisa.fakes.kinds import (
    Bucket,
    FakeKind,
    Method,
    Prefix,
    Sql,
    Truncate,
    kind_name,
    parse,
    references,
)

LEAK_RATE = "leak_rate"
KIND_MISMATCH = "kind_mismatch"
COPIES_REAL = "copies_real"
CHECKS = (LEAK_RATE, KIND_MISMATCH, COPIES_REAL)

PASSED = "passed"
FAILED = "failed"
NOT_RUN = "not_run"

#: The fakes computed from the real value itself, keeping some of it or encoding it.
DERIVES_FROM_REAL = (
    "truncate",
    "prefix",
    "bucket",
    "hash",
    "encrypt",
    "after",
    "before",
    "greater_than",
    "less_than",
    "sql",
    "sql_group",
)

#: The fakes that keep part of the real value, whose leak rate also counts that part.
KEEPS_PART = (Truncate, Prefix, Bucket)

#: Rows a leak-rate run reads.
SAMPLE_ROWS = 1000


@dataclass(frozen=True)
class Result:
    check: str
    status: str
    measure: float | None
    detail: str


def _rule_references(kind: FakeKind) -> frozenset[str]:
    """The columns a fake or rule names: a relative fake's, an ``sql`` expression's."""
    from provisa.fakes.sql_subset import read as read_sql

    if isinstance(kind, Sql):
        return read_sql(kind.expression, group=kind.group).columns
    return references(kind)


def copies_real(fake: str | None, rule: str | None, unfaked_sensitive: frozenset[str]) -> Result:
    """``copies_real``: whether a column's fake or synthetic rule names a sensitive column
    of the table that has no fake (``unfaked_sensitive``), and so reads its real value."""
    named: dict[str, frozenset[str]] = {}
    if fake:
        named["fake"] = _rule_references(parse(fake))
    if rule:
        named["synthetic rule"] = _rule_references(parse(rule, rule=True))
    copied = sorted(
        f"its {what} reads {other}"
        for what, refs in named.items()
        for other in refs & unfaked_sensitive
    )
    if copied:
        return Result(
            COPIES_REAL,
            FAILED,
            float(len(copied)),
            "; ".join(copied) + ", a sensitive column with no fake, so its real value",
        )
    return Result(COPIES_REAL, PASSED, 0.0, "names no sensitive column read real")


def kind_mismatch(fake: str | None, plausible_type: str | None, confidence: float | None) -> Result:
    """``kind_mismatch``: a fake of an identifying kind other than the one the column's latest
    profile run proposes for what the column plausibly holds."""
    from provisa.fakes.propose import _FAKES_BY_TYPE, CONFIDENT

    if not fake:
        return Result(KIND_MISMATCH, NOT_RUN, None, "no fake declared")
    if plausible_type is None:
        return Result(KIND_MISMATCH, NOT_RUN, None, "no profile run holds the column")
    expected = _FAKES_BY_TYPE.get(plausible_type)
    if expected is None or (confidence or 0.0) < CONFIDENT:
        return Result(
            KIND_MISMATCH,
            PASSED,
            None,
            f"the profile reads the column as {plausible_type}, which no one fake kind fits",
        )
    kind = parse(fake)
    declared = kind_name(kind)
    identifying = {v.removesuffix("()") for v in _FAKES_BY_TYPE.values()}
    want = expected.removesuffix("()")
    if isinstance(kind, Method) and declared in identifying and declared != want:
        return Result(
            KIND_MISMATCH,
            FAILED,
            None,
            f"the column plausibly holds {plausible_type} values, and its fake makes {declared}",
        )
    return Result(KIND_MISMATCH, PASSED, None, f"{declared} fits {plausible_type}")


def _kept(kind: FakeKind, real: str, fake: str) -> bool:
    """Whether ``fake`` keeps the part of ``real`` a part-keeping fake is meant to keep."""
    if isinstance(kind, Prefix):
        return real[: kind.length] == fake[: kind.length]
    # truncate and bucket keep the value's leading part (a date's year and month, a bucket's
    # lower edge); the shared leading characters say how much survives.
    shared = 0
    for a, b in zip(real, fake):
        if a != b:
            break
        shared += 1
    return shared >= max(1, min(len(real), len(fake)) // 2)


def leak_rate(kind: FakeKind, pairs: list[tuple[Any, Any]], threshold: float) -> Result:
    """``leak_rate`` over ``pairs`` of (real, fake) values: the share whose fake equals the real
    value; for a part-keeping fake, also the share keeping that part. Failed above
    ``threshold``. Only shares are returned."""
    shown = [(str(r), str(f)) for r, f in pairs if r is not None and f is not None]
    if not shown:
        return Result(LEAK_RATE, NOT_RUN, None, "the sample holds no value")
    equal = sum(1 for r, f in shown if r == f) / len(shown)
    status = FAILED if equal > threshold else PASSED
    detail = f"{equal:.4f} of {len(shown)} sampled values faked as themselves"
    if status == FAILED:
        detail += f", above the threshold {threshold}"
    if isinstance(kind, KEEPS_PART):
        part = sum(1 for r, f in shown if _kept(kind, r, f)) / len(shown)
        detail += f"; {part:.4f} keep the part {kind_name(kind)} keeps"
    return Result(LEAK_RATE, status, equal, detail)


# --- computing a sample's fakes --------------------------------------------------------------------


def fake_pairs(
    column: str,
    data_type: str,
    kind: FakeKind,
    stable: bool,
    stable_version: int | None,
    values: list[Any],
) -> list[tuple[Any, Any]]:
    """(real, fake) for each of ``values``: the column's faked projection, the same statement a
    faked read runs, over the sample in this process's embedded engine, the real value carried
    beside its fake. Under the process's platform key; nothing leaves the process."""
    from provisa.core import engine_memory
    import pandas as pd
    import sqlglot

    from provisa.fakes import duckdb_functions
    from provisa.fakes.checks import family
    from provisa.fakes.digest import fingerprint, platform_key
    from provisa.fakes.projection import faked_projection
    from provisa.fakes.read_sql import Column, _q

    fam = family(data_type)
    faked = Column(column, data_type, fam, kind, stable, None, stable_version)
    projection = faked_projection(
        "sample",
        "t",
        [(column, data_type, fam)],
        {column: faked},
        fingerprint(platform_key()),
        None,
        carried={"__real__": f"t.{_q(column)}"},
    )
    statement = sqlglot.transpile(
        f"SELECT {_q('__real__')}, {_q(column)} FROM {projection} AS t",
        read="postgres",
        write="duckdb",
    )[0]
    con = engine_memory.connect()
    try:
        duckdb_functions.register(con)
        con.register("sample", pd.DataFrame({column: values}))
        return [(r[0], r[1]) for r in con.execute(statement).fetchall()]
    finally:
        con.close()


# --- running the checks ---------------------------------------------------------------------------

#: How often the scheduler runs an org's checks.
CHECK_HOURS = 24
JOB_ID = "fake_adequacy"


def _threshold() -> float:
    from provisa.core import settings_registry

    return settings_registry.value("fakes.leak_rate_threshold")


async def _sample(state: Any, reg: dict, column: str) -> list[Any] | None:
    """Up to SAMPLE_ROWS of ``column``'s real values, read as the profiler reads a table (the org
    admin through the governed pipeline, fakes not substituted); None when the org admin cannot
    read the column."""
    from provisa.fakes.measurement import MEASURING
    from provisa.profiler.run import _governed, resolve_target
    from provisa.profiler.statement import _ident, qualified

    target = resolve_target(state, reg["id"], reg["table_name"], {})
    exposed = {c.physical: c.name for c in target.columns}
    name = exposed.get(column)
    if name is None:
        return None
    token = MEASURING.set(True)
    try:
        _names, rows = await _governed(
            f"SELECT x.{_ident(name)} AS v FROM {qualified(target.pgwire_name)} x "
            f"WHERE x.{_ident(name)} IS NOT NULL LIMIT {SAMPLE_ROWS}"
        )
    finally:
        MEASURING.reset(token)
    return [r[0] for r in rows]


async def _leak(state: Any, reg: dict, col: dict, threshold: float) -> Result:
    from provisa.fakes.kinds import INDEPENDENT
    from provisa.fakes.measured import needs

    if not col["fake"]:
        return Result(LEAK_RATE, NOT_RUN, None, "no fake declared")
    kind = parse(col["fake"])
    if not isinstance(kind, INDEPENDENT) or needs(kind):
        return Result(
            LEAK_RATE,
            NOT_RUN,
            None,
            f"{kind_name(kind)}() is computed from measurement or other columns, not sampled",
        )
    values = await _sample(state, reg, col["column_name"])
    if values is None:
        return Result(LEAK_RATE, NOT_RUN, None, "the org admin cannot read the column")
    pairs = fake_pairs(
        col["column_name"],
        col["data_type"],
        kind,
        bool(col["fake_stable"]),
        col["fake_stable_version"],
        values,
    )
    return leak_rate(kind, pairs, threshold)


async def _plausible(
    conn: Any, org_id: str, env: str | None, reg: dict
) -> dict[str, tuple[str, float]]:
    """Each column's plausible type and confidence from the table's latest profile run; empty
    when the table has none."""
    from provisa.fakes.propose import latest_facts
    from provisa.synthetic.plan import DatasetRefused

    try:
        found = await latest_facts(conn, org_id=org_id, env=env, reg=reg)
    except DatasetRefused:  # not a Data Profiler's member: no run holds it
        return {}
    if found is None:
        return {}
    return {f.name: (f.plausible_type, f.confidence) for f in found[1]}


async def sensitive_columns_declared(conn: Any) -> list[dict]:
    """Every sensitive column of the org with its table and what it declares."""
    from sqlalchemy import select

    from provisa.core.schema_org import (
        glossary_term_refs,
        glossary_terms,
        registered_tables,
        table_columns,
        tag_assignments,
        tags,
    )
    from provisa.security.sensitive import sensitive_assignments

    sens = sensitive_assignments(
        tag_assignments, registered_tables, tags, glossary_term_refs, glossary_terms
    ).subquery()
    tc = table_columns
    rows = await conn.execute_core(
        select(
            sens.c.table_id,
            sens.c.column_name,
            tc.c.data_type,
            tc.c.fake,
            tc.c.fake_stable,
            tc.c.fake_stable_version,
            tc.c.synthetic_rule,
        )
        .select_from(
            sens.join(
                tc, (tc.c.table_id == sens.c.table_id) & (tc.c.column_name == sens.c.column_name)
            )
        )
        .distinct()
    )
    return [dict(r._mapping) for r in rows.fetchall()]


async def run_org(state: Any, org_id: str, env: str | None = None) -> dict[str, int]:
    """:func:`run_checks`, answered as how many checks passed, failed and did not run."""
    return counts(await run_checks(state, org_id, env))


def counts(rows: list[dict]) -> dict[str, int]:
    """How many of the check ``rows`` passed, failed and did not run."""
    tally = {PASSED: 0, FAILED: 0, NOT_RUN: 0}
    for r in rows:
        tally[r["status"]] += 1
    return tally


async def run_checks(state: Any, org_id: str, env: str | None = None) -> list[dict]:
    """Run every check on every sensitive column of ``org_id`` in ``env`` (both bound by the
    caller; None: prod) and store the results under one run time. Returns each check's row
    (``table_id``, ``column_name``, ``check_name``, ``status``...)."""
    import datetime as dt

    from sqlalchemy import insert, select

    from provisa.core.schema_org import fake_adequacy_checks, registered_tables

    db = state.model_db
    assert db is not None, "the org's model store is bound with its runtime"
    threshold = _threshold()
    checked_at = dt.datetime.now(dt.timezone.utc)
    async with db.acquire() as conn:
        columns = await sensitive_columns_declared(conn)
        table_ids = sorted({c["table_id"] for c in columns})
        regs = {
            r["id"]: r
            for r in (
                dict(x._mapping)
                for x in (
                    await conn.execute_core(
                        select(registered_tables).where(registered_tables.c.id.in_(table_ids))
                    )
                ).fetchall()
            )
        }
        plausible = {tid: await _plausible(conn, org_id, env, regs[tid]) for tid in table_ids}
    unfaked: dict[int, frozenset[str]] = {
        tid: frozenset(c["column_name"] for c in columns if c["table_id"] == tid and not c["fake"])
        for tid in table_ids
    }
    rows: list[dict] = []
    for col in columns:
        tid, name = col["table_id"], col["column_name"]
        pt = plausible[tid].get(name)
        results = [
            await _leak(state, regs[tid], col, threshold),
            kind_mismatch(col["fake"], pt[0] if pt else None, pt[1] if pt else None),
            copies_real(col["fake"], col["synthetic_rule"], unfaked[tid] - {name}),
        ]
        rows += [
            {
                "table_id": tid,
                "column_name": name,
                "check_name": r.check,
                "checked_at": checked_at,
                "status": r.status,
                "measure": r.measure,
                "detail": r.detail,
            }
            for r in results
        ]
    async with db.acquire() as conn:
        if rows:
            await conn.execute_core(insert(fake_adequacy_checks), rows)
        await _record_findings(conn, rows, checked_at)
    return rows


async def _record_findings(conn: Any, rows: list[dict], at: Any) -> None:
    """REQ-2094: each failed check as a ``wrong`` finding on the column's fake in the review
    queue; a check that passes again clears its finding."""
    from provisa.judgments import findings as fq

    seen = [
        await fq.record_finding(
            conn,
            object_type="column",
            object_key=f"{r['table_id']}:{r['column_name']}",
            judgment=fq.FAKE,
            category=fq.WRONG,
            finding=r["check_name"],
            observed={"measure": r["measure"], "detail": r["detail"]},
            at=at,
        )
        for r in rows
        if r["status"] == FAILED
    ]
    await fq.clear_unseen(conn, seen, judgments=(fq.FAKE,), findings=CHECKS, at=at)


async def scheduled_run(org_id: str) -> dict[str, int]:
    """``org_id``'s checks, run by the scheduler."""
    from provisa.api.app import state
    from provisa.core.request_context import reset_current_org, set_current_org

    token = set_current_org(org_id)
    try:
        return await run_org(state, org_id)
    finally:
        reset_current_org(token)


def job_id(org_id: str) -> str:
    return f"{JOB_ID}:org_{org_id}"


def register_org(scheduler: Any, org_id: str) -> None:
    """Run ``org_id``'s checks every CHECK_HOURS. A job already held is kept."""
    from apscheduler.triggers.interval import IntervalTrigger

    if scheduler.get_job(job_id(org_id)) is not None:
        return
    scheduler.add_job(
        scheduled_run,
        trigger=IntervalTrigger(hours=CHECK_HOURS),
        args=[org_id],
        id=job_id(org_id),
        name=JOB_ID,
        replace_existing=True,
        max_instances=1,
        coalesce=True,
    )
