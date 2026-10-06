# Copyright (c) 2026 Kenneth Stott
# Canary: b41e7d93-2c65-4a08-9f1d-6e3a8c5b0d27
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Constraints a profile proposes, and the ones the operator accepted (REQ-1934, PROPOSED
CONSTRAINTS; maintainer ruling: profiler checks plus optional export).

A run PROPOSES what its evidence supports -- a column never null, unique, holding only its recorded
values, a number or date within its observed range, one never above or after another -- each with
its evidence and the share of rows it held for, and the sample it rests on (``constraints`` result
rows). Nothing is applied by proposing it. The operator accepts (as proposed, or edited) or
dismisses each one in the table editor; the decision is stored in ``profiler_constraints``. EVERY
later run checks the table's accepted constraints in its own governed statement and records each
one's pass share and violations as measures of the run (``constraint_checks``), compared and
watched for drift like the rest. :func:`accepted_constraints` is what synthetic generation reads
(REQ-1939). Exporting one to a Soda or Great Expectations checker is ``provisa.profiler.export``.
"""

# Requirements: REQ-1934, REQ-1939

from __future__ import annotations

import json
import uuid
from dataclasses import dataclass
from typing import Any

from sqlalchemy import delete, insert, select, update

from provisa.profiler.statement import CheckSpec, ColumnAggregates, ProfileAggregates

CONSTRAINT_KINDS: tuple[str, ...] = ("not_null", "unique", "value_set", "range", "ordering")
# A constraint is proposed where it held for at least this share of the rows it speaks of: a rare
# breach may be a defect the constraint should catch, not evidence against it.
PROPOSAL_MIN_SHARE = 0.99
# The kinds whose definition holds values of the column (governed as values).
VALUE_KINDS = frozenset({"value_set", "range"})


@dataclass(frozen=True)
class Constraint:
    """``column`` (and ``other`` for an ordering: ``column`` never above or after ``other``) as the
    org admin reads the table; ``definition`` the kind's parameters -- ``values`` for a value set,
    ``min``/``max`` (epoch seconds for a temporal column) and ``min_text``/``max_text`` for a range,
    ``family`` for both of those and an ordering."""

    kind: str
    column: str
    other: str | None
    definition: dict

    def __post_init__(self) -> None:
        if self.kind not in CONSTRAINT_KINDS:
            raise ValueError(f"unknown constraint kind {self.kind!r}; expected {CONSTRAINT_KINDS}")
        if (self.kind == "ordering") != (self.other is not None):
            raise ValueError("an ordering constraint, and only one, names a second column")

    @property
    def signature(self) -> str:
        return f"{self.kind}|{self.column}|{self.other or ''}"

    @property
    def involved(self) -> list[str]:
        return [self.column] + ([self.other] if self.other is not None else [])

    def check(self) -> CheckSpec:
        d = self.definition
        if self.kind == "value_set":
            return CheckSpec("value_set", self.column, values=tuple(str(v) for v in d["values"]))
        if self.kind == "range":
            return CheckSpec("range", self.column, low=float(d["min"]), high=float(d["max"]))
        return CheckSpec(self.kind, self.column, self.other)


@dataclass(frozen=True)
class Proposal:
    constraint: Constraint
    evidence: str
    share: float


def _proposal(kind: str, col: ColumnAggregates, definition: dict, evidence: str, share: float):
    return Proposal(Constraint(kind, col.spec.name, None, definition), evidence, share)


def propose_for_column(
    col: ColumnAggregates, rows: int, low_cardinality_max: int
) -> list[Proposal]:
    """What one column's measures support."""
    out: list[Proposal] = []
    if rows == 0:
        return out
    nulls = rows - col.non_null
    if col.non_null / rows >= PROPOSAL_MIN_SHARE:
        out.append(
            _proposal(
                "not_null",
                col,
                {},
                f"{nulls} of {rows} rows profiled are null",
                col.non_null / rows,
            )
        )
    if col.non_null == 0:
        return out
    held = 1 - col.repeated_rows / col.non_null
    if col.spec.family != "boolean" and col.distinct >= 2 and held >= PROPOSAL_MIN_SHARE:
        out.append(
            _proposal(
                "unique",
                col,
                {},
                f"{col.repeated_rows} of {col.non_null} non-null rows repeat a value",
                held,
            )
        )
    full = col.distinct + (1 if nulls else 0) <= low_cardinality_max
    if full and col.spec.family in ("text", "boolean"):
        values = sorted(v for v, _ in col.values if v is not None)
        out.append(
            _proposal(
                "value_set",
                col,
                {"values": values, "family": col.spec.family},
                f"every one of {col.non_null} non-null rows holds one of {len(values)} values",
                1.0,
            )
        )
    if col.spec.family in ("numeric", "temporal") and col.vmin is not None:
        out.append(
            _proposal(
                "range",
                col,
                {
                    "min": col.vmin,
                    "max": col.vmax,
                    "min_text": col.min_text,
                    "max_text": col.max_text,
                    "family": col.spec.family,
                },
                f"every one of {col.non_null} non-null rows lies between {col.min_text} and "
                f"{col.max_text}",
                1.0,
            )
        )
    return out


def propose_orderings(pairs: dict, cols: list) -> list[Proposal]:
    """One column never above or after another, from the dependence pairs statement's row-by-row
    comparison of two own numbers of one family (``dependence.PairStats``)."""
    out = []
    for (a, b), s in pairs.items():
        if s.compared == 0 or not (cols[a].own and cols[b].own):
            continue
        if s.a_le_b / s.compared >= PROPOSAL_MIN_SHARE:
            first, second, held = cols[a], cols[b], s.a_le_b
        elif s.a_ge_b / s.compared >= PROPOSAL_MIN_SHARE:
            first, second, held = cols[b], cols[a], s.a_ge_b
        else:
            continue
        out.append(
            Proposal(
                Constraint("ordering", first.name, second.name, {"family": first.family}),
                f"{first.name} <= {second.name} in {held} of {s.compared} rows holding both",
                held / s.compared,
            )
        )
    return out


def proposal_rows(
    proposals: list[Proposal], decisions: dict[str, str], sampled: bool
) -> list[dict]:
    """``constraints`` result rows (without the run key); ``decisions``: signature -> status."""
    return [
        {
            "constraint": p.constraint.kind,
            "column_name": p.constraint.column,
            "other_column": p.constraint.other,
            "involved_columns": json.dumps(p.constraint.involved),
            "value_bearing": p.constraint.kind in VALUE_KINDS,
            "definition": json.dumps(p.constraint.definition),
            "evidence": p.evidence,
            "share": p.share,
            "sampled": sampled,
            "status": decisions.get(p.constraint.signature, "proposed"),
        }
        for p in proposals
    ]


def check_rows(
    accepted: list["AcceptedConstraint"],
    readable: list["AcceptedConstraint"],
    agg: ProfileAggregates,
) -> list[dict]:
    """``constraint_checks`` rows (without the run key): each accepted constraint's violations
    among the rows profiled, ``agg.violations`` holding those of ``readable`` in order. A
    constraint on a column the org admin can no longer read is recorded unchecked, by name."""
    found = dict(zip((c.id for c in readable), agg.violations))
    rows = agg.profiled_rows
    out = []
    for c in accepted:
        broken = found.get(c.id)
        out.append(
            {
                "constraint_id": c.id,
                "constraint": c.constraint.kind,
                "column_name": c.constraint.column,
                "other_column": c.constraint.other,
                "involved_columns": json.dumps(c.constraint.involved),
                "value_bearing": c.constraint.kind in VALUE_KINDS,
                "definition": json.dumps(c.constraint.definition),
                "rows_checked": rows if broken is not None else None,
                "violations": broken,
                "pass_share": None if broken is None or rows == 0 else 1 - broken / rows,
                "detail": None
                if broken is not None
                else "a column it names is not one the org admin can read",
            }
        )
    return out


# -- the decisions (profiler_constraints) --------------------------------------------------------


@dataclass(frozen=True)
class AcceptedConstraint:
    """An accepted constraint as synthetic generation reads it (REQ-1939)."""

    id: str
    constraint: Constraint
    evidence: str
    share: float | None
    sampled: bool


def _row_constraint(r: Any) -> Constraint:
    return Constraint(r.kind, r.column_name, r.other_column, json.loads(r.definition))


async def accepted_constraints(conn: Any, table_id: int) -> list[AcceptedConstraint]:
    """The table's accepted constraints, as the operator accepted or edited them (REQ-1934,
    REQ-1939): what every profile run checks and what binds synthetic generation."""
    from provisa.core.schema_org import profiler_constraints as pc

    result = await conn.execute_core(
        select(pc)
        .where(pc.c.table_id == table_id, pc.c.status == "accepted")
        .order_by(pc.c.signature)
    )
    return [
        AcceptedConstraint(r.id, _row_constraint(r), r.evidence, r.share, r.sampled)
        for r in result.fetchall()
    ]


async def decisions(conn: Any, table_id: int) -> list[dict]:
    """Every decision on the table's constraints, accepted and dismissed."""
    from provisa.core.schema_org import profiler_constraints as pc

    result = await conn.execute_core(
        select(pc).where(pc.c.table_id == table_id).order_by(pc.c.signature)
    )
    return [dict(r._mapping) for r in result.fetchall()]


DECISION_STATUSES = ("accepted", "dismissed")


async def decide(
    conn: Any,
    table_id: int,
    constraint: Constraint,
    *,
    status: str,
    evidence: str,
    share: float | None,
    sampled: bool,
    run_id: str,
) -> str:
    """Record the operator's decision on ``constraint`` -- accepted as given (an edit is an accept
    with the edited definition) or dismissed -- replacing any earlier one. Returns its id."""
    from datetime import UTC, datetime

    from provisa.core.schema_org import profiler_constraints as pc

    if status not in DECISION_STATUSES:
        raise ValueError(f"a decision is one of {DECISION_STATUSES}, got {status!r}")
    values = {
        "kind": constraint.kind,
        "column_name": constraint.column,
        "other_column": constraint.other,
        "definition": json.dumps(constraint.definition),
        "evidence": evidence,
        "share": share,
        "sampled": sampled,
        "status": status,
        "run_id": run_id,
        "decided_at": datetime.now(UTC),
    }
    found = (
        await conn.execute_core(
            select(pc.c.id).where(pc.c.table_id == table_id, pc.c.signature == constraint.signature)
        )
    ).fetchone()
    if found is not None:
        await conn.execute_core(update(pc).where(pc.c.id == found[0]).values(**values))
        return found[0]
    cid = str(uuid.uuid4())
    await conn.execute_core(
        insert(pc).values(id=cid, table_id=table_id, signature=constraint.signature, **values)
    )
    return cid


async def forget(conn: Any, table_id: int, constraint_id: str) -> bool:
    """Withdraw a decision: the constraint is proposed as new again. False when there is none."""
    from provisa.core.schema_org import profiler_constraints as pc

    result = await conn.execute_core(
        delete(pc).where(pc.c.table_id == table_id, pc.c.id == constraint_id)
    )
    return bool(result.rowcount)
