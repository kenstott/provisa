# Copyright (c) 2026 Kenneth Stott
# Canary: 5e2a8c71-0d43-4f96-b8e1-c7a3f9d2b064
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A profile's governance, read off the profiled table's CURRENT column rules (REQ-1934).

A run is stored once, as the org admin read it. Two things derive from the profiled table's rules
when the profile is shown or exposed:

* :func:`safe_run` -- the View Profile Runs view of one run, made safe for its viewer on read. A
  column the viewer cannot see is omitted; a column masked to the viewer, or tagged pii, shows only
  its shape (null share, distinct ratio, data type, length range, plausible type); every other
  column shows in full.
* :func:`prefill` -- the grants, masks and row rules a result table is registered with by default:
  readable by the roles that can read the profiled table; per described column, its rows visible
  to the roles that see that column, and its values only to the roles that see it unmasked. Once
  saved they are the result table's own rules and do not follow later changes.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

import json

from sqlalchemy import select

from provisa.compiler.sql_literals import sql_literal
from provisa.profiler.schema import field_names

# The kinds whose every row holds values of the column it describes.
VALUE_KINDS = frozenset(
    {"quantiles", "histogram", "top_values", "shapes", "fits", "fit_quality", "joint_counts"}
)
# The fields of a columns row that hold values of the column it describes.
COLUMN_VALUE_FIELDS = (
    "min_value",
    "max_value",
    "mean",
    "stddev",
    "variance",
    "skewness",
    "kurtosis",
    "log_mean",
    "log_variance",
    "integer_only",
    "zero_share",
)
# What a restricted column shows of its columns row.
_SHAPE_FIELDS = frozenset(
    {
        "run_id",
        "run_time",
        "column_name",
        "physical_column",
        "data_type",
        "family",
        "null_share",
        "distinct_ratio",
        "length_min",
        "length_max",
    }
)
# The kinds keyed by a described column, and so filtered column by column.
COLUMN_KINDS = VALUE_KINDS | {"columns", "plausible_type"}
# The kinds whose rows list every described column they speak of in ``involved_columns`` (a JSON
# array), the ``column_name`` of a row among them; a row whose ``value_bearing`` is set holds values
# of those columns.
INVOLVED_KINDS = frozenset(
    {
        "duplicates",
        "drift",
        "correlations",
        "dependencies",
        "joint_counts",
        "constraints",
        "constraint_checks",
    }
)


@dataclass(frozen=True)
class ColumnRule:
    physical: str
    exposed: str  # the column as the profile names it (as the org admin reads it)
    visible_to: frozenset[str]
    unmasked_to: frozenset[str]
    masked: bool
    pii: bool
    # The many-to-one relationship a parent table's column is reached through (named
    # ``<relationship>.<column>`` in the dependence measures); None for the profiled table's own.
    relationship: str | None = None

    def seen_by(self, roles: frozenset[str]) -> bool:
        return bool(self.visible_to & roles)

    def restricted_for(self, roles: frozenset[str]) -> bool:
        """Masked to every one of ``roles``, or tagged pii."""
        return self.pii or (self.masked and not (self.unmasked_to & roles))


async def column_rules(conn: Any, state: Any, table_id: int) -> list[ColumnRule]:
    """The current column rules of the profiled table, for the columns its profile describes, and
    of each parent table its dependence measures reach (REQ-1934): a parent column is seen, and
    seen unmasked, as its own table's rules say."""
    from provisa.profiler.run import parents_of

    rules = await _table_rules(conn, state, table_id, None)
    for parent in parents_of(state, table_id):
        rules += await _table_rules(conn, state, parent.table_id, parent.relationship)
    return rules


async def _table_rules(
    conn: Any, state: Any, table_id: int, relationship: str | None
) -> list[ColumnRule]:
    from provisa.core.schema_org import table_columns as tc
    from provisa.profiler.run import PROFILE_ROLE, column_tags

    ctx = state.contexts.get(PROFILE_ROLE)
    if ctx is None:
        raise ValueError(f"no compiled schema for role {PROFILE_ROLE!r}")
    tags = await column_tags(conn, table_id)
    result = await conn.execute_core(
        select(tc.c.column_name, tc.c.visible_to, tc.c.unmasked_to, tc.c.mask_type).where(
            tc.c.table_id == table_id
        )
    )
    rules = []
    for name, visible_to, unmasked_to, mask_type in result.fetchall():
        exposed = ctx.physical_to_sql.get((table_id, name))
        if exposed is None:
            continue  # the org admin cannot read it, so no profile describes it
        rules.append(
            ColumnRule(
                physical=name,
                exposed=exposed if relationship is None else f"{relationship}.{exposed}",
                visible_to=frozenset(visible_to),
                unmasked_to=frozenset(unmasked_to),
                masked=mask_type is not None,
                pii="pii" in tags.get(name, set()),
                relationship=relationship,
            )
        )
    return rules


def _involved(kind: str, row: dict) -> list[str]:
    """Every described column ``row`` of ``kind`` speaks of: its ``column_name`` and the columns
    its ``involved_columns`` lists."""
    names = []
    if kind in COLUMN_KINDS or kind in INVOLVED_KINDS:
        if row.get("column_name") is not None:
            names.append(row["column_name"])
    if kind in INVOLVED_KINDS:
        names += json.loads(row["involved_columns"])
    return names


def _value_bearing(kind: str, row: dict) -> bool:
    return kind in VALUE_KINDS or bool(row.get("value_bearing"))


def safe_run(
    results: dict[str, list[dict]], rules: list[ColumnRule], roles: frozenset[str]
) -> dict[str, list[dict]]:
    """One run's results as ``roles`` may see them (the View Profile Runs view): a row speaking of
    a column the viewer cannot see is left out; a value-bearing row speaking of a column restricted
    to the viewer is left out; a restricted column's columns row shows its shape only."""
    by_name = {r.exposed: r for r in rules}
    out: dict[str, list[dict]] = {}
    for kind, rows in results.items():
        if kind not in COLUMN_KINDS and kind not in INVOLVED_KINDS:
            out[kind] = rows
            continue
        kept = []
        for row in rows:
            named = [by_name.get(c) for c in _involved(kind, row)]
            if any(rule is None or not rule.seen_by(roles) for rule in named):
                continue
            restricted = any(rule.restricted_for(roles) for rule in named if rule is not None)
            if restricted:
                if _value_bearing(kind, row):
                    continue
                if kind == "columns":
                    row = {k: (v if k in _SHAPE_FIELDS else None) for k, v in row.items()}
            kept.append(row)
        out[kind] = kept
    return out


def _in_list(columns: list[str]) -> str:
    if not columns:
        return "1 = 0"
    return "column_name IN (" + ", ".join(sql_literal(c, "postgres") for c in columns) + ")"


def _not_involved(column: str) -> str:
    """The row's ``involved_columns`` (a JSON array) does not list ``column``."""
    token = sql_literal(json.dumps(column), "postgres")
    return f"POSITION({token} IN involved_columns) = 0"


def _involved_filter(kind: str, rules: list[ColumnRule], role: str) -> str | None:
    """The row rule of a kind whose rows list their columns, for ``role``; None when it sees all."""
    one = frozenset({role})
    conditions = []
    for r in rules:
        if not r.seen_by(one):
            conditions.append(_not_involved(r.exposed))
        elif r.restricted_for(one):
            if kind in VALUE_KINDS:
                conditions.append(_not_involved(r.exposed))
            elif "value_bearing" in field_names(kind):
                conditions.append(f"(value_bearing = FALSE OR {_not_involved(r.exposed)})")
    return " AND ".join(conditions) if conditions else None


def prefill(kind: str, fields: list[str], rules: list[ColumnRule]) -> dict:
    """The default rules a result table of ``kind`` is registered with.

    ``{"columns": [{name, visibleTo, unmaskedTo, maskType}], "rowRules": [{roleId, filter}]}``:
    every field granted to the profiled table's readers; on the columns table the value fields
    masked (to NULL) for each reader with a column masked to it or tagged pii; and per reader a
    row rule keeping the rows that speak only of described columns it may see -- for the value
    kinds and value-bearing rows, only of those it sees unmasked and untagged.
    """
    own = [r for r in rules if r.relationship is None]
    readers = sorted(set().union(*(r.visible_to for r in own)) if own else set())
    restricted_readers = {
        role
        for role in readers
        if any(r.seen_by(frozenset({role})) and r.restricted_for(frozenset({role})) for r in own)
    }
    columns = []
    for name in fields:
        masked = kind == "columns" and name in COLUMN_VALUE_FIELDS and restricted_readers
        columns.append(
            {
                "name": name,
                "visibleTo": readers,
                "unmaskedTo": [r for r in readers if r not in restricted_readers] if masked else [],
                "maskType": "constant" if masked else None,
            }
        )
    row_rules = []
    if kind in INVOLVED_KINDS:
        # Each row lists every column it speaks of (column_name among them), so one rule on the
        # list covers both.
        for role in readers:
            found = _involved_filter(kind, rules, role)
            if found is not None:
                row_rules.append({"roleId": role, "filter": found})
    elif kind in COLUMN_KINDS:
        everything = [r.exposed for r in own]
        for role in readers:
            one = frozenset({role})
            allowed = [
                r.exposed
                for r in own
                if r.seen_by(one) and (kind not in VALUE_KINDS or not r.restricted_for(one))
            ]
            if allowed != everything:
                row_rules.append({"roleId": role, "filter": _in_list(allowed)})
    return {"columns": columns, "rowRules": row_rules}
