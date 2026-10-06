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

from sqlalchemy import select

from provisa.compiler.sql_literals import sql_literal

# The kinds whose every row holds values of the column it describes.
VALUE_KINDS = frozenset({"quantiles", "histogram", "top_values", "shapes", "fits", "fit_quality"})
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


@dataclass(frozen=True)
class ColumnRule:
    physical: str
    exposed: str  # the column as the profile names it (as the org admin reads it)
    visible_to: frozenset[str]
    unmasked_to: frozenset[str]
    masked: bool
    pii: bool

    def seen_by(self, roles: frozenset[str]) -> bool:
        return bool(self.visible_to & roles)

    def restricted_for(self, roles: frozenset[str]) -> bool:
        """Masked to every one of ``roles``, or tagged pii."""
        return self.pii or (self.masked and not (self.unmasked_to & roles))


async def column_rules(conn: Any, state: Any, table_id: int) -> list[ColumnRule]:
    """The profiled table's current column rules, for the columns its profile describes."""
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
                exposed=exposed,
                visible_to=frozenset(visible_to),
                unmasked_to=frozenset(unmasked_to),
                masked=mask_type is not None,
                pii="pii" in tags.get(name, set()),
            )
        )
    return rules


def safe_run(
    results: dict[str, list[dict]], rules: list[ColumnRule], roles: frozenset[str]
) -> dict[str, list[dict]]:
    """One run's results as ``roles`` may see them (the View Profile Runs view)."""
    by_name = {r.exposed: r for r in rules}

    def _shown(column: str) -> ColumnRule | None:
        rule = by_name.get(column)
        return rule if rule is not None and rule.seen_by(roles) else None

    out: dict[str, list[dict]] = {}
    for kind, rows in results.items():
        if kind not in COLUMN_KINDS:
            out[kind] = rows
            continue
        kept = []
        for row in rows:
            rule = _shown(row["column_name"])
            if rule is None:
                continue
            if rule.restricted_for(roles):
                if kind in VALUE_KINDS:
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


def prefill(kind: str, fields: list[str], rules: list[ColumnRule]) -> dict:
    """The default rules a result table of ``kind`` is registered with.

    ``{"columns": [{name, visibleTo, unmaskedTo, maskType}], "rowRules": [{roleId, filter}]}``:
    every field granted to the profiled table's readers; on the columns table the value fields
    masked (to NULL) for each reader with a column masked to it or tagged pii; and per reader a
    row rule on ``column_name`` listing the described columns it may see -- for the value kinds,
    only those it sees unmasked and untagged.
    """
    readers = sorted(set().union(*(r.visible_to for r in rules)) if rules else set())
    restricted_readers = {
        role
        for role in readers
        if any(r.seen_by(frozenset({role})) and r.restricted_for(frozenset({role})) for r in rules)
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
    if kind in COLUMN_KINDS:
        everything = [r.exposed for r in rules]
        for role in readers:
            one = frozenset({role})
            allowed = [
                r.exposed
                for r in rules
                if r.seen_by(one) and (kind not in VALUE_KINDS or not r.restricted_for(one))
            ]
            if allowed != everything:
                row_rules.append({"roleId": role, "filter": _in_list(allowed)})
    return {"columns": columns, "rowRules": row_rules}
