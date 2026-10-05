# Copyright (c) 2026 Kenneth Stott
# Canary: 219f2ddb-f5ba-4cbc-8bc8-b2901b893ce9
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""What a region may keep of the rows it fetches by key (REQ-1865, REQ-1921, REQ-1922).

A row-level table's rows are fetched by key into this region's store. Where the platform declares
regions, that fetch is the region's own work, governed as its administrator
(:mod:`provisa.core.region_admin`): a row the administrator's row rule — resolved with this
node's region — does not admit is never landed here, so a row that may be held only in another
region is neither kept here nor returned from here.

The fetched rows are bound as a relation and governed by the ONE governance stage
(:func:`provisa.compiler.stage2.apply_governance`), as a tracked action's response is
(``api.data.action_governance``); only the row filter decides admission — what the cache holds is
then read under each reader's own rules. The governed statement is evaluated in-process over the
fetched batch, which may be millions of rows, never sent back to a source. A rule that cannot be
evaluated over the batch alone (it reads another relation) refuses the fetch by name: nothing is
landed that was not admitted.
"""

# Requirements: REQ-1865, REQ-1921, REQ-1922

from __future__ import annotations

from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    import pyarrow as pa

#: The name the fetched batch is bound under while it is governed.
_RELATION = "region_fetched_rows"


class RowRuleNotEvaluable(RuntimeError):
    """The administrator's row rule on a row-level table cannot be judged over its fetched rows."""

    code = "query.region_row_rule_not_evaluable"

    def __init__(self, table: str, why: str) -> None:
        self.table = table
        self.params = {"table": table}
        super().__init__(
            f"the rows of table {table!r} fetched by key cannot be kept in this region: the "
            f"organisation administrator's row rule on it cannot be judged over them ({why})"
        )


def fetch_predicate(state: Any, table: Any) -> str | None:
    """The administrator's row rule on ``table`` with this region's values resolved — PostgreSQL
    text a source's own keyed fetch can carry, so a row this region may not keep never leaves the
    source. None with no platform regions, no rule, or a rule that reads another relation (no
    source fetch of this one table can evaluate it; the rows are judged after the fetch)."""
    from provisa.core import region_admin

    if not region_admin.governs_region_work():
        return None
    rule = region_admin.row_rule(state, table)
    if rule is None:
        return None
    import sqlglot
    import sqlglot.expressions as exp

    from provisa.pgwire._pipeline import _resolve_session_settings

    resolved = _resolve_session_settings(rule, region_admin.session_vars(state), "postgres")
    tree = sqlglot.parse_one(resolved, read="postgres")
    if tree.find(exp.Select) is not None or tree.find(exp.Table) is not None:
        return None
    return resolved


def admit_rows(state: Any, table: Any, data: "pa.Table") -> "pa.Table":
    """The rows of ``data`` (fetched by key for ``table``) this region may keep: all of them with
    no platform regions or no administrator row rule on the table, else those the rule admits."""
    from provisa.core import region_admin

    if not region_admin.governs_region_work() or data.num_rows == 0:
        return data
    rule = region_admin.row_rule(state, table)
    if rule is None:
        return data

    import sqlglot

    from provisa.compiler.stage2 import GovernanceContext, apply_governance
    from provisa.pgwire._pipeline import _resolve_session_settings

    names = list(data.column_names)
    gov = GovernanceContext(role_id=region_admin.ROLE)
    gov.table_map[_RELATION] = table.id
    gov.all_columns[table.id] = [(n, "varchar") for n in names]
    gov.visible_columns[table.id] = frozenset(names)
    gov.rls_rules[table.id] = rule
    projected = ", ".join(f'"{n}"' for n in names)
    governed = apply_governance(f'SELECT {projected} FROM "{_RELATION}"', gov)
    governed = _resolve_session_settings(governed, region_admin.session_vars(state), "postgres")
    from provisa.federation.duckdb_backend import LocalEvaluationFailed, evaluate_over_arrow

    local = sqlglot.transpile(governed, read="postgres", write="duckdb")[0]
    try:
        return evaluate_over_arrow(local, _RELATION, data)
    except LocalEvaluationFailed as exc:
        raise RowRuleNotEvaluable(table.table_name, str(exc)) from exc


def admit_row_dicts(state: Any, table: Any, rows: list[dict]) -> list[dict]:
    """:func:`admit_rows` for rows as dicts (the keyed fetch's shape)."""
    from provisa.core import region_admin

    if not rows or not region_admin.governs_region_work():
        return rows
    if region_admin.row_rule(state, table) is None:
        return rows
    import pyarrow as pa

    return admit_rows(state, table, pa.Table.from_pylist(rows)).to_pylist()
