# Copyright (c) 2026 Kenneth Stott
# Canary: 5c1e8a37-d2b4-4f69-9a03-e7b6f1c4d822
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""How a profile run samples its table (REQ-1934).

A table that fits the cell budget is read whole. A larger one is sampled, by the first of these the
table's REACH offers -- where the governed pipeline sends the statement and what that reader can do:

1. ``block``: TABLESAMPLE SYSTEM executed at the source, which then reads only a share of its blocks;
2. ``key_range``: K ranges of a single-column integer primary key, each answered from the source's
   index (``statement.key_ranges``);
3. ``random``: the ``RANDOM() < f`` row filter, which cuts the profile's work but not the read.

The reach is read from declared traits, never from the SQL: a DIRECT route is the source's own SQL
(``executor.drivers.registry.direct_block_sample`` / ``direct_key_range``); an ENGINE route reading
the table live is the engine's connector for the source type (``Capability.block_sample`` /
``Capability.key_range``); a table read from its replica has no sampling trait measured, so it is
sampled by the row filter.
"""

# Requirements: REQ-1934

from __future__ import annotations

from dataclasses import dataclass
from typing import Any


@dataclass(frozen=True)
class SampleReach:
    """Where the profile statement reads the table, and the sampling that executes there."""

    where: str  # for messages: "direct postgresql", "duckdb postgresql", "replica on trino", ...
    block: bool
    key_range: bool


def sample_reach(state: Any, route: Any, table: Any) -> SampleReach:
    """The reach of ``table`` (the org admin's ``TableMeta``) on ``route``, the route the governed
    pipeline chose for the profile statement."""
    from provisa.executor.drivers.registry import direct_block_sample, direct_key_range
    from provisa.federation.strategy import engine_attaches
    from provisa.transpiler.router import Route

    stype = state.source_types[table.source_id]
    if route == Route.DIRECT:
        return SampleReach(f"direct {stype}", direct_block_sample(stype), direct_key_range(stype))
    if route == Route.ENGINE:
        engine = state.federation_engine.engine
        routes = state.replica_routes
        replica = (
            table.table_id in routes.floored
            or (table.source_id, table.schema_name, table.table_name) in routes.serving
            or not engine_attaches(engine, stype)
        )
        if replica:
            return SampleReach(f"replica on {engine.name}", False, False)
        cap = engine.connector_pushdown(stype)
        return SampleReach(f"{engine.name} {stype}", cap.block_sample, cap.key_range)
    return SampleReach(f"{getattr(route, 'value', route)} {stype}", False, False)


def choose_method(reach: SampleReach, has_key: bool) -> str:
    """The sample method for a table too large to read whole (REQ-1934 maintainer ruling)."""
    if reach.block:
        return "block"
    if reach.key_range and has_key:
        return "key_range"
    return "random"


class SampleClauseLost(Exception):
    """The statement bound for the engine or source no longer carries its sample: running it
    would read the whole table where a sample was chosen."""


def require_sample_clause(
    method: str, executed_sql: str, dialect: str, where: str, ranges: int
) -> None:
    """Refuse an executable statement that lost the sample ``method`` put in it -- a transpile
    that drops TABLESAMPLE (sqlglot does for MySQL) would read the whole table silently."""
    import sqlglot
    import sqlglot.expressions as exp

    if method not in ("block", "key_range"):
        return
    tree = sqlglot.parse_one(executed_sql, read=dialect)
    if method == "block":
        found = len(list(tree.find_all(exp.TableSample)))
        if found == 0:
            raise SampleClauseLost(
                f"block sample lost in the {dialect!r} statement for {where}: no TABLESAMPLE "
                "survived governance and transpile"
            )
        return
    found = len(list(tree.find_all(exp.Between)))
    if found < ranges:
        raise SampleClauseLost(
            f"key-range sample lost in the {dialect!r} statement for {where}: {found} of "
            f"{ranges} key ranges survived governance and transpile"
        )
