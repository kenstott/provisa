# Copyright (c) 2026 Kenneth Stott
# Canary: 7b3d9e05-2a1c-4f86-9e40-d5c8b1a6f273
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Hold on failing checks (REQ-2133): a materialized view's built-in preflight.

A refresh is aborted, and the view keeps its previous copy, when the latest run of any checker table
scanning one of the view's inputs failed. The inputs are its nearest physical datasets (REQ-2126):
what its refresh reads. Every checker table scanning an input counts, whatever product it is
assigned to: a failing check on an input means the view would land data built from it. The abort
names each failing checker table and the input it scans, and propagates downstream as any preflight
abort does (REQ-941).

The check is ordinary preflight code that calls dq (:data:`SOURCE`); the option shows it, and a
steward can copy it into the view's own preflight to change it. It combines with a preflight the
view declares: the refresh lands only if both continue."""

# Requirements: REQ-2133, REQ-2126, REQ-1443

from __future__ import annotations

#: The built-in check, exactly as the option shows it and as it runs.
SOURCE = '''import provisa_services as ps


def preflight(streams, ctx):
    """Hold the refresh while a check on one of the view's inputs failed."""
    failing = []
    for table_id in ctx.nearest_inputs:
        found = ps.dq.dataset(table_id)
        for check in found["checker_tables"]:
            run = check["latest_run"]
            if run is not None and run["outcome"] == "fail":
                failing.append(check["checker_table"] + " failed on " + found["dataset"])
    if failing:
        return ctx.abort("a check on its inputs failed: " + "; ".join(failing))
    return ctx.ok()
'''


async def nearest_inputs(table_id: int) -> tuple[int, ...]:
    """The table ids of the view table ``table_id``'s nearest physical inputs (REQ-2126)."""
    from provisa.api.app import state
    from provisa.lineage.inputs import nearest_physical_inputs

    assert state.model_db is not None, "the control plane holds the model's lineage"
    async with state.model_db.acquire() as conn:
        found = await nearest_physical_inputs(conn, table_id)
    return tuple(i.table_id for i in found)
