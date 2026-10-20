# Copyright (c) 2026 Kenneth Stott
# Canary: 89fdfd23-45fc-4253-b951-3828b3328f00
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.
"""The governance variants a pass runs, and its table (REQ-2092): one knob at a time against the
request with every knob at zero, then all together, then zero again; each cached and uncached.
The local pass (local_pass.py) and the three-device run (three_device.py) run the same ones."""

# Requirements: REQ-2092

from __future__ import annotations

from collections.abc import Mapping, Sequence
from typing import Any

ZERO = {
    "row_rules_per_table": 0,
    "masked_column_share": 0,
    "central_redaction": False,
    "role_depth": 0,
    "derived_roles": 0,
}
# One knob at a time, then all together: the difference from "zero" is that knob's cost.
VARIANTS: dict[str, dict[str, Any]] = {
    "zero": {},
    "row_rules_2": {"row_rules_per_table": 2},
    "masked_quarter": {"masked_column_share": 0.25},
    "redaction": {"central_redaction": True},
    "role_depth_2": {"role_depth": 2},
    "derived_roles_2": {"derived_roles": 2},
    "all": {
        "row_rules_per_table": 2,
        "masked_column_share": 0.25,
        "central_redaction": True,
        "role_depth": 2,
        "derived_roles": 2,
    },
    # zero again, last: how far the server drifted over the pass
    "zero_end": {},
}
CACHE = {"cached": 1.0, "uncached": 0.0}


def governance(variant: str, *, telemetry: bool) -> dict[str, Any]:
    """The contract's ``knobs.governance`` for ``variant``."""
    return {**ZERO, **VARIANTS[variant], "telemetry": telemetry}


def table(results: Mapping[str, Sequence[Mapping[str, Any]]]) -> list[dict[str, Any]]:
    """One row per variant, cache setting, transport and step from each run's transport reports
    (``results``: ``"<variant>/<cache>"`` to them): server CPU per request, its difference from
    the zero variant at the same cache setting, transport and concurrency, the stages' thread CPU
    per request (when the run tallied them) and the paging."""
    rows: list[dict[str, Any]] = []
    for key, reports in results.items():
        variant, cache = key.split("/")
        for report in reports:
            for step in report["steps"]:
                if "error" in step:
                    rows.append(
                        {
                            "variant": variant,
                            "cache": cache,
                            "transport": report["transport"],
                            "concurrency": step["concurrency"],
                            "error": step["error"],
                        }
                    )
                    continue
                stages = {
                    f"{t}.{name}": s["cpu_ms_per_request"]
                    for t, v in step.get("stages", {}).items()
                    for name, s in v["stages"].items()
                }
                rows.append(
                    {
                        "variant": variant,
                        "cache": cache,
                        "transport": report["transport"],
                        "concurrency": step["concurrency"],
                        "req_per_s": step["req_per_s"],
                        "errors": step["errors"],
                        "server_cpu_ms_per_request": step.get("server_cpu_ms_per_request"),
                        "stage_cpu_ms_per_request": stages,
                        "paging": step["paging"],
                    }
                )
    zero = {
        (r["cache"], r["transport"], r["concurrency"]): r.get("server_cpu_ms_per_request")
        for r in rows
        if r["variant"] == "zero"
    }
    for r in rows:
        base = zero.get((r["cache"], r["transport"], r["concurrency"]))
        cpu = r.get("server_cpu_ms_per_request")
        r["over_zero_ms"] = round(cpu - base, 3) if cpu is not None and base is not None else None
    return rows
