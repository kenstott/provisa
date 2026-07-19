# Copyright (c) 2026 Kenneth Stott
# Canary: 5c1a8f42-3d67-4b90-a2e5-9f0c7d6b41e8
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Demo Provisa-hosted (python impl_kind) functions — REQ-885.

A ``python`` tracked function is dispatched by provisa.executor.function_dispatch._exec_python as
``callable(payload, session)`` and returns a list of row dicts. ``random_dataset`` is a set-returning
command example: it fabricates N random-valued rows so a "command that returns a table" can be
demonstrated over every surface (GraphQL query field, SQL SELECT, etc.) without an external system.
"""

from __future__ import annotations

import random
from typing import Any

_REGIONS = ("north", "south", "east", "west")


def random_dataset(payload: dict[str, Any], _session: Any) -> list[dict[str, Any]]:
    """Return `rows` (default 5, arg-overridable) random rows: id, region, amount, active.

    Deterministic when a `seed` argument is supplied, so the demo/tests are reproducible; otherwise
    a fresh random sample each call. Runs in-process on the Provisa host (not the sandbox), so the
    standard ``random`` module is available.
    """
    args = payload.get("args", payload) if isinstance(payload, dict) else {}
    rng = random.Random(args.get("seed")) if args.get("seed") is not None else random.Random()
    n = int(args.get("rows", 5))
    return [
        {
            "id": i,
            "region": rng.choice(_REGIONS),
            "amount": round(rng.uniform(0, 1000), 2),
            "active": rng.random() > 0.5,
        }
        for i in range(1, n + 1)
    ]
