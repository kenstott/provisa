#!/usr/bin/env python3
# Copyright (c) 2026 Kenneth Stott
# Canary: 4e8b2d71-9a3c-4f05-b6e1-0c7d5a2f9e38
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Each grid dialect's provenance (REQ-2236, EVERY ROW NAMES WHAT IT WAS MADE ON; amended
2026-11-27), read from the configuration of the test that proves its rows, Conformance (functions):

- made on: each leg of function_legs.yaml whose engine Provisa transpiles to the dialect
  (polyfill_check.ENGINE_DIALECTS);
- verified on, and the last run that verified it: the legs among those that the run recorded in
  expected/functions.json judged (``measured``: its run id and commit; ``engines``: its legs).

A dialect with a row and no leg, or whose legs the run did not judge, names why none verified it.
scripts/gen_polyfill_docs.py prints it on the pages."""

# Requirements: REQ-2236

from __future__ import annotations

import json
import sys
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(Path(__file__).resolve().parent))

import function_legs  # noqa: E402
import polyfill_check  # noqa: E402

from provisa.transpiler.polyfill.grid import DIALECTS, Grid  # noqa: E402
from provisa.transpiler.polyfill.provenance import Leg, Provenance, Run, required  # noqa: E402

#: The record of the last measured Conformance (functions) run (functions_baseline.py writes it).
MEASURED = Path(__file__).resolve().parent / "expected" / "functions.json"


def _image(leg: dict[str, Any]) -> str:
    """What a leg's engine runs in, as its configuration names it."""
    image = leg.get("image")
    if not image:
        raise ValueError(f"{leg['engine']}: a leg names what its engine runs in (image)")
    return str(image)


def provenance(grid: Grid, legs: dict[str, Any], measured: dict[str, Any]) -> dict[str, Provenance]:
    """Each dialect's provenance, by dialect, from ``legs`` (function_legs.yaml) and ``measured``
    (expected/functions.json), for every dialect of the grid with a row."""
    run = Run(str(measured["measured"]["run"]), str(measured["measured"]["sha"]))
    judged = frozenset(measured["engines"])
    of: dict[str, Provenance] = {}
    for dialect in DIALECTS:
        if not grid.on(dialect):
            continue
        made = tuple(
            Leg(leg["engine"], _image(leg))
            for leg in legs["legs"]
            if polyfill_check.ENGINE_DIALECTS[leg["engine"]] == dialect
        )
        verified = tuple(leg for leg in made if leg.engine in judged)
        if verified:
            of[dialect] = Provenance(made, verified, run)
        elif made:
            of[dialect] = Provenance(
                made,
                (),
                None,
                f"run {run.run}, the last measured, judged none of its legs",
            )
        else:
            of[dialect] = Provenance(
                (), (), None, "function_legs.yaml has no leg whose engine runs this dialect"
            )
    required(of, grid)
    return of


def read(grid: Grid) -> dict[str, Provenance]:
    """Each dialect's provenance from the committed configuration."""
    return provenance(grid, function_legs.load(), json.loads(MEASURED.read_text()))
