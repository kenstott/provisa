# Copyright (c) 2026 Kenneth Stott
# Canary: 7c1e5a93-2b84-4f6d-9e0a-3d5b8c1f6a27
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""What a grid row was made and verified on, and the last run that verified it (REQ-2236, EVERY
ROW NAMES WHAT IT WAS MADE ON; EACH ENGINE HAS A BASELINE PAIR; amended 2026-11-27).

A row's provenance is read from the configuration of the test that proves it, never written by
hand: the legs of Conformance (functions) (scripts/conformance/function_legs.yaml) whose engine
Provisa transpiles to the row's dialect are what it was made on, and those of them the last
measured run judged (scripts/conformance/expected/functions.json) are what it was verified on, by
that run. scripts/conformance/polyfill_provenance.py reads them; the pages print them
(provisa.transpiler.polyfill.docs)."""

# Requirements: REQ-2236

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass

from provisa.transpiler.polyfill.grid import DIALECTS, Grid, Hint, RuleError


@dataclass(frozen=True)
class Leg:
    """A leg of Conformance (functions): its engine kind and what the engine runs in, as the
    leg's configuration names it."""

    engine: str
    image: str


@dataclass(frozen=True)
class Run:
    """A Conformance (functions) run: its GitHub Actions run id and the commit it tested."""

    run: str
    sha: str


@dataclass(frozen=True)
class Provenance:
    """A dialect's rows' provenance. ``made_on``: every leg of the dialect. ``verified_on``: those
    of them ``run`` judged. A dialect no leg runs has neither, and its rows say so by
    ``unverified``: why the configuration verifies none of them."""

    made_on: tuple[Leg, ...]
    verified_on: tuple[Leg, ...]
    run: Run | None
    unverified: str | None = None

    def __post_init__(self) -> None:
        if (self.run is None) != bool(self.unverified):
            raise RuleError("a provenance names its verifying run, or why there is none")
        if self.run is None and self.verified_on:
            raise RuleError("a provenance verified on a leg names the run that verified it")
        if self.run is not None and not self.verified_on:
            raise RuleError("a provenance with a verifying run names the legs it judged")
        if not set(self.verified_on) <= set(self.made_on):
            raise RuleError("a row is verified only on a leg it is made on")


@dataclass(frozen=True)
class HintProvenance:
    """What a hint was made and verified on (REQ-2236, a hint on its engine and source pair): the
    docker-compose.test.yml service its reproducing test runs the engine and source in, and that
    service's image, as the configuration names it."""

    service: str
    image: str

    def __post_init__(self) -> None:
        if not self.service or not self.image:
            raise RuleError("a hint's provenance names its service and the service's image")


def hints_required(provenance: Mapping[str, HintProvenance], grid: Grid) -> None:
    """Refuses a hint provenance that leaves out a hint's service: every hint names its own."""
    missing = sorted({h.service for h in grid.hints} - set(provenance))
    if missing:
        raise RuleError(f"hints with no provenance of service {missing}")


def hint_line(hint: Hint, provenance: Mapping[str, HintProvenance]) -> str:
    """What a hint prints of its provenance, as plain text."""
    of = provenance.get(hint.service)
    if of is None:
        raise RuleError(f"hint {hint.name}: no provenance of service {hint.service!r}")
    on = "its own tables" if hint.source is None else f"source {hint.source}"
    setting = "" if hint.setting is None else f" ({hint.setting})"
    return (
        f"Made and verified on: {hint.engine}, {on}{setting}, in docker-compose.test.yml service"
        f" {of.service} ({of.image}), by its reproducing test {hint.test}."
    )


def required(provenance: Mapping[str, Provenance], grid: Grid) -> None:
    """Refuses a provenance that leaves out a dialect with a row: every row names its own."""
    missing = sorted(
        dialect for dialect in DIALECTS if grid.on(dialect) and dialect not in provenance
    )
    if missing:
        raise RuleError(f"rows with no provenance on {missing}")
    unknown = sorted(set(provenance) - set(DIALECTS))
    if unknown:
        raise RuleError(f"provenance of no dialect of the grid: {unknown}")


def legs_text(legs: tuple[Leg, ...]) -> str:
    return ", ".join(leg.engine for leg in legs)


def line(provenance: Provenance) -> str:
    """What each row of a dialect prints of its provenance, as plain text: the dialect's rows
    share its legs and its run."""
    if provenance.run is None:
        assert provenance.unverified is not None
        made = legs_text(provenance.made_on) if provenance.made_on else "no leg"
        return f"Made on: {made}. Verified on: none ({provenance.unverified})."
    return (
        f"Made on: {legs_text(provenance.made_on)}. "
        f"Verified on: {legs_text(provenance.verified_on)}, "
        f"last by run {provenance.run.run} at {provenance.run.sha}."
    )
