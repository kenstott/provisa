# Copyright (c) 2026 Kenneth Stott
# Canary: 637bddf7-7e79-476a-8ad5-776fa5cfba3d
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Polyfills (REQ-2236): how pgwire gives PostgreSQL's behaviour on an engine that is not
PostgreSQL, as one grid of rules the translator runs and the documentation prints.

:data:`GRID` is the grid. Each family of features is a module of its rows, collected by module; a
family moves here from the per-engine tables of provisa.transpiler
(scripts/conformance/polyfill_audit.md) one at a time."""

# Requirements: REQ-2236

from __future__ import annotations

import importlib
import pkgutil

from provisa.transpiler.polyfill.grid import (
    DIALECTS,
    Applied,
    Connector,
    Feature,
    Grid,
    Limitation,
    Rule,
    Side,
    Status,
    column_of,
    written_in,
)
from provisa.transpiler.polyfill import stated_refusals
from provisa.transpiler.polyfill.matching import Bindings, graft, match
from provisa.transpiler.polyfill.notation import Refusal, Rewrite, RuleError, hole_name, holes

#: The package's modules that hold no family: the grid, its notation and matching, the pages, and
#: the Trino helpers families share.
_NOT_FAMILIES = frozenset(
    {"docs", "grid", "inet_literal", "matching", "notation", "stated_refusals", "trino"}
)


def _features() -> tuple[Feature, ...]:
    """Every family's features, the families in their ORDER, refusing two families of one place."""
    families = [
        importlib.import_module(f"{__name__}.{m.name}")
        for m in pkgutil.iter_modules(__path__)
        if m.name not in _NOT_FAMILIES
    ]
    placed: dict[int, str] = {}
    for family in families:
        if family.ORDER in placed:
            raise RuleError(
                f"{placed[family.ORDER]} and {family.__name__} are both ORDER {family.ORDER}"
            )
        placed[family.ORDER] = family.__name__
    families.sort(key=lambda family: family.ORDER)
    return tuple(feature for family in families for feature in family.FEATURES)


#: The one grid: every family's features, each family from its own module (its FEATURES, at its
#: ORDER), so a family is edited in its module alone and a new family needs no edit here.
#: The grid: each family's rows, with the old table's refusals stated where a row keeps its form.
GRID = Grid(stated_refusals.stated(_features()))

__all__ = [
    "DIALECTS",
    "GRID",
    "Applied",
    "Bindings",
    "Connector",
    "Feature",
    "Grid",
    "Limitation",
    "Refusal",
    "Rewrite",
    "Rule",
    "RuleError",
    "Side",
    "Status",
    "column_of",
    "graft",
    "hole_name",
    "holes",
    "match",
    "written_in",
]
