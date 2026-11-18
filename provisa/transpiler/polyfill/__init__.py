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

:data:`GRID` is the grid. Each family of features is a module of its rows; a family moves here from
the per-engine tables of provisa.transpiler (scripts/conformance/polyfill_audit.md) one at a time."""

# Requirements: REQ-2236

from __future__ import annotations

from provisa.transpiler.polyfill import (
    aggregates,
    array_dimensions,
    array_sets,
    arrays,
    bits,
    byte_compares,
    bytea,
    calls,
    casts,
    clickhouse,
    clocks,
    collations,
    dates,
    float_aggregates,
    floats,
    geometric,
    instants,
    intervals,
    json_built,
    json_keys,
    json_members,
    json_values,
    money,
    network,
    numbers,
    ported,
    ranges,
    records,
    regex,
    server,
    snapshots,
    texts,
    textsearch,
    type_io,
    unheld,
    zones,
)
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
from provisa.transpiler.polyfill.matching import Bindings, graft, match
from provisa.transpiler.polyfill.notation import Refusal, Rewrite, RuleError, hole_name, holes

#: The one grid: every family's features.
GRID = Grid(
    # an array of dimensions is read before any row of its call's name
    array_dimensions.FEATURES
    + arrays.FEATURES
    + array_sets.FEATURES
    + zones.FEATURES
    + records.FEATURES
    + money.FEATURES
    + type_io.FEATURES
    + regex.FEATURES
    + collations.FEATURES
    + textsearch.FEATURES
    + network.FEATURES
    + unheld.FEATURES
    + numbers.FEATURES
    + floats.FEATURES
    + float_aggregates.FEATURES
    + dates.FEATURES
    + calls.FEATURES
    + casts.FEATURES
    + server.FEATURES
    + ranges.FEATURES
    + json_keys.FEATURES
    + json_built.FEATURES
    + json_members.FEATURES
    + json_values.FEATURES
    + clickhouse.FEATURES
    + clocks.FEATURES
    + aggregates.FEATURES
    + snapshots.FEATURES
    # intervals' isfinite row is read before instants' (its call of no interval is that row's).
    + intervals.FEATURES
    + instants.FEATURES
    + geometric.FEATURES
    # a text's rows are read before bytea's, whose rows of one name keep what is no bytea
    + texts.FEATURES
    + bits.FEATURES
    + byte_compares.FEATURES
    + bytea.FEATURES
    + ported.FEATURES
)

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
