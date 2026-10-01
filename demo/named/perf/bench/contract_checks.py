# Copyright (c) 2026 Kenneth Stott
# Canary: 6add0262-38ae-49b2-9dff-e138fce44b38
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.
"""The setup contract's consistency checks (REQ-1911).

A knob that is set but not implemented is refused by name (``IMPLEMENTED_KNOBS`` lists what the load
generator honours); a distribution must not draw what the deployment cannot serve; a transport
must be able to spell what it is asked to read."""

from __future__ import annotations

from contract_joins import _JOIN_KINDS, reachable_joins, transport_targets
from contract_model import (
    JOIN_TRANSPORTS,
    MAX_LOOKBACK,
    TRANSPORT_LANGUAGE,
    Column,
    Distribution,
    Setup,
    SetupError,
    Table,
)

# Knobs the load generator honours. Each knob is added here when it is implemented and tested;
# until then a contract that sets it is refused. Paths follow the contract: ``knobs.<name>``,
# ``knobs.source_weights``, ``knobs.route``, ``load.mode``,
# ``deployment.roles``, ``tables.weight``. ``knobs.cache`` is implemented: it is the probability
# that a request opts into the response cache.
IMPLEMENTED_KNOBS: frozenset[str] = frozenset(
    {
        "knobs.fields",
        "knobs.filters",
        "knobs.rows",
        "knobs.source_weights",
        "tables.weight",
        "knobs.joins_same_source",
        "knobs.joins_cross_source",
        "knobs.cache_repetition",
        "knobs.route",
        "deployment.roles",
        "load.mode",
    }
)


# --------------------------------------------------------------------------------------------
# Not-yet-implemented knobs
# --------------------------------------------------------------------------------------------


def _refuse(knob: str) -> None:
    raise SetupError(f"not implemented yet: {knob}")


def _check_implemented(setup: Setup) -> None:
    """A knob that is set but not implemented is an error, never ignored."""
    for name, knob in setup.knobs.distributions.items():
        if knob.probability != 0 and f"knobs.{name}" not in IMPLEMENTED_KNOBS:
            _refuse(f"knobs.{name}")
    for transport, override in setup.transports.items():
        for name, knob in override.knobs.items():
            if knob.probability != 0 and f"knobs.{name}" not in IMPLEMENTED_KNOBS:
                _refuse(f"transports.{transport}.knobs.{name}")
        if override.source_weights is not None and "knobs.source_weights" not in IMPLEMENTED_KNOBS:
            _refuse(f"transports.{transport}.knobs.source_weights")
    if sum(1 for w in setup.knobs.source_weights.values() if w > 0) != 1 and (
        "knobs.source_weights" not in IMPLEMENTED_KNOBS
    ):
        _refuse("knobs.source_weights")
    for source, rm in setup.knobs.route.items():
        if rm.mode != "auto" and "knobs.route" not in IMPLEMENTED_KNOBS:
            _refuse(f"knobs.route.{source}")
    for transport, override in setup.transports.items():
        for source, rm in (override.route or {}).items():
            if rm.mode != "auto" and "knobs.route" not in IMPLEMENTED_KNOBS:
                _refuse(f"transports.{transport}.knobs.route.{source}")
    if setup.load.mode != "closed_loop" and "load.mode" not in IMPLEMENTED_KNOBS:
        _refuse(f"load.mode {setup.load.mode}")
    if len(setup.roles) != 1 and "deployment.roles" not in IMPLEMENTED_KNOBS:
        _refuse("deployment.roles (role mix)")
    for source, src in setup.sources.items():
        if (
            sum(1 for t in src.tables if t.weight > 0) > 1
            and "tables.weight" not in IMPLEMENTED_KNOBS
        ):
            _refuse(f"tables.weight (source {source})")


def distribution_range(dist: Distribution) -> tuple[int, int]:
    """The smallest and largest value a distribution can draw."""
    p = dist.params
    if dist.kind == "constant":
        return int(p["value"]), int(p["value"])
    if dist.kind == "uniform":
        return int(p["min"]), int(p["max"])
    return 1, int(p["max"])  # zipf, geometric


def _usable_tables(setup: Setup) -> list[tuple[str, Table]]:
    """Tables a request can read: every table with weight in every source with weight, on any
    transport."""
    weighted = {
        sid for t in setup.transports for sid, w in setup.source_weights(t).items() if w > 0
    }
    return [
        (sid, t)
        for sid, src in setup.sources.items()
        if sid in weighted
        for t in src.tables
        if t.weight > 0
    ]


def _check_sources_reachable(setup: Setup) -> None:
    """Every source with weight on a transport must have a table, and every table with weight must
    be nameable in that transport's language: a transport cannot send what it cannot spell."""
    for transport in setup.transports:
        if transport not in TRANSPORT_LANGUAGE:
            raise SetupError(f"transports.{transport}: no request language is known for it")
        language = TRANSPORT_LANGUAGE[transport]
        where = (
            f"transports.{transport}.knobs.source_weights"
            if setup.transports[transport].source_weights is not None
            else "knobs.source_weights"
        )
        for sid, weight in setup.source_weights(transport).items():
            if weight <= 0:
                continue
            tables = [t for t in setup.sources[sid].tables if t.weight > 0]
            if not tables:
                raise SetupError(f"{where}: source {sid} has weight but no table")
            for table in tables:
                if not table.has(language):
                    raise SetupError(
                        f"transports.{transport}: source {sid} table {table.name} is not exposed in "
                        f"{language} by the deployment; set the source's weight to 0 for {transport}"
                    )


def _check_joins(setup: Setup) -> None:
    """A join knob must be expressible, and reachable, on every transport it applies to."""
    for transport in setup.transports:
        highs: dict[str, int] = {}
        for kind, name in _JOIN_KINDS.items():
            knob = setup.knob(transport, name)
            if knob.probability == 0:
                continue
            path = (
                f"transports.{transport}.knobs.{name}"
                if name in setup.transports[transport].knobs
                else f"knobs.{name}"
            )
            if transport not in JOIN_TRANSPORTS:
                raise SetupError(
                    f"transports.{transport}: joins cannot be expressed on {transport}; set "
                    f"{name} probability to 0 for it in an override"
                )
            high = distribution_range(knob.distribution)[1]
            reach = max(
                (
                    reachable_joins(setup, transport, sid, t.name, kind)
                    for sid, t in transport_targets(setup, transport)
                ),
                default=0,
            )
            if reach < high:
                raise SetupError(
                    f"{path}.distribution: draws up to {high} {kind}-source joins; no table "
                    f"drawn on {transport} reaches more than {reach}"
                )
            highs[kind] = high
        if len(highs) == 2 and not any(
            reachable_joins(setup, transport, sid, t.name, "same") >= highs["same"]
            and reachable_joins(setup, transport, sid, t.name, "cross") >= highs["cross"]
            for sid, t in transport_targets(setup, transport)
        ):
            raise SetupError(
                f"transports.{transport}: no table reaches {highs['same']} same-source and "
                f"{highs['cross']} cross-source joins together"
            )


def _check_route(setup: Setup) -> None:
    """A transport with no route hint (gRPC, REST, JSON:API) can only read sources whose route
    mode is `auto`."""
    for transport in setup.transports:
        if TRANSPORT_LANGUAGE[transport] not in ("grpc", "rest", "jsonapi"):
            continue
        for sid, weight in setup.source_weights(transport).items():
            mode = setup.route_mode(transport, sid).mode
            if weight > 0 and mode != "auto":
                raise SetupError(
                    f"transports.{transport}: no route hint exists on {transport}, so source "
                    f"{sid} (route mode {mode}) cannot be read there; set its weight to 0 for "
                    f"{transport} or its route mode to auto"
                )


def filter_columns(table: Table) -> list[Column]:
    """Columns an equality filter can use: filterable, with a value domain."""
    return [c for c in table.columns if c.filterable and c.domain is not None]


def _check_ranges(setup: Setup) -> None:
    """A distribution must not draw a value the deployment cannot serve."""
    usable = [t for _, t in _usable_tables(setup)]
    # knob -> (smallest count, largest count the deployment can serve or None, noun, what limits it)
    limits = {
        "fields": (1, min(len(t.columns) for t in usable), "fields", "declares {n} columns"),
        "filters": (
            0,
            min(len(filter_columns(t)) for t in usable),
            "filters",
            "has {n} filterable columns with a domain",
        ),
        "rows": (1, None, "rows", ""),
        "cache_repetition": (1, None, "requests back", ""),
    }
    for name, (floor, ceiling, noun, have) in limits.items():
        knobs = [(f"knobs.{name}", setup.knobs.distributions[name])] + [
            (f"transports.{t}.knobs.{name}", o.knobs[name])
            for t, o in setup.transports.items()
            if name in o.knobs
        ]
        for path, knob in knobs:
            if knob.probability == 0:
                continue
            low, high = distribution_range(knob.distribution)
            if low < floor:
                what = (
                    "a lookback distance" if name == "cache_repetition" else f"a {noun[:-1]} count"
                )
                raise SetupError(
                    f"{path}.distribution: {what} must be >= {floor}, it can draw {low}"
                )
            if name == "cache_repetition" and high > MAX_LOOKBACK:
                raise SetupError(
                    f"{path}.distribution: draws up to {high} requests back; the window is "
                    f"limited to {MAX_LOOKBACK}"
                )
            if name == "cache_repetition":
                continue
            if ceiling is not None and high > ceiling:
                raise SetupError(
                    f"{path}.distribution: draws up to {high} {noun}, a usable table {have.format(n=ceiling)}"
                )
