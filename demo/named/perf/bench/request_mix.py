# Copyright (c) 2026 Kenneth Stott
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.
"""Seeded per-request draws for the benchmark's knobs (REQ-1911).

Each client draws from its own stream, named by what it is (transport / process / thread), so one
seed gives one request sequence per client and a run is reproducible. Every knob has its own
generator seeded from that stream and the knob's name, so turning one knob on never changes the
sequence another knob draws.

``RequestGenerator`` turns the contract's knobs into one ``RequestSpec`` per request;
``request_render`` turns a spec into the text each transport sends.
"""

from __future__ import annotations

import random
from collections import deque
from dataclasses import dataclass, field, replace

import contract_checks
import contract_joins
import contract_model
import setup_contract as sc


class CacheDraw:
    """Whether a request opts into the response cache: true with ``probability``."""

    def __init__(self, probability: float, seed: int, stream: str) -> None:
        self._probability = probability
        self._rng = random.Random(f"{seed}/{stream}")

    def next(self) -> bool:
        return self._rng.random() < self._probability


class Sampler:
    """Draws integers from a contract distribution."""

    def __init__(self, dist: contract_model.Distribution, rng: random.Random) -> None:
        self._rng = rng
        self._dist = dist
        p = dist.params
        self._support: list[int] = []
        self._weights: list[float] = []
        if dist.kind == "zipf":
            self._support = list(range(1, int(p["max"]) + 1))
            self._weights = [1.0 / k ** p["exponent"] for k in self._support]
        elif dist.kind == "geometric":
            self._support = list(range(1, int(p["max"]) + 1))
            self._weights = [(1 - p["p"]) ** (k - 1) * p["p"] for k in self._support]

    def next(self) -> int:
        d = self._dist
        if d.kind == "constant":
            return int(d.params["value"])
        if d.kind == "uniform":
            return self._rng.randint(int(d.params["min"]), int(d.params["max"]))
        return self._rng.choices(self._support, weights=self._weights)[0]


class KnobDraw:
    """A distribution knob: with ``probability`` the request's value is drawn from the
    distribution, otherwise the knob does not apply (``None``)."""

    def __init__(self, knob: contract_model.Knob, seed: int, stream: str, name: str) -> None:
        self._rng = random.Random(f"{seed}/{stream}/{name}")
        self._probability = knob.probability
        self._sampler = Sampler(knob.distribution, self._rng)

    def next(self) -> int | None:
        if self._rng.random() >= self._probability:
            return None
        return self._sampler.next()


@dataclass(frozen=True)
class RequestSpec:
    """One request, independent of the transport that carries it."""

    source: str
    table: str
    columns: tuple[str, ...]
    rows: int
    cached: bool
    # equality predicates: (column, value), one per distinct filterable column
    filters: tuple[tuple[str, str | int | float], ...] = ()
    # tables joined onto the request's own, in the order they were added
    joins: tuple[contract_model.JoinStep, ...] = ()
    # 0: a fresh request; k > 0: it repeats the request k requests back. Not part of the request's
    # identity (two equal requests render and cache alike).
    repeat: int = field(default=0, compare=False)
    # the route hint the request carries: None (the deployment decides), "direct", "federated"
    route: str | None = None
    # the role the request is sent as (drawn from the contract's weighted roles)
    role: str | None = None


class RequestGenerator:
    """The request sequence of one client: seeded, one spec per ``next()``.

    ``cacheable``: the transport has a per-request cache opt-in (REST and JSON:API have none, so
    their requests never carry one and the cache draw is not consumed)."""

    def __init__(
        self, setup: contract_model.Setup, transport: str, stream: str, *, cacheable: bool
    ) -> None:
        self._setup = setup
        self._transport = transport
        self._cacheable = cacheable
        self._cache = CacheDraw(setup.cache_probability(transport), setup.seed, stream)
        self._base_rows = setup.base["rows"]
        self._fields = KnobDraw(setup.knob(transport, "fields"), setup.seed, stream, "fields")
        self._rows = KnobDraw(setup.knob(transport, "rows"), setup.seed, stream, "rows")
        self._filters = KnobDraw(setup.knob(transport, "filters"), setup.seed, stream, "filters")
        self._source_rng = random.Random(f"{setup.seed}/{stream}/source")
        self._column_rng = random.Random(f"{setup.seed}/{stream}/field-choice")
        self._filter_rng = random.Random(f"{setup.seed}/{stream}/filter-choice")
        self._joins_same = KnobDraw(
            setup.knob(transport, "joins_same_source"), setup.seed, stream, "joins_same_source"
        )
        self._joins_cross = KnobDraw(
            setup.knob(transport, "joins_cross_source"), setup.seed, stream, "joins_cross_source"
        )
        self._join_rng = random.Random(f"{setup.seed}/{stream}/join-choice")
        self._route_rng = random.Random(f"{setup.seed}/{stream}/read-mode")
        self._role_rng = random.Random(f"{setup.seed}/{stream}/role")
        self._roles = [r for r in setup.roles if r.weight > 0]
        self._can_join = transport in contract_model.JOIN_TRANSPORTS
        repetition = setup.knob(transport, "cache_repetition")
        self._repeat = KnobDraw(repetition, setup.seed, stream, "cache_repetition")
        window = (
            contract_checks.distribution_range(repetition.distribution)[1]
            if repetition.probability
            else 0
        )
        self._history: deque[RequestSpec] = deque(maxlen=window)
        # (source, table) with the weight it is drawn at: source weight x table weight
        weights = setup.source_weights(transport)
        self._targets: list[tuple[str, contract_model.Table]] = []
        self._target_weights: list[float] = []
        for sid, src in setup.sources.items():
            if weights[sid] <= 0:
                continue
            for table in src.tables:
                if table.weight > 0:
                    self._targets.append((sid, table))
                    self._target_weights.append(weights[sid] * table.weight)
        # how many joins of each kind a request can carry from each target (a request that joins
        # starts only at a table that can carry them)
        self._reach = [
            {
                kind: contract_joins.reachable_joins(setup, transport, sid, t.name, kind)
                if self._can_join
                else 0
                for kind in ("same", "cross")
            }
            for sid, t in self._targets
        ]

    def next(self) -> RequestSpec:
        spec = self._fresh()
        back = self._repeat.next()  # drawn every request, so the other knobs never shift
        if back is not None and back <= len(self._history):
            spec = replace(self._history[-back], repeat=back)
        if self._history.maxlen:
            self._history.append(spec)
        return spec

    def _fresh(self) -> RequestSpec:
        cached = self._cache.next() if self._cacheable else False
        n_fields = self._fields.next()
        n_filters = self._filters.next()
        n_rows = self._rows.next()
        n_same = self._joins_same.next() or 0
        n_cross = self._joins_cross.next() or 0
        source, table = self._start(n_same, n_cross)
        return RequestSpec(
            source=source,
            table=table.name,
            columns=self._columns(table, n_fields),
            rows=self._base_rows if n_rows is None else n_rows,
            cached=cached,
            filters=self._draw_filters(table, n_filters),
            joins=self._draw_joins(source, table, n_same, n_cross),
            route=self._route(source),
            role=self._role_rng.choices(
                [r.role for r in self._roles], weights=[r.weight for r in self._roles]
            )[0],
        )

    def _route(self, source: str) -> str | None:
        """The route hint for a request whose own table is in ``source``, from its route mode."""
        mode = self._setup.route_mode(self._transport, source)
        if mode.mode == "auto":
            return None
        if mode.mode in ("direct", "federated"):
            return mode.mode
        return "federated" if self._route_rng.random() < mode.federated_probability else "direct"

    def _start(self, n_same: int, n_cross: int) -> tuple[str, contract_model.Table]:
        """The request's own table: drawn by weight among the targets that can carry the joins."""
        ok = [
            i
            for i, reach in enumerate(self._reach)
            if reach["same"] >= n_same and reach["cross"] >= n_cross
        ]
        if not ok:
            raise RuntimeError(
                f"no table can carry {n_same} same-source and {n_cross} cross-source joins on "
                f"{self._transport}: the contract validator should have refused this"
            )
        i = self._source_rng.choices(ok, weights=[self._target_weights[j] for j in ok])[0]
        return self._targets[i]

    def _draw_joins(
        self, source: str, table: contract_model.Table, n_same: int, n_cross: int
    ) -> tuple[contract_model.JoinStep, ...]:
        members = [(source, table.name)]
        steps: list[contract_model.JoinStep] = []
        weights = self._setup.source_weights(self._transport)
        for kind, n in (("same", n_same), ("cross", n_cross)):
            for _ in range(n):
                options = contract_joins.expandable_joins(
                    self._setup, self._transport, members, kind
                )
                if not options:
                    raise RuntimeError(
                        f"no {kind}-source join left from {members} on {self._transport}"
                    )
                step = self._join_rng.choices(
                    options, weights=[weights[o.child_source] for o in options]
                )[0]
                steps.append(step)
                members.append((step.child_source, step.child_table))
        return tuple(steps)

    def _draw_filters(
        self, table: contract_model.Table, n_filters: int | None
    ) -> tuple[tuple[str, str | int | float], ...]:
        if not n_filters:
            return ()
        usable = contract_checks.filter_columns(table)
        chosen = sorted(self._filter_rng.sample(range(len(usable)), n_filters))
        out = []
        for i in chosen:
            col = usable[i]
            dom = col.domain
            assert dom is not None
            if dom.kind == "int_range":
                value: str | int | float = self._filter_rng.randint(dom.low, dom.high)
            else:
                value = self._filter_rng.choice(dom.values)
            out.append((col.name, value))
        return tuple(out)

    def _columns(self, table: contract_model.Table, n_fields: int | None) -> tuple[str, ...]:
        if n_fields is None:
            return (table.base_column,)
        names = [c.name for c in table.columns]
        chosen = sorted(self._column_rng.sample(range(len(names)), n_fields))
        return tuple(names[i] for i in chosen)

    def base_spec(self, cached: bool, role: str) -> RequestSpec:
        """The request when no knob applies, on this client's transport, sent as ``role``."""
        source, table = sc.base_table(self._setup, self._transport)
        return RequestSpec(
            source, table.name, (table.base_column,), self._base_rows, cached, role=role
        )
