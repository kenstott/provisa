# Copyright (c) 2026 Kenneth Stott
# Canary: 4f8dd53a-7daf-4be5-aff2-579ced519951
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.
"""The results derived from a benchmark's knob sweeps (REQ-1911): a cost model (base cost plus a
cost per field, filter, row and join) and the break-even between live and replica reads.

Plain least squares, no dependencies: the system is a handful of knobs wide."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass

KNOBS = ("fields", "filters", "rows", "joins_same", "joins_cross")
_SINGULAR = 1e-9


@dataclass(frozen=True)
class Observation:
    """One measured mix: the mean of each knob over the requests, and the CPU one request cost."""

    knob_means: Mapping[str, float]
    cpu_ms: float


@dataclass(frozen=True)
class Check:
    predicted_ms: float
    measured_ms: float
    relative_error: float  # |measured - predicted| / measured


@dataclass(frozen=True)
class CostModel:
    base: float  # CPU ms of a request with every knob at zero
    per_unit: Mapping[str, float]  # CPU ms each knob adds per unit
    r2: float
    n: int

    def predict(self, knob_means: Mapping[str, float]) -> float:
        return self.base + sum(c * knob_means[k] for k, c in self.per_unit.items())

    def marginal(self) -> dict[str, float]:
        return dict(self.per_unit)

    def check(self, observation: Observation) -> Check:
        predicted = self.predict(observation.knob_means)
        return Check(
            predicted,
            observation.cpu_ms,
            abs(observation.cpu_ms - predicted) / observation.cpu_ms,
        )


def _solve(a: list[list[float]], b: list[float], names: Sequence[str]) -> list[float]:
    """Gaussian elimination with partial pivoting; a (near) zero pivot names the dependent knob."""
    n = len(b)
    m = [row[:] + [b[i]] for i, row in enumerate(a)]
    for col in range(n):
        pivot = max(range(col, n), key=lambda r: abs(m[r][col]))
        if abs(m[pivot][col]) < _SINGULAR:
            raise ValueError(
                f"{names[col]!r} is a linear combination of the other knobs in these "
                "observations (knobs move together); sweep it alone"
            )
        m[col], m[pivot] = m[pivot], m[col]
        for r in range(col + 1, n):
            f = m[r][col] / m[col][col]
            for c in range(col, n + 1):
                m[r][c] -= f * m[col][c]
    x = [0.0] * n
    for r in range(n - 1, -1, -1):
        x[r] = (m[r][n] - sum(m[r][c] * x[c] for c in range(r + 1, n))) / m[r][r]
    return x


def varying_knobs(observations: Sequence[Observation]) -> list[str]:
    """The knobs whose mean differs across the observations."""
    return [
        k
        for k in KNOBS
        if max(o.knob_means[k] for o in observations) - min(o.knob_means[k] for o in observations)
        >= 1e-12
    ]


def fit(observations: Sequence[Observation], knobs: Sequence[str] = KNOBS) -> CostModel:
    """Least-squares CPU ms = base + sum(cost_k x knob_k) over ``knobs`` (every knob by default).
    A knob that does not vary, or varies together with others, cannot be costed and is an error:
    pass the knobs that do (``varying_knobs``)."""
    need = len(knobs) + 1
    if len(observations) < need:
        raise ValueError(
            f"the cost model needs at least {need} observations, got {len(observations)}"
        )
    for k in knobs:
        values = [o.knob_means[k] for o in observations]
        if max(values) - min(values) < 1e-12:
            raise ValueError(f"knob {k!r} does not vary across the observations")
    rows = [[1.0] + [o.knob_means[k] for k in knobs] for o in observations]
    y = [o.cpu_ms for o in observations]
    names = ["base", *knobs]
    xtx = [[sum(r[i] * r[j] for r in rows) for j in range(need)] for i in range(need)]
    xty = [sum(r[i] * yi for r, yi in zip(rows, y)) for i in range(need)]
    beta = _solve(xtx, xty, names)
    mean_y = sum(y) / len(y)
    ss_tot = sum((v - mean_y) ** 2 for v in y)
    ss_res = sum((yi - sum(b * x for b, x in zip(beta, r))) ** 2 for r, yi in zip(rows, y))
    return CostModel(
        base=beta[0],
        per_unit=dict(zip(knobs, beta[1:])),
        r2=1.0 if ss_tot == 0 else 1 - ss_res / ss_tot,
        n=len(observations),
    )


@dataclass(frozen=True)
class BreakEven:
    requests_per_s: float | None  # above this rate a replica costs less CPU than reading live
    reason: str | None  # why there is no break-even rate


def break_even(
    *, live_ms: float, replica_ms: float, refresh_cpu_s: float, ttl_s: float
) -> BreakEven:
    """The request rate at which reading from a replica starts to cost less CPU than reading live.

    A live request costs ``live_ms`` of CPU (Provisa plus source). A replica request costs
    ``replica_ms`` but the replica is refreshed every ``ttl_s`` seconds at ``refresh_cpu_s`` of
    CPU, a standing cost of ``refresh_cpu_s / ttl_s`` CPU-s per second."""
    if ttl_s <= 0:
        raise ValueError(f"ttl_s must be > 0, got {ttl_s}")
    saving = live_ms - replica_ms
    if saving <= 0:
        return BreakEven(None, "a replica request costs no less than a live one")
    standing_ms_per_s = refresh_cpu_s / ttl_s * 1000
    return BreakEven(standing_ms_per_s / saving, None)
