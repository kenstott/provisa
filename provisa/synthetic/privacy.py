# Copyright (c) 2026 Kenneth Stott
# Canary: a081fa43-aa30-47fa-987d-b267f40507da
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The privacy budget of a synthetic dataset declared private (REQ-1939, DIFFERENTIAL PRIVACY).

A private dataset is generated only from statistics measured under pure ε-differential privacy
with the Laplace mechanism (maintainer ruling X1; :mod:`provisa.synthetic.private_stats`). ε is
split evenly across the families of statistics the dataset measures, and each family's share
evenly across its statistics; by composition the dataset as a whole is ε-differentially private.
The report states ε, the split, and every cell the noise dropped.
"""

# Requirements: REQ-1939

from __future__ import annotations

import math
import random
from dataclasses import dataclass, field

MECHANISM = "laplace"
#: A noised count below ``scale · DROP_FACTOR`` is dropped (a zero count survives one time in 40).
DROP_FACTOR = math.log(20)
#: A statistic whose noise scale reaches this share of its noised value is reported as mostly noise.
MOSTLY_NOISE = 0.5


@dataclass
class Budget:
    """ε, its split by family and statistic, and what the noise dropped, for the report."""

    epsilon: float
    families: dict[str, float] = field(default_factory=dict)  # family -> its ε
    per_statistic: dict[str, float] = field(default_factory=dict)  # family -> each statistic's ε
    statistics: dict[str, int] = field(default_factory=dict)  # family -> how many
    charged: float = 0.0  # the ε every statistic measured so far was charged, summed
    dropped: list[tuple[str, str | None, str, str]] = field(default_factory=list)
    # (table, column, family, what)
    noise: list[tuple[str, str | None, str, float, float]] = field(default_factory=list)
    # (table, column, family, noised value, noise scale): each statistic as measured


def laplace(rng: random.Random, scale: float) -> float:
    """A draw of the Laplace distribution centred on 0 with ``scale``."""
    u = rng.random() - 0.5
    return -scale * math.copysign(1.0, u) * math.log(1 - 2 * abs(u))


def budget_for(epsilon: float, statistics: dict[str, int]) -> Budget:
    """ε split evenly across the families with any statistic, and each family's share evenly
    across its statistics."""
    if not epsilon > 0:
        raise ValueError(f"a private dataset's ε must be above 0, got {epsilon!r}")
    present = {f: n for f, n in statistics.items() if n > 0}
    share = epsilon / len(present) if present else epsilon
    return Budget(
        epsilon,
        {f: share for f in present},
        {f: share / n for f, n in present.items()},
        present,
    )


def report_entries(budget: Budget | None) -> list[dict]:
    """The report's privacy rows: ε, its split and what was dropped; or that the dataset carries
    no privacy guarantee."""

    def row(measure: str, value: float | None, note: str, table: str = "", column=None) -> dict:
        return {
            "table_name": table,
            "column_name": column,
            "measure": measure,
            "source_value": None,
            "synthetic_value": value,
            "delta": None,
            "note": note,
        }

    if budget is None:
        return [row("privacy_guarantee", None, "none: the dataset is not declared private")]
    out = [
        row(
            "privacy_epsilon_charged",
            budget.charged,
            "the ε charged by every statistic measured, summed: by composition, the dataset's ε",
        ),
        row(
            "privacy_epsilon",
            budget.epsilon,
            f"{MECHANISM}; split evenly across {len(budget.families)} families of statistics, "
            f"each measured from the tables under its share",
        ),
    ]
    for family, eps in sorted(budget.families.items()):
        out.append(
            row(
                "privacy_epsilon_family",
                eps,
                f"{family}: {budget.statistics[family]} statistics, "
                f"{budget.per_statistic[family]:.6g} each",
            )
        )
    for table, column, family, value, scale in budget.noise:
        if scale >= MOSTLY_NOISE * value:
            out.append(
                row(
                    "privacy_mostly_noise",
                    value,
                    f"{family}: its noise (scale {scale:.3g}) is comparable to its value "
                    f"({value:.3g}) at ε {budget.epsilon:g}; a larger ε, more rows, or a declared "
                    f"distribution as the column's synthetic rule makes it more than noise",
                    table,
                    column,
                )
            )
    for table, column, family, what in budget.dropped:
        out.append(row("privacy_dropped", None, f"{family}: {what}", table, column))
    return out
