# Copyright (c) 2026 Kenneth Stott
# Canary: bd08f8b8-c32a-4b3d-bf54-cc29fcbe0b3b
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A column's measurement, as a faked read computes from it (REQ-1494). Taken at model build by
provisa.fakes.measured; a plain value here so the compiler reads it without the layers that take it."""

# Requirements: REQ-1494

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class Measured:
    """What a faked read of one column computes from. ``refused`` names why it cannot be read."""

    refused: str | None = None
    run_id: str | None = None  # the profile run read, when one was
    values: tuple[tuple[str, float], ...] | None = None  # (value as text, share), non-null values
    true_share: float | None = None
    points: tuple[tuple[float, float], ...] | None = None  # (quantile, value), joined linearly
    shapes: tuple[tuple[str, float], ...] | None = None  # (shape, share)
