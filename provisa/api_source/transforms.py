# Copyright (c) 2026 Kenneth Stott
# Canary: e81f8352-9450-4dea-a16d-913394b22896
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Built-in value transforms for API source columns (Phase U)."""

from __future__ import annotations

from datetime import datetime, timezone
from decimal import Decimal
from typing import Callable


def from_unix_timestamp(value: int | float | str) -> datetime:
    """Convert a Unix timestamp (seconds) to a datetime."""
    return datetime.fromtimestamp(int(value), tz=timezone.utc)


def cents_to_decimal(value: int | str) -> Decimal:
    """Convert cents (integer) to a Decimal dollar amount."""
    return Decimal(str(value)) / Decimal("100")


# Transform registry: name -> function
TRANSFORM_REGISTRY: dict[str, Callable] = {
    "from_unix_timestamp": from_unix_timestamp,
    "cents_to_decimal": cents_to_decimal,
}


def apply_transform(name: str, value: object) -> object:
    """Apply a named transform to a value."""
    fn = TRANSFORM_REGISTRY.get(name)
    if fn is None:
        raise ValueError(f"Unknown transform: {name!r}")
    return fn(value)
