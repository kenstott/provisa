# Copyright (c) 2026 Kenneth Stott
# Canary: 25780176-3fd2-4829-9f17-b4088a8ec6ae
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.

"""Billing domain models."""

# Requirements: REQ-073, REQ-074, REQ-1075

from __future__ import annotations

import uuid
from dataclasses import dataclass
from datetime import datetime
from enum import Enum


class Plan(str, Enum):  # REQ-073, REQ-074
    trial = "trial"
    starter = "starter"
    pro = "pro"


PLAN_LIMITS: dict[str, int] = {"trial": 2, "starter": 10, "pro": 100}


def plan_from_variant(variant_name: str) -> str:  # REQ-1075
    """Map a Lemon Squeezy variant name to a plan tier. The variant name is matched
    case-insensitively against the known plan tiers. An unrecognized variant is an
    error, never a silent default (REQ-1075)."""
    name = (variant_name or "").lower()
    plan = next((p for p in ("trial", "starter", "pro") if p in name), None)
    if plan is None:
        raise ValueError(f"Unrecognized Lemon Squeezy variant name: {variant_name!r}")
    return plan


@dataclass
class Tenant:  # REQ-073, REQ-074, REQ-1075
    id: uuid.UUID
    kms_key_arn: str
    ls_customer_id: str | None
    plan: Plan
    source_limit: int
    created_at: datetime
