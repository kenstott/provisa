# Copyright (c) 2026 Kenneth Stott
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.

"""Billing domain models."""

from __future__ import annotations

import uuid
from dataclasses import dataclass
from datetime import datetime
from enum import Enum


class Plan(str, Enum):
    trial = "trial"
    starter = "starter"
    pro = "pro"


PLAN_LIMITS: dict[str, int] = {"trial": 2, "starter": 10, "pro": 100}


@dataclass
class Tenant:
    id: uuid.UUID
    kms_key_arn: str
    stripe_customer_id: str | None
    plan: Plan
    source_limit: int
    created_at: datetime
