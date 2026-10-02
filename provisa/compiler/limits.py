# Copyright (c) 2026 Kenneth Stott
# Canary: 9d8ba2c2-8cfd-4005-9fa0-517c4a79f38c
# Canary: placeholder
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A role's execution time limit (REQ-1174).

A role's ``max_query_time_ms`` caps the wall-time of one request, applied where execution is
wrapped. What a statement may ask for is capped by the complexity guard
(:mod:`provisa.compiler.complexity`), which measures the semantic statement on every surface.
"""

from __future__ import annotations

from typing import Any


def role_max_query_time_ms(role: Any) -> int | None:
    """A role's ``max_query_time_ms``, from the dict a role loads as or the model. None when the
    role sets no limits or not this one."""
    rate_limit = (
        role.get("rate_limit") if isinstance(role, dict) else getattr(role, "rate_limit", None)
    )
    if rate_limit is None:
        return None
    if isinstance(rate_limit, dict):
        return rate_limit.get("max_query_time_ms")
    return rate_limit.max_query_time_ms
