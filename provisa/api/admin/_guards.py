# Copyright (c) 2026 Kenneth Stott
# Canary: 54daaafa-4459-431a-a486-f69abff1234e
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Fail-closed request guards for admin resolvers."""

from __future__ import annotations

from typing import Any


def require_active_org_id(request: Any) -> str:
    """Return request.state.active_org_id, or raise if unset.

    Tenant isolation depends on this filter — there is no default org, since an
    unset value would leak cross-tenant rows. Callers must have it set upstream.
    """
    org = getattr(request.state, "active_org_id", None)
    if not org:
        raise ValueError("active_org_id is not set on request.state")
    return org
