# Copyright (c) 2026 Kenneth Stott
# Canary: b02798e5-fe0c-4264-bb65-ec0580deb25d
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Source adapter for OpenAPI sources — thin dispatch shim over provisa.openapi.executor."""

from __future__ import annotations

# Requirements: REQ-314, REQ-316, REQ-317, REQ-318, REQ-319, REQ-320

from provisa.openapi.executor import _build_auth_headers, execute, fetch

__all__ = ["_build_auth_headers", "execute", "fetch"]
