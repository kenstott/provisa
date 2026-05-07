# Copyright (c) 2026 Kenneth Stott
# Canary: b07c8d9e-0f1a-2345-6789-012345678901
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Integration tests for Apollo Federation v2 subgraph endpoint.

All pure-logic federation tests (TestFederationSDL, TestExtractPKColumns,
TestEntityResolution, TestFederationEndpoint) have been moved to
tests/unit/test_federation.py — they require no infrastructure.

Integration tests requiring live services (e.g., PG-backed entity resolution
with a running postgres instance) belong here when implemented.
"""

from __future__ import annotations

import pytest

pytestmark = [pytest.mark.integration, pytest.mark.asyncio(loop_scope="session")]
