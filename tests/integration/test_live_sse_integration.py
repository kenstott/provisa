# Copyright (c) 2026 Kenneth Stott
# Canary: c1d2e3f4-a5b6-7890-cdef-012345678abc
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Integration tests for live SSE query engine.

All SSE fanout and engine tests that use mocked PG connections have been
moved to tests/unit/test_live_sse.py — they require no infrastructure.

Integration tests requiring a live PG LISTEN/NOTIFY connection belong here
when implemented.
"""

from __future__ import annotations

import pytest

pytestmark = [pytest.mark.integration, pytest.mark.asyncio(loop_scope="session")]
