# Copyright (c) 2026 Kenneth Stott
# Canary: 0556b6e5-1033-42c1-a2ae-47fc6340913d
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Tests for connection pool exhaustion / graceful degradation.

# NOTE — NO POOL EXHAUSTION / QUEUE-VS-REJECT SOURCE IMPLEMENTATION FOUND
#
# provisa/executor/pool.py implements SourcePool which wraps asyncpg pools
# (and other driver pools). Pool exhaustion behaviour is entirely delegated
# to asyncpg's internal queue; SourcePool itself has no custom:
#   - maximum-wait / rejection policy
#   - "graceful degradation" path
#   - queue-vs-reject configuration knob
#
# The pool lifecycle (add, get, has, close, execute) is already covered by
# tests/unit/test_pool.py (integration tests requiring a live PG instance).
#
# Tests for graceful degradation and configurable queue/reject semantics will
# be added here when corresponding source logic is added to SourcePool or a
# new pool-management module (e.g., provisa/executor/pool_policy.py).
"""

import pytest

# Placeholder: this file intentionally contains no test functions.
# When pool exhaustion handling is implemented, remove this module-level
# skip and add tests below.
pytestmark = pytest.mark.skip(
    reason="Pool exhaustion source implementation not yet present (see module docstring)"
)
