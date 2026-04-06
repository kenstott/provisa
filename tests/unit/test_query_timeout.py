# Copyright (c) 2026 Kenneth Stott
# Canary: e61ee5c0-c37d-4c20-92e5-d61e27420d3c
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Tests for query timeout enforcement.

# NOTE — NO DEDICATED QUERY TIMEOUT SOURCE IMPLEMENTATION FOUND
#
# A search of provisa/executor/ (pool.py, direct.py, drivers/postgresql.py,
# drivers/base.py) and provisa/api/data/endpoint.py found no implementation
# of a per-query timeout, asyncio.wait_for wrapper, or REQ-064 fail-fast
# timeout for GraphQL/SQL queries.
#
# Timeouts that DO exist in the codebase are limited to:
#   - HTTP-level request_timeout on the Trino DBAPI connection (app.py:149)
#   - httpx.Timeout on webhook delivery (webhooks/executor.py)
#   - asyncio.wait_for(queue.get(), timeout=30) in subscription streaming
#     (api/data/subscribe.py, subscriptions/pg_provider.py)
#
# None of these constitute a query-level timeout with cancellation and cleanup
# semantics matching the "Timeout fires and cancels the query / Cleanup after
# timeout / Error message format (REQ-064)" requirements.
#
# Tests will be added here when a source implementation lands.
# See: provisa/executor/pool.py, provisa/executor/direct.py
"""

import pytest

# Placeholder: this file intentionally contains no test functions.
# When a query-timeout feature is implemented, remove this module-level
# skip and add tests below.
pytestmark = pytest.mark.skip(
    reason="Query timeout source implementation not yet present (see module docstring)"
)
