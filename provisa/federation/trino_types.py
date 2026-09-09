# Copyright (c) 2026 Kenneth Stott
# Canary: 1f6d3b8a-7e2c-4a9f-b4d1-8c5e0a2f7d63
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The Trino client's connection type and exception classes, as one leaf module (REQ-1678).

Callers that only need the connection type or the exception classes — never
``trino.dbapi.connect()`` itself — import them from here, so the compiler's introspection and the
core catalog code stay off the lifecycle module, which reaches the executor and the API layer.
"""

# Requirements: REQ-1678

from __future__ import annotations

import trino

TrinoConnection = trino.dbapi.Connection
TrinoConnectionError = trino.exceptions.TrinoConnectionError
TrinoQueryError = trino.exceptions.TrinoQueryError
TrinoUserError = trino.exceptions.TrinoUserError
TrinoError = trino.exceptions.Error
