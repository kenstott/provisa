# Copyright (c) 2026 Kenneth Stott
# Canary: 8439eb3e-e658-446c-b44c-421e4819f63b
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A GraphQL answer's body, its errors told as a client is told them (REQ-2087).

An answer that carries both data and errors is sent with a 200, past the error-response middleware,
so each error -- its message and its extensions -- is told where the body is built. The data is a
result and is never redacted."""

# Requirements: REQ-2087

from __future__ import annotations

from typing import Any

from graphql import ExecutionResult

from provisa.api.engine_error_names import client_value


def graphql_body(result: ExecutionResult) -> dict[str, Any]:
    """``{"data", "errors", "extensions"}`` for ``result``, as graphql-core formats each, each
    error told through client_text."""
    body: dict[str, Any] = {"data": result.data}
    if result.errors:
        body["errors"] = client_value([err.formatted for err in result.errors])
    if result.extensions:
        body["extensions"] = result.extensions
    return body
