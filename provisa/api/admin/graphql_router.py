# Copyright (c) 2026 Kenneth Stott
# Canary: 0fc9f68e-4f4c-46a4-a390-78e8d8885753
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The admin GraphQL API's router: a resolver's error told in its 200 as a client is told one
(REQ-2087).

Strawberry answers an operation whose resolver raised with a 200 carrying ``data`` and ``errors``,
past the error-response middleware; its own router writes each error's text as it is."""

# Requirements: REQ-2087

from __future__ import annotations

from typing import Any, cast

from strawberry.fastapi import GraphQLRouter
from strawberry.http import GraphQLHTTPResponse

from provisa.api.graphql_answer import graphql_body


class AdminGraphQLRouter(GraphQLRouter):
    async def process_result(self, request: Any, result: Any) -> GraphQLHTTPResponse:
        return cast(GraphQLHTTPResponse, graphql_body(result))
