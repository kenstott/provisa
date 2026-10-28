# Copyright (c) 2026 Kenneth Stott
# Canary: 0da1aafb-3be5-4a71-9458-27566127620e
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The principal's graph session over HTTP (REQ-2176): every Cypher statement a principal sends
here runs in its one graph session (:mod:`provisa.api.graph_session`), as on every other client,
and its answer says when it wrote to it. The Cypher page reads what the session holds and clears
it here."""

# Requirements: REQ-2176

from __future__ import annotations

import json
from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager

from fastapi import APIRouter, Request
from fastapi.responses import JSONResponse, Response

from provisa.api import graph_session
from provisa.api.graph_session import GraphSession

router = APIRouter()


@asynccontextmanager
async def statement(request: Request, query: str) -> AsyncGenerator[GraphSession]:
    """The requesting principal's graph session, bound for one statement ``query``."""
    from provisa.api.app import state
    from provisa.cypher.parser import writes_in

    user_id, claims = graph_session.principal(state, request.state.identity)
    async with graph_session.session_for(state, user_id, claims, writes=writes_in(query)) as held:
        yield held


def answered(response: Response, held: GraphSession) -> Response:
    """The statement's answer, saying it wrote to the graph session when it did."""
    if not held.changed or not isinstance(response, JSONResponse):
        return response
    content = json.loads(bytes(response.body))
    if not isinstance(content, dict):
        return response
    content["session_changed"] = True
    headers = {k: v for k, v in response.headers.items() if k.lower() != "content-length"}
    return JSONResponse(content=content, status_code=response.status_code, headers=headers)


@router.get("/data/cypher/session")
async def describe(request: Request) -> Response:
    """The principal's graph session: its labels and relationship types with their counts, and
    whether it holds nothing."""
    from provisa.api.app import state

    async with statement(request, "") as held:
        return JSONResponse(content=await graph_session.contents(state, held.session))


@router.delete("/data/cypher/session")
async def clear(request: Request) -> Response:
    """Clear the principal's graph session: its objects are dropped."""
    from provisa.api.app import state

    await graph_session.clear(state, request.state.identity.user_id)
    return JSONResponse(content={"cleared": True})
