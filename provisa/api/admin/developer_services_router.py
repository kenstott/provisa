# Copyright (c) 2026 Kenneth Stott
# Canary: 0e6b2f94-7c1d-4a58-b3e9-8d4a1c6f2e70
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A dev-mode code source's services, reached by its developer's personal access token
(REQ-2106, amended 2026-11-22; REQ-2112).

``/admin/orgs/{org_id}/code-sources/{source_id}/services/{token}/{service}/{call}``: the address
the VS Code extension forwards as ``$PROVISA_SERVICES``, with what the module appends. The token
is in the path, since the module sends no header, so the auth middleware lets this path through
without a credential and :mod:`provisa.codesource.developer_services` resolves it here.
"""

# Requirements: REQ-2106, REQ-2112
from __future__ import annotations

import asyncio
import concurrent.futures
import contextvars
import functools

from fastapi import APIRouter, Request, Response

from provisa.codesource import developer_services, services

router = APIRouter(prefix="/admin/orgs/{org_id}/code-sources/{source_id}/services", tags=["admin"])

#: A call is answered as the worker's proxy answers it, on a thread of its own: the services run
#: their coroutines on a connection loop they check out (provisa.core.connection_loop), which a
#: thread already running the request's loop cannot.
_answering = concurrent.futures.ThreadPoolExecutor(
    max_workers=4, thread_name_prefix="developer-services"
)

_METHODS = ["GET", "POST", "PUT", "DELETE"]


def _state():
    from provisa.api.app import state

    return state


@router.api_route("/{token}/{rest:path}", methods=_METHODS, include_in_schema=False)
async def call(request: Request, org_id: str, source_id: str, token: str, rest: str) -> Response:
    from provisa.core.request_context import reset_current_org, set_current_org

    service, _, sub = rest.partition("/")
    bound = set_current_org(org_id)
    try:
        state = _state()
        try:
            grant, developer = await developer_services.resolve(state, org_id, source_id, token)
        except services.ServiceRefused as refused:
            return _response(services.refusal(service, refused))
        body = await request.body()
        answered = functools.partial(
            services.answer_grant, grant, request.method, service, sub, body, developer.role
        )
        status, kind, out = await asyncio.wrap_future(
            _answering.submit(contextvars.copy_context().run, answered)
        )
        await developer_services.audit(org_id, source_id, developer, service, sub, status)
        return _response((status, kind, out))
    finally:
        reset_current_org(bound)


def _response(answered: tuple[int, str, bytes]) -> Response:
    status, kind, out = answered
    return Response(content=out, status_code=status, media_type=kind)
