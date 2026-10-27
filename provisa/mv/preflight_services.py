# Copyright (c) 2026 Kenneth Stott
# Canary: 2e9b7c41-6f0a-4d58-a1c3-8b5e0f7d4a62
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Provisa's services, in-process, for a Python preflight that imports them (REQ-1165 amended,
REQ-2133).

A preflight that imports ``provisa_services`` gets the module a code source's worker gets -- the same
calls, by the same names -- answered here in the server rather than over ``$PROVISA_SERVICES``: while
the check runs, the module's calls are routed to :func:`provisa.codesource.services.answer` under a
grant issued for the view and revoked when the check returns, so each call is metered and traced as
a code source's is. A code source's sandbox does not apply: the check runs in the server process.

A check runs with an event loop already running on its thread (the refresh's, or the replica
build's own). A service call is synchronous and runs its work on a connection loop, which cannot
start on such a thread, so each call is answered on a thread of its own while the check waits."""

# Requirements: REQ-1165, REQ-2133

from __future__ import annotations

import concurrent.futures
import contextvars
import functools
import importlib.util
import inspect
import sys
import threading
from collections.abc import Callable
from pathlib import Path
from types import ModuleType
from typing import Any

from provisa.mv.preprocess import SERVICES_MODULE

_CLIENT = (
    Path(__file__).resolve().parents[1] / "python" / "services" / SERVICES_MODULE / "__init__.py"
)
_loaded: ModuleType | None = None
_load_lock = threading.Lock()
#: The threads service calls are answered on, a few at once across every running check.
_answering = concurrent.futures.ThreadPoolExecutor(max_workers=4, thread_name_prefix="services")


def services_module() -> ModuleType:
    """The ``provisa_services`` module a preflight imports: the code sources' own client, loaded once
    in this process under its own name."""
    global _loaded
    with _load_lock:
        if _loaded is None:
            spec = importlib.util.spec_from_file_location(SERVICES_MODULE, _CLIENT)
            assert spec is not None and spec.loader is not None
            module = importlib.util.module_from_spec(spec)
            # registered before it runs: its dataclasses resolve their annotations in it
            sys.modules[SERVICES_MODULE] = module
            spec.loader.exec_module(module)
            _loaded = module
        return _loaded


def _transport(token: str) -> Callable[[str, str, bytes | None, dict], tuple[int, bytes]]:
    from provisa.codesource.services import answer

    def send(method: str, path: str, body: bytes | None, headers: dict) -> tuple[int, bytes]:
        call = functools.partial(answer, method, f"/{token}/{path}", body or b"", headers)
        status, _kind, out = _answering.submit(contextvars.copy_context().run, call).result()
        return status, out

    return send


class _Bound:
    """The services bound for one run of a check: a grant for the view, routed to in-process."""

    def __init__(self, subject: str, role: str, product_id: str | None) -> None:
        from provisa.api.app import state
        from provisa.codesource.services import ServiceGrant, issue
        from provisa.core.request_context import require_current_org

        self._token = issue(
            ServiceGrant(
                org_id=require_current_org(),
                source_id=subject,
                admin_db=state.admin_db,
                role=role,
                product_id=product_id,
            )
        )
        self._set = services_module()._transport.set(_transport(self._token))

    def close(self) -> None:
        from provisa.codesource.services import revoke

        services_module()._transport.reset(self._set)
        revoke(self._token)


def bind_services(
    fn: Callable[..., Any], *, subject: str, role: str, product_id: str | None = None
) -> Callable[..., Any]:
    """``fn``, a compiled check that imports ``provisa_services``, run with the services bound:
    dq answering as ``role`` (the role the view's build runs as) and relating checkers to
    ``product_id`` (the view's product). ``subject`` names the view in what the calls record."""
    if inspect.iscoroutinefunction(fn):

        async def checked_async(streams: Any, ctx: Any) -> Any:
            bound = _Bound(subject, role, product_id)
            try:
                return await fn(streams, ctx)
            finally:
                bound.close()

        return checked_async

    def checked(streams: Any, ctx: Any) -> Any:
        bound = _Bound(subject, role, product_id)
        try:
            return fn(streams, ctx)
        finally:
            bound.close()

    return checked


def compile_check(
    source: str | None, *, subject: str, product_id: str | None = None
) -> Callable[..., Any] | None:
    """The compiled check ``source`` (None: no check), with Provisa's services bound when it
    imports them: dq answers as the role every build runs as, the region's administrator."""
    from provisa.core import region_admin
    from provisa.mv.preprocess import compile_preprocess, uses_services

    fn = compile_preprocess(source)
    if fn is None or not uses_services(source):
        return fn
    return bind_services(fn, subject=subject, role=region_admin.ROLE, product_id=product_id)
