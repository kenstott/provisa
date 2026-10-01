# Copyright (c) 2026 Kenneth Stott
# Canary: 9a4c7e13-6b2d-4f58-8e01-3d7f5a9c2b64
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""No HTTP request is handed to Starlette's shared worker pool (REQ-1882).

A request runs on its own thread, and a request loop's default executor runs blocking work inline
there. Starlette's own thread pool (anyio) is a different pool, shared by every request: it is used
for a route handler declared with a plain ``def`` and for ``run_in_threadpool``. Either one moves
the request's work onto a shared worker and leaves the request thread waiting on it."""

# Requirements: REQ-1882

from __future__ import annotations

import ast
from pathlib import Path

import pytest

pytestmark = pytest.mark.unit

_PROVISA = Path(__file__).resolve().parents[2] / "provisa"
_ROUTE_METHODS = {"get", "post", "put", "delete", "patch", "head", "options", "api_route"}
# The OTLP collector (``python -m provisa.observability.otlp2sql``) is its own process with one
# event loop and no per-request threads; it serves no Provisa request. Its insert hop is justified
# inline at the call.
_NOT_A_PROVISA_SERVER = {"provisa/observability/otlp2sql.py"}


def _sources():
    for path in sorted(_PROVISA.rglob("*.py")):
        yield path.relative_to(_PROVISA.parent), ast.parse(path.read_text())


def test_no_route_handler_is_a_plain_def():
    found = []
    for rel, tree in _sources():
        for node in ast.walk(tree):
            if not isinstance(node, ast.FunctionDef):
                continue
            for dec in node.decorator_list:
                func = dec.func if isinstance(dec, ast.Call) else None
                if isinstance(func, ast.Attribute) and func.attr in _ROUTE_METHODS:
                    found.append(f"{rel}:{node.lineno} {node.name}")
    assert not found, f"route handlers Starlette runs on its shared worker pool: {found}"


def test_no_request_path_calls_run_in_threadpool():
    found = []
    for rel, tree in _sources():
        for node in ast.walk(tree):
            if isinstance(node, ast.Call):
                name = getattr(node.func, "id", getattr(node.func, "attr", ""))
                if name in ("run_in_threadpool", "iterate_in_threadpool"):
                    if str(rel) not in _NOT_A_PROVISA_SERVER:
                        found.append(f"{rel}:{node.lineno}")
    assert not found, f"request work handed to Starlette's shared worker pool: {found}"
