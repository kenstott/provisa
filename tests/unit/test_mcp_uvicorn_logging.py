# Copyright (c) 2026 Kenneth Stott
# Canary: 7b3e9d21-4c58-4f0a-9e16-d2a85c70b3f4
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The MCP server's in-thread uvicorn must not reset the main server's uvicorn loggers."""

from __future__ import annotations

import logging

import pytest

from provisa.api.mcp.server import _uvicorn_config

_LOGGERS = ("uvicorn.error", "uvicorn.access", "uvicorn.asgi")


async def _app(scope, receive, send):  # pragma: no cover - never served
    raise AssertionError("not served")


@pytest.fixture
def uvicorn_levels():
    saved = {n: logging.getLogger(n).level for n in _LOGGERS}
    yield
    for n, lvl in saved.items():
        logging.getLogger(n).setLevel(lvl)


def test_mcp_uvicorn_config_leaves_logger_levels_unchanged(uvicorn_levels):
    logging.getLogger("uvicorn.error").setLevel(logging.INFO)
    logging.getLogger("uvicorn.access").setLevel(logging.INFO)
    logging.getLogger("uvicorn.asgi").setLevel(logging.DEBUG)

    _uvicorn_config(_app, 0, {})

    assert logging.getLogger("uvicorn.error").level == logging.INFO
    assert logging.getLogger("uvicorn.access").level == logging.INFO
    assert logging.getLogger("uvicorn.asgi").level == logging.DEBUG
