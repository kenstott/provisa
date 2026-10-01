# Copyright (c) 2026 Kenneth Stott
# Canary: 2d8f4b61-7c3e-4a95-b1d0-6e9a3c5f8b24
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""ASGI entry point identical to ``main:app`` minus the REQ-1867 ``uvloop.install()``.

A test that must run the real server on asyncio's own event loops launches
``uvicorn tests.integration.stdlib_loop_app:app --loop asyncio``; ``main:app`` installs uvloop
unconditionally on every non-Windows host, so it cannot serve that case."""

from provisa.api.app import create_app

app = create_app()
