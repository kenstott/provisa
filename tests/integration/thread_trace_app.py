# Copyright (c) 2026 Kenneth Stott
# Canary: 4e9c2a70-5b1d-4f86-93a7-1d6f0e8b2c35
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""ASGI entry point: the deployed ``main:app`` (uvloop and all) with the test-only thread tracer.

``main`` is imported first, so the app is built exactly as deployed; the tracer then wraps the
transports' entry points and the thread-hop primitives (``tests.thread_trace``) and writes to
``$PROVISA_TEST_THREAD_TRACE``. Nothing it wraps has run yet — the servers start in the lifespan."""

import os

from main import app  # noqa: F401  (the ASGI app uvicorn serves)

from tests import thread_trace

thread_trace.install(os.environ["PROVISA_TEST_THREAD_TRACE"])
