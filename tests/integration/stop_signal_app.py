# Copyright (c) 2026 Kenneth Stott
# Canary: 2b7f4e91-6c3d-4a58-9e07-d1a5c8f3b624
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""ASGI app for ``test_stop_signal_ends_inflight_request``: one request that cannot finish.

``/slow`` runs on its own thread (a sync endpoint) inside a 10-minute request deadline and issues
short statements one after another, each registered with the deadline as a driver statement is.
The lifespan chains the stop-signal expiry exactly as the application's does, unless
``PROVISA_TEST_NO_STOP_EXPIRY`` is set — the control run."""

from __future__ import annotations

import os
import time
from contextlib import asynccontextmanager

from fastapi import FastAPI

from provisa.core import request_deadline

_STATE = {"statements": 0}


@asynccontextmanager
async def _lifespan(_app: FastAPI):
    if os.environ.get("PROVISA_TEST_NO_STOP_EXPIRY"):
        yield
        return
    with request_deadline.expire_on_stop_signals():
        yield


app = FastAPI(lifespan=_lifespan)


@app.get("/state")
def state() -> dict:
    return dict(_STATE)


@app.get("/slow")
def slow() -> dict:
    with request_deadline.within(600.0):
        while True:
            with request_deadline.cancel_on_deadline(lambda: None):
                time.sleep(0.02)
            _STATE["statements"] += 1
