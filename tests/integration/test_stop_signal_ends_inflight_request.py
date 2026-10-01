# Copyright (c) 2026 Kenneth Stott
# Canary: 9c4a7d25-1e8b-4f63-a2d9-6b0f3e5c7a18
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A real worker stops on SIGTERM while a request is inside a statement run that cannot finish
(REQ-1882, REQ-1905).

Observed on the benchmark VM: a /data/sql request upserting a 20M-row replica one row at a time
held its worker; SIGTERM was ignored because the server's shutdown waits for in-flight requests
and the request thread never learned the process was stopping. Here a real uvicorn process
serves ``stop_signal_app``; the control run (no stop-signal expiry) shows the worker outliving
the signal, the real run shows it exiting and the request failing with a clear error."""

# Requirements: REQ-1882, REQ-1905

from __future__ import annotations

import os
import signal
import socket
import subprocess
import sys
import threading
import time
from pathlib import Path

import httpx
import pytest

pytestmark = [pytest.mark.integration]

_REPO = Path(__file__).resolve().parents[2]


def _free_port() -> int:
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


class _Worker:
    def __init__(self, *, stop_expiry: bool) -> None:
        self.port = _free_port()
        env = {**os.environ, "PYTHONPATH": str(_REPO)}
        env.pop("PROVISA_TEST_NO_STOP_EXPIRY", None)
        if not stop_expiry:
            env["PROVISA_TEST_NO_STOP_EXPIRY"] = "1"
        self.proc = subprocess.Popen(
            [
                sys.executable,
                "-m",
                "uvicorn",
                "tests.integration.stop_signal_app:app",
                "--host",
                "127.0.0.1",
                "--port",
                str(self.port),
                "--log-level",
                "warning",
            ],
            cwd=_REPO,
            env=env,
        )
        self.base = f"http://127.0.0.1:{self.port}"
        self.response: list[object] = []

    def wait_serving(self) -> None:
        deadline = time.monotonic() + 30
        while time.monotonic() < deadline:
            try:
                if httpx.get(f"{self.base}/state", timeout=1).status_code == 200:
                    return
            except httpx.HTTPError:
                time.sleep(0.1)
        raise AssertionError("the worker did not start serving")

    def start_slow_request(self) -> threading.Thread:
        def _call() -> None:
            try:
                self.response.append(httpx.get(f"{self.base}/slow", timeout=120))
            except httpx.HTTPError as exc:
                self.response.append(exc)

        thread = threading.Thread(target=_call, daemon=True)
        thread.start()
        deadline = time.monotonic() + 30
        while time.monotonic() < deadline:
            if httpx.get(f"{self.base}/state", timeout=5).json()["statements"] > 5:
                return thread
            time.sleep(0.05)
        raise AssertionError("the slow request never started issuing statements")

    def kill(self) -> None:
        if self.proc.poll() is None:
            self.proc.kill()  # the process this test started
        self.proc.wait(10)


def test_a_worker_inside_a_request_that_cannot_finish_stops_on_sigterm():
    worker = _Worker(stop_expiry=True)
    try:
        worker.wait_serving()
        request = worker.start_slow_request()
        worker.proc.send_signal(signal.SIGTERM)
        try:
            worker.proc.wait(20)
        except subprocess.TimeoutExpired:
            pytest.fail("the worker was still running 20 s after SIGTERM")
        request.join(10)
        assert worker.response, "the in-flight request never got an answer"
        answer = worker.response[0]
        assert isinstance(answer, httpx.Response) and answer.status_code == 500, answer
    finally:
        worker.kill()


def test_without_the_stop_signal_expiry_the_worker_outlives_sigterm():
    """The control: the same worker and request with the expiry not chained. The signal is
    received and the worker keeps running, held by the request."""
    worker = _Worker(stop_expiry=False)
    try:
        worker.wait_serving()
        worker.start_slow_request()
        worker.proc.send_signal(signal.SIGTERM)
        with pytest.raises(subprocess.TimeoutExpired):
            worker.proc.wait(6)
        assert worker.response == []
    finally:
        worker.kill()
