# Copyright (c) 2026 Kenneth Stott
# Canary: 7c1e4b52-93a0-4f6e-8d27-5b1f0a6c3e94
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A Bun source's built image (REQ-1989, REQ-2100): the front end hands Bun its connections
already connected, and Bun sizes its thread pool once, as it starts. Runs the front end with the
pinned Bun as its worker, started as an image starts it."""

# Requirements: REQ-1989, REQ-2100

from __future__ import annotations

import os
import queue
import subprocess
import sys
import threading
from concurrent.futures import ThreadPoolExecutor

import pytest

from provisa.codehost import front
from provisa.codesource import remote
from provisa.core import request_deadline
from provisa.node.install import code_command
from provisa.node.worker import HOST
from provisa.runtime_deps.code_runtimes import CodeRuntimeResolver

pytestmark = [pytest.mark.integration, pytest.mark.local_platform]

BOUNDS = {"memory_mb": 256, "threads": 2, "spill_cap_mb": 1024}
_CODE = """
export async function held(n) {
  await new Promise((resolve) => setTimeout(resolve, 1000));
  return n;
}
"""
#: How long the front end has to start, and each call to be answered: a hang fails by name.
_WITHIN_S = 30


@pytest.fixture
def started(tmp_path):
    """The front end on a free port with Bun as its worker, Bun's cores held to two (GOMAXPROCS
    stands in for them); its port and pairing code."""
    (tmp_path / "index.mjs").write_text(_CODE)
    names = tmp_path / "names.json"
    names.write_text("{}")
    bun = CodeRuntimeResolver(cache_root=tmp_path / "runtime-deps").resolve("bun")
    env = {**os.environ, "PROVISA_CODE_HASH": "h1", "GOMAXPROCS": "2"}
    env.pop("UV_THREADPOOL_SIZE", None)  # as an image starts: nothing has set it
    process = subprocess.Popen(  # noqa: S603 -- the pinned Bun and Provisa's front end
        [
            sys.executable,
            front.__file__,
            "0",
            str(tmp_path / "state"),
            str(tmp_path / "spill"),
            str(names),
            *code_command("bun", bun),
            str(HOST),
            "{fd}",
            str(tmp_path / "index.mjs"),
        ],
        env=env,
        stderr=subprocess.PIPE,
        text=True,
    )
    stderr = process.stderr
    assert stderr is not None
    lines: queue.Queue[str] = queue.Queue()
    threading.Thread(target=lambda: [lines.put(ln) for ln in stderr], daemon=True).start()
    pairing, port = None, None
    try:
        while port is None:
            line = lines.get(timeout=_WITHIN_S)
            if line.startswith("pairing code: "):
                pairing = line.removeprefix("pairing code: ").strip()
            if line.startswith("remote host listening on "):
                port = int(line.rsplit(" ", 1)[1])
        assert pairing is not None, "the host printed no pairing code"
        yield port, pairing
    finally:
        process.kill()
        process.wait(timeout=10)


def test_every_connection_of_a_bun_image_is_served_on_a_host_with_few_cores(started):
    """Each inherited connection's read holds one of Bun's pool threads while it waits: with fewer
    threads than connections, a call on a connection no thread is reading is never heard. Every
    one of the front end's connections to Bun is in use at once."""
    port, pairing = started
    identity = remote.new_identity()
    pin = remote.pair("127.0.0.1", port, pairing, identity, BOUNDS).pin
    worker = remote.RemoteWorker(
        "shop", "h1", address="127.0.0.1", port=port, pin=pin, identity=identity
    )
    worker.start()

    def call(n: int) -> int:
        with request_deadline.within(_WITHIN_S):
            return worker.command("held", [n])

    try:
        with ThreadPoolExecutor(front.CONNECTIONS) as pool:
            answers = list(pool.map(call, range(front.CONNECTIONS)))
        assert answers == list(range(front.CONNECTIONS))
    finally:
        worker.stop()
