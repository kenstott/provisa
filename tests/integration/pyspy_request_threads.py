# Copyright (c) 2026 Kenneth Stott
# Canary: 1f7b3c95-6a2d-4e08-8c41-9d5e2f0a7b16
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Live py-spy sample of the request threads (REQ-1882). NOT part of the suite: the file name
keeps it out of collection, because it needs passwordless sudo for py-spy. Run it by path:

    PROVISA_PYSPY_DUMP_DIR=<dir> pytest tests/integration/pyspy_request_threads.py

It writes ``py-spy dump`` output of the real server while 10 requests of one transport are in
flight, for reading alongside ``test_request_thread_exclusivity_e2e``'s trace assertions."""

from __future__ import annotations

import os
import subprocess
import threading
import time
from pathlib import Path

import pytest

from tests.integration.test_request_thread_exclusivity_e2e import (  # noqa: F401  (fixture)
    _TRANSPORTS,
    server,
)

pytestmark = [pytest.mark.integration]

_PY_SPY = "/Users/kennethstott/homebrew/bin/py-spy"


@pytest.mark.parametrize("name", list(_TRANSPORTS))
def test_py_spy_dump_under_concurrent_load(server, name):  # noqa: F811  (the fixture)
    call, base, _entry = _TRANSPORTS[name]
    out = Path(os.environ["PROVISA_PYSPY_DUMP_DIR"])
    out.mkdir(parents=True, exist_ok=True)
    stop = threading.Event()
    errors: list[BaseException] = []

    def _client(tag: int) -> None:
        try:
            while not stop.is_set():
                call(server, tag)
        except BaseException as exc:  # reported by the assertion below with the real cause
            errors.append(exc)

    threads = [threading.Thread(target=_client, args=(base + 50 + i,)) for i in range(10)]
    for t in threads:
        t.start()
    try:
        pid = server.srv._proc.pid  # noqa: SLF001 - the harness owns this process
        for i in range(3):
            time.sleep(1.5)
            dump = subprocess.run(
                ["sudo", "-n", _PY_SPY, "dump", "--pid", str(pid)],
                capture_output=True,
                text=True,
                timeout=120,
                check=False,
            )
            (out / f"{name}-{i}.txt").write_text(dump.stdout + dump.stderr)
            assert dump.returncode == 0, dump.stderr
    finally:
        stop.set()
        for t in threads:
            t.join(timeout=180)
    assert not errors, errors
