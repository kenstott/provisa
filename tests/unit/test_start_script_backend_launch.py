# Copyright (c) 2026 Kenneth Stott
# Canary: 2c9e4a17-6d0b-4f3a-8e25-71b4c6d8f302
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.
"""start-ui-install.sh's backend launch ends when the backend cannot start.

The launch command is cut out of the script and run on a throwaway app in a temp directory, on a
free loopback port: the wait loop's only crash check is whether that process is still alive.
"""

from __future__ import annotations

import os
import re
import signal
import socket
import subprocess
import sys
import time
import urllib.error
import urllib.request
from collections.abc import Generator
from contextlib import contextmanager
from pathlib import Path

import pytest

_REPO = Path(__file__).resolve().parents[2]
_TEXT = (_REPO / "start-ui-install.sh").read_text()

_FAILS_IN_LIFESPAN = """
from contextlib import asynccontextmanager
from starlette.applications import Starlette

@asynccontextmanager
async def lifespan(app):
    raise RuntimeError("boom at startup")
    yield

app = Starlette(lifespan=lifespan)
"""

_FAILS_ON_IMPORT = 'raise RuntimeError("boom at import")\n'

_SERVES = """
from starlette.applications import Starlette
from starlette.responses import PlainTextResponse
from starlette.routing import Route

async def health(request):
    return PlainTextResponse("ok")

app = Starlette(routes=[Route("/health", health)])
"""


def _launch_command() -> str:
    """The backend launch from start_backend(), with no redirect, on the running interpreter."""
    match = re.search(
        r'env "\$\{_BACKEND_ENV\[@\]\}" \\\n(.*?\$\{_bind_args\[@\]\}")', _TEXT, re.DOTALL
    )
    assert match, "start_backend()'s launch command not found"
    bin_dir = str(Path(sys.executable).parent)
    return match.group(1).replace("$SCRIPT_DIR/.venv/bin", bin_dir)


def _free_port() -> int:
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


@contextmanager
def _launched(
    app_dir: Path, serve_args: str, port: int
) -> Generator[subprocess.Popen[bytes], None, None]:
    program = (
        f'SCRIPT_DIR="{_REPO}"\n'
        f"_serve_args=( {serve_args} )\n"
        f"_bind_args=( --host 127.0.0.1 --port {port} )\n"
        f"exec {_launch_command()}\n"
    )
    log = (app_dir / "server.log").open("wb")
    process = subprocess.Popen(
        ["bash", "-c", program],
        cwd=app_dir,
        stdout=log,
        stderr=log,
        start_new_session=True,
    )
    try:
        yield process
    finally:
        try:
            os.killpg(process.pid, signal.SIGKILL)
        except ProcessLookupError:
            pass  # it has already ended, the outcome most of these tests expect
        process.wait()
        log.close()


def _healthy(port: int) -> bool:
    try:
        with urllib.request.urlopen(f"http://127.0.0.1:{port}/health", timeout=2) as response:
            return response.status == 200
    except (urllib.error.URLError, OSError):
        return False


def _wait_healthy(port: int, process: subprocess.Popen[bytes]) -> None:
    deadline = time.monotonic() + 30
    while time.monotonic() < deadline:
        assert process.poll() is None, "the launch ended while it should be serving"
        if _healthy(port):
            return
        time.sleep(0.2)
    raise AssertionError("the backend never answered /health")


@pytest.mark.parametrize(
    "source", [_FAILS_IN_LIFESPAN, _FAILS_ON_IMPORT], ids=["lifespan", "import"]
)
def test_a_failed_start_under_reload_ends_the_launch_non_zero(tmp_path: Path, source: str) -> None:
    (tmp_path / "main.py").write_text(source)
    with _launched(tmp_path, f'--reload --reload-dir "{tmp_path}"', _free_port()) as process:
        assert process.wait(timeout=30) != 0


def test_a_failed_start_under_workers_ends_the_launch(tmp_path: Path) -> None:
    (tmp_path / "main.py").write_text(_FAILS_IN_LIFESPAN)
    with _launched(tmp_path, "--workers 2", _free_port()) as process:
        process.wait(timeout=30)


def test_a_bad_edit_after_a_good_start_keeps_the_reloader_waiting(tmp_path: Path) -> None:
    main = tmp_path / "main.py"
    main.write_text(_SERVES)
    port = _free_port()
    with _launched(tmp_path, f'--reload --reload-dir "{tmp_path}"', port) as process:
        _wait_healthy(port, process)

        deadline = time.monotonic() + 30
        while _healthy(port):
            assert time.monotonic() < deadline, "the reloader never picked up the bad edit"
            # Repeated: StatReload's first scan takes a file's mtime as its baseline, so an edit
            # landing before that scan is never seen.
            main.write_text(_FAILS_IN_LIFESPAN)
            time.sleep(0.5)
        time.sleep(3)  # longer than the reloader's poll: it would have stopped by now
        assert process.poll() is None

        main.write_text(_SERVES)
        _wait_healthy(port, process)
