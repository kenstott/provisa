# Copyright (c) 2026 Kenneth Stott
# Canary: be5aefb1-047c-45bf-bbd3-3d7280b5f906
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Retry ``docker compose pull`` before a stack is brought up.

A registry download that is cut off mid-stream (truncated read, unexpected EOF,
connection reset) leaves compose with a corrupted layer.  ``compose up`` does
NOT re-pull a layer it thinks it already has, so the broken image causes the
start-up to fail.  Pulling explicitly before ``up``, and retrying only the pull
on download errors, recovers the common transient case without hiding real
failures.

Usage::

    pull_images_with_retry(
        compose_cmd=["docker", "compose", *args],
        cwd=repo_root,
        env=env_dict,          # or None for inherited
    )
    # Then run compose up as usual.
"""

import subprocess
import time
from typing import Callable

# Errors that indicate a partial / truncated download.  Compose prints the
# docker-daemon message in its stderr; we match a substring.
_PULL_TRANSIENT_NEEDLES = (
    "unexpected EOF",
    "short read",
    "connection reset by peer",
    "i/o timeout",
    "TLS handshake timeout",
    "net/http: TLS handshake timeout",
    "context deadline exceeded",
    "read: connection reset",
    "unexpected end of JSON input",
)

_DEFAULT_MAX_ATTEMPTS = 3
_DEFAULT_BACKOFF_SECONDS = 5


def _is_pull_transient(output: str) -> bool:
    low = output.lower()
    return any(needle.lower() in low for needle in _PULL_TRANSIENT_NEEDLES)


def pull_images_with_retry(
    compose_cmd: list[str],
    cwd: str | None = None,
    env: dict | None = None,
    max_attempts: int = _DEFAULT_MAX_ATTEMPTS,
    backoff_seconds: float = _DEFAULT_BACKOFF_SECONDS,
    *,
    _runner: Callable[..., subprocess.CompletedProcess] | None = None,
) -> None:
    """Run ``<compose_cmd> pull`` and retry on transient download errors.

    Parameters
    ----------
    compose_cmd:
        The full base command up to and including the compose file arguments,
        e.g. ``["docker", "compose", "-p", project, "-f", file]``.
    cwd:
        Working directory for the subprocess.
    env:
        Environment for the subprocess.  ``None`` inherits the current process
        environment.
    max_attempts:
        How many pull attempts to make before giving up.  Must be >= 1.
    backoff_seconds:
        Seconds to wait between attempts.
    _runner:
        Injected for unit tests.  Defaults to ``subprocess.run``.
    """
    runner = _runner if _runner is not None else subprocess.run

    pull_cmd = [*compose_cmd, "pull"]
    last_stderr = ""
    for attempt in range(1, max_attempts + 1):
        result: subprocess.CompletedProcess = runner(
            pull_cmd,
            cwd=cwd,
            env=env,
            capture_output=True,
            text=True,
        )
        if result.returncode == 0:
            return

        combined = result.stdout + result.stderr
        print(
            f"[compose pull] attempt {attempt}/{max_attempts} failed "
            f"(rc={result.returncode}):\n{combined.rstrip()}"
        )
        last_stderr = combined

        if not _is_pull_transient(combined):
            # Not a download error — retrying will not help.
            raise subprocess.CalledProcessError(
                result.returncode,
                pull_cmd,
                output=result.stdout,
                stderr=result.stderr,
            )

        if attempt < max_attempts:
            time.sleep(backoff_seconds)

    raise RuntimeError(
        f"docker compose pull failed after {max_attempts} attempts. "
        f"Last error:\n{last_stderr.rstrip()}"
    )
