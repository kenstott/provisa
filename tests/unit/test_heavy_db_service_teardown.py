# Copyright (c) 2026 Kenneth Stott
# Canary: 0e966c09-a4bf-4f29-a22a-14a50300e074
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Regression test: _heavy_db_service must tear down containers even when setup fails.

Root cause: subprocess.run(docker compose up, check=True) was placed OUTSIDE the
try/yield block. CalledProcessError from a failed `up` (e.g. OOM exit 137 from Druid)
propagated before `try:` was entered, so the `finally:` cleanup (`docker compose rm`)
never ran. Crash-looping containers starved core services of memory.
"""

import subprocess
from unittest.mock import MagicMock, patch


# ── helpers ────────────────────────────────────────────────────────────────────


def _run_fixture_with_marker(marker_name: str, up_side_effect):
    """Drive the _heavy_db_service generator with a fake request node.

    Returns the list of subprocess.run calls that were made.
    """
    from tests.conftest import _heavy_db_service

    calls_made: list = []

    def mock_run(cmd, **kwargs):
        calls_made.append(cmd)
        if "up" in cmd and callable(up_side_effect):
            up_side_effect(cmd, **kwargs)
        mock_result = MagicMock()
        mock_result.returncode = 0
        return mock_result

    # Build a minimal fake request that reports the desired marker
    marker = MagicMock()
    marker.name = marker_name
    node = MagicMock()
    node.get_closest_marker = lambda m: marker if m == marker_name else None
    request = MagicMock()
    request.node = node

    gen = (
        _heavy_db_service.__wrapped__(request)
        if hasattr(_heavy_db_service, "__wrapped__")
        else _heavy_db_service(request)
    )

    with patch("tests.conftest.subprocess.run", side_effect=mock_run):
        try:
            next(gen)  # run setup up to yield
        except StopIteration:
            pass
        except subprocess.CalledProcessError:
            pass  # setup failure — teardown should still have run
        else:
            # yield point reached — advance past it
            try:
                gen.throw(RuntimeError("simulated test body failure"))
            except (RuntimeError, StopIteration):
                pass

    return calls_made


# ── tests ──────────────────────────────────────────────────────────────────────


class TestHeavyDbServiceTeardownOnSetupFailure:
    """Teardown must run even when docker compose up raises CalledProcessError."""

    def test_rm_called_when_up_raises(self):
        """When docker compose up fails, docker compose rm must still be called."""

        def up_fails(cmd, **_kwargs):
            if "up" in cmd:
                raise subprocess.CalledProcessError(1, cmd)

        calls = _run_fixture_with_marker("requires_druid", up_fails)

        rm_calls = [c for c in calls if "rm" in c]
        assert rm_calls, (
            "_heavy_db_service did not call 'docker compose rm' after 'docker compose up' failed. "
            "Crash-looping containers are left running and starve core services."
        )

    def test_rm_called_when_up_succeeds(self):
        """Sanity: teardown also runs on success."""
        calls = _run_fixture_with_marker("requires_druid", None)

        up_calls = [c for c in calls if "up" in c]
        rm_calls = [c for c in calls if "rm" in c]
        assert up_calls, "expected docker compose up to be called"
        assert rm_calls, "expected docker compose rm to be called after successful test"
