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

    # `docker compose config` is not run: each service mounts one named volume, proj_<service>_data.
    with (
        patch("tests.conftest.subprocess.run", side_effect=mock_run),
        patch(
            "tests.conftest._named_volumes",
            side_effect=lambda services: [f"proj_{s}_data" for s in services],
        ),
    ):
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


class TestHeavyDbServiceTeardownRemovesNamedVolumes:
    """A per-test service gets cold volumes: teardown drops the named volumes it mounted.

    Root cause: teardown ran `docker compose rm -fsv`, which drops anonymous volumes only. The
    next test bringing up the same service reused the named volume. Firebird keeps test.fdb on its
    named volume and its security database on the container filesystem, so the second firebird
    test in a session found test.fdb present, skipped the image's FIREBIRD_USER creation, and
    every login as that user was refused.
    """

    def test_named_volumes_removed_after_the_test(self):
        calls = _run_fixture_with_marker("requires_firebird", None)

        volume_rm = [c for c in calls if c[:3] == ["docker", "volume", "rm"]]
        assert volume_rm == [["docker", "volume", "rm", "-f", "proj_firebird_data"]], calls
        rm_index = next(i for i, c in enumerate(calls) if "rm" in c and "compose" in c)
        assert calls.index(volume_rm[0]) > rm_index, "volumes go after their containers"
