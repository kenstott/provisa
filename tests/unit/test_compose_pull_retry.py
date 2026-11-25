# Copyright (c) 2026 Kenneth Stott
# Canary: b6051b4e-8db0-4448-b790-a2388c9bad4d
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Unit tests for tests/compose_pull_retry.py using a fake subprocess runner."""

import subprocess

import pytest

from tests.compose_pull_retry import pull_images_with_retry


def _make_runner(outcomes: list[tuple[int, str]]):
    """Return a fake runner that cycles through (returncode, stderr) pairs."""
    calls: list[list] = []
    outcomes_iter = iter(outcomes)

    def runner(cmd, *, cwd=None, env=None, capture_output=False, text=False):
        calls.append(list(cmd))
        rc, stderr = next(outcomes_iter)
        return subprocess.CompletedProcess(cmd, rc, stdout="", stderr=stderr)

    runner.calls = calls  # type: ignore[attr-defined]
    return runner


_CMD = ["docker", "compose", "-p", "test-project", "-f", "docker-compose.test.yml"]


class TestPullSucceedsFirstAttempt:
    def test_no_retry_on_success(self):
        runner = _make_runner([(0, "")])
        pull_images_with_retry(_CMD, _runner=runner, backoff_seconds=0)
        assert runner.calls == [[*_CMD, "pull"]]


class TestRetryOnTruncatedDownload:
    def test_retries_unexpected_eof_and_succeeds(self):
        """A truncated pull (unexpected EOF) is retried and eventually succeeds."""
        runner = _make_runner(
            [
                (1, "error response from daemon: unexpected EOF"),
                (1, "short read: expected 210511838 bytes but got 14680064: unexpected EOF"),
                (0, ""),
            ]
        )
        pull_images_with_retry(_CMD, _runner=runner, max_attempts=3, backoff_seconds=0)
        assert len(runner.calls) == 3

    def test_fails_by_name_after_max_attempts(self):
        """After exhausting all attempts on a transient error, RuntimeError names the error."""
        truncated_stderr = "short read: expected 210511838 bytes but got 14680064: unexpected EOF"
        runner = _make_runner(
            [
                (1, truncated_stderr),
                (1, truncated_stderr),
                (1, truncated_stderr),
            ]
        )
        with pytest.raises(RuntimeError) as exc_info:
            pull_images_with_retry(_CMD, _runner=runner, max_attempts=3, backoff_seconds=0)

        assert "3 attempts" in str(exc_info.value)
        assert "unexpected EOF" in str(exc_info.value) or "short read" in str(exc_info.value)
        assert len(runner.calls) == 3

    def test_connection_reset_is_retried(self):
        runner = _make_runner(
            [
                (1, "read: connection reset by peer"),
                (0, ""),
            ]
        )
        pull_images_with_retry(_CMD, _runner=runner, max_attempts=3, backoff_seconds=0)
        assert len(runner.calls) == 2


class TestNoRetryOnNonPullError:
    def test_non_transient_error_raises_immediately(self):
        """An error that is not a download failure is not retried."""
        runner = _make_runner(
            [
                (
                    1,
                    "pull access denied for private.example.com/image:tag, repository does not exist or may require 'docker login'",
                ),
            ]
        )
        with pytest.raises(subprocess.CalledProcessError):
            pull_images_with_retry(_CMD, _runner=runner, max_attempts=3, backoff_seconds=0)

        # Only one attempt — no retry for auth/non-transient errors.
        assert len(runner.calls) == 1

    def test_missing_image_raises_immediately(self):
        runner = _make_runner(
            [
                (1, "manifest unknown: manifest unknown"),
            ]
        )
        with pytest.raises(subprocess.CalledProcessError):
            pull_images_with_retry(_CMD, _runner=runner, max_attempts=3, backoff_seconds=0)

        assert len(runner.calls) == 1


# Requirements: REQ-1742
class TestPullNamesUpServices:
    def test_pull_cmd_includes_named_services(self):
        """pull command lists exactly the services passed — no extra images are pulled."""
        runner = _make_runner([(0, "")])
        services = ["kafka", "schema-registry", "postgres"]
        pull_images_with_retry(_CMD, services=services, _runner=runner, backoff_seconds=0)
        assert runner.calls == [[*_CMD, "pull", *services]]

    def test_pull_cmd_excludes_services_not_in_up(self):
        """Services absent from the up list must not appear in the pull command."""
        runner = _make_runner([(0, "")])
        services = ["postgres", "redis"]
        pull_images_with_retry(_CMD, services=services, _runner=runner, backoff_seconds=0)
        issued = runner.calls[0]
        # Only "pull" and the named services — no other service names
        assert issued == [*_CMD, "pull", "postgres", "redis"]
        assert "kafka" not in issued
        assert "cassandra" not in issued

    def test_services_preserved_across_retries(self):
        """Service names appear in every retry attempt."""
        runner = _make_runner(
            [
                (1, "unexpected EOF"),
                (0, ""),
            ]
        )
        services = ["trino", "minio"]
        pull_images_with_retry(
            _CMD, services=services, _runner=runner, max_attempts=3, backoff_seconds=0
        )
        for call in runner.calls:
            assert call == [*_CMD, "pull", *services]
