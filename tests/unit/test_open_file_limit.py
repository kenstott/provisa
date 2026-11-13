# Copyright (c) 2026 Kenneth Stott
# Canary: 8b0f6d2a-7c41-4e93-9a1d-3f5e2c7b0d64
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The server raises its soft open-file limit to the hard limit at start (#338): DuckDB opens many
files, and a soft limit of 256 (macOS's default) failed a read of the native traces folder with
"Too many open files"."""

# Requirements: REQ-862

from __future__ import annotations

import logging

import pytest

pytestmark = pytest.mark.unit


class _Resource:
    """The parts of the ``resource`` module the step uses, with a soft and hard limit of its own."""

    RLIMIT_NOFILE = 7
    RLIM_INFINITY = -1

    def __init__(self, soft: int, hard: int) -> None:
        self.limits = (soft, hard)
        self.set_calls: list[tuple[int, tuple[int, int]]] = []

    def getrlimit(self, which: int) -> tuple[int, int]:
        assert which == self.RLIMIT_NOFILE
        return self.limits

    def setrlimit(self, which: int, limits: tuple[int, int]) -> None:
        self.set_calls.append((which, limits))
        self.limits = limits


def test_the_soft_limit_is_raised_to_the_hard_limit_and_logged(monkeypatch, caplog):
    from provisa.core import open_files

    fake = _Resource(256, 10240)
    monkeypatch.setattr(open_files, "resource", fake)
    with caplog.at_level(logging.INFO, logger="test.open_files"):
        open_files.raise_open_file_limit(logging.getLogger("test.open_files"))
    assert fake.set_calls == [(fake.RLIMIT_NOFILE, (10240, 10240))]
    assert fake.limits == (10240, 10240)
    [record] = caplog.records
    assert "256" in record.getMessage() and "10240" in record.getMessage()


def test_an_unlimited_hard_limit_is_named_unlimited(monkeypatch, caplog):
    from provisa.core import open_files

    fake = _Resource(256, _Resource.RLIM_INFINITY)
    monkeypatch.setattr(open_files, "resource", fake)
    with caplog.at_level(logging.INFO, logger="test.open_files"):
        open_files.raise_open_file_limit(logging.getLogger("test.open_files"))
    assert fake.limits == (_Resource.RLIM_INFINITY, _Resource.RLIM_INFINITY)
    [record] = caplog.records
    assert "256" in record.getMessage() and "unlimited" in record.getMessage()


def test_the_app_raises_the_limit_where_it_starts():
    """The step runs at the head of the app's lifespan, once per process that serves."""
    import inspect

    from provisa.api import app

    source = inspect.getsource(app.lifespan)
    assert "raise_open_file_limit(" in source
