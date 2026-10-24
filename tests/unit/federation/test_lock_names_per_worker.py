# Copyright (c) 2026 Kenneth Stott
# Canary: 224f3255-0247-4aa4-8391-f6ffd823640d
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The replica tests' advisory locks are their worker's own.

PostgreSQL advisory locks are held server-wide, not per database, and every xdist worker's control
plane is a database on one server. A replica's lock is named by its org and key, an engine's job
slot by its engine: two workers' tests naming the same org (``org1``) or engine took each other's
locks -- a runner read its replicas as held by another build ("waits on its build lock: another
build of it holds it") and its build ran past the test's bound. Each module that takes these
locks names its org and engine anew in each process."""

from __future__ import annotations

import importlib
import json
import subprocess
import sys

import pytest

pytestmark = pytest.mark.unit

_MODULES = (
    "tests.unit.federation.test_off_peak_window_builds",
    "tests.unit.federation.test_replica_converge",
    "tests.unit.federation.test_replica_indexes",
    "tests.unit.federation.test_replica_lock_region",
    "tests.unit.federation.test_replica_retry_backoff",
    "tests.unit.federation.test_replica_runner",
    "tests.unit.federation.test_replica_write_lock",
)


def _names(module) -> dict[str, str]:
    return {name: getattr(module, name) for name in ("ORG", "ENGINE") if hasattr(module, name)}


@pytest.mark.parametrize("name", _MODULES)
def test_another_worker_names_other_locks(name):
    here = _names(importlib.import_module(name))
    assert "ORG" in here, f"{name} names no ORG"
    there = subprocess.run(  # noqa: S603 -- the test's own interpreter, as another worker
        [
            sys.executable,
            "-c",
            "import importlib, json, sys; m = importlib.import_module(sys.argv[1]); "
            "print(json.dumps({n: getattr(m, n) for n in ('ORG', 'ENGINE') if hasattr(m, n)}))",
            name,
        ],
        capture_output=True,
        text=True,
        check=True,
    )
    other = json.loads(there.stdout.strip().splitlines()[-1])
    assert other.keys() == here.keys()
    for kind, value in here.items():
        assert other[kind] != value, f"{name}.{kind} is {value!r} in every worker"
