# Copyright (c) 2026 Kenneth Stott
# Canary: b13e85eb-e9a2-4f6f-a21c-e3b5f463bce9
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A pool worker's late thread exception is charged to the test that started it (REQ-1989).

Each scenario runs in its own pytest, as psycopg's suite runs, with thread-exception warnings as
errors. The worker of ``test_origin`` raises at a moment the scenario fixes: without the plugin,
once the next test has started; with it, inside the origin's join, because the next test cannot
start while the origin is being torn down.
"""

# Requirements: REQ-1989

from __future__ import annotations

import importlib.util
import os
import subprocess  # nosec B404 -- runs this interpreter on a scenario this file wrote
import sys
from pathlib import Path

REPO = Path(__file__).resolve().parents[2]
DRIVERS = REPO / "scripts" / "conformance" / "drivers"
WORKFLOW = REPO / ".github" / "workflows" / "conformance-drivers.yml"


def _driver_suite():
    path = REPO / "scripts" / "conformance" / "driver_suite.py"
    spec = importlib.util.spec_from_file_location("driver_suite_join_threads", path)
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


#: The shape of test_pool_null::test_intrans_rollback: a queued worker, a body that fails, the
#: worker raising after the failure. Below BOUND_S with room, so the join outlasts it.
_LATE_WORKER = """
import threading

import pytest

next_test_started = threading.Event()
workers = []


def start_late_worker():
    released = threading.Event()

    def worker():
        released.wait()
        next_test_started.wait(1.5)
        raise RuntimeError("the pool is closed")

    workers.append(threading.Thread(target=worker))
    workers[-1].start()
    return released
"""

ORIGIN_FAILS = (
    _LATE_WORKER
    + """

def test_origin():
    released = start_late_worker()
    try:
        raise AssertionError("CREATE TABLE refused")
    finally:
        released.set()


def test_innocent():
    next_test_started.set()
    workers[-1].join()
"""
)

RELEASED_BY_FIXTURE = (
    _LATE_WORKER
    + """

@pytest.fixture
def pool():
    released = start_late_worker()
    yield
    released.set()


def test_origin(pool):
    pass


def test_innocent():
    next_test_started.set()
    workers[-1].join()
"""
)

NEVER_ENDS = """
import threading
import time

stop = threading.Event()
body_ended = []


def test_origin():
    threading.Thread(target=stop.wait, daemon=True).start()
    body_ended.append(time.monotonic())


def test_next():
    gap = time.monotonic() - body_ended[0]
    stop.set()
    assert 1.9 <= gap < 10.0, gap
"""

STARTED_BY_SESSION_FIXTURE = """
import threading
import time

import pytest

stop = threading.Event()
ended = []


@pytest.fixture(scope="session", autouse=True)
def server():
    threading.Thread(target=stop.wait, daemon=True).start()
    yield
    stop.set()


def test_first():
    ended.append(time.monotonic())


def test_second():
    assert time.monotonic() - ended[0] < 1.5
"""


def _run(tmp_path: Path, scenario: str, *, plugin: bool) -> dict[str, dict]:
    (tmp_path / "pytest.ini").write_text("[pytest]\n")
    (tmp_path / "test_scenario.py").write_text(scenario)
    junit = tmp_path / "junit.xml"
    env = {k: v for k, v in os.environ.items() if not k.startswith("PYTEST_")}
    env["PYTEST_DISABLE_PLUGIN_AUTOLOAD"] = "1"
    env["PYTHONPATH"] = str(DRIVERS)
    args = [sys.executable, "-m", "pytest", "test_scenario.py", "-q", "-p", "no:cacheprovider"]
    if plugin:
        args += ["-p", "join_test_threads"]
    args += ["-W", "error::pytest.PytestUnhandledThreadExceptionWarning", f"--junitxml={junit}"]
    done = subprocess.run(  # nosec B603 -- see the import
        args, cwd=tmp_path, env=env, capture_output=True, text=True, timeout=60
    )
    assert junit.exists(), done.stdout + done.stderr
    return _driver_suite().read_results(str(junit))


def _outcomes(results: dict[str, dict]) -> dict[str, str]:
    return {test.split("::")[1]: result["outcome"] for test, result in results.items()}


def test_without_the_plugin_the_late_exception_lands_on_the_next_test(tmp_path: Path) -> None:
    assert _outcomes(_run(tmp_path, ORIGIN_FAILS, plugin=False)) == {
        "test_origin": "failed",
        "test_innocent": "failed",
    }


def test_with_the_plugin_the_late_exception_lands_on_the_test_that_started_it(
    tmp_path: Path,
) -> None:
    assert _outcomes(_run(tmp_path, ORIGIN_FAILS, plugin=True)) == {
        "test_origin": "failed",
        "test_innocent": "passed",
    }


def test_a_thread_a_fixture_releases_lands_on_the_test_too(tmp_path: Path) -> None:
    """The join comes after the test's fixtures are finalized, before pytest's collection. Here
    the exception is the only thing wrong with the origin, so it stays detected."""
    assert _outcomes(_run(tmp_path, RELEASED_BY_FIXTURE, plugin=True)) == {
        "test_origin": "failed",
        "test_innocent": "passed",
    }


def test_a_call_failure_and_its_late_teardown_error_classify_as_the_one_listed_failure(
    tmp_path: Path,
) -> None:
    """pytest writes two testcases for a call failure plus a teardown error. driver_suite reads
    them as one failed test keeping the call's message, so a listed test stays what it was."""
    driver_suite = _driver_suite()
    results = _run(tmp_path, ORIGIN_FAILS, plugin=True)
    test = next(t for t in results if t.endswith("::test_origin"))
    assert "CREATE TABLE refused" in results[test]["message"]
    expected = driver_suite.Expected(
        "psycopg", "1", {"ddl": driver_suite.Reason("ddl", defect="refused")}, {test: "ddl"}
    )
    doc = driver_suite.summarise(expected, "1", results, "suite")
    assert doc["unexpected_failures"] == []
    assert doc["unexpected_passes"] == []
    assert doc["failed"] == 1
    assert not driver_suite.gate_failed(doc)


def test_the_wait_is_bounded_for_a_thread_that_never_ends(tmp_path: Path) -> None:
    """The next test starts once the bound has passed: it was waited for, and no longer."""
    assert _outcomes(_run(tmp_path, NEVER_ENDS, plugin=True)) == {
        "test_origin": "passed",
        "test_next": "passed",
    }


def test_a_thread_from_before_the_test_is_not_waited_for(tmp_path: Path) -> None:
    assert _outcomes(_run(tmp_path, STARTED_BY_SESSION_FIXTURE, plugin=True)) == {
        "test_first": "passed",
        "test_second": "passed",
    }


def test_the_psycopg_run_loads_the_plugin() -> None:
    text = WORKFLOW.read_text()
    assert "-p join_test_threads" in text
    assert 'PYTHONPATH="$GITHUB_WORKSPACE/scripts/conformance/drivers"' in text
