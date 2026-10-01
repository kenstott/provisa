# Copyright (c) 2026 Kenneth Stott
# Canary: b3f70a56-8e2d-4c19-a7d4-0c5e9f1b2a68
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Tests never write the user's own licensing state — and nothing can relocate a trial.

Every server a test starts evaluates licensing at startup, and licensing persists its trial
anchors and high-water mark in the user's ``~/.provisa`` and the OS per-user data directory: the
maintainer's own installation. Two rules hold together:

* ``PROVISA_HOME`` does not move licensing state. It relocates the ops store and certificates;
  if it also moved the anchors, setting it would start a fresh trial.
* The test session sets ``PROVISA_LICENSING_SANDBOX_DIR``. Under it licensing still READS the
  real anchors and high-water mark — so the trial clock is the real one, and the variable cannot
  be used to reset it — but WRITES only inside the sandbox directory."""

# Requirements: REQ-1135, REQ-1136

from __future__ import annotations

import datetime
import os
from pathlib import Path

import pytest

from provisa.licensing import anchors, license as license_mod, monotonic
from provisa.licensing.machine_id import stable_machine_id
from provisa.licensing.state import evaluate

_DAY = 86400


def _epoch(day: datetime.date) -> float:
    return float(day.toordinal() * _DAY)


@pytest.fixture
def a_users_home(tmp_path, monkeypatch):
    """A stand-in for the user's real home, holding an installation first used 400 days ago."""
    home = tmp_path / "user-home"
    home.mkdir()
    monkeypatch.setenv("HOME", str(home))
    monkeypatch.delenv("XDG_DATA_HOME", raising=False)
    monkeypatch.delenv("PROVISA_LICENSING_SANDBOX_DIR", raising=False)
    monkeypatch.delenv("PROVISA_HOME", raising=False)
    today = datetime.date.today()
    first_use = today - datetime.timedelta(days=400)
    evaluate(now_epoch=_epoch(first_use), today_iso=first_use.isoformat())
    return home, today, first_use


def _snapshot(root: Path) -> dict[str, tuple[int, bytes]]:
    return {
        str(p.relative_to(root)): (p.stat().st_mtime_ns, p.read_bytes())
        for p in sorted(root.rglob("*"))
        if p.is_file()
    }


def test_provisa_home_does_not_move_the_trial(a_users_home, tmp_path, monkeypatch):
    home, today, first_use = a_users_home
    real_paths = anchors.default_anchor_paths()
    monkeypatch.setenv("PROVISA_HOME", str(tmp_path / "relocated"))

    assert anchors.default_anchor_paths() == real_paths
    assert monotonic.default_highwater_path() == home / ".provisa" / "highwater.json"
    state = evaluate(now_epoch=_epoch(today), today_iso=today.isoformat())
    assert state.first_seen == first_use.isoformat()
    assert state.trial_expired
    assert not (tmp_path / "relocated").exists()


def test_the_sandbox_reads_the_real_trial_and_writes_only_itself(
    a_users_home, tmp_path, monkeypatch
):
    home, today, first_use = a_users_home
    before = _snapshot(home)
    assert before, "the stand-in installation wrote its anchors"
    sandbox = tmp_path / "sandbox"
    monkeypatch.setenv("PROVISA_LICENSING_SANDBOX_DIR", str(sandbox))

    state = evaluate(now_epoch=_epoch(today), today_iso=today.isoformat())

    # The trial clock is the real installation's: the sandbox cannot be used to restart it.
    assert state.first_seen == first_use.isoformat()
    assert state.trial_expired
    # ...and not one byte of the real installation was written.
    assert _snapshot(home) == before
    assert (sandbox / "highwater.json").exists()
    assert (sandbox / "anchor.json").exists()


def test_the_sandbox_cannot_roll_the_clock_back_past_the_real_high_water(
    a_users_home, tmp_path, monkeypatch
):
    home, today, first_use = a_users_home
    evaluate(now_epoch=_epoch(today), today_iso=today.isoformat())  # the real mark is now today
    monkeypatch.setenv("PROVISA_LICENSING_SANDBOX_DIR", str(tmp_path / "sandbox"))
    earlier = first_use + datetime.timedelta(days=1)
    state = evaluate(now_epoch=_epoch(earlier), today_iso=earlier.isoformat())
    assert state.trial_expired  # measured against the real high-water mark, not the fake "now"


def test_a_fresh_sandbox_directory_is_not_a_fresh_trial(a_users_home, tmp_path, monkeypatch):
    _home, today, first_use = a_users_home
    for name in ("one", "two"):
        monkeypatch.setenv("PROVISA_LICENSING_SANDBOX_DIR", str(tmp_path / name))
        state = evaluate(now_epoch=_epoch(today), today_iso=today.isoformat())
        assert state.first_seen == first_use.isoformat()


def test_a_license_installed_under_the_sandbox_stays_in_it(a_users_home, tmp_path, monkeypatch):
    home, _today, _first = a_users_home
    sandbox = tmp_path / "sandbox"
    monkeypatch.setenv("PROVISA_LICENSING_SANDBOX_DIR", str(sandbox))
    assert license_mod.default_license_path() == sandbox / "license.json"
    assert home not in license_mod.default_license_path().parents


# --- the guard on THIS test session -------------------------------------------------------------


def test_this_session_runs_licensing_in_a_sandbox():
    sandbox = Path(os.environ["PROVISA_LICENSING_SANDBOX_DIR"]).resolve()
    assert sandbox.is_dir()
    real = (Path.home() / ".provisa").resolve()
    assert real not in (sandbox, *sandbox.parents)


def test_evaluating_licensing_leaves_the_real_locations_untouched():
    # Checked BEFORE anything is written: were the sandbox missing, evaluate() below would be
    # writing the real installation, and this test must fail without doing that.
    assert os.environ.get("PROVISA_LICENSING_SANDBOX_DIR")
    real = [monotonic.default_highwater_path(), *anchors.default_anchor_paths()]
    assert all(Path.home() in p.parents for p in real)
    before = {p: (p.stat().st_mtime_ns if p.exists() else None) for p in real}

    today = datetime.date.today()
    evaluate(now_epoch=_epoch(today), today_iso=today.isoformat())

    assert {p: (p.stat().st_mtime_ns if p.exists() else None) for p in real} == before
    sandbox = Path(os.environ["PROVISA_LICENSING_SANDBOX_DIR"])
    assert monotonic.read_highwater(sandbox / "highwater.json") >= _epoch(today)
    assert anchors.read_anchor(sandbox / "anchor.json", stable_machine_id()) is not None


def test_a_spawned_test_server_inherits_the_sandbox():
    """IsolatedServer builds the server's environment from os.environ, so the servers tests
    spawn run licensing in the session's sandbox; the worker-boot harness, which is also run as
    a script outside any test session, names a sandbox of its own."""
    import inspect

    from tests.integration import isolated_server, worker_boot_harness

    assert "**os.environ," in inspect.getsource(isolated_server.IsolatedServer.start)
    assert '"PROVISA_LICENSING_SANDBOX_DIR": os.path.join(self.data_dir' in inspect.getsource(
        worker_boot_harness.WorkerBoot.start
    )
