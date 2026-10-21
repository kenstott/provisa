# Copyright (c) 2026 Kenneth Stott
# Canary: 4a1f7c92-6b3e-4d08-8f21-5e9c0a7b21d4
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""REQ-1158: the embedded control-plane pgserver stages the PyPI-delivered FDW/extension bundle into the
shared pginstall at boot, so the pg federation engine (PgFederationRuntime, connecting to this same
Postgres) can CREATE EXTENSION the bundled FDWs offline. Best-effort by design: a BYO-Postgres tier that
does not ship provisa-pg-ext is a clean no-op (its FDWs come from the system PG)."""

from __future__ import annotations

import builtins

from provisa.core import control_plane_pg


def test_no_op_when_wheel_absent(monkeypatch):
    """provisa-pg-ext not installed → staging must be a silent no-op, not an error (BYO tier)."""
    real_import = builtins.__import__

    def _fake_import(name, *a, **k):
        if name == "provisa_pg_ext":
            raise ModuleNotFoundError("No module named 'provisa_pg_ext'")
        return real_import(name, *a, **k)

    monkeypatch.setattr(builtins, "__import__", _fake_import)
    control_plane_pg._stage_bundled_extensions()  # must not raise


def test_stages_into_pginstall_when_wheel_present(monkeypatch):
    """When the wheel IS installed, staging is invoked against the shared pgserver pginstall."""
    import sys
    import types
    from pathlib import Path

    # Present a fake provisa_pg_ext and pgserver so the presence probe + path resolution succeed.
    monkeypatch.setitem(sys.modules, "provisa_pg_ext", types.ModuleType("provisa_pg_ext"))
    fake_utils = types.ModuleType("pixeltable_pgserver.utils")
    fake_utils.POSTGRES_BIN_PATH = Path("/fake/pixeltable_pgserver/pginstall/bin")
    monkeypatch.setitem(sys.modules, "pixeltable_pgserver.utils", fake_utils)

    called: list[Path] = []
    import provisa.pg_extensions.staging as staging

    monkeypatch.setattr(staging, "stage_bundled_pg_extensions", lambda p: called.append(p))

    control_plane_pg._stage_bundled_extensions()

    assert called == [Path("/fake/pixeltable_pgserver/pginstall")]


class TestStopActuallyStopsTheProcess:
    """stop() used to be ``_server(datadir).cleanup()``, a silent no-op: this module always
    constructs with cleanup_mode=None (start()'s own docstring says the instance is deliberately
    persistent across process exits), and pgserver's _cleanup() returns before touching the
    process at all when cleanup_mode is None. Confirmed live: start-ui-install.sh's demo-reset
    block called stop() then rm -rf'd the data directory, leaving the postmaster running against
    a deleted directory — the next boot's fresh initdb then hit pgserver's own
    "assert not proc.is_running()" and silently fell back to reusing the orphaned process with
    its stale (pre-reset) data. stop() now runs pg_ctl directly, with no cleanup_mode gate."""

    def test_no_postmaster_pid_is_a_noop(self, tmp_path):
        # No postmaster.pid in the directory at all — nothing to stop, must not raise or try to
        # start/attach a server just to stop it.
        control_plane_pg.stop(str(tmp_path))

    def test_running_server_stopped_via_pg_ctl(self, tmp_path, monkeypatch):
        import sys
        import types

        (tmp_path / "postmaster.pid").write_text("12345\n")

        calls: list[tuple[str, list[str]]] = []
        fake_pgexec = types.ModuleType("pixeltable_pgserver.pgexec")
        fake_pgexec.pgexec = lambda command, args, **kw: calls.append((command, args))
        monkeypatch.setitem(sys.modules, "pixeltable_pgserver.pgexec", fake_pgexec)

        control_plane_pg.stop(str(tmp_path))

        assert len(calls) == 1
        command, args = calls[0]
        assert command == "pg_ctl" and "stop" in args
        assert args[args.index("-D") + 1] == str(tmp_path)

    def test_pg_ctl_failure_falls_back_to_sigkill_by_pid(self, tmp_path, monkeypatch):
        import os
        import subprocess
        import sys
        import types

        (tmp_path / "postmaster.pid").write_text("999999999\nother line\n")

        def _raise(*_a, **_k):
            raise subprocess.CalledProcessError(1, "pg_ctl")

        fake_pgexec = types.ModuleType("pixeltable_pgserver.pgexec")
        fake_pgexec.pgexec = _raise
        monkeypatch.setitem(sys.modules, "pixeltable_pgserver.pgexec", fake_pgexec)

        killed: list[tuple[int, int]] = []

        def _kill(pid: int, sig: int) -> None:
            if sig == 0:  # is it gone yet: it is, once killed
                raise ProcessLookupError
            killed.append((pid, sig))

        monkeypatch.setattr(os, "kill", _kill)
        released: list = []
        monkeypatch.setattr(
            "provisa.core.pg_shared_memory.release_segment", lambda d: released.append(d)
        )

        control_plane_pg.stop(str(tmp_path))

        assert killed == [(999999999, __import__("signal").SIGKILL)]
        # A killed postmaster leaves its System V segment: released before the directory goes.
        assert released == [tmp_path]


class TestHasTableToleratesNoDatabaseYet:
    """dump_table's own docstring: 'a control plane that has never held the table yields False
    and no file — a fresh install has nothing to retain, which is a fact about the plane rather
    than a failure.' Confirmed live: on a first-ever boot (or right after reset(), which drops the
    provisa database), _has_table's old `\\c provisa` blindly assumed that database exists —
    psql's script errors out mid-way through a piped/non-tty invocation, which makes it exit
    non-zero, turning the documented graceful False into an uncaught CalledProcessError."""

    def test_no_provisa_database_returns_false_without_raising(self):
        class _FakeServer:
            def psql(self, command: str) -> str:
                assert "pg_database" in command, "must check existence before \\c-ing into it"
                return ""  # no matching row — "provisa" database does not exist

        assert control_plane_pg._has_table(_FakeServer(), "org_settings") is False

    def test_database_exists_checks_the_table_too(self):
        class _FakeServer:
            def __init__(self):
                self.commands: list[str] = []

            def psql(self, command: str) -> str:
                self.commands.append(command)
                if "pg_database" in command:
                    return "1\n"
                return "1\n"

        srv = _FakeServer()
        assert control_plane_pg._has_table(srv, "org_settings") is True
        assert len(srv.commands) == 2


class TestConnectionCapacity:
    """REQ-1900: every worker process of a `--workers N` server keeps its own control-plane pools,
    so the embedded control plane's stock 100-connection ceiling is reached at a worker per core
    ("FATAL: sorry, too many clients already" — the workers then die at boot)."""

    def _harness(self, monkeypatch, current: int):
        import sys
        import types

        calls: list = []

        class _Srv:
            def psql(self, sql: str) -> str:
                calls.append(sql)
                if sql.strip().upper().startswith("SHOW MAX_CONNECTIONS"):
                    return f" max_connections \n-----------------\n {current}\n(1 row)\n"
                return ""

        fake = types.ModuleType("pixeltable_pgserver.pgexec")
        fake.pgexec = lambda command, args, **kw: calls.append((command, args))
        monkeypatch.setitem(sys.modules, "pixeltable_pgserver.pgexec", fake)
        return _Srv(), calls

    def test_a_stock_ceiling_is_raised_and_the_server_restarted(self, tmp_path, monkeypatch):
        from provisa.core import control_plane_pg as cpp

        srv, calls = self._harness(monkeypatch, current=100)
        cpp._ensure_connection_capacity(srv, str(tmp_path))
        assert f"ALTER SYSTEM SET max_connections = {cpp.MAX_CONNECTIONS}" in calls
        assert ("pg_ctl", ["-D", str(tmp_path), "-w", "restart", "-m", "fast"]) in calls

    def test_an_adequate_ceiling_is_left_alone(self, tmp_path, monkeypatch):
        from provisa.core import control_plane_pg as cpp

        srv, calls = self._harness(monkeypatch, current=cpp.MAX_CONNECTIONS)
        cpp._ensure_connection_capacity(srv, str(tmp_path))
        assert calls == ["SHOW max_connections"]
