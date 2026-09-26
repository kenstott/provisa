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
    fake_pgserver = types.ModuleType("pgserver")
    fake_pgserver.__file__ = "/fake/pgserver/__init__.py"
    monkeypatch.setitem(sys.modules, "pgserver", fake_pgserver)

    called: list[Path] = []
    import provisa.pg_extensions.staging as staging

    monkeypatch.setattr(staging, "stage_bundled_pg_extensions", lambda p: called.append(p))

    control_plane_pg._stage_bundled_extensions()

    assert called == [Path("/fake/pgserver/pginstall")]


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

        calls: list[tuple[list[str], object]] = []
        fake_pgserver_postgres_server = types.ModuleType("pgserver.postgres_server")
        fake_pgserver_postgres_server.pg_ctl = lambda args, pgdata=None, **kw: calls.append(
            (args, pgdata)
        )
        monkeypatch.setitem(sys.modules, "pgserver.postgres_server", fake_pgserver_postgres_server)

        control_plane_pg.stop(str(tmp_path))

        assert len(calls) == 1
        args, pgdata = calls[0]
        assert "stop" in args
        assert pgdata == tmp_path

    def test_pg_ctl_failure_falls_back_to_sigkill_by_pid(self, tmp_path, monkeypatch):
        import os
        import subprocess
        import sys
        import types

        (tmp_path / "postmaster.pid").write_text("999999999\nother line\n")

        def _raise(*_a, **_k):
            raise subprocess.CalledProcessError(1, "pg_ctl")

        fake_pgserver_postgres_server = types.ModuleType("pgserver.postgres_server")
        fake_pgserver_postgres_server.pg_ctl = _raise
        monkeypatch.setitem(sys.modules, "pgserver.postgres_server", fake_pgserver_postgres_server)

        killed: list[tuple[int, int]] = []
        monkeypatch.setattr(os, "kill", lambda pid, sig: killed.append((pid, sig)))

        control_plane_pg.stop(str(tmp_path))

        assert killed == [(999999999, __import__("signal").SIGKILL)]


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
