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
