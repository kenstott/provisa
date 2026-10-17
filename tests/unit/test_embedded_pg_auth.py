# Copyright (c) 2026 Kenneth Stott
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Who may connect to the embedded PostgreSQL: the data directory's owner over its socket, and over
TCP (Windows) only with the password generated for the directory. Applied at every start, so a
directory made before keeps working. Never started as root."""

from __future__ import annotations

import os
import re

import pytest

from provisa.core import control_plane_pg as cp


@pytest.fixture
def datadir(tmp_path):
    # pgserver puts the socket in the data directory when its path fits a socket name.
    path = tmp_path / "cp"
    yield str(path)
    cp.stop(str(path))


def _hba_methods(datadir: str) -> dict[str, set[str]]:
    methods: dict[str, set[str]] = {}
    for line in (cp.Path(datadir) / "pg_hba.conf").read_text().splitlines():
        fields = line.split()
        if fields and not fields[0].startswith("#"):
            methods.setdefault(fields[0], set()).add(fields[-1])
    return methods


def test_a_new_directory_asks_tcp_for_its_own_password(datadir):
    host, port, password = cp.start(datadir)
    kept = cp.Path(datadir) / cp.PASSWORD_FILE
    assert kept.read_text() == password and re.fullmatch(r"[0-9a-f]{48}", password)
    assert kept.stat().st_mode & 0o777 == 0o600
    methods = _hba_methods(datadir)
    assert methods["host"] == {"scram-sha-256"}
    assert methods["local"] == {"trust"}
    assert "PGPASSWORD" not in os.environ
    import psycopg

    with psycopg.connect(
        host=host, port=port, user="provisa", password=password, dbname="provisa"
    ) as conn:
        assert conn.execute("select 1").fetchone() == (1,)


def test_a_directory_made_before_keeps_working_and_is_hardened(datadir):
    cp.start(datadir)
    # As a data directory made before this change: trust on TCP, no password file.
    hba = cp.Path(datadir) / "pg_hba.conf"
    hba.write_text(hba.read_text().replace("scram-sha-256", "trust"))
    (cp.Path(datadir) / cp.PASSWORD_FILE).unlink()
    host, port, password = cp.start(datadir)
    assert (cp.Path(datadir) / cp.PASSWORD_FILE).read_text() == password
    assert _hba_methods(datadir)["host"] == {"scram-sha-256"}
    assert cp.start(datadir)[2] == password


def test_root_is_refused_before_anything_starts(tmp_path, monkeypatch):
    monkeypatch.setattr(os, "geteuid", lambda: 0, raising=False)
    with pytest.raises(cp.EmbeddedPostgresAsRoot, match="not started as root"):
        cp.start(str(tmp_path / "cp"))
    assert not (tmp_path / "cp").exists()
