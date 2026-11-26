# Copyright (c) 2026 Kenneth Stott
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""REQ-2269 (amended 2026-11-26): ``provisa run`` uses the PostgreSQL its URLs name.

A host install that runs Trino starts its own PostgreSQL and writes PLATFORM_DATABASE_URL and
TENANT_DATABASE_URL into the service's environment; with both set, the native profile's control
plane is that server ("postgresql") and no embedded pgserver starts. One without the other is
refused, naming the missing one.
"""

from __future__ import annotations

import os
from pathlib import Path

import pytest

from provisa import cli
from provisa.core import control_plane_pg, desktop_profile

_URL = "postgresql+psycopg://provisa:pw@127.0.0.1:5432/provisa"
_URL_KEYS = ("PLATFORM_DATABASE_URL", "TENANT_DATABASE_URL")


@pytest.fixture
def data_dir(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> Path:
    # Skip the offline DuckDB extension staging: it is not under test here.
    monkeypatch.setenv("PROVISA_DUCKDB_EXT_DIR", str(tmp_path / "ext"))
    for key in (*_URL_KEYS, "PROVISA_ENGINE", "PROVISA_REDIS_EMBEDDED", "PROVISA_CONFIG"):
        monkeypatch.delenv(key, raising=False)

    def _no_embedded(_datadir: str) -> tuple[str, int, str]:
        raise AssertionError("the embedded control plane was started")

    monkeypatch.setattr(control_plane_pg, "start", _no_embedded)
    path = tmp_path / "native"
    path.mkdir()
    return path


def test_both_urls_select_the_named_postgresql(
    data_dir: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    for key in _URL_KEYS:
        monkeypatch.setenv(key, _URL)
    notes = cli._apply_embedded_env(data_dir)
    assert os.environ["PLATFORM_DATABASE_URL"] == _URL
    assert os.environ["TENANT_DATABASE_URL"] == _URL
    assert not any("embedded PostgreSQL" in note for note in notes)
    # The rest of the native profile still applies.
    assert os.environ["PROVISA_REDIS_EMBEDDED"] == "1"


@pytest.mark.parametrize(("present", "missing"), [_URL_KEYS, tuple(reversed(_URL_KEYS))])
def test_one_url_without_the_other_is_refused(
    data_dir: Path, monkeypatch: pytest.MonkeyPatch, present: str, missing: str
) -> None:
    monkeypatch.setenv(present, _URL)
    with pytest.raises(SystemExit) as raised:
        cli._apply_embedded_env(data_dir)
    assert present in str(raised.value) and missing in str(raised.value)


def test_no_urls_keep_the_embedded_control_plane(
    data_dir: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    chosen: list[str | None] = []
    real = desktop_profile.load_profile

    def _load(preset: str, **kwargs: object) -> desktop_profile.LaunchProfile:
        chosen.append(kwargs.get("control_plane"))  # type: ignore[arg-type]
        return real(preset, **{**kwargs, "control_plane": "postgresql"})  # type: ignore[arg-type]

    monkeypatch.setattr(desktop_profile, "load_profile", _load)
    cli._apply_embedded_env(data_dir)
    assert chosen == [None]
