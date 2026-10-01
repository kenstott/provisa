# Copyright (c) 2026 Kenneth Stott
# Canary: 9a2d6f48-5c1e-4b73-8d09-3e7f1a5c9b24
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The platform config the request path asks for is held in memory.

``platform_config()`` (engine selection, the materialize-store URL) is asked on request paths.
Its provider used to call ``read_config()`` every time — a ``stat`` of the config file per call.
It now answers from a copy held in the process and replaced only when the config changes through
the mechanisms that already change it: the file being loaded (startup, a rebuild) or written
(the admin config write, the setup wizard).
"""

# Requirements: REQ-164, REQ-1678

from __future__ import annotations

import yaml

import provisa.api.app as app_mod
from provisa.api.admin import _config_io
from provisa.core import config_location


def _config(tmp_path, monkeypatch, **content):
    path = tmp_path / "provisa.yaml"
    path.write_text(yaml.safe_dump(content))
    monkeypatch.setenv("PROVISA_CONFIG", str(path))
    config_location.note_config_changed()
    return path


def _count_reads(monkeypatch) -> list[int]:
    reads: list[int] = []
    real = _config_io.read_config

    def counting():
        reads.append(1)
        return real()

    monkeypatch.setattr(_config_io, "read_config", counting)
    return reads


def test_repeated_requests_read_the_config_file_once(tmp_path, monkeypatch):
    _config(tmp_path, monkeypatch, federation_engine="pg")
    reads = _count_reads(monkeypatch)
    for _ in range(50):
        assert app_mod._read_platform_config() == {"federation_engine": "pg"}
    assert len(reads) == 1


def test_a_written_config_is_what_the_next_request_sees(tmp_path, monkeypatch):
    path = _config(tmp_path, monkeypatch, federation_engine="pg")
    assert app_mod._read_platform_config()["federation_engine"] == "pg"
    _config_io.write_config(path, {"federation_engine": "duckdb"})
    assert app_mod._read_platform_config()["federation_engine"] == "duckdb"


def test_a_loaded_config_is_what_the_next_request_sees(tmp_path, monkeypatch):
    """Loading the config file (startup, a rebuild) is the change mechanism every worker runs."""
    path = _config(tmp_path, monkeypatch, federation_engine="pg")
    assert app_mod._read_platform_config()["federation_engine"] == "pg"
    path.write_text(yaml.safe_dump({"federation_engine": "trino"}))  # edited on disk
    assert app_mod._read_platform_config()["federation_engine"] == "pg"  # not re-read per request
    config_location.note_config_changed()  # what loading the config file does
    assert app_mod._read_platform_config()["federation_engine"] == "trino"


def test_loading_the_config_file_marks_it_changed():
    import inspect

    from provisa.core import config_loader

    assert "note_config_changed()" in inspect.getsource(config_loader.read_config_with_includes)
    assert "note_config_changed()" in inspect.getsource(_config_io.write_config)
