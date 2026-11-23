# Copyright (c) 2026 Kenneth Stott
# Canary: 9b2e7c41-5a8d-4f36-b1e0-6d4c3a9f2e58
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""packaging/macos/prune_runtime.py removes package test payload and finds unreadable archives.

Dry run 45212409648's notarization rejected joblib/test/data/*.gz: joblib's zlib pickles carry a
.gz name but are not gzip, so Apple could not unpack them.
"""

from __future__ import annotations

import gzip
import importlib.util
import subprocess
import sys
import zipfile
import zlib
from pathlib import Path

_SCRIPT = Path(__file__).resolve().parents[2] / "packaging" / "macos" / "prune_runtime.py"
_spec = importlib.util.spec_from_file_location("prune_runtime", _SCRIPT)
assert _spec is not None and _spec.loader is not None
prune_runtime = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(prune_runtime)


def _site(tmp_path: Path) -> Path:
    site = tmp_path / "site-packages"
    files = {
        "joblib/__init__.py": "",
        "joblib/testing.py": "",  # a module, kept
        "joblib/test/__init__.py": "",
        "joblib/test/data/__init__.py": "",
        "numpy/_core/tests/test_x.py": "",
        "numpy/testing/__init__.py": "",  # numpy.testing is runtime, kept
        "jinja2/tests.py": "TESTS = {}",  # jinja2's runtime module named tests, kept
        "pandas/tests/frame/test_y.py": "",
        "pandas/_testing/__init__.py": "",
    }
    for rel, text in files.items():
        path = site / rel
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(text)
    # joblib's fixture: a zlib stream under a .gz name, as in the rejected dry run.
    (site / "joblib/test/data/joblib_0.9.2_compressed_pickle_py35_np19.gz").write_bytes(
        zlib.compress(b"pickle")
    )
    return site


def test_prune_removes_only_test_directories(tmp_path: Path) -> None:
    site = _site(tmp_path)
    removed = prune_runtime.prune_test_dirs(site)
    assert [p.relative_to(site).as_posix() for p in removed] == [
        "joblib/test",
        "numpy/_core/tests",
        "pandas/tests",
    ]
    assert not (site / "joblib/test").exists()
    assert not (site / "numpy/_core/tests").exists()
    assert not (site / "pandas/tests").exists()
    for kept in (
        "joblib/__init__.py",
        "joblib/testing.py",
        "numpy/testing/__init__.py",
        "jinja2/tests.py",
        "pandas/_testing/__init__.py",
    ):
        assert (site / kept).is_file(), kept


def test_joblib_fixture_is_unreadable_and_gone_after_prune(tmp_path: Path) -> None:
    site = _site(tmp_path)
    fixture = site / "joblib/test/data/joblib_0.9.2_compressed_pickle_py35_np19.gz"
    assert prune_runtime.unreadable_archives(site) == [fixture]
    prune_runtime.prune_test_dirs(site)
    assert prune_runtime.unreadable_archives(site) == []


def test_unreadable_archives_reads_by_suffix(tmp_path: Path) -> None:
    good_gz = tmp_path / "ok.gz"
    good_gz.write_bytes(gzip.compress(b"data"))
    good_zip = tmp_path / "ok.whl"
    with zipfile.ZipFile(good_zip, "w") as archive:
        archive.writestr("a.txt", "a")
    bad_zip = tmp_path / "bad.jar"
    bad_zip.write_bytes(b"not a zip")
    bad_xz = tmp_path / "bad.xz"
    bad_xz.write_bytes(b"not xz")
    plain = tmp_path / "data.pkl"
    plain.write_bytes(b"not an archive name")
    assert prune_runtime.unreadable_archives(tmp_path) == [bad_zip, bad_xz]


def test_cli_fails_on_archive_left_outside_test_dirs(tmp_path: Path) -> None:
    site = _site(tmp_path)
    (site / "joblib/fixture.gz").write_bytes(zlib.compress(b"x"))
    result = subprocess.run(
        [sys.executable, "-I", str(_SCRIPT), str(site)], capture_output=True, text=True
    )
    assert result.returncode == 1
    assert "pruned joblib/test" in result.stdout
    assert "unreadable archive: joblib/fixture.gz" in result.stderr


def test_cli_succeeds_once_test_payload_is_pruned(tmp_path: Path) -> None:
    site = _site(tmp_path)
    result = subprocess.run(
        [sys.executable, "-I", str(_SCRIPT), str(site)], capture_output=True, text=True
    )
    assert result.returncode == 0, result.stderr
    assert "removed 3 test directories" in result.stdout
