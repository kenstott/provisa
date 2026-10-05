# Copyright (c) 2026 Kenneth Stott
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The stack's Trino plugins are exactly the pinned build, or the run refuses.

A worktree borrowed the primary checkout's locally built plugin directories, so a test pinned to
the 0.106.2 trino-sharepoint plugin ran against whatever had been built there, and passed or
failed for reasons that had nothing to do with the pin.
"""

from __future__ import annotations

import os

import pytest

import tests.conftest as harness


@pytest.fixture
def plugins(tmp_path, monkeypatch):
    root = tmp_path / "repo"
    (root / "trino" / "plugins").mkdir(parents=True)
    monkeypatch.setattr(harness, "_REPO_ROOT", str(root))
    fetched: list[str] = []

    def _fetch(target: str, name: str) -> None:
        os.makedirs(target, exist_ok=True)
        open(os.path.join(target, f"{name}-{harness._TRINO_PLUGIN_VERSION}.jar"), "w").close()
        fetched.append(name)

    monkeypatch.setattr(harness, "_download_trino_plugin", _fetch)
    return root / "trino" / "plugins", fetched


def test_missing_and_empty_directories_get_the_pinned_build(plugins):
    root, fetched = plugins
    (root / "trino-splunk").mkdir()  # what a docker bind mount leaves behind
    harness._populate_trino_plugins()
    assert sorted(fetched) == sorted(harness._TRINO_PLUGINS)


def test_a_directory_holding_the_pinned_jar_is_used_as_is(plugins):
    root, fetched = plugins
    for name in harness._TRINO_PLUGINS:
        (root / name).mkdir()
        (root / name / f"{name}-{harness._TRINO_PLUGIN_VERSION}.jar").write_text("")
    harness._populate_trino_plugins()
    assert fetched == []


def test_another_build_is_refused_by_name(plugins):
    root, _ = plugins
    (root / "trino-sharepoint").mkdir()
    (root / "trino-sharepoint" / "calcite-trino-sharepoint-1.42.0-SNAPSHOT.jar").write_text("")
    with pytest.raises(RuntimeError, match="trino-sharepoint.*not the pinned"):
        harness._populate_trino_plugins()


def test_a_borrowed_symlink_is_replaced_by_the_pinned_build(plugins, tmp_path):
    root, fetched = plugins
    elsewhere = tmp_path / "primary-build"
    elsewhere.mkdir()
    (elsewhere / "local.jar").write_text("")
    os.symlink(elsewhere, root / "trino-file")
    harness._populate_trino_plugins()
    assert "trino-file" in fetched and not os.path.islink(root / "trino-file")
    assert (elsewhere / "local.jar").exists()  # the borrowed directory itself is left alone
