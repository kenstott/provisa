# Copyright (c) 2026 Kenneth Stott
# Canary: db899185-835b-474f-b4d1-175a510303aa
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
        open(os.path.join(target, f"{name}-{harness._trino_plugin_version(name)}.jar"), "w").close()
        fetched.append(name)

    monkeypatch.setattr(harness, "_download_trino_plugin", _fetch)

    def _build_functions() -> None:  # REQ-1494: Provisa's own plugin is built, not downloaded
        functions = root / "trino" / "plugins" / "provisa-functions"
        functions.mkdir(exist_ok=True)
        (functions / "provisa-functions.jar").touch()

    monkeypatch.setattr(harness, "_build_provisa_functions", _build_functions)
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
        (root / name / f"{name}-{harness._trino_plugin_version(name)}.jar").write_text("")
    harness._populate_trino_plugins()
    assert fetched == []


def test_each_plugin_is_fetched_at_its_own_version(monkeypatch, tmp_path):
    # REQ-1946, REQ-1947, REQ-1960: the Salesforce, cloud inventory and file plugins come from the
    # first release that has what Provisa needs of them; every other plugin stays at the one pinned
    # version.
    assert harness._TRINO_PLUGIN_VERSIONS == {
        "trino-salesforce": "0.108.0",
        "trino-cloudops": "0.108.0",
        "trino-file": "0.109.0",
    }
    assert set(harness._TRINO_PLUGIN_VERSIONS) <= set(harness._TRINO_PLUGINS)
    urls: list[str] = []
    monkeypatch.setattr(
        "urllib.request.urlretrieve", lambda url, path: urls.append(f"{url} -> {path}")
    )
    for name in ("trino-sharepoint", "trino-salesforce", "trino-cloudops", "trino-file"):
        harness._download_trino_plugin(str(tmp_path / name), name)
    base = "https://repo1.maven.org/maven2/io/simpleishard"
    pinned = harness._TRINO_PLUGIN_VERSION
    assert urls == [
        f"{base}/trino-sharepoint/{pinned}/trino-sharepoint-{pinned}.jar"
        f" -> {tmp_path}/trino-sharepoint/trino-sharepoint-{pinned}.jar",
        f"{base}/trino-salesforce/0.108.0/trino-salesforce-0.108.0.jar"
        f" -> {tmp_path}/trino-salesforce/trino-salesforce-0.108.0.jar",
        f"{base}/trino-cloudops/0.108.0/trino-cloudops-0.108.0.jar"
        f" -> {tmp_path}/trino-cloudops/trino-cloudops-0.108.0.jar",
        f"{base}/trino-file/0.109.0/trino-file-0.109.0.jar"
        f" -> {tmp_path}/trino-file/trino-file-0.109.0.jar",
    ]


def test_the_workflows_fetch_each_plugin_at_the_harness_version():
    # CI and the harness must fetch the same build of each plugin.
    from pathlib import Path

    workflows = Path(__file__).resolve().parents[2] / ".github" / "workflows"
    wanted = " ".join(
        f"{name}:{harness._TRINO_PLUGIN_VERSIONS.get(name, '$VERSION')}"
        for name in harness._TRINO_PLUGINS
    )
    for name in ("build-dmg.yml", "ui-e2e-swap-amd64.yml", "ui-e2e-trino.yml"):
        text = (workflows / name).read_text()
        assert f'VERSION="{harness._TRINO_PLUGIN_VERSION}"' in text, name
        loop = " ".join(
            text[text.index("for spec in") : text.index("; do", text.index("for spec in"))]
            .replace("\\", " ")
            .split()
        )
        assert loop == f"for spec in {wanted}", name


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


def _earlier_pin_downloaded_here(root, name: str, body: str = "jar-0.106.2") -> None:
    """What an earlier run of the harness left: its own fetch of the previous pin, recorded."""
    (root / name).mkdir()
    jar = root / name / f"{name}-0.106.2.jar"
    jar.write_text(body)
    harness._record_download(str(root), name, str(jar))


def test_the_harness_own_fetch_of_an_earlier_pin_is_replaced_by_the_pinned_build(plugins):
    """A repin must not leave every worktree refusing the jars the harness itself fetched."""
    root, fetched = plugins
    _earlier_pin_downloaded_here(root, "trino-splunk")
    harness._populate_trino_plugins()
    pinned = f"trino-splunk-{harness._TRINO_PLUGIN_VERSION}.jar"
    assert sorted(os.listdir(root / "trino-splunk")) == [pinned]
    assert "trino-splunk" in fetched
    assert harness._downloads(str(root))["trino-splunk"]["file"] == pinned


def test_an_earlier_pin_the_harness_did_not_fetch_is_refused(plugins):
    root, _ = plugins
    (root / "trino-splunk").mkdir()
    (root / "trino-splunk" / "trino-splunk-0.106.2.jar").write_text("built somewhere else")
    with pytest.raises(RuntimeError, match="trino-splunk.*not the pinned"):
        harness._populate_trino_plugins()


def test_a_fetched_jar_changed_since_is_refused(plugins):
    root, _ = plugins
    _earlier_pin_downloaded_here(root, "trino-file")
    (root / "trino-file" / "trino-file-0.106.2.jar").write_text("rebuilt in place")
    with pytest.raises(RuntimeError, match="trino-file.*not the pinned"):
        harness._populate_trino_plugins()
