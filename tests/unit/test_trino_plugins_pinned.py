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

import email.message
import hashlib
import os
import urllib.error

import pytest

import tests.conftest as harness
import tests.trino_plugins as pins

_BODY = b"the pinned build"
_REAL_DOWNLOAD = harness._download_trino_plugin


def _pin_every_sum_to(monkeypatch, body: bytes) -> None:
    """Stand-in jars with recorded sums of their own: the real ones are hundreds of megabytes."""
    digest = hashlib.sha256(body).hexdigest()
    monkeypatch.setattr(pins, "_TRINO_PLUGIN_SHA256", {n: digest for n in pins._TRINO_PLUGINS})


@pytest.fixture
def plugins(tmp_path, monkeypatch):
    root = tmp_path / "repo"
    (root / "trino" / "plugins").mkdir(parents=True)
    monkeypatch.setattr(harness, "_REPO_ROOT", str(root))
    # A local run: CI never downloads at collection (test_a_ci_run_never_downloads_at_collection).
    monkeypatch.delenv("GITHUB_ACTIONS", raising=False)
    _pin_every_sum_to(monkeypatch, _BODY)
    fetched: list[str] = []

    def _fetch(target: str, name: str) -> None:
        os.makedirs(target, exist_ok=True)
        with open(os.path.join(target, pins.jar_name(name)), "wb") as f:
            f.write(_BODY)
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
    assert sorted(fetched) == sorted(pins._TRINO_PLUGINS)


def test_a_directory_holding_the_pinned_jar_is_used_as_is(plugins):
    root, fetched = plugins
    for name in pins._TRINO_PLUGINS:
        (root / name).mkdir()
        (root / name / pins.jar_name(name)).write_bytes(_BODY)
    harness._populate_trino_plugins()
    assert fetched == []


def test_the_pinned_file_name_with_other_bytes_is_refused_by_name(plugins):
    root, fetched = plugins
    for name in pins._TRINO_PLUGINS:
        (root / name).mkdir()
        (root / name / pins.jar_name(name)).write_bytes(_BODY)
    (root / "trino-cloudops" / pins.jar_name("trino-cloudops")).write_bytes(b"truncated")
    with pytest.raises(pins.TrinoPluginRefused, match=r"trino-cloudops: .*has sha256 "):
        harness._populate_trino_plugins()
    assert fetched == []


def test_a_ci_run_never_downloads_at_collection(plugins, monkeypatch):
    """Release 510: every lane missed the plugin cache and fetched from Maven Central inside
    collection at once; one was refused with HTTP 429 as an INTERNALERROR. On a runner the jars
    come from the workflow's restore, so a missing one is refused by name and nothing is fetched."""
    _, fetched = plugins
    monkeypatch.setenv("GITHUB_ACTIONS", "true")
    monkeypatch.setattr(harness, "_download_trino_plugin", _REAL_DOWNLOAD)
    urls = _serve(monkeypatch, _BODY)
    with pytest.raises(pins.TrinoPluginRefused, match=r"trino-sharepoint: .* is missing"):
        harness._populate_trino_plugins()
    assert fetched == [] and urls == []


def test_a_refused_plugin_ends_the_session_by_name_not_as_an_internal_error(monkeypatch):
    def _refuse() -> None:
        raise pins.TrinoPluginRefused("Trino plugin trino-file: trino-file-0.109.0.jar is missing")

    monkeypatch.delenv("PYTEST_NO_DOCKER", raising=False)
    monkeypatch.setattr(harness, "_needs_shared_stack", lambda items: True)
    monkeypatch.setattr(harness, "_marker_batches", lambda items: [[]])
    monkeypatch.setattr(harness, "_name_the_broker", lambda batches: None)
    monkeypatch.setattr(harness, "_populate_trino_plugins", _refuse)
    with pytest.raises(
        pytest.exit.Exception, match="trino-file: trino-file-0.109.0.jar is missing"
    ):
        harness._DockerServiceManager().pytest_collection_finish(type("S", (), {"items": []})())


def _serve(monkeypatch, body: bytes | Exception) -> list[str]:
    urls: list[str] = []

    def _retrieve(url: str, path: str) -> None:
        urls.append(url)
        if isinstance(body, Exception):
            raise body
        with open(path, "wb") as f:
            f.write(body)

    monkeypatch.setattr("urllib.request.urlretrieve", _retrieve)
    return urls


def test_a_fetch_with_other_bytes_is_refused_by_name_and_leaves_no_jar(monkeypatch, tmp_path):
    _pin_every_sum_to(monkeypatch, _BODY)
    _serve(monkeypatch, b"an html error page")
    with pytest.raises(pins.TrinoPluginRefused, match=r"trino-splunk from https://.*has sha256 "):
        pins.fetch(str(tmp_path / "trino-splunk"), "trino-splunk")
    assert os.listdir(tmp_path / "trino-splunk") == []


def _too_many(retry_after: str | None = None) -> urllib.error.HTTPError:
    headers = email.message.Message()
    if retry_after is not None:
        headers["Retry-After"] = retry_after
    return urllib.error.HTTPError("u", 429, "Too Many Requests", headers, None)


def _sleeps(monkeypatch) -> list[float]:
    slept: list[float] = []
    monkeypatch.setattr(pins.time, "sleep", slept.append)
    return slept


def _serve_in_turn(monkeypatch, answers: list[bytes | Exception]) -> list[str]:
    urls: list[str] = []

    def _retrieve(url: str, path: str) -> None:
        urls.append(url)
        answer = answers.pop(0)
        if isinstance(answer, Exception):
            with open(path, "wb") as f:
                f.write(b"partial")  # a refused try leaves no .part behind
            raise answer
        with open(path, "wb") as f:
            f.write(answer)

    monkeypatch.setattr("urllib.request.urlretrieve", _retrieve)
    return urls


def test_a_fetch_refused_429_every_try_fails_by_name_after_the_bounded_tries(monkeypatch, tmp_path):
    """Run 45293530114: the priming job's first jar got HTTP 429. Each 429 waits and tries again,
    a bounded number of times; the last refusal fails naming the plugin and the URL."""
    slept = _sleeps(monkeypatch)
    urls = _serve(monkeypatch, _too_many())
    with pytest.raises(
        pins.TrinoPluginRefused,
        match=r"trino-file: https://.* was refused after 5 tries: .*429",
    ):
        pins.fetch(str(tmp_path / "trino-file"), "trino-file")
    assert len(urls) == pins._FETCH_ATTEMPTS == 5
    assert slept == [15, 30, 60, 120]
    assert os.listdir(tmp_path / "trino-file") == []


def test_a_429_then_the_jar_takes_the_jar(monkeypatch, tmp_path):
    _pin_every_sum_to(monkeypatch, _BODY)
    slept = _sleeps(monkeypatch)
    urls = _serve_in_turn(monkeypatch, [_too_many(), _too_many(), _BODY])
    path = pins.fetch(str(tmp_path / "trino-splunk"), "trino-splunk")
    assert len(urls) == 3 and slept == [15, 30]
    assert os.listdir(tmp_path / "trino-splunk") == [pins.jar_name("trino-splunk")]
    assert open(path, "rb").read() == _BODY


def test_a_longer_retry_after_is_waited_and_capped(monkeypatch, tmp_path):
    _pin_every_sum_to(monkeypatch, _BODY)
    slept = _sleeps(monkeypatch)
    _serve_in_turn(monkeypatch, [_too_many("90"), _too_many("100000"), _too_many("5"), _BODY])
    pins.fetch(str(tmp_path / "trino-cloudops"), "trino-cloudops")
    assert slept == [90, 300, 60]


def test_a_refusal_other_than_429_is_not_retried(monkeypatch, tmp_path):
    slept = _sleeps(monkeypatch)
    urls = _serve(
        monkeypatch, urllib.error.HTTPError("u", 404, "Not Found", email.message.Message(), None)
    )
    with pytest.raises(pins.TrinoPluginRefused, match=r"trino-file: https://.* was refused: .*404"):
        pins.fetch(str(tmp_path / "trino-file"), "trino-file")
    assert len(urls) == 1 and slept == []
    assert os.listdir(tmp_path / "trino-file") == []


def _tree(root, monkeypatch) -> None:
    _pin_every_sum_to(monkeypatch, _BODY)
    for name in pins._TRINO_PLUGINS:
        (root / "trino" / "plugins" / name).mkdir(parents=True)
        (root / "trino" / "plugins" / name / pins.jar_name(name)).write_bytes(_BODY)


def test_verify_passes_every_pinned_jar(monkeypatch, tmp_path):
    _tree(tmp_path, monkeypatch)
    pins.verify(str(tmp_path))


def test_verify_refuses_a_missing_jar_by_name(monkeypatch, tmp_path):
    _tree(tmp_path, monkeypatch)
    (
        tmp_path / "trino" / "plugins" / "trino-salesforce" / pins.jar_name("trino-salesforce")
    ).unlink()
    with pytest.raises(pins.TrinoPluginRefused) as refused:
        pins.verify(str(tmp_path))
    assert str(refused.value).splitlines() == [
        f"Trino plugin trino-salesforce: {tmp_path}/trino/plugins/trino-salesforce/"
        f"trino-salesforce-0.108.0.jar is missing (the pinned trino-salesforce-0.108.0.jar)"
    ]


def test_verify_refuses_a_mismatched_jar_by_name(monkeypatch, tmp_path):
    _tree(tmp_path, monkeypatch)
    jar = tmp_path / "trino" / "plugins" / "trino-splunk" / pins.jar_name("trino-splunk")
    jar.write_bytes(b"another build")
    with pytest.raises(pins.TrinoPluginRefused) as refused:
        pins.verify(str(tmp_path))
    (line,) = str(refused.value).splitlines()
    assert line.startswith(f"Trino plugin trino-splunk: {jar} has sha256 ")
    assert line.endswith(
        f"not the pinned {pins.jar_name('trino-splunk')}'s {hashlib.sha256(_BODY).hexdigest()}"
    )


def test_the_verify_command_fails_naming_the_jar(monkeypatch, tmp_path, capsys):
    _tree(tmp_path, monkeypatch)
    (tmp_path / "trino" / "plugins" / "trino-file" / pins.jar_name("trino-file")).unlink()
    monkeypatch.setattr(pins, "_REPO_ROOT", str(tmp_path))
    assert pins.main(["verify"]) == 1
    assert "::error::Trino plugin trino-file: " in capsys.readouterr().err


def test_every_pinned_plugin_has_a_recorded_sum():
    assert set(pins._TRINO_PLUGIN_SHA256) == set(pins._TRINO_PLUGINS)
    assert all(len(s) == 64 and int(s, 16) >= 0 for s in pins._TRINO_PLUGIN_SHA256.values())


def test_each_plugin_is_fetched_at_its_own_version(monkeypatch, tmp_path):
    # REQ-1946, REQ-1947, REQ-2280: the Salesforce, cloud inventory and file plugins come from the
    # first release that has what Provisa needs of them (file: storage-config for drive globs); every other plugin stays at the one pinned
    # version.
    assert pins._TRINO_PLUGIN_VERSIONS == {
        "trino-salesforce": "0.108.0",
        "trino-cloudops": "0.108.0",
        "trino-file": "0.130.0",
    }
    assert set(pins._TRINO_PLUGIN_VERSIONS) <= set(pins._TRINO_PLUGINS)
    monkeypatch.delenv("GITHUB_ACTIONS", raising=False)
    _pin_every_sum_to(monkeypatch, _BODY)
    urls: list[str] = []

    def _retrieve(url: str, path: str) -> None:
        urls.append(f"{url} -> {path}")
        with open(path, "wb") as f:
            f.write(_BODY)

    monkeypatch.setattr("urllib.request.urlretrieve", _retrieve)
    for name in ("trino-sharepoint", "trino-salesforce", "trino-cloudops", "trino-file"):
        harness._download_trino_plugin(str(tmp_path / name), name)
        assert os.listdir(tmp_path / name) == [pins.jar_name(name)]
    base = "https://repo1.maven.org/maven2/io/simpleishard"
    pinned = pins._TRINO_PLUGIN_VERSION
    assert urls == [
        f"{base}/trino-sharepoint/{pinned}/trino-sharepoint-{pinned}.jar"
        f" -> {tmp_path}/trino-sharepoint/trino-sharepoint-{pinned}.jar.part",
        f"{base}/trino-salesforce/0.108.0/trino-salesforce-0.108.0.jar"
        f" -> {tmp_path}/trino-salesforce/trino-salesforce-0.108.0.jar.part",
        f"{base}/trino-cloudops/0.108.0/trino-cloudops-0.108.0.jar"
        f" -> {tmp_path}/trino-cloudops/trino-cloudops-0.108.0.jar.part",
        f"{base}/trino-file/0.130.0/trino-file-0.130.0.jar"
        f" -> {tmp_path}/trino-file/trino-file-0.130.0.jar.part",
    ]


def test_the_release_and_the_trino_stack_take_the_harness_pin_from_the_action():
    # CI and the harness must use the same build of each plugin: the release's plugin tarball
    # and the stack both Trino-backed UI lanes start no longer fetch at versions of their own.
    # The release primes the action's cache (the pin and sha256s of tests/trino_plugins.py); the
    # stack script checks the jars its caller restored and fetches nothing.
    from pathlib import Path

    import yaml

    repo = Path(__file__).resolve().parents[2]
    release = yaml.safe_load((repo / ".github/workflows/release-prebuilt.yml").read_text())
    steps = release["jobs"]["download-plugins"]["steps"]
    assert {"uses": "./.github/actions/trino-plugins", "with": {"phase": "prime"}}.items() <= next(
        s for s in steps if s.get("uses") == "./.github/actions/trino-plugins"
    ).items()
    stack = (repo / "scripts/ci/lanes/trino-stack-prepare.sh").read_text()
    assert "python3 tests/trino_plugins.py verify" in stack
    for name, text in (("release-prebuilt.yml", str(release)), ("trino-stack-prepare.sh", stack)):
        assert pins._TRINO_PLUGIN_MAVEN not in text and "curl" not in text, name


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
    pinned = f"trino-splunk-{pins._TRINO_PLUGIN_VERSION}.jar"
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
