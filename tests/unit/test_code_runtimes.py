# Copyright (c) 2026 Kenneth Stott
# Canary: 07f9f9b2-a98f-4bfe-9cfd-22229466fcc2
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The JavaScript runtimes a Node source names (REQ-1984): fetched on demand at a pinned version,
checked against the recorded SHA-256 before anything is extracted, cached, and never fetched
again once cached. No network: the archives are built here and the fetcher is a stand-in."""

# Requirements: REQ-1984
from __future__ import annotations

import hashlib
import io
import os
import tarfile
import zipfile

import pytest

from provisa.runtime_deps import code_runtimes
from provisa.runtime_deps.code_runtimes import (
    DARWIN_ARM64,
    LINUX_X64,
    Asset,
    CodeRuntimeResolver,
    RuntimePin,
    RuntimeUnavailable,
)


def _zip(entries: dict[str, bytes]) -> bytes:
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w") as z:
        for name, data in entries.items():
            z.writestr(name, data)
    return buf.getvalue()


def _tgz(entries: dict[str, bytes]) -> bytes:
    buf = io.BytesIO()
    with tarfile.open(fileobj=buf, mode="w:gz") as t:
        for name, data in entries.items():
            info = tarfile.TarInfo(name)
            info.size = len(data)
            info.mode = 0o755
            t.addfile(info, io.BytesIO(data))
    return buf.getvalue()


def _pin(monkeypatch, name: str, filename: str, data: bytes, executable: str) -> None:
    pin = RuntimePin(
        name,
        "9.9.9",
        "https://example.test/{version}/{filename}",
        {LINUX_X64: Asset(filename, hashlib.sha256(data).hexdigest(), executable)},
    )
    monkeypatch.setitem(code_runtimes.RUNTIMES, name, pin)


class _Fetcher:
    def __init__(self, data: bytes) -> None:
        self.data = data
        self.urls: list[str] = []

    def __call__(self, url: str) -> bytes:
        self.urls.append(url)
        return self.data


def test_every_pinned_runtime_names_an_archive_for_each_platform_provisa_runs_on():
    assert set(code_runtimes.RUNTIMES) == {
        "node",
        "bun",
        "deno",
        "python",
        "uv",
        "pyarrow",
        "apache-arrow",
        "temurin",
    }
    for name, pin in code_runtimes.RUNTIMES.items():
        # REQ-2098: what goes into an image is pinned for 64-bit ARM Linux too; uv installs on
        # the server, for any image's platform, and runs nowhere else.
        image = set() if name == "uv" else {code_runtimes.LINUX_ARM64}
        assert set(pin.assets) == {
            code_runtimes.DARWIN_ARM64,
            code_runtimes.LINUX_X64,
            code_runtimes.WINDOWS_X64,
            *image,
        }
        for asset in pin.assets.values():
            assert len(asset.sha256) == 64 and int(asset.sha256, 16) >= 0
            assert pin.url(asset).startswith("https://")


def test_a_pinned_wheel_is_unpacked_as_the_zip_it_is(tmp_path, monkeypatch):
    """REQ-2098: the host's pyarrow is a wheel, unpacked onto the host's path."""
    data = _zip({"pyarrow/__init__.py": b"", "pyarrow/lib.py": b""})
    _pin(monkeypatch, "pyarrow", "ab/cd/pyarrow-9-cp312.whl", data, "pyarrow/__init__.py")
    resolver = CodeRuntimeResolver(cache_root=tmp_path, fetcher=_Fetcher(data), host=LINUX_X64)
    marker = resolver.resolve("pyarrow")
    assert (marker.parent / "lib.py").is_file()


def test_a_runtime_is_fetched_checked_extracted_and_made_executable(tmp_path, monkeypatch):
    data = _zip({"bun-linux-x64/bun": b"#!/bin/sh\necho bun\n"})
    _pin(monkeypatch, "bun", "bun-linux-x64.zip", data, "bun-linux-x64/bun")
    fetch = _Fetcher(data)
    resolver = CodeRuntimeResolver(cache_root=tmp_path, fetcher=fetch, host=LINUX_X64)
    exe = resolver.resolve("bun")
    assert exe == tmp_path / "code" / "bun" / "9.9.9" / "bun-linux-x64" / "bun"
    assert exe.read_bytes() == b"#!/bin/sh\necho bun\n"
    assert os.access(exe, os.X_OK)  # a zip keeps no mode bits; it is made executable
    assert fetch.urls == ["https://example.test/9.9.9/bun-linux-x64.zip"]


def test_a_cached_runtime_is_never_fetched_again(tmp_path, monkeypatch):
    data = _tgz({"node-v9/bin/node": b"node"})
    _pin(monkeypatch, "node", "node-v9.tar.gz", data, "node-v9/bin/node")
    fetch = _Fetcher(data)
    resolver = CodeRuntimeResolver(cache_root=tmp_path, fetcher=fetch, host=LINUX_X64)
    first = resolver.resolve("node")
    assert resolver.resolve("node") == first
    assert len(fetch.urls) == 1


def test_an_archive_whose_hash_is_not_the_pinned_one_is_refused_before_extraction(
    tmp_path, monkeypatch
):
    good = _zip({"deno": b"deno"})
    _pin(monkeypatch, "deno", "deno.zip", good, "deno")
    tampered = _zip({"deno": b"something else"})
    resolver = CodeRuntimeResolver(cache_root=tmp_path, fetcher=_Fetcher(tampered), host=LINUX_X64)
    with pytest.raises(RuntimeUnavailable, match="not the pinned"):
        resolver.resolve("deno")
    assert not (tmp_path / "code" / "deno").exists() or not any(
        (tmp_path / "code" / "deno").iterdir()
    )


def test_an_archive_without_the_executable_is_refused_and_leaves_nothing(tmp_path, monkeypatch):
    data = _zip({"README": b"no binary here"})
    _pin(monkeypatch, "bun", "bun.zip", data, "bun-linux-x64/bun")
    resolver = CodeRuntimeResolver(cache_root=tmp_path, fetcher=_Fetcher(data), host=LINUX_X64)
    with pytest.raises(RuntimeUnavailable, match="holds no"):
        resolver.resolve("bun")
    assert not resolver.executable("bun").exists()
    assert [p.name for p in (tmp_path / "code" / "bun").iterdir()] == []


def test_an_archive_entry_that_escapes_the_cache_is_refused(tmp_path, monkeypatch):
    data = _zip({"../../evil": b"x", "bun": b"bun"})
    _pin(monkeypatch, "bun", "bun.zip", data, "bun")
    resolver = CodeRuntimeResolver(cache_root=tmp_path, fetcher=_Fetcher(data), host=LINUX_X64)
    with pytest.raises(RuntimeUnavailable, match="escapes"):
        resolver.resolve("bun")
    assert not (tmp_path.parent / "evil").exists()


def test_an_unknown_runtime_and_an_unpinned_platform_are_refused_by_name(tmp_path):
    resolver = CodeRuntimeResolver(cache_root=tmp_path, fetcher=_Fetcher(b""), host=DARWIN_ARM64)
    with pytest.raises(RuntimeUnavailable, match="'ruby' is not a runtime"):
        resolver.resolve("ruby")
    other = CodeRuntimeResolver(cache_root=tmp_path, fetcher=_Fetcher(b""), host=("Plan9", "mips"))
    with pytest.raises(RuntimeUnavailable, match="Plan9/mips"):
        other.resolve("node")


def test_only_https_is_fetched():
    with pytest.raises(RuntimeUnavailable, match="non-https"):
        code_runtimes.fetch_https("http://example.test/node.tar.gz")


def test_two_resolutions_at_once_in_one_process_each_extract_whole(tmp_path, monkeypatch):
    """Two code sources prepared at once resolve the host's pyarrow on two threads of one process.
    Staged under one name (the process id), the second's start cleared what the first had
    extracted, and the first moved the second's half-written directory into place: the cache
    held pyarrow without libarrow_acero.so, for good. Here the second is held half-way through its
    extraction while the first finishes."""
    import threading

    entries = {f"pyarrow/lib{i}.so": bytes([i]) * 64 for i in range(8)}
    entries = {"pyarrow/__init__.py": b"", **entries}
    data = _zip(entries)
    _pin(monkeypatch, "pyarrow", "ab/cd/pyarrow-9-cp312.whl", data, "pyarrow/__init__.py")
    names = list(entries)
    half = _zip({n: entries[n] for n in names[: len(names) // 2]})
    rest = _zip({n: entries[n] for n in names[len(names) // 2 :]})
    first_extracted, second_half_way, first_done = (threading.Event() for _ in range(3))
    extract = code_runtimes._extract

    def interleaved(filename: str, archive: bytes, dest) -> None:
        if threading.current_thread().name == "first":
            extract(filename, archive, dest)
            first_extracted.set()
            assert second_half_way.wait(10)
        else:
            extract(filename, half, dest)
            second_half_way.set()
            assert first_done.wait(10)
            extract(filename, rest, dest)

    monkeypatch.setattr(code_runtimes, "_extract", interleaved)
    found: dict[str, object] = {}

    def resolve() -> None:
        resolver = CodeRuntimeResolver(cache_root=tmp_path, fetcher=_Fetcher(data), host=LINUX_X64)
        try:
            found[threading.current_thread().name] = resolver.resolve("pyarrow")
        except Exception as exc:  # recorded for the assertion below
            found[threading.current_thread().name] = exc

    first = threading.Thread(target=resolve, name="first")
    second = threading.Thread(target=resolve, name="second")
    first.start()
    assert first_extracted.wait(10)
    second.start()
    first.join(10)
    first_done.set()
    second.join(10)
    marker = tmp_path / "code" / "pyarrow" / "9.9.9" / "pyarrow" / "__init__.py"
    assert found == {"first": marker, "second": marker}
    home = marker.parent.parent
    assert sorted(str(p.relative_to(home)) for p in home.rglob("*") if p.is_file()) == sorted(names)
    assert [p.name for p in home.parent.iterdir()] == ["9.9.9"]  # no staging left behind
