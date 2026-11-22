# Copyright (c) 2026 Kenneth Stott
# Canary: 7d1c3f0e-5b2a-4e8f-9a61-2c4b8e0d9f37
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A pgwire bundle download is verified and installed atomically (REQ-956).

A first govdata download once left a cache directory holding only ``jars/`` and ``jre/``, and the
resolver then failed with "download did not produce launcher". The tarball is checked against the
release's ``.sha256`` asset before it is extracted, and the tree is moved into place only once it
is fully extracted and holds the launcher: a cut-off or tampered download, a missing checksum, or
an interrupted extraction leaves no cache directory and no staging behind."""

# Requirements: REQ-956

from __future__ import annotations

import hashlib
import io
import tarfile
from contextlib import contextmanager

import httpx
import pytest

from provisa.runtime_deps import pgwire_bundles
from provisa.runtime_deps.pgwire_bundles import BundleResolver, BundleSpec, BundleUnavailable

pytestmark = pytest.mark.unit

_SPEC = BundleSpec(connector="govdata", version="engine-v9.9.9", variant="linux-x86_64")


def _tarball(*, launcher: bool = True) -> bytes:
    out = io.BytesIO()
    entries = [
        ("jre/bin/java", b"java"),
        ("jars/govdata.jar", bytes(256 * 1024)),
        ("model/model.json", b'{"schemas": []}'),
    ]
    if launcher:
        entries.append((f"bin/{_SPEC.artifact_name}", b"#!/bin/sh\n"))
    with tarfile.open(fileobj=out, mode="w:gz") as tar:
        for name, data in entries:
            info = tarfile.TarInfo(f"{_SPEC.asset_stem}/{name}")
            info.size = len(data)
            tar.addfile(info, io.BytesIO(data))
    return out.getvalue()


class _Response:
    def __init__(self, status: int, body: bytes) -> None:
        self.status_code = status
        self._body = body

    def iter_bytes(self):
        for i in range(0, len(self._body), 8192):
            yield self._body[i : i + 8192]


def _serve(monkeypatch, *, tarball: bytes, checksum: bytes | None) -> list[str]:
    """Stub the release: the tarball at its URL and ``checksum`` at its ``.sha256`` URL (404 when
    None). Returns the URLs fetched, in order."""
    fetched: list[str] = []

    @contextmanager
    def _stream(method, url, **_kw):
        assert method == "GET"
        fetched.append(url)
        if url == _SPEC.download_url:
            yield _Response(200, tarball)
        elif url == _SPEC.checksum_url:
            yield _Response(404, b"Not Found") if checksum is None else _Response(200, checksum)
        else:
            raise AssertionError(f"unexpected URL {url}")

    monkeypatch.setattr(httpx, "stream", _stream)
    monkeypatch.setattr(pgwire_bundles, "named_bundle_dir", lambda _connector: None)
    return fetched


def _sum(body: bytes) -> bytes:
    return f"{hashlib.sha256(body).hexdigest()}  {_SPEC.asset_filename}\n".encode()


def _left_beside(tmp_path) -> list[str]:
    """What the cache holds beside the bundle's lock after a resolve."""
    release = tmp_path / _SPEC.version
    if not release.exists():
        return []
    return sorted(p.name for p in release.iterdir() if p.name != f".{_SPEC.artifact_name}.lock")


def test_checksum_url_is_the_tarball_asset_plus_sha256():
    assert _SPEC.checksum_url == f"{_SPEC.download_url}.sha256"


def test_good_tarball_installs_whole(tmp_path, monkeypatch):
    body = _tarball()
    fetched = _serve(monkeypatch, tarball=body, checksum=_sum(body))
    root = BundleResolver(cache_root=tmp_path).resolve(_SPEC)
    assert (root / "bin" / _SPEC.artifact_name).read_bytes() == b"#!/bin/sh\n"
    assert (root / "jars" / "govdata.jar").stat().st_size == 256 * 1024
    assert (root / "model" / "model.json").is_file()
    # The checksum is read before the tarball, and only the bundle is left in the cache.
    assert fetched == [_SPEC.checksum_url, _SPEC.download_url]
    assert _left_beside(tmp_path) == [_SPEC.artifact_name]


def test_checksum_with_bare_digest_is_accepted(tmp_path, monkeypatch):
    body = _tarball()
    _serve(monkeypatch, tarball=body, checksum=hashlib.sha256(body).hexdigest().encode())
    root = BundleResolver(cache_root=tmp_path).resolve(_SPEC)
    assert (root / "bin" / _SPEC.artifact_name).is_file()


def test_truncated_tarball_is_refused_and_leaves_no_cache_directory(tmp_path, monkeypatch):
    body = _tarball()
    # The connection was cut off: the release's checksum is of the whole tarball.
    _serve(monkeypatch, tarball=body[: len(body) // 2], checksum=_sum(body))
    with pytest.raises(BundleUnavailable, match="SHA-256"):
        BundleResolver(cache_root=tmp_path).resolve(_SPEC)
    assert _left_beside(tmp_path) == []


def test_checksum_mismatch_is_refused(tmp_path, monkeypatch):
    body = _tarball()
    tampered = _tarball(launcher=False)
    _serve(monkeypatch, tarball=tampered, checksum=_sum(body))
    with pytest.raises(BundleUnavailable) as err:
        BundleResolver(cache_root=tmp_path).resolve(_SPEC)
    assert hashlib.sha256(tampered).hexdigest() in str(err.value)
    assert hashlib.sha256(body).hexdigest() in str(err.value)
    assert _left_beside(tmp_path) == []


def test_missing_checksum_asset_is_refused(tmp_path, monkeypatch):
    body = _tarball()
    fetched = _serve(monkeypatch, tarball=body, checksum=None)
    with pytest.raises(BundleUnavailable, match=r"no checksum asset .*\.tar\.gz\.sha256"):
        BundleResolver(cache_root=tmp_path).resolve(_SPEC)
    # Nothing is downloaded that cannot be verified.
    assert fetched == [_SPEC.checksum_url]
    assert _left_beside(tmp_path) == []


def test_malformed_checksum_asset_is_refused(tmp_path, monkeypatch):
    body = _tarball()
    _serve(monkeypatch, tarball=body, checksum=b"<html>rate limited</html>")
    with pytest.raises(BundleUnavailable, match="does not hold a SHA-256 digest"):
        BundleResolver(cache_root=tmp_path).resolve(_SPEC)
    assert _left_beside(tmp_path) == []


def test_tarball_without_launcher_is_refused_and_not_moved_into_place(tmp_path, monkeypatch):
    body = _tarball(launcher=False)
    _serve(monkeypatch, tarball=body, checksum=_sum(body))
    with pytest.raises(BundleUnavailable, match="does not contain launcher"):
        BundleResolver(cache_root=tmp_path).resolve(_SPEC)
    assert _left_beside(tmp_path) == []


def test_interrupted_extract_leaves_no_partial_tree(tmp_path, monkeypatch):
    body = _tarball()
    _serve(monkeypatch, tarball=body, checksum=_sum(body))
    real_extract = tarfile.TarFile.extract

    def _extract_until_jars(self, member, path="", *args, **kwargs):
        if "/jars/" in getattr(member, "name", str(member)):
            raise OSError(28, "No space left on device")
        return real_extract(self, member, path, *args, **kwargs)

    # extractall writes one member at a time; the disk fills after jre/ is laid down.
    monkeypatch.setattr(tarfile.TarFile, "extract", _extract_until_jars)
    monkeypatch.setattr(
        tarfile.TarFile,
        "extractall",
        lambda self, path=".", members=None, *, filter=None: [
            self.extract(m, path, filter=filter) for m in self
        ],
    )
    with pytest.raises(BundleUnavailable, match="No space left on device"):
        BundleResolver(cache_root=tmp_path).resolve(_SPEC)
    assert _left_beside(tmp_path) == []
