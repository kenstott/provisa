# Copyright (c) 2026 Kenneth Stott
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Several fetches of one connector bundle at once (REQ-956).

A demo's replica builds each resolve the connector bundle they read through; several started at
once fetched the same bundle into the same directory, each clearing the others' staging and the
bundle they had laid down ("Directory not empty", "seeking backwards is not allowed", a server
missing its model.json). One fetch downloads it; the others wait for it, and every one finds the
whole bundle."""

# Requirements: REQ-956

from __future__ import annotations

import io
import tarfile
import threading
import time
from contextlib import contextmanager

import pytest

from provisa.runtime_deps import pgwire_bundles
from provisa.runtime_deps.pgwire_bundles import BundleResolver, BundleSpec

pytestmark = pytest.mark.unit

_SPEC = BundleSpec(connector="govdata", version="engine-v9.9.9", variant="linux-x86_64")


def _tarball() -> bytes:
    """The release asset: everything under ``<asset_stem>/``, its launcher, a model and a body
    large enough that extracting it takes a while."""
    out = io.BytesIO()
    with tarfile.open(fileobj=out, mode="w:gz") as tar:
        for name, data in (
            (f"bin/{_SPEC.artifact_name}", b"#!/bin/sh\n"),
            ("model/model.json", b'{"schemas": []}'),
            *((f"lib/site-packages/pyarrow/part{i}.bin", bytes(64 * 1024)) for i in range(40)),
        ):
            info = tarfile.TarInfo(f"{_SPEC.asset_stem}/{name}")
            info.size = len(data)
            tar.addfile(info, io.BytesIO(data))
    return out.getvalue()


class _Response:
    def __init__(self, body: bytes) -> None:
        self.status_code = 200
        self._body = body

    def iter_bytes(self):
        for i in range(0, len(self._body), 8192):
            time.sleep(0.001)  # a network that is not instant: fetches overlap
            yield self._body[i : i + 8192]


def test_concurrent_fetches_of_one_bundle_download_it_once_and_each_finds_it_whole(
    tmp_path, monkeypatch
):
    body = _tarball()
    downloads: list[int] = []

    @contextmanager
    def _stream(method, url, **_kw):
        downloads.append(1)
        yield _Response(body)

    import httpx

    monkeypatch.setattr(httpx, "stream", _stream)
    monkeypatch.setattr(pgwire_bundles, "named_bundle_dir", lambda _connector: None)
    results: list[object] = []

    def _resolve() -> None:
        try:
            root = BundleResolver(cache_root=tmp_path).resolve(_SPEC)
            # What the connector server reads as it starts: the launcher, the model, its packages.
            assert (root / "bin" / _SPEC.artifact_name).is_file()
            assert (root / "model" / "model.json").read_bytes() == b'{"schemas": []}'
            assert len(list((root / "lib" / "site-packages" / "pyarrow").iterdir())) == 40
            results.append(root)
        except BaseException as exc:  # noqa: BLE001 -- the outcome is what this test reports
            results.append(exc)

    threads = [threading.Thread(target=_resolve) for _ in range(6)]
    for t in threads:
        t.start()
    for t in threads:
        t.join(timeout=60)
    failures = [r for r in results if isinstance(r, BaseException)]
    assert failures == []
    assert len(results) == 6
    assert len(downloads) == 1
    # Nothing is left beside the bundle but its lock.
    beside = sorted(p.name for p in (tmp_path / _SPEC.version).iterdir())
    assert beside == sorted([_SPEC.artifact_name, f".{_SPEC.artifact_name}.lock"])
