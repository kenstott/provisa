# Copyright (c) 2026 Kenneth Stott
# Canary: 2f9c4a81-7d36-4e15-8b09-3a6e2c7f41d8
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""REQ-1148: sentinel freshness probe — a HASH-shaped token from a zero-byte marker.

Covers the file/HTTP transports, the token shape (exists+mtime+size / ETag / Last-Modified), the
absent-marker → None (TTL degrade) rule, scheme dispatch, and wiring through ``build_probe``.
"""

from __future__ import annotations

import os

import pytest

from provisa.events.probes import HASH, build_probe
from provisa.events.sentinel_probe import (
    _file_token,
    _http_token,
    _stat_token,
    build_sentinel_probe,
)


def test_stat_token_shape():
    assert _stat_token(True, 1700000000, 0) == "1700000000:0"


def test_stat_token_absent_is_none():
    # a missing marker degrades to TTL, never a definite "0:0" state
    assert _stat_token(False, None, None) is None


def test_file_token_changes_on_touch(tmp_path):
    marker = tmp_path / "sentinel"
    marker.write_bytes(b"")
    t1 = _file_token(str(marker))
    assert t1 is not None
    # rewrite with different size + mtime → token changes
    os.utime(marker, (1700000000, 1700000000))
    marker.write_bytes(b"x")
    os.utime(marker, (1700000100, 1700000100))
    t2 = _file_token(str(marker))
    assert t2 != t1


def test_file_token_missing_is_none(tmp_path):
    assert _file_token(str(tmp_path / "nope")) is None


def test_http_token_prefers_etag():
    def head(url):
        return {"etag": '"abc123"', "last-modified": "Wed, 01 Jan 2026 00:00:00 GMT"}

    assert _http_token("https://x/s", head) == '"abc123"'


def test_http_token_falls_back_to_last_modified():
    def head(url):
        return {"last-modified": "Wed, 01 Jan 2026 00:00:00 GMT"}

    assert _http_token("https://x/s", head) == "Wed, 01 Jan 2026 00:00:00 GMT"


def test_http_token_unreachable_is_none():
    assert _http_token("https://x/s", lambda url: None) is None


def test_unsupported_scheme_fails_loud():
    with pytest.raises(ValueError, match="unsupported sentinel_path scheme"):
        build_sentinel_probe("s3://bucket/marker")


@pytest.mark.asyncio
async def test_build_sentinel_probe_file(tmp_path):
    marker = tmp_path / "m"
    marker.write_bytes(b"")
    probe = build_sentinel_probe(f"file://{marker}")
    token = await probe()
    assert token is not None and ":" in token


@pytest.mark.asyncio
async def test_build_sentinel_probe_http_injected():
    probe = build_sentinel_probe("https://host/marker", http_head=lambda url: {"etag": "v1"})
    assert await probe() == "v1"


@pytest.mark.asyncio
async def test_build_probe_hash_uses_sentinel(tmp_path):
    # REQ-1148 wiring: a hash probe with a sentinel_path reads the marker (not the data)
    marker = tmp_path / "m"
    marker.write_bytes(b"")
    probe = build_probe(HASH, sentinel_path=f"file://{marker}")
    assert await probe() is not None


@pytest.mark.asyncio
async def test_build_probe_hash_without_sentinel_degrades():
    # hash with no sentinel and no cheap token → None (TTL degrade)
    probe = build_probe(HASH)
    assert await probe() is None
