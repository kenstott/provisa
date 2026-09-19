# Copyright (c) 2026 Kenneth Stott
# Canary: c1a2b3d4-e5f6-4789-a0b1-c2d3e4f56789
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Unit tests for HTTP-crawl-only settings on the file connector (REQ-1785).

``simple_links`` is a pass-through to ``fsspec.core.url_to_fs`` and is not
exercised here (it's fsspec's own concern) -- these tests cover the
``same_domain`` and ``exclude_pattern`` behavior implemented directly in
``_walk_fsspec_recursive``, using a fake filesystem object so no real HTTP
call is made.
"""

# Requirements: REQ-1785

from __future__ import annotations

from provisa.file_source.crawler import _walk_fsspec_recursive


class _FakeHttpFs:
    """Minimal stand-in for fsspec's HTTPFileSystem, keyed by URL path."""

    def __init__(self, listing: dict[str, list[dict]]) -> None:
        self._listing = listing

    def ls(self, path: str, detail: bool = True) -> list[dict]:
        return self._listing.get(path, [])


def test_same_domain_excludes_cross_domain_links() -> None:
    fs = _FakeHttpFs(
        {
            "example.com/data/": [
                {"name": "example.com/data/a.csv", "type": "file"},
                {"name": "evil.com/tracker.csv", "type": "file"},
            ]
        }
    )
    results: list[str] = []
    _walk_fsspec_recursive(
        fs,
        "example.com/data/",
        "example.com/data/",
        0,
        None,
        results,
        "https://example.com/data/",
        same_domain=True,
        root_netloc="example.com",
        exclude_pattern=None,
    )
    assert results == ["https://example.com/data/a.csv"]


def test_same_domain_false_allows_cross_domain_links() -> None:
    fs = _FakeHttpFs(
        {
            "example.com/data/": [
                {"name": "example.com/data/a.csv", "type": "file"},
                {"name": "other.com/b.csv", "type": "file"},
            ]
        }
    )
    results: list[str] = []
    _walk_fsspec_recursive(
        fs,
        "example.com/data/",
        "example.com/data/",
        0,
        None,
        results,
        "https://example.com/data/",
        same_domain=False,
        root_netloc="example.com",
        exclude_pattern=None,
    )
    assert sorted(results) == [
        "https://example.com/data/a.csv",
        "https://other.com/b.csv",
    ]


def test_exclude_pattern_skips_matching_basenames() -> None:
    fs = _FakeHttpFs(
        {
            "example.com/data/": [
                {"name": "example.com/data/keep.csv", "type": "file"},
                {"name": "example.com/data/archive_old.csv", "type": "file"},
            ]
        }
    )
    results: list[str] = []
    _walk_fsspec_recursive(
        fs,
        "example.com/data/",
        "example.com/data/",
        0,
        None,
        results,
        "https://example.com/data/",
        same_domain=True,
        root_netloc="example.com",
        exclude_pattern="archive_*",
    )
    assert results == ["https://example.com/data/keep.csv"]


def test_exclude_pattern_applies_to_directories_too() -> None:
    fs = _FakeHttpFs(
        {
            "example.com/data/": [
                {"name": "example.com/data/skip_me/", "type": "directory"},
                {"name": "example.com/data/keep/", "type": "directory"},
            ],
            "example.com/data/keep/": [
                {"name": "example.com/data/keep/a.csv", "type": "file"},
            ],
        }
    )
    results: list[str] = []
    _walk_fsspec_recursive(
        fs,
        "example.com/data/",
        "example.com/data/",
        0,
        None,
        results,
        "https://example.com/data/",
        same_domain=True,
        root_netloc="example.com",
        exclude_pattern="skip_me",
    )
    assert results == ["https://example.com/data/keep/a.csv"]


def test_defaults_are_permissive_same_domain_no_exclude() -> None:
    fs = _FakeHttpFs(
        {
            "example.com/data/": [
                {"name": "example.com/data/a.csv", "type": "file"},
            ]
        }
    )
    results: list[str] = []
    _walk_fsspec_recursive(
        fs,
        "example.com/data/",
        "example.com/data/",
        0,
        None,
        results,
        "https://example.com/data/",
    )
    assert results == ["https://example.com/data/a.csv"]
