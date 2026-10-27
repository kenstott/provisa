# Copyright (c) 2026 Kenneth Stott
# Canary: 30cd17cb-081d-44dd-a398-b276e6152c7d
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Every page of the site carries the same primary navigation: the same links in the same order,
with the home page's own sections addressed in place and every other page's through "/"."""

from __future__ import annotations

import re
from pathlib import Path

_SITE = Path(__file__).resolve().parents[2] / "site"


def _links(page: Path) -> list[tuple[str, str]]:
    nav = re.search(
        r'<nav class="nav-links" aria-label="Primary">(.*?)</nav>', page.read_text(), re.S
    )
    assert nav, f"{page} has no primary nav"
    return re.findall(r'<a[^>]*href="([^"]+)"[^>]*>([^<]+)</a>', nav.group(1))


def _as_from_elsewhere(href: str) -> str:
    return "/" + href if href.startswith("#") else href


def test_every_page_has_the_home_pages_navigation():
    home = [(_as_from_elsewhere(h), label) for h, label in _links(_SITE / "index.html")]
    pages = sorted(
        [
            *_SITE.glob("*.html"),
            *_SITE.glob("vs/*.html"),
            *_SITE.glob("why/*.html"),
            *_SITE.glob("start/*.html"),
        ]
    )
    assert len(pages) > 1
    for page in pages:
        if page.name == "index.html" and page.parent == _SITE:
            continue
        assert _links(page) == home, page
