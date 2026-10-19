# Copyright (c) 2026 Kenneth Stott
# Canary: 6d1f8b42-3a7e-4c95-b0d2-e9c4a7f15b83
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""MkDocs hook: each locale's docs search only that locale's pages.

mkdocs-static-i18n builds every locale into one site and merges every locale's pages into one
``search/search_index.json`` at the site root, which every page's search reads (Material fetches
``search/search_index.json`` relative to the page's ``__config.base``, the site root). A search
from a French page answered English, Chinese and Hebrew pages too.

After the build, the merged index is split by each entry's locale folder: the default locale keeps
the root index, and each other locale gets ``<locale>/search/search_index.json`` with its entries'
locations made relative to its own folder. Each locale page's ``__config.base`` then names its
locale's folder, so its search reads its own index.
"""

from __future__ import annotations

import json
import os
import re
from pathlib import Path
from typing import Any

_BASE = re.compile(r'(<script id="__config" type="application/json">\{[^<]*?"base": ")([^"]*)(")')


def _locales(config: Any) -> list[str]:
    """The non-default locales the i18n plugin builds: each one's pages are under its folder."""
    plugin = config["plugins"]["i18n"]
    return [lang.locale for lang in plugin.config.languages if lang.build and not lang.default]


def _split(index: dict, locales: list[str]) -> tuple[list[dict], dict[str, list[dict]]]:
    default: list[dict] = []
    by_locale: dict[str, list[dict]] = {loc: [] for loc in locales}
    for entry in index["docs"]:
        first = entry["location"].split("/", 1)[0].split("#", 1)[0]
        if first in by_locale:
            by_locale[first].append(
                {**entry, "location": entry["location"][len(first) :].lstrip("/")}
            )
        else:
            default.append(entry)
    return default, by_locale


def _point_base_at(site: Path, locale: str) -> None:
    """Each page of ``locale`` reads its search index from its locale's folder."""
    root = site / locale
    for page in root.rglob("*.html"):
        base = os.path.relpath(root, page.parent)
        html = page.read_text(encoding="utf-8")
        rebased, n = _BASE.subn(lambda m: m.group(1) + base + m.group(3), html, count=1)
        if n:
            page.write_text(rebased, encoding="utf-8")


def on_post_build(config: Any, **_kwargs: Any) -> None:
    site = Path(config["site_dir"])
    merged = site / "search" / "search_index.json"
    if not merged.is_file():
        return  # no search plugin output: nothing to split
    index = json.loads(merged.read_text(encoding="utf-8"))
    locales = _locales(config)
    default, by_locale = _split(index, locales)
    merged.write_text(json.dumps({**index, "docs": default}), encoding="utf-8")
    for locale, entries in by_locale.items():
        out = site / locale / "search" / "search_index.json"
        out.parent.mkdir(parents=True, exist_ok=True)
        out.write_text(json.dumps({**index, "docs": entries}), encoding="utf-8")
        _point_base_at(site, locale)


# Last: mkdocs-static-i18n writes the merged index in its own post-build (priority -100), after
# building every locale; this runs after it. The attribute mkdocs.plugins.event_priority sets,
# set here so the hook needs nothing of MkDocs to import.
on_post_build.mkdocs_priority = -1000  # type: ignore[attr-defined]
