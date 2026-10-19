# Copyright (c) 2026 Kenneth Stott
# Canary: 4b8e2d17-9c6a-4f31-a5e0-7d3c1b9f6e24
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Each locale's docs search reads only that locale's pages (scripts/mkdocs_search_per_locale.py):
the merged index mkdocs-static-i18n writes is split by locale folder, and each locale page's
search reads its own folder's index."""

from __future__ import annotations

import importlib.util
import json
from pathlib import Path
from types import SimpleNamespace

_SPEC = importlib.util.spec_from_file_location(
    "mkdocs_search_per_locale",
    Path(__file__).resolve().parents[2] / "scripts" / "mkdocs_search_per_locale.py",
)
hook = importlib.util.module_from_spec(_SPEC)
assert _SPEC.loader is not None
_SPEC.loader.exec_module(hook)

_CONFIG_SCRIPT = (
    '<script id="__config" type="application/json">{{"base": "{base}", "x": 1}}</script>'
)


def _config(site: Path) -> dict:
    languages = [
        SimpleNamespace(locale=loc, build=True, default=loc == "en")
        for loc in ("en", "fr", "zh", "zh-HK")
    ]
    plugin = SimpleNamespace(config=SimpleNamespace(languages=languages))
    return {"site_dir": str(site), "plugins": {"i18n": plugin}}


def test_each_locale_gets_an_index_of_its_own_pages_and_its_pages_read_it(tmp_path):
    entries = [
        {"location": "security/", "title": "Security"},
        {"location": "fr/security/", "title": "Sécurité"},
        {"location": "zh/security/#tls", "title": "安全"},
        {"location": "zh-HK/security/", "title": "安全"},
    ]
    (tmp_path / "search").mkdir()
    (tmp_path / "search" / "search_index.json").write_text(
        json.dumps({"config": {"lang": ["en"]}, "docs": entries})
    )
    page = tmp_path / "fr" / "security" / "index.html"
    page.parent.mkdir(parents=True)
    page.write_text(_CONFIG_SCRIPT.format(base="../.."))
    home = tmp_path / "security" / "index.html"
    home.parent.mkdir(parents=True)
    home.write_text(_CONFIG_SCRIPT.format(base=".."))

    hook.on_post_build(_config(tmp_path))

    def index(locale: str = "") -> list[str]:
        path = tmp_path / locale / "search" / "search_index.json"
        return [e["location"] for e in json.loads(path.read_text())["docs"]]

    assert index() == ["security/"]
    assert index("fr") == ["security/"]
    assert index("zh") == ["security/#tls"]
    assert index("zh-HK") == ["security/"]
    assert '"base": ".."' in page.read_text()  # its locale's folder: /fr/search/search_index.json
    assert '"base": ".."' in home.read_text()  # the default locale's pages are untouched
