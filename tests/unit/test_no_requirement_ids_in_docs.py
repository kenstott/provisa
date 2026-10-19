# Copyright (c) 2026 Kenneth Stott
# Canary: 8c4e1f27-5b9a-4d63-a0e8-2f7c3b6d9e15
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The published docs name no requirement, and link to no page that is not published.

A requirement id (``REQ-1234``) belongs to the people who build the product, as the messages a
client receives do not carry one (test_no_requirement_ids_in_messages). Every page the docs site
builds -- every page under docs/ and every locale's, less what mkdocs.yml's ``exclude_docs``
leaves out and the requirements page itself -- and every README is read by customers. Which
pages describe a requirement is traced in docs/arch/requirements.yaml (each requirement's
``docs``), not in their text."""

from __future__ import annotations

import posixpath
import re
from pathlib import Path

import yaml

ROOT = Path(__file__).resolve().parents[2]
DOCS = ROOT / "docs"
LOCALES = ("de", "es", "fr", "he", "it", "pt", "ru", "zh", "zh-HK")
#: The requirements page is where requirement ids belong.
REQUIREMENTS_PAGE = "arch/requirements.md"
_ID = re.compile(r"REQ-\d")
_LINK = re.compile(r"\]\(([^)#\s]+\.md)(?:#[^)]*)?\)")


def _excluded() -> list[str]:
    block = (ROOT / "mkdocs.yml").read_text().split("exclude_docs: |\n", 1)[1].split("\n\n", 1)[0]
    return [
        ln.strip() for ln in block.splitlines() if ln.strip() and not ln.strip().startswith("#")
    ]


def _is_excluded(page: str, excluded: list[str]) -> bool:
    return any(page == e or (e.endswith("/") and page.startswith(e)) for e in excluded)


def _english(rel: str) -> str:
    first, _, rest = rel.partition("/")
    return rest if first in LOCALES and rest else rel


def _published() -> list[tuple[Path, str]]:
    """Each published page with its path under docs/ (README.<lng>.md stands for its locale's
    index page)."""
    excluded = _excluded()
    pages = []
    for path in sorted(DOCS.rglob("*.md")):
        rel = path.relative_to(DOCS).as_posix()
        page = _english(rel)
        if page == REQUIREMENTS_PAGE or _is_excluded(page, excluded):
            continue
        pages.append((path, rel))
    pages += [(path, "index.md") for path in sorted(ROOT.glob("README*.md"))]
    return pages


def test_there_are_published_pages_to_check():
    assert len(_published()) > 100


def test_no_published_page_names_a_requirement():
    named = [
        f"{path.relative_to(ROOT)}:{n}: {line.strip()[:100]}"
        for path, _ in _published()
        for n, line in enumerate(path.read_text(encoding="utf-8").splitlines(), 1)
        if _ID.search(line)
    ]
    assert named == [], "\n".join(named[:20])


def test_no_published_page_links_to_one_that_is_not_published():
    excluded = _excluded()
    broken = []
    for path, rel in _published():
        here = posixpath.dirname(rel)
        for target in _LINK.findall(path.read_text(encoding="utf-8")):
            if target.startswith(("http://", "https://")):
                continue
            target = target.removeprefix("docs/") if path.name.startswith("README") else target
            page = _english(posixpath.normpath(posixpath.join(here, target)))
            if _is_excluded(page, excluded):
                broken.append(f"{path.relative_to(ROOT)} -> {target}")
    assert broken == [], "\n".join(broken)


def test_every_requirement_traces_only_published_pages():
    excluded = _excluded()
    entries = yaml.safe_load((DOCS / "arch" / "requirements.yaml").read_text())
    unpublished = [
        f"{e['id']}: {page}"
        for e in entries
        for page in e.get("docs") or []
        if _is_excluded(page, excluded)
    ]
    assert unpublished == []
