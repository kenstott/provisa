# Copyright (c) 2026 Kenneth Stott
# Canary: 97dc2f9c-3dd3-44ad-ae11-6863007ac322
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""REQ-2114: Polly searches and reads the built product docs (provisa/api/mcp/docs_tools.py).

The site is built here in the shape MkDocs Material and scripts/mkdocs_search_per_locale.py give
it: the English index at the site root, each other locale's under its own folder, and each page's
headings in its index.html."""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from provisa.api.mcp import docs_tools


def _page(site: Path, folder: str, headings: list[tuple[int, str]]) -> None:
    out = site / folder / "index.html" if folder else site / "index.html"
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(
        "".join(f'<h{level} id="{anchor}">x</h{level}>' for level, anchor in headings),
        encoding="utf-8",
    )


def _index(site: Path, locale: str, docs: list[dict]) -> None:
    out = (site if locale == "en" else site / locale) / "search" / "search_index.json"
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(json.dumps({"config": {}, "docs": docs}), encoding="utf-8")


@pytest.fixture
def site(tmp_path: Path) -> Path:
    _index(
        tmp_path,
        "en",
        [
            {"location": "", "title": "Provisa", "text": "<p>Home.</p>"},
            {
                "location": "code-sources/",
                "title": "Code Resolvers",
                "text": "<p>Business logic in Python.</p>",
            },
            {
                "location": "code-sources/#code-resolvers",
                "title": "Relating two tables: code resolvers",
                "text": "<p>A code resolver is a <code>@query</code> that takes the source "
                "table and the target table.</p>",
            },
            {
                "location": "code-sources/#code-resolver-change",
                "title": "Keeping the matches incrementally: <code>@change</code>",
                "text": "<p>Tag a second function <code>@change</code>.</p>",
            },
            {
                "location": "code-sources/#go",
                "title": "Go",
                "text": "<pre><code>func Score() int {\n  return 1\n}</code></pre>",
            },
            {
                "location": "sources/",
                "title": "Sources",
                "text": "<p>Every source type &amp; its setup.</p>",
            },
        ],
    )
    _page(tmp_path, "", [])
    _page(
        tmp_path,
        "code-sources",
        [(1, "code-sources_1"), (3, "code-resolvers"), (4, "code-resolver-change"), (3, "go")],
    )
    _page(tmp_path, "sources", [])
    # French translates code-sources only: sources is untranslated.
    _index(
        tmp_path,
        "fr",
        [
            {
                "location": "code-sources/",
                "title": "Resolvers de code",
                "text": "<p>Logique métier en Python.</p>",
            },
            {
                "location": "code-sources/#code-resolvers",
                "title": "Relier deux tables : resolvers de code",
                "text": "<p>Un resolver de code est une <code>@query</code>.</p>",
            },
        ],
    )
    _page(tmp_path, "fr/code-sources", [(1, "code-sources_1"), (3, "code-resolvers")])
    return tmp_path


class TestSearchDocs:
    def test_finds_a_known_section_with_its_address_and_snippet(self, site):
        found = docs_tools.search_docs("code resolver", "en", site=site)["results"]
        top = found[0]
        assert top["address"] == "code-sources#code-resolvers"
        assert top["title"] == "Relating two tables: code resolvers"
        assert "`@query`" in top["snippet"]
        assert top["language"] == "en"

    def test_searches_the_users_language(self, site):
        found = docs_tools.search_docs("resolver", "fr", site=site)["results"]
        assert found[0]["language"] == "fr"
        assert found[0]["title"] == "Relier deux tables : resolvers de code"
        # The English original of a translated page is never mixed in.
        assert not [
            r for r in found if r["address"].startswith("code-sources") and r["language"] == "en"
        ]

    def test_an_untranslated_page_is_searched_in_english(self, site):
        found = docs_tools.search_docs("source type setup", "fr-CA", site=site)["results"]
        assert {"address": "sources", "language": "en"}.items() <= found[0].items()

    def test_a_site_not_in_the_install_is_refused_by_name(self, tmp_path):
        with pytest.raises(docs_tools.DocsUnavailable, match="not in this install"):
            docs_tools.search_docs("anything", "en", site=tmp_path / "docs-site")


class TestReadDoc:
    def test_returns_a_sections_text_with_its_subsections(self, site):
        doc = docs_tools.read_doc("code-sources#code-resolvers", "en", site=site)
        assert doc["sections"] == [
            "code-sources#code-resolvers",
            "code-sources#code-resolver-change",
        ]
        assert "A code resolver is a `@query`" in doc["text"]
        assert "Tag a second function `@change`." in doc["text"]
        assert "func Score" not in doc["text"]

    def test_returns_a_whole_page_and_keeps_code_blocks(self, site):
        doc = docs_tools.read_doc("code-sources", "en", site=site)
        assert doc["title"] == "Code Resolvers"
        assert "```\nfunc Score() int {\n  return 1\n}\n```" in doc["text"]

    def test_reads_the_translation_where_there_is_one(self, site):
        doc = docs_tools.read_doc("code-sources#code-resolvers", "fr", site=site)
        assert doc["language"] == "fr"
        assert "Un resolver de code est une `@query`." in doc["text"]

    def test_reads_an_untranslated_page_in_english(self, site):
        doc = docs_tools.read_doc("sources", "fr", site=site)
        assert doc["language"] == "en"
        assert "Every source type & its setup." in doc["text"]

    def test_the_home_page_is_index(self, site):
        assert docs_tools.read_doc("index", "en", site=site)["text"] == "# Provisa\n\nHome."

    def test_an_unknown_page_is_refused_by_name(self, site):
        with pytest.raises(docs_tools.UnknownDocAddress, match="no page 'nowhere'"):
            docs_tools.read_doc("nowhere#x", "en", site=site)

    def test_an_unknown_section_is_refused_by_name_with_the_pages_sections(self, site):
        with pytest.raises(docs_tools.UnknownDocAddress) as refused:
            docs_tools.read_doc("code-sources#python-reference", "en", site=site)
        message = str(refused.value)
        assert "no section 'python-reference'" in message
        assert "code-sources#code-resolvers" in message
