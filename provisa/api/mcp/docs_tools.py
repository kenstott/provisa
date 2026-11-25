# Copyright (c) 2026 Kenneth Stott
# Canary: 55bdc8e5-c280-4243-b621-12a217e50874
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Polly searches and reads the product documentation (REQ-2114).

She reads the built MkDocs site the product ships, the one the in-app Docs page frames from
/docs-site/: provisa-ui's prebuild (scripts/build-docs-site.mjs) builds it into the UI's dist,
which the wheel stages as provisa/_ui/docs-site and the image as static/docs-site. Nothing is
fetched from provisa.dev.

Each locale's search index is ``<site>/<locale>/search/search_index.json`` and the default
locale's (English) is ``<site>/search/search_index.json`` (scripts/mkdocs_search_per_locale.py).
An entry's ``location`` is ``<page>/#<section>``, or ``<page>/`` for the page's top. An address,
as Polly gives and reads it, is ``<page>`` or ``<page>#<section>`` (``index`` is the home page).
"""

from __future__ import annotations

import html
import json
import re
from dataclasses import dataclass
from pathlib import Path

from provisa.docs_site_path import DOCS_SITE  # noqa: F401 — re-exported; callers may import it here

DEFAULT_LOCALE = "en"
HOME = "index"
_SNIPPET = 240
_HEADING = re.compile(r'<h([1-6]) id="([^"]+)"')
_TERM = re.compile(r"[\w@.]+", re.UNICODE)


@dataclass(frozen=True)
class _Section:
    page: str
    anchor: str  # "" for the page's top
    title: str
    text: str  # plain text
    locale: str

    @property
    def address(self) -> str:
        return f"{self.page}#{self.anchor}" if self.anchor else self.page


class DocsUnavailable(LookupError):
    """The built docs site is not in this install."""


class UnknownDocAddress(LookupError):
    """An address the docs do not have."""


def _plain(fragment: str) -> str:
    """The text of a search-index HTML fragment: code kept as Markdown code, tags dropped."""
    text = re.sub(
        r"<pre><code[^>]*>(.*?)</code></pre>",
        lambda m: f"\n```\n{m.group(1).strip()}\n```\n",
        fragment,
        flags=re.S,
    )
    text = re.sub(r"<code[^>]*>(.*?)</code>", r"`\1`", text, flags=re.S)
    text = re.sub(r"<li[^>]*>", "\n- ", text)
    text = re.sub(r"</(p|ul|ol|table|tr|h[1-6])>", "\n", text)
    text = re.sub(r"<[^>]+>", "", text)
    text = html.unescape(text)
    text = re.sub(r"[ \t]+\n", "\n", text)
    return re.sub(r"\n{3,}", "\n\n", text).strip()


def _split_location(location: str) -> tuple[str, str]:
    page, _, anchor = location.partition("#")
    return (page.strip("/") or HOME), anchor


def _locale_dir(site: Path, locale: str) -> Path:
    return site if locale == DEFAULT_LOCALE else site / locale


def _index_path(site: Path, locale: str) -> Path:
    return _locale_dir(site, locale) / "search" / "search_index.json"


def _require_site(site: Path) -> None:
    if not _index_path(site, DEFAULT_LOCALE).is_file():
        raise DocsUnavailable(
            f"the built docs site is not in this install: no search index at "
            f"{_index_path(site, DEFAULT_LOCALE)}"
        )


def _docs_locale(site: Path, language: str) -> str:
    """The docs locale for ``language``, matched as the Docs page matches the UI language
    (DocsPage.tsx): the exact locale, else its base language. A language the docs are not built
    in at all has every page untranslated, so it reads English (see _sections)."""
    lang = language.strip()
    for candidate in (lang, lang.split("-")[0]):
        if candidate == DEFAULT_LOCALE or (candidate and _index_path(site, candidate).is_file()):
            return candidate
    return DEFAULT_LOCALE


_CACHE: dict[tuple[Path, int], list[_Section]] = {}


def _load(site: Path, locale: str) -> list[_Section]:
    path = _index_path(site, locale)
    key = (path, path.stat().st_mtime_ns)
    held = _CACHE.get(key)
    if held is not None:
        return held
    docs = json.loads(path.read_text(encoding="utf-8"))["docs"]
    sections = []
    for entry in docs:
        page, anchor = _split_location(entry["location"])
        sections.append(
            _Section(page, anchor, _plain(entry["title"]), _plain(entry["text"]), locale)
        )
    _CACHE[key] = sections
    return sections


def _sections(site: Path, language: str) -> list[_Section]:
    """Every section in ``language``'s docs. REQ-2114: the user's language first; a page that has
    no translation in it is read in English, the docs' default locale, and only such a page --
    a translated page is never mixed with its English original."""
    _require_site(site)
    locale = _docs_locale(site, language)
    english = _load(site, DEFAULT_LOCALE)
    if locale == DEFAULT_LOCALE:
        return english
    translated = _load(site, locale)
    pages = {s.page for s in translated}
    return translated + [s for s in english if s.page not in pages]


def _terms(query: str) -> list[str]:
    words = (t.strip(".") for t in _TERM.findall(query.lower()))
    return [t for t in words if len(t) > 1 or (t and not t.isascii())]


def _snippet(text: str, terms: list[str]) -> str:
    flat = " ".join(text.split())
    lower = flat.lower()
    hits = [i for i in (lower.find(t) for t in terms) if i >= 0]
    start = max(0, min(hits) - _SNIPPET // 3) if hits else 0
    piece = flat[start : start + _SNIPPET]
    return ("…" if start else "") + piece + ("…" if start + _SNIPPET < len(flat) else "")


def search_docs(query: str, language: str, *, site: Path = DOCS_SITE, k: int = 8) -> dict:
    """The sections best matching ``query``, each with its title, address and a snippet."""
    terms = _terms(query)
    if not terms:
        raise ValueError("search_docs needs a query with at least one word")
    scored = []
    for section in _sections(site, language):
        title, text = section.title.lower(), section.text.lower()
        matched = [t for t in terms if t in title or t in text]
        if not matched:
            continue
        score = len(matched) * 10 + sum(5 * title.count(t) + min(text.count(t), 5) for t in matched)
        scored.append((score, section))
    scored.sort(key=lambda pair: -pair[0])
    return {
        "query": query,
        "results": [
            {
                "title": s.title,
                "address": s.address,
                "language": s.locale,
                "snippet": _snippet(s.text, terms),
            }
            for _, s in scored[:k]
        ],
    }


def _heading_levels(site: Path, page: str, locale: str) -> dict[str, int]:
    folder = _locale_dir(site, locale)
    page_html = folder / "index.html" if page == HOME else folder / page / "index.html"
    return {
        anchor: int(level)
        for level, anchor in _HEADING.findall(page_html.read_text(encoding="utf-8"))
    }


def read_doc(address: str, language: str, *, site: Path = DOCS_SITE) -> dict:
    """The text of a page, or of one section with its subsections, by its address."""
    page, anchor = _split_location(address.strip())
    sections = [s for s in _sections(site, language) if s.page == page]
    if not sections:
        raise UnknownDocAddress(f"the docs have no page {page!r}")
    locale = sections[0].locale
    if anchor:
        at = next((i for i, s in enumerate(sections) if s.anchor == anchor), None)
        if at is None:
            raise UnknownDocAddress(
                f"the docs page {page!r} has no section {anchor!r} in {locale!r}; its sections: "
                + ", ".join(s.address for s in sections if s.anchor)
            )
        levels = _heading_levels(site, page, locale)
        level = levels[anchor]
        end = next(
            (j for j in range(at + 1, len(sections)) if levels[sections[j].anchor] <= level),
            len(sections),
        )
        sections = sections[at:end]
    body = "\n\n".join(
        f"## {s.title}\n\n{s.text}" if s.anchor else f"# {s.title}\n\n{s.text}" for s in sections
    )
    return {
        "address": address.strip(),
        "language": locale,
        "title": sections[0].title,
        "sections": [s.address for s in sections],
        "text": body.strip(),
    }
