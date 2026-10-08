# Copyright (c) 2026 Kenneth Stott
# Canary: 9d4c7a26-3f1b-4e8a-b5d2-6a0e3c9f7b18
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Wikipedia, a branded source carried by the ``files`` source's HTML crawl (REQ-1960).

The brand adds to a crawl only what is particular to Wikipedia: which part of a page is the
article, what an article link is, which tables hold data, and how to ask politely. A page's
links are told apart by what the page's own markup says each one is -- never by a name in its
address, which differs in every language edition and also occurs in ordinary titles.

Adding the source registers nothing: the tables the crawl finds are available, and a steward
registers the ones wanted.
"""

from __future__ import annotations

import re
from urllib.parse import quote

# Requirements: REQ-1960

BRAND = "wikipedia"

#: The article's own region of a page: everything read, links and tables, is inside it.
CONTENT_SELECTOR = "#mw-content-text .mw-parser-output"

#: Dropped from the article before anything is read: navigation and information boxes, notes
#: and citations, maintenance messages, media and the table of contents. Their links have the
#: same form as the article's own, so only where they sit tells them apart.
REMOVE_SELECTORS: tuple[str, ...] = (
    ".navbox",
    ".navbox-styles",
    ".sidebar",
    ".hatnote",
    ".infobox",
    ".reflist",
    ".references",
    ".mw-references-wrap",
    ".refbegin",
    "sup.reference",
    "cite",
    ".citation",
    ".cs1-maint",
    ".cs1-hidden-error",
    ".cs1-visible-error",
    ".mw-editsection",
    ".metadata",
    ".ambox",
    ".noprint",
    ".catlinks",
    ".toc",
    ".thumb",
    "figure",
    ".gallery",
    ".side-box",
    ".sistersitebox",
    ".portalbox",
    ".authority-control",
    ".shortdescription",
    ".sortkey",
    "style",
    "#coordinates",
)

#: A link to another article, as the page marks one: not a page that is not written, not the
#: page itself, not a book-source lookup. File pages, other wikis and external sites carry
#: other marks and so are not matched at all.
LINK_SELECTOR = "a[rel='mw:WikiLink']:not(.new):not(.mw-selflink):not(.mw-magiclink-isbn)"

#: Tables of data, as editors mark them; layout tables are not.
TABLE_SELECTOR = "table.wikitable"

DATA_FILE_EXTENSIONS: tuple[str, ...] = ("csv", "tsv", "xlsx", "xls", "json", "parquet")

#: The id of the "See also" heading by language edition. It is the one thing here that is a
#: name: an edition has no mark for that section other than its heading. An edition absent
#: here is given its section's title by the operator.
SEE_ALSO_HEADING: dict[str, str] = {
    "en": "See_also",
    "de": "Siehe_auch",
    "es": "Véase_también",
    "it": "Voci_correlate",
    "pt": "Ver_também",
    "nl": "Zie_ook",
    "pl": "Zobacz_też",
    "sv": "Se_även",
    "ja": "関連項目",
    "zh": "参见",
    "ru": "См._также",
}

DEFAULTS: dict = {
    "language": "en",
    "max_depth": 1,
    "max_pages": 25,
    "follow_see_also": False,
    "request_delay": "1 seconds",
    "html_cache_ttl": "1 days",
    "html_table_min_rows": 2,
}

_LANGUAGE = re.compile(r"^[a-z]{2,3}(-[a-z0-9]+)*$")


class InvalidWikipediaSource(ValueError):
    """What the operator gave cannot be made into a crawl; ``code`` names why."""

    def __init__(self, code: str, message: str, **params) -> None:
        super().__init__(message)
        self.code = code
        self.params = params


def page_url(language: str, page: str) -> str:
    """The address of ``page`` -- a title as the operator types it, or an address of that
    edition, which is kept."""
    host = f"https://{language}.wikipedia.org"
    if page.startswith(("http://", "https://")):
        if not page.startswith(f"{host}/wiki/"):
            raise InvalidWikipediaSource(
                "wikipedia.page_not_of_edition",
                f"{page!r} is not a page of {host}",
                page=page,
                host=host,
            )
        return page
    return f"{host}/wiki/{quote(page.strip().replace(' ', '_'), safe='()_,:%!*~-.')}"


def user_agent(version: str, contact: str | None) -> str:
    """What the crawl names itself as: Provisa and its version, and the operator's contact when
    one is given, as the site's user-agent policy asks."""
    agent = f"Provisa/{version} (+https://provisa.dev) file-crawler"
    return f"{agent}; {contact.strip()}" if contact and contact.strip() else agent


def crawl_settings(settings: dict, version: str) -> dict:
    """The ``crawl`` of a ``files`` source's mapping for a Wikipedia source, from what the
    operator gave: the brand's defaults, with every setting the operator stated in its place.

    ``settings`` carries the setup form's own fields (``language``, ``pages``,
    ``follow_see_also``, ``see_also_heading``, ``contact``) and, under ``crawl``, any crawl
    setting stated outright.
    """
    given = {**DEFAULTS, **{k: v for k, v in settings.items() if v is not None}}
    language = str(given["language"]).strip().lower()
    if not _LANGUAGE.match(language):
        raise InvalidWikipediaSource(
            "wikipedia.unknown_language",
            f"{language!r} is not a language edition",
            language=language,
        )
    pages = [p for p in (given.get("pages") or []) if str(p).strip()]
    if not pages:
        raise InvalidWikipediaSource("wikipedia.no_pages", "name at least one page to start from")
    remove = list(REMOVE_SELECTORS)
    if not given["follow_see_also"]:
        # The operator gives the section's title as a page shows it ("Voir aussi"); a heading's
        # id is its title with each space an underscore.
        title = str(given.get("see_also_heading") or "").strip().replace(" ", "_")
        heading = title or SEE_ALSO_HEADING.get(language)
        if not heading:
            raise InvalidWikipediaSource(
                "wikipedia.see_also_heading_needed",
                f"what the {language!r} edition calls its 'See also' section is not known: give "
                "the section's title, or follow those links",
                language=language,
            )
        # A section is wrapped in an element labelled with its heading's id.
        remove.append(f"section[aria-labelledby='{heading}']")
    crawl = {
        "start_urls": [page_url(language, str(p)) for p in pages],
        "max_depth": int(given["max_depth"]),
        "max_pages": int(given["max_pages"]),
        "request_delay": given["request_delay"],
        "html_cache_ttl": given["html_cache_ttl"],
        "user_agent": user_agent(version, given.get("contact")),
        "content_selector": CONTENT_SELECTOR,
        "remove_selectors": remove,
        "link_selector": LINK_SELECTOR,
        "follow_external_links": False,
        "table_selector": TABLE_SELECTOR,
        "html_table_min_rows": int(given["html_table_min_rows"]),
        "allowed_file_extensions": list(DATA_FILE_EXTENSIONS),
    }
    crawl.update(given.get("crawl") or {})
    return crawl
