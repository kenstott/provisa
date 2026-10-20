"""The security team's policy, read from its published HTML: one row a control, with its section
number, heading, the clause's words and the day it took effect.

The document ships inside this package (cloud-security-policy.html), so reading it fetches
nothing: ``url`` is its address within the package."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from html.parser import HTMLParser
from pathlib import Path

_HERE = Path(__file__).parent.resolve()


@dataclass
class PolicySection:
    section: str
    heading: str
    clause: str
    effective_date: date


class _Sections(HTMLParser):
    """Each <section data-section data-effective>: its <h2> heading and its paragraphs' words."""

    def __init__(self) -> None:
        super().__init__()
        self.found: list[PolicySection] = []
        self._open: dict[str, str] | None = None
        self._tag: str | None = None
        self._heading: list[str] = []
        self._clause: list[str] = []

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        if tag == "section":
            self._open = {k: v or "" for k, v in attrs}
            self._heading, self._clause = [], []
        elif self._open is not None and tag in ("h2", "p"):
            self._tag = tag

    def handle_endtag(self, tag: str) -> None:
        if tag == "section" and self._open is not None:
            number = self._open["data-section"]
            heading = " ".join("".join(self._heading).split())
            self.found.append(
                PolicySection(
                    section=number,
                    heading=heading.removeprefix(number).strip(),
                    clause=" ".join(" ".join(self._clause).split()),
                    effective_date=date.fromisoformat(self._open["data-effective"]),
                )
            )
            self._open = None
        elif tag == self._tag:
            self._tag = None

    def handle_data(self, data: str) -> None:
        if self._tag == "h2":
            self._heading.append(data)
        elif self._tag == "p":
            self._clause.append(data)


def policy_sections(url: str = "cloud-security-policy.html") -> list[PolicySection]:
    """The controls of the policy published at ``url`` (the shipped policy when left out), in the
    document's order.

    @table
    """
    document = (_HERE / url).resolve()
    if _HERE not in document.parents:
        raise ValueError(f"{url} is not a document of this package")
    parser = _Sections()
    parser.feed(document.read_text(encoding="utf-8"))
    return parser.found
