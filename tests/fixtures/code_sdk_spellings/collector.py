# Copyright (c) 2026 Kenneth Stott
# Canary: f4d192a6-1bb5-474b-8374-5748ee951a7b
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The binding corpus of the optional code-source SDKs (REQ-1984): each case is one tag form,
spelled twice in each language the SDK is built for, once with the docstring or JSDoc tag and once
with the SDK, and the two must read as the same manifest.

Two places hold cases:

- this directory, ``<case>/<language>/<spelling>/<file>``, with ``<spelling>`` ``doc`` or ``sdk``
  and an optional ``<case>/case.json`` stating what each spelling must give when it is not plain
  acceptance;
- docs/code-sources.md, whose every tagged example in a built language is a tab pair, the
  docstring (or JSDoc) tab first and the SDK tab second. Each pair is a case that must be accepted.

``case.json`` holds, per language, per spelling, ``problems`` (the manifest's refusal, every
problem in order; absent means accepted) and ``checker`` (``accepts`` or ``rejects``: whether the
language's type checker, pyright, the pinned tsc or cargo check, passes the SDK spelling; absent
means ``accepts``). A TypeScript SDK spelling the checker rejects does not build, so it is never
read and states no problems. Rust's reader parses without compiling, so a Rust spelling cargo
rejects is still read and states what the reader gives. ``unspelled`` names a language, or a ``<language>/<spelling>``, the case leaves
out, with the reason.
"""

# Requirements: REQ-1984
from __future__ import annotations

import json
import re
from dataclasses import dataclass
from pathlib import Path

from provisa.codesource.manifest import LOADER, PROVISA_TAGS, REQUIRES, WRITE_TAGS

HERE = Path(__file__).resolve().parent
DOCS = HERE.parents[2] / "docs" / "code-sources.md"

#: The languages an SDK is built for. Go joins when its SDK does.
BUILT = ("python", "typescript", "rust")
SPELLINGS = ("doc", "sdk")
#: Each language's file of a spelling.
FILES = {"python": "source.py", "typescript": "index.ts", "rust": "lib.rs"}
#: The fence language of each built language in the docs.
FENCES = {"python": "python", "typescript": "typescript", "rust": "rust"}
#: The label of a docs example's first tab, which holds the tag form.
DOC_TABS = {"python": "Docstring", "typescript": "JSDoc", "rust": "Doc comment"}
SDK_TAB = "SDK"
#: Every tag a code source's function may carry.
TAGS = frozenset(PROVISA_TAGS | {LOADER, REQUIRES, *WRITE_TAGS})
#: How each language's code imports the SDK.
SDK_IMPORTS = {
    "python": re.compile(r"^\s*(from provisa_sdk import|import provisa_sdk)\b", re.M),
    "typescript": re.compile(r"""from ["']@provisa/sdk["']"""),
    "rust": re.compile(r"\bprovisa_sdk::"),
}

_TAG_LINE = re.compile(r'^\s*(?:/\*\*|\*|"""|///)?\s*@(?P<tag>[a-z][a-z-]*)\b', re.M)
_FENCE = re.compile(r"^(?P<indent> *)```(?P<lang>\w+)\s*$")
_TAB = re.compile(r'^=== "(?P<label>[^"]+)"\s*$')
_ANCHOR = re.compile(r"^#+ .*\{: #(?P<anchor>[\w-]+) \}\s*$")


@dataclass(frozen=True)
class Spelling:
    """One spelling of a case: its code and what it must give."""

    code: str
    #: The manifest's problems, in order; empty when it is accepted; None when it is never read.
    problems: tuple[str, ...] | None
    #: Whether the language's type checker passes it (asked of SDK spellings only).
    checker_accepts: bool


@dataclass(frozen=True)
class Case:
    name: str
    language: str
    spellings: dict[str, Spelling]
    #: Where it is written: its directory, or the docs line of its first tab.
    origin: str

    def write(self, root: Path, spelling: str) -> Path:
        """The directory holding ``spelling``'s code, written under ``root``."""
        directory = root / self.name / spelling
        directory.mkdir(parents=True, exist_ok=True)
        (directory / FILES[self.language]).write_text(self.spellings[spelling].code)
        return directory

    @property
    def accepted(self) -> bool:
        return all(s.problems == () for s in self.spellings.values())


def tags_of(code: str) -> set[str]:
    """The Provisa tags a spelling's code carries, on docstring, JSDoc, doc-comment or decorator
    lines."""
    return {m.group("tag") for m in _TAG_LINE.finditer(code)} & TAGS


def imports_sdk(language: str, code: str) -> bool:
    return bool(SDK_IMPORTS[language].search(code))


def _spelling(code: str, stated: dict, language: str, spelling: str) -> Spelling:
    checker = stated.get("checker", "accepts")
    if checker not in ("accepts", "rejects") or (spelling == "doc" and "checker" in stated):
        raise ValueError(f"{language}/{spelling}: checker is accepts or rejects, on sdk only")
    unread = language == "typescript" and checker == "rejects"
    if unread and "problems" in stated:
        raise ValueError(f"{language}/{spelling}: a spelling tsc rejects is never read")
    problems = None if unread else tuple(stated.get("problems", ()))
    return Spelling(code, problems, checker == "accepts")


def fixture_cases(root: Path = HERE) -> list[Case]:
    """Every case written in this directory, by name and language."""
    found: list[Case] = []
    for directory in sorted(p for p in root.iterdir() if p.is_dir() and p.name != "__pycache__"):
        spec_file = directory / "case.json"
        spec = json.loads(spec_file.read_text()) if spec_file.is_file() else {}
        for language in BUILT:
            spellings = {}
            for spelling in SPELLINGS:
                path = directory / language / spelling / FILES[language]
                if path.is_file():
                    stated = spec.get(language, {}).get(spelling, {})
                    spellings[spelling] = _spelling(path.read_text(), stated, language, spelling)
            if spellings:
                found.append(Case(directory.name, language, spellings, str(directory)))
    return found


def unspelled(root: Path = HERE) -> dict[tuple[str, str], str]:
    """``(case, language or language/spelling)`` to the reason the case leaves it out."""
    out: dict[tuple[str, str], str] = {}
    for spec_file in sorted(root.glob("*/case.json")):
        for left_out, reason in json.loads(spec_file.read_text()).get("unspelled", {}).items():
            out[(spec_file.parent.name, left_out)] = reason
    return out


@dataclass(frozen=True)
class _Block:
    language: str
    code: str
    line: int
    tab: str | None
    section: str


def _blocks(lines: list[str]) -> list[_Block]:
    blocks: list[_Block] = []
    tab: str | None = None
    section = "top"
    index = 0
    while index < len(lines):
        line = lines[index]
        header = _TAB.match(line)
        anchor = _ANCHOR.match(line)
        fence = _FENCE.match(line)
        if header:
            tab = header.group("label")
        elif anchor:
            section, tab = anchor.group("anchor"), None
        elif fence:
            indent = fence.group("indent")
            end = index + 1
            while lines[end] != f"{indent}```":
                end += 1
            code = "\n".join(text.removeprefix(indent) for text in lines[index + 1 : end]) + "\n"
            in_tab = tab if len(indent) == 4 else None
            blocks.append(_Block(fence.group("lang"), code, index + 1, in_tab, section))
            index = end
        elif line and not line.startswith(" "):
            tab = None
        index += 1
    return blocks


def _tagged(language: str, code: str) -> bool:
    return bool(tags_of(code)) or imports_sdk(language, code)


def doc_cases(markdown: Path = DOCS) -> tuple[list[Case], list[str]]:
    """The tab pairs of ``markdown`` as cases, and every tagged example that is not one."""
    languages = {fence: language for language, fence in FENCES.items()}
    cases: list[Case] = []
    problems: list[str] = []
    per_section: dict[str, int] = {}
    blocks = _blocks(markdown.read_text().splitlines())
    at = f"{markdown.name}:{{}}"
    index = 0
    while index < len(blocks):
        block = blocks[index]
        language = languages.get(block.language)
        if language is None or not _tagged(language, block.code):
            index += 1
            continue
        where = at.format(block.line)
        twin = blocks[index + 1] if index + 1 < len(blocks) else None
        if block.tab != DOC_TABS[language]:
            problems.append(
                f"{where}: a tagged {language} example is not the {DOC_TABS[language]!r} tab "
                f"of a {DOC_TABS[language]!r}/{SDK_TAB!r} pair"
            )
            index += 1
            continue
        if twin is None or twin.tab != SDK_TAB or twin.language != block.language:
            problems.append(f"{where}: the {DOC_TABS[language]!r} tab has no {SDK_TAB!r} tab next")
            index += 1
            continue
        key = f"{block.section}-{language}"
        per_section[key] = per_section.get(key, 0) + 1
        name = f"docs-{key}" + (f"-{per_section[key]}" if per_section[key] > 1 else "")
        cases.append(
            Case(
                name,
                language,
                {
                    "doc": Spelling(block.code, (), True),
                    "sdk": Spelling(twin.code, (), True),
                },
                where,
            )
        )
        index += 2
    return cases, problems


def cases() -> list[Case]:
    """Every case: this directory's, then the docs' pairs."""
    return fixture_cases() + doc_cases()[0]
