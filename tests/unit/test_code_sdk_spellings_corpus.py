# Copyright (c) 2026 Kenneth Stott
# Canary: cdd33f5b-2fca-4fae-8720-5fd6f83a6651
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The binding corpus is whole (REQ-1984): every tagged example in docs/code-sources.md is a
docstring/SDK tab pair, docstring first; every case has every language the SDK is built for, or
says why not; every tag form has an accepted case in each built language; and a docstring spelling
never imports the SDK while an SDK spelling always does."""

# Requirements: REQ-1984
from __future__ import annotations

from pathlib import Path

import pytest

from tests.fixtures.code_sdk_spellings.collector import (
    BUILT,
    SPELLINGS,
    TAGS,
    cases,
    doc_cases,
    fixture_cases,
    imports_sdk,
    tags_of,
    unspelled,
)

pytestmark = pytest.mark.unit


def test_every_tagged_docs_example_is_a_tab_pair():
    pairs, problems = doc_cases()
    assert problems == []
    assert {c.language for c in pairs} == set(BUILT)


def test_the_docs_show_the_docstring_tab_before_the_sdk_tab(tmp_path):
    markdown = tmp_path / "page.md"
    markdown.write_text(
        '## A {: #a }\n\n=== "SDK"\n\n    ```python\n    from provisa_sdk import table\n'
        '    ```\n\n=== "Docstring"\n\n    ```python\n    """@table"""\n    ```\n'
    )
    pairs, problems = doc_cases(markdown)
    assert pairs == []
    assert problems == [
        "page.md:7: a tagged python example is not the 'Docstring' tab of a 'Docstring'/'SDK' pair",
        "page.md:13: the 'Docstring' tab has no 'SDK' tab next",
    ]


def test_a_tagged_example_outside_a_pair_is_found(tmp_path):
    markdown = tmp_path / "page.md"
    markdown.write_text(
        "## A {: #a }\n\n```typescript\n/** @table */\nexport function f(): R[] {}\n```\n\n"
        "```go\n// @table\nfunc F() []R\n```\n\n```python\nx = 1\n```\n"
    )
    pairs, problems = doc_cases(markdown)
    assert pairs == []
    assert problems == [
        "page.md:3: a tagged typescript example is not the 'JSDoc' tab of a 'JSDoc'/'SDK' pair"
    ]


def test_a_pair_becomes_a_case_named_by_its_section(tmp_path):
    markdown = tmp_path / "page.md"
    markdown.write_text(
        '### B {: #batched-keys }\n\n=== "Docstring"\n\n    ```python\n    def f():\n'
        '        """@table"""\n    ```\n\n=== "SDK"\n\n    ```python\n'
        "    from provisa_sdk import table\n    ```\n"
    )
    (pair,), problems = doc_cases(markdown)
    assert problems == []
    assert (pair.name, pair.language, pair.origin) == (
        "docs-batched-keys-python",
        "python",
        "page.md:5",
    )
    assert pair.spellings["doc"].code == 'def f():\n    """@table"""\n'
    assert pair.spellings["sdk"].code == "from provisa_sdk import table\n"


def test_every_case_has_every_built_language_or_says_why():
    left_out = unspelled()
    spelled = {(c.name, c.language): set(c.spellings) for c in fixture_cases()}
    names = {c.name for c in fixture_cases()}
    missing = [
        f"{name}/{language}/{spelling}"
        for name in sorted(names)
        for language in BUILT
        if (name, language) not in left_out
        for spelling in SPELLINGS
        if spelling not in spelled.get((name, language), set())
        and (name, f"{language}/{spelling}") not in left_out
    ]
    assert missing == []
    stale = [f"{n}: {s}" for n, s in left_out if _spelled(spelled, n, s)]
    assert stale == []


def _spelled(spelled: dict[tuple[str, str], set[str]], name: str, left_out: str) -> bool:
    """Whether a case holds what its ``unspelled`` entry says it leaves out."""
    language, _, spelling = left_out.partition("/")
    have = spelled.get((name, language), set())
    return spelling in have if spelling else bool(have)


@pytest.mark.parametrize("language", BUILT)
def test_every_tag_form_has_an_accepted_case(language):
    covered = set()
    for case in cases():
        if case.language == language and case.accepted and "doc" in case.spellings:
            covered |= tags_of(case.spellings["doc"].code)
    excused = set()
    for case in fixture_cases():
        if (case.name, language) in unspelled() and "doc" in case.spellings:
            excused |= tags_of(case.spellings["doc"].code)
    assert TAGS - excused - covered == set()


def test_a_docstring_spelling_never_imports_the_sdk_and_an_sdk_spelling_always_does():
    wrong = [
        f"{c.origin} {c.language}/{spelling}"
        for c in cases()
        for spelling, written in c.spellings.items()
        if imports_sdk(c.language, written.code) != (spelling == "sdk")
    ]
    assert wrong == []


def test_an_sdk_spelling_carries_no_tag_line_of_its_own():
    """The SDK spelling states each tag with the SDK, never as a docstring or JSDoc line too. A
    Python decorator sits at the margin; a docstring's tag line is indented."""
    tagged = [
        f"{c.origin} {c.language}"
        for c in cases()
        if "sdk" in c.spellings and tags_of(_docstring_lines(c.language, c.spellings["sdk"].code))
    ]
    assert tagged == []


def _docstring_lines(language: str, code: str) -> str:
    if language == "typescript":
        return code
    return "\n".join(line for line in code.splitlines() if line[:1].isspace())


def test_the_fixture_directory_holds_only_cases():
    root = Path(__file__).resolve().parents[1] / "fixtures" / "code_sdk_spellings"
    names = {c.name for c in fixture_cases()}
    strays = [
        p.name
        for p in root.iterdir()
        if p.is_dir() and p.name != "__pycache__" and p.name not in names
    ]
    assert strays == []
