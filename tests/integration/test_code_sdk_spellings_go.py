# Copyright (c) 2026 Kenneth Stott
# Canary: 581334cc-3493-4ec0-9f33-e4876ccd12d7
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The binding corpus in Go (REQ-1984): every case's doc-comment spelling and SDK spelling are
built by the pinned Go as an author's module requiring provisa.dev/sdk/go, then read by Provisa's
own reader and manifest. The two give the same manifest, or the refusal the case states; go build
passes each SDK spelling or rejects it, as the case states, and always passes the doc-comment
spelling."""

# Requirements: REQ-1984
from __future__ import annotations

import pytest

from tests.fixtures.code_sdk_spellings.collector import cases
from tests.fixtures.code_sdk_spellings.go_module import GoToolchain, go_mod

pytestmark = [pytest.mark.integration]

CASES = {c.name: c for c in cases() if c.language == "go"}
SPELLED = [(c.name, s) for c in CASES.values() for s in c.spellings]
TWINNED = [c.name for c in CASES.values() if len(c.spellings) == 2 and c.accepted]


@pytest.fixture(scope="module")
def toolchain(tmp_path_factory) -> GoToolchain:
    return GoToolchain.resolve(tmp_path_factory.mktemp("go-cache"))


@pytest.fixture(scope="module")
def modules(tmp_path_factory):
    root = tmp_path_factory.mktemp("spellings")

    def module(name: str, spelling: str):
        directory = CASES[name].write(root, spelling)
        (directory / "go.mod").write_text(go_mod())
        return directory

    return module


@pytest.fixture(scope="module")
def read(toolchain, modules):
    found: dict[tuple[str, str], dict | tuple[str, ...]] = {}

    def reading(name: str, spelling: str) -> dict | tuple[str, ...]:
        if (name, spelling) not in found:
            found[(name, spelling)] = toolchain.read(modules(name, spelling))
        return found[(name, spelling)]

    return reading


def test_the_corpus_has_go_cases():
    assert len(TWINNED) >= 14
    assert any(name.startswith("docs-") for name in TWINNED)


@pytest.mark.parametrize(("name", "spelling"), SPELLED)
def test_go_build_passes_or_rejects_each_spelling_as_its_case_states(
    toolchain, modules, name, spelling
):
    accepts = spelling == "doc" or CASES[name].spellings[spelling].checker_accepts
    built = toolchain.build(modules(name, spelling))
    assert (built.returncode == 0) is accepts, built.stderr


@pytest.mark.parametrize(
    ("name", "spelling"),
    [(n, s) for n, s in SPELLED if CASES[n].spellings[s].problems is not None],
)
def test_each_spelling_gives_what_its_case_states(read, name, spelling):
    expected = CASES[name].spellings[spelling].problems
    got = read(name, spelling)
    if expected:
        assert got == expected
    else:
        assert isinstance(got, dict), got
        assert got["exports"]


@pytest.mark.parametrize("name", TWINNED)
def test_the_sdk_spelling_gives_its_twins_manifest(read, name):
    assert read(name, "sdk") == read(name, "doc")
