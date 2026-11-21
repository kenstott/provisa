# Copyright (c) 2026 Kenneth Stott
# Canary: 3e0f7f0e-8a8b-4f0b-9a57-2b6f7f9c1d42
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The binding corpus in TypeScript (REQ-1984): every case's JSDoc spelling and SDK spelling are
compiled by the pinned tsc as an author's project, then read by Provisa's own reader and manifest.
The two give the same manifest, or the refusal the case states; the pinned tsc passes each SDK
spelling or rejects it, as the case states, and always passes the JSDoc spelling."""

# Requirements: REQ-1984
from __future__ import annotations

import json
import re
import subprocess  # nosec B404 - argv-list invocation of the pinned runtime and compiler
from pathlib import Path

import pytest

from provisa.codesource.manifest import ManifestRefused, manifest_json
from provisa.node.reader import ensure_typescript, read_bundle
from provisa.runtime_deps.code_runtimes import CodeRuntimeResolver
from tests.fixtures.code_sdk_spellings.collector import cases

pytestmark = [pytest.mark.integration]

REPO = Path(__file__).resolve().parents[2]
SDK_TYPES = REPO / "provisa-sdk" / "typescript" / "index.d.ts"
SERVICES_TYPES = REPO / "provisa" / "node" / "services.d.ts"
CASES = {c.name: c for c in cases() if c.language == "typescript"}
SPELLED = [(c.name, s) for c in CASES.values() for s in c.spellings]
TWINNED = [c.name for c in CASES.values() if len(c.spellings) == 2 and c.accepted]
_ERROR = re.compile(r"^src/(?P<case>[^/]+)/(?P<spelling>doc|sdk)/index\.ts\(\d+,\d+\): error TS\d+")


@pytest.fixture(scope="module")
def toolchain(tmp_path_factory):
    cache = tmp_path_factory.mktemp("runtime-deps")
    return CodeRuntimeResolver(cache_root=cache).resolve("node"), ensure_typescript(cache, [])


@pytest.fixture(scope="module")
def project(tmp_path_factory, toolchain) -> tuple[Path, dict[tuple[str, str], int]]:
    """Every spelling compiled in one author's project, and the errors tsc gave each file."""
    node, compiler = toolchain
    root = tmp_path_factory.mktemp("spellings")
    for case in CASES.values():
        for spelling in case.spellings:
            case.write(root / "src", spelling)
    (root / "tsconfig.json").write_text(
        json.dumps(
            {
                "compilerOptions": {
                    "target": "ES2022",
                    "module": "NodeNext",
                    "moduleResolution": "NodeNext",
                    "strict": True,
                    "declaration": True,
                    "skipLibCheck": True,
                    "rootDir": "src",
                    "outDir": "dist",
                    "paths": {
                        "@provisa/sdk": [str(SDK_TYPES)],
                        "@provisa/services": [str(SERVICES_TYPES)],
                    },
                },
                "include": ["src"],
            }
        )
    )
    tsc = compiler.parent.parent / "bin" / "tsc"
    done = subprocess.run(  # nosec B603 - the pinned runtime and compiler, no shell
        [str(node), str(tsc), "-p", str(root), "--pretty", "false"],
        capture_output=True,
        text=True,
        cwd=root,
    )
    errors = dict.fromkeys(SPELLED, 0)
    for line in done.stdout.splitlines():
        found = _ERROR.match(line)
        if found:
            errors[(found["case"], found["spelling"])] += 1
        else:
            assert line.startswith(" "), done.stdout  # a continuation of the error above
    return root / "dist", errors


@pytest.fixture(scope="module")
def read(project, toolchain):
    node, compiler = toolchain
    dist, _ = project
    found: dict[tuple[str, str], dict | tuple[str, ...]] = {}

    def reading(name: str, spelling: str) -> dict | tuple[str, ...]:
        if (name, spelling) not in found:
            bundle = dist / name / spelling
            try:
                manifest = read_bundle(
                    runtime="node", executable=node, compiler=compiler, bundle=bundle
                )
                found[(name, spelling)] = manifest_json(manifest)
            except ManifestRefused as refused:
                found[(name, spelling)] = tuple(refused.problems)
        return found[(name, spelling)]

    return reading


def test_the_corpus_has_typescript_cases():
    assert len(TWINNED) >= 15
    assert any(name.startswith("docs-") for name in TWINNED)


@pytest.mark.parametrize(("name", "spelling"), SPELLED)
def test_tsc_passes_or_rejects_each_spelling_as_its_case_states(project, name, spelling):
    _, errors = project
    accepts = spelling == "doc" or CASES[name].spellings[spelling].checker_accepts
    assert (errors[(name, spelling)] == 0) is accepts, errors[(name, spelling)]


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
