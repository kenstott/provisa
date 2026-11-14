# Copyright (c) 2026 Kenneth Stott
# Canary: 3c7d2a91-5e08-4b6f-9d1a-8f2e4c6b0a17
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The Dockerfile copies only what the repository holds (issue #340). main runs no image build
(docker-image.yml runs on release branches), so a COPY of a deleted file, or a renderer import the
graph-render stage never copies, went unnoticed until a release. These tests read the Dockerfile
and the tree, so main notices.

- Every COPY source taken from the build context is a path git tracks.
- Every provisa-ui file the render_graph renderer (REQ-1990) imports, followed transitively from
  provisa-ui/graph-render/src, lies under a path the graph-render stage copies, at the same place
  relative to its WORKDIR, so the bundle's relative imports resolve inside the stage."""

# Requirements: REQ-1990
from __future__ import annotations

import posixpath
import re
import subprocess
from pathlib import Path

import pytest

pytestmark = pytest.mark.unit

REPO = Path(__file__).resolve().parents[2]
UI = REPO / "provisa-ui"
RENDERER_SRC = UI / "graph-render" / "src"

# A relative import or re-export in TypeScript; `import type` / `export type` are erased by the
# bundler and so need no file in the stage. Group 1 is the `type` keyword, 2 and 3 the specifier.
_RELATIVE_IMPORT = re.compile(
    r"""(?:^|;)\s*(?:import|export)\s+(type\s+)?(?:[^;'"]*?\s+from\s+)?["'](\.{1,2}/[^"']+)["']"""
    r"""|import\(\s*["'](\.{1,2}/[^"']+)["']\s*\)""",
    re.MULTILINE,
)
_TS_CANDIDATES = ("", ".ts", ".tsx", "/index.ts", "/index.tsx")


def _instructions() -> list[str]:
    """The Dockerfile's instructions, continuation lines joined and comments dropped."""
    lines = [
        line
        for line in (REPO / "Dockerfile").read_text().splitlines()
        if not line.lstrip().startswith("#")
    ]
    return [i.strip() for i in re.sub(r"\\\n", " ", "\n".join(lines)).splitlines() if i.strip()]


def _context_copies(instructions: list[str]) -> list[tuple[list[str], str]]:
    """(sources, destination) of each COPY that reads the build context (no --from)."""
    copies = []
    for instruction in instructions:
        words = instruction.split()
        if words[0].upper() != "COPY":
            continue
        flags = [w for w in words[1:] if w.startswith("--")]
        if any(f.startswith("--from=") for f in flags):
            continue
        paths = [w for w in words[1:] if not w.startswith("--")]
        copies.append((paths[:-1], paths[-1]))
    return copies


def _stage(name: str) -> tuple[str, list[str]]:
    """(WORKDIR, instructions) of the named stage."""
    instructions = _instructions()
    start = next(
        i
        for i, ins in enumerate(instructions)
        if ins.upper().startswith("FROM ") and ins.split()[-1] == name
    )
    end = next(
        (
            i
            for i in range(start + 1, len(instructions))
            if instructions[i].upper().startswith("FROM ")
        ),
        len(instructions),
    )
    stage = instructions[start:end]
    [workdir] = [ins.split()[1] for ins in stage if ins.upper().startswith("WORKDIR ")]
    return workdir, stage


def _tracked(path: str) -> bool:
    """Whether git tracks this path, or any file under it when it is a directory."""
    out = subprocess.run(
        ["git", "-C", str(REPO), "ls-files", "--", path.rstrip("/")],
        capture_output=True,
        text=True,
        check=True,
    ).stdout
    return bool(out.strip())


def _resolve(importer: Path, spec: str) -> Path:
    base = importer.parent / spec
    for suffix in _TS_CANDIDATES:
        candidate = Path(f"{base}{suffix}")
        if candidate.is_file():
            return candidate.resolve()
    raise AssertionError(
        f"{importer.relative_to(REPO)} imports {spec!r}, which resolves to no file"
    )


def _renderer_imports() -> set[Path]:
    """Every file the renderer's sources reach through value imports, transitively."""
    seen: set[Path] = set()
    pending = [f.resolve() for f in RENDERER_SRC.rglob("*.ts")]
    while pending:
        current = pending.pop()
        if current in seen:
            continue
        seen.add(current)
        for match in _RELATIVE_IMPORT.finditer(current.read_text()):
            if match.group(1):
                continue
            pending.append(_resolve(current, match.group(2) or match.group(3)))
    return seen


def test_every_copy_source_from_the_build_context_is_tracked_in_the_repository():
    copies = _context_copies(_instructions())
    assert copies, "the Dockerfile has no COPY from the build context"
    missing = [src for sources, _ in copies for src in sources if not _tracked(src)]
    assert not missing, f"the Dockerfile copies paths the repository does not hold: {missing}"


def test_the_graph_render_stage_keeps_provisa_ui_layout_for_every_copy():
    workdir, stage = _stage("graph-render")
    assert workdir == "/build/provisa-ui", workdir
    for sources, dest in _context_copies(stage):
        for src in sources:
            assert src.startswith("provisa-ui/"), src
            if not (REPO / src).exists():
                # A missing source is reported by the tracked-sources test; whether it was a file
                # or a directory, and so where Docker would place it, cannot be read from the tree.
                continue
            rel = src.removeprefix("provisa-ui/").rstrip("/")
            target = posixpath.normpath(dest)
            if dest.endswith("/") and (REPO / src).is_file():
                target = posixpath.join(target, posixpath.basename(rel))
            assert target == posixpath.normpath(rel), (
                f"COPY {src} {dest} moves it away from its provisa-ui path"
            )


def test_every_provisa_ui_file_the_renderer_imports_is_copied_into_the_graph_render_stage():
    _, stage = _stage("graph-render")
    copied = [(REPO / src).resolve() for sources, _ in _context_copies(stage) for src in sources]
    imports = _renderer_imports()
    assert any(RENDERER_SRC.resolve() not in p.parents for p in imports), (
        "the import walk found nothing outside graph-render/src: the walk no longer parses imports"
    )
    uncopied = sorted(
        str(p.relative_to(UI)) for p in imports if not any(p == c or c in p.parents for c in copied)
    )
    assert not uncopied, f"graph-render imports files its Docker stage never copies: {uncopied}"
