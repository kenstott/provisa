# Copyright (c) 2026 Kenneth Stott
# Canary: 6b2e9f04-1c7a-4d3e-8a5f-0e9d2c4b7a13
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""An image built from a source checkout serves the UI. A checkout's static/ is empty, and the image
once copied it as it was, so install.sh from a checkout gave a UI container that answered GET / with
503 "UI not bundled". The Dockerfile builds provisa-ui itself, in its ui-build stage, with the build
the release runs, and the runtime image takes static/ from that stage.

- The ui-build stage runs `npm ci` and `npm run build` in provisa-ui, the release's UI build.
- It installs the docs-site packages the release installs (the prebuild renders the offline docs).
- It copies every file outside provisa-ui the build reads, at its checkout path under /build.
- The runtime image takes static/ from the stage's dist/, never from the build context.
- Dockerfile.dev builds the UI with the same ui-build stage and takes static/ from it too.
- No workflow job builds the UI into static/ before it builds the app image: the image never reads
  it, so the step only costs the job its time.
- .dockerignore keeps provisa-ui's sources and lockfile in the context and node_modules and a
  checkout's own build output out of it."""

from __future__ import annotations

import fnmatch
import posixpath
import re
import shlex

import yaml

import pytest

from tests.unit.test_dockerfile_copy_sources import REPO, UI, _context_copies, _instructions, _stage

pytestmark = pytest.mark.unit

STAGE = "ui-build"
STAGE_ROOT = "/build"


def _runs(stage: list[str]) -> str:
    return "\n".join(ins for ins in stage if ins.upper().startswith("RUN "))


def _copied_paths(stage: list[str]) -> set[str]:
    """The checkout path each context file lands at, relative to the stage's repo root."""
    workdir, _ = _stage(STAGE)
    landed: set[str] = set()
    for sources, dest in _context_copies(stage):
        base = dest if dest.startswith("/") else posixpath.join(workdir, dest)
        for src in sources:
            matches = (
                [p.relative_to(REPO).as_posix() for p in REPO.glob(src)]
                if "*" in src
                else [src.rstrip("/")]
            )
            for match in matches:
                if base.endswith("/") and (REPO / match).is_file():
                    target = posixpath.join(base, posixpath.basename(match))
                else:
                    target = base
                target = posixpath.normpath(target)
                assert target.startswith(STAGE_ROOT + "/"), f"COPY {src} lands outside /build"
                assert target.removeprefix(STAGE_ROOT + "/") == match, (
                    f"COPY {src} {dest} moves it away from its checkout path"
                )
                landed.add(match)
    return landed


def _covered(path: str, landed: set[str]) -> bool:
    return any(path == c or path.startswith(c + "/") for c in landed)


def test_the_ui_build_stage_runs_the_release_ui_build_in_provisa_ui():
    workdir, stage = _stage(STAGE)
    assert workdir == f"{STAGE_ROOT}/provisa-ui", workdir
    runs = _runs(stage)
    assert "npm ci" in runs, runs
    assert "npm run build" in runs, runs
    release = (REPO / ".github/workflows/release-packages.yml").read_text()
    assert "cd provisa-ui && npm run build" in release, (
        "the release no longer builds the UI with `npm run build`: the image must follow it"
    )


def test_the_ui_build_stage_installs_the_docs_site_packages_the_release_installs():
    action = yaml.safe_load((REPO / ".github/actions/docs-site-deps/action.yml").read_text())
    [step] = action["runs"]["steps"]
    released = [w for w in shlex.split(step["run"]) if not w.startswith("-")]
    assert released[:2] == ["pip", "install"], step["run"]
    _, stage = _stage(STAGE)
    runs = _runs(stage)
    missing = [pkg for pkg in released[2:] if pkg not in runs]
    assert not missing, f"the ui-build stage lacks the docs-site packages: {missing}"


def test_the_ui_build_stage_copies_every_file_outside_provisa_ui_the_build_reads():
    _, stage = _stage(STAGE)
    landed = _copied_paths(stage)
    assert _covered("provisa-ui/package-lock.json", landed)
    assert _covered("provisa-ui/src", landed)
    # The offline docs site: the prebuild script runs scripts/gen_docs_index.py and mkdocs.
    prebuild = (UI / "scripts/build-docs-site.mjs").read_text()
    needed = set(re.findall(r'"(scripts/[\w./-]+\.py)"', prebuild))
    assert needed, "build-docs-site.mjs no longer names a repo script: the parse is stale"
    needed |= {"mkdocs.yml", "docs"}
    needed |= {p.name for p in REPO.glob("README*.md")}
    mkdocs = (REPO / "mkdocs.yml").read_text()
    needed |= set(re.findall(r"^\s*-\s*(scripts/[\w./-]+\.py)\s*$", mkdocs, re.MULTILINE))
    # `tsc -b` checks every file under provisa-ui/src, tests included, and their relative imports.
    importer = re.compile(r"""(?:from|import)\s*\(?\s*["'](\.{1,2}/[^"']+)["']""")
    for source in [*UI.joinpath("src").rglob("*.ts"), *UI.joinpath("src").rglob("*.tsx")]:
        for spec in importer.findall(source.read_text()):
            target = (source.parent / spec).resolve()
            if UI.resolve() not in target.parents:
                needed.add(target.relative_to(REPO.resolve()).as_posix())
    uncopied = sorted(p for p in needed if not _covered(p, landed))
    assert not uncopied, f"the UI build reads files its Docker stage never copies: {uncopied}"


@pytest.mark.parametrize("dockerfile", ["Dockerfile", "Dockerfile.dev"])
def test_the_runtime_image_serves_static_from_the_ui_build_stage(dockerfile):
    instructions = _instructions(dockerfile)
    froms = [i for i, ins in enumerate(instructions) if ins.upper().startswith("FROM ")]
    runtime = instructions[froms[-1] :]
    copies = [ins.split() for ins in runtime if ins.upper().startswith("COPY ")]
    assert ["COPY", f"--from={STAGE}", f"{STAGE_ROOT}/provisa-ui/dist/", "./static/"] in copies
    from_context = [src for sources, _ in _context_copies(runtime) for src in sources]
    assert not [s for s in from_context if s.rstrip("/") == "static"], (
        "the runtime image copies the checkout's static/, which nothing builds"
    )


def test_dockerfile_dev_builds_the_ui_with_the_dockerfile_ui_build_stage():
    assert _stage(STAGE, "Dockerfile.dev") == _stage(STAGE), (
        "Dockerfile.dev's ui-build stage drifted from Dockerfile's"
    )


def _workflow_jobs() -> list[tuple[str, str, list[dict]]]:
    jobs = []
    for path in sorted((REPO / ".github/workflows").glob("*.yml")):
        workflow = yaml.safe_load(path.read_text())
        for name, job in (workflow.get("jobs") or {}).items():
            jobs.append((path.name, name, job.get("steps") or []))
    return jobs


def test_no_job_builds_the_ui_into_static_for_the_app_image():
    image_jobs = 0
    for workflow, name, steps in _workflow_jobs():
        runs = [step.get("run") or "" for step in steps]
        if not any(
            re.search(r"docker build\b[^\n]*(?:\\\n[^\n]*)*-t provisa/provisa:", run)
            for run in runs
        ):
            continue
        image_jobs += 1
        where = f"{workflow}:{name}"
        assert not [run for run in runs if "static/" in run], (
            f"{where} copies a UI build into static/, which the image builds itself"
        )
        assert not [run for run in runs if "npm run build" in run], (
            f"{where} builds the UI, which the image builds itself"
        )
        uses = [step.get("uses") or "" for step in steps]
        assert "./.github/actions/docs-site-deps" not in uses, (
            f"{where} installs the docs-site packages, which only the image's UI build uses"
        )
    assert image_jobs >= 2, "no workflow job builds provisa/provisa: the parse is stale"


def _ignored(path: str, patterns: list[str]) -> bool:
    """Whether .dockerignore leaves `path` out (Docker's last-match-wins, `!` re-includes)."""
    ignored = False
    for pattern in patterns:
        negate = pattern.startswith("!")
        pat = pattern.removeprefix("!").rstrip("/")
        parts = path.split("/")
        prefixes = ["/".join(parts[: i + 1]) for i in range(len(parts))]
        if pat.startswith("**/"):
            hit = any(fnmatch.fnmatchcase(part, pat[3:]) for part in parts)
        else:
            hit = any(fnmatch.fnmatchcase(prefix, pat) for prefix in prefixes)
        if hit:
            ignored = not negate
    return ignored


def test_dockerignore_sends_ui_sources_and_keeps_node_modules_and_build_output_out():
    patterns = [
        line.strip()
        for line in (REPO / ".dockerignore").read_text().splitlines()
        if line.strip() and not line.lstrip().startswith("#")
    ]
    for path in (
        "provisa-ui/package.json",
        "provisa-ui/package-lock.json",
        "provisa-ui/src/main.tsx",
        "provisa-ui/scripts/build-docs-site.mjs",
        "docs/index.md",
        "mkdocs.yml",
        "README.md",
        "scripts/gen_docs_index.py",
    ):
        assert not _ignored(path, patterns), f".dockerignore leaves {path} out of the context"
    for path in (
        "node_modules/x",
        "provisa-ui/node_modules/x",
        "provisa-ui/graph-render/node_modules/x",
        "provisa-ui/dist/index.html",
        "provisa-ui/public/docs-site/index.html",
    ):
        assert _ignored(path, patterns), f".dockerignore sends {path} into the build context"


def test_the_image_build_workflow_checks_the_built_image_serves_the_ui():
    workflow = (REPO / ".github/workflows/docker-image.yml").read_text()
    run = re.search(
        r"run: docker run [^\n]*provisa/provisa:ci python - < scripts/ci/ui_static_smoke\.py",
        workflow,
    )
    assert run is not None, "docker-image.yml does not check the built image serves the UI"
    assert (REPO / "scripts/ci/ui_static_smoke.py").is_file()
