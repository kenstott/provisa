#!/usr/bin/env python3
# Copyright (c) 2026 Kenneth Stott
# Canary: a9c2e574-3f1d-4b8a-c317-5d7e0f2a9b63
#
# This source code is licensed under the Business Source License 1.1
"""Generate tests/features/REQ-NNN.feature from docs/arch/requirements.yaml.

One .feature file per behavioral requirement that has a scenario.

Usage:
  python scripts/gen_features.py            # write files
  python scripts/gen_features.py --check    # exit 1 if any file differs or is missing
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))
from provisa.tools.req_schema import RequirementsFile, ReqType, Status  # type: ignore[import]

YAML_PATH = Path("docs/arch/requirements.yaml")
FEATURES_DIR = Path("tests/features")
STEPS_DIR = Path("tests/steps")

_SKIP_STATUSES = {Status.proposed, Status.rejected}

# First line of every generated feature. Files WITHOUT it are hand-authored and this generator
# never writes or prunes them.
_GENERATED_HEADER = "# Generated from docs/arch/requirements.yaml. Do not hand-edit."


def _wired_stems() -> set[str]:
    """Feature stems (``REQ-NNN``) referenced by a pytest-bdd step file. Every scenarios()/
    scenario() call in tests/steps uses a literal ``"../features/<REQ>.feature"`` path, so a
    substring scan of the step sources is exact.

    A generated feature exists ONLY when its steps are wired — decoupling the two is what let
    features accumulate that never execute. Wire the steps first, then this generator materializes
    (or keeps) the feature; drop the steps and the generator prunes the feature."""
    src = "\n".join(p.read_text(errors="ignore") for p in STEPS_DIR.rglob("*.py"))
    return {feat.stem for feat in FEATURES_DIR.glob("*.feature") if f"{feat.stem}.feature" in src}


def _generated_on_disk() -> set[Path]:
    """Feature files this generator owns — those carrying the generated header."""
    return {
        f
        for f in FEATURES_DIR.glob("*.feature")
        if f.read_text(errors="ignore").startswith(_GENERATED_HEADER)
    }


# Tokens that begin a new Gherkin line; anything else is a wrapped
# continuation of the preceding step and must be joined onto it (a step may
# not span multiple physical lines — pytest-bdd's parser rejects it).
_GHERKIN_KEYWORDS = (
    "Given ",
    "When ",
    "Then ",
    "And ",
    "But ",
    "* ",
    "Feature:",
    "Scenario:",
    "Scenario Outline:",
    "Background:",
    "Rule:",
    "Examples:",
    "Scenarios:",
)
_GHERKIN_PREFIXES = ("|", "@", "#", '"""', "```")


def _joined_scenario_lines(scenario: str) -> list[str]:
    """Collapse wrapped continuation lines into their preceding step line."""
    out: list[str] = []
    for raw in scenario.rstrip().splitlines():
        stripped = raw.strip()
        if not stripped:
            out.append("")
            continue
        starts_new = stripped.startswith(_GHERKIN_KEYWORDS) or stripped.startswith(
            _GHERKIN_PREFIXES
        )
        if not starts_new and out and out[-1].strip():
            out[-1] = f"{out[-1]} {stripped}"
        else:
            out.append(stripped)
    return out


def feature_content(req_id: str, category: str, description: str, scenario: str) -> str:
    desc_short = description[:120].replace("\n", " ").strip()
    if len(description) > 120:
        desc_short += "…"
    lines = [
        "# Generated from docs/arch/requirements.yaml. Do not hand-edit.",
        f"Feature: {req_id} — {category}",
        f"  # {desc_short}",
        "",
        f"  Scenario: {req_id} default behaviour",
    ]
    for line in _joined_scenario_lines(scenario):
        lines.append(f"    {line}" if line else "")
    lines.append("")
    return "\n".join(lines)


def generate_all(rf: RequirementsFile) -> dict[Path, str]:
    wired = _wired_stems()
    files: dict[Path, str] = {}
    for req in rf.requirements:
        if req.type != ReqType.behavioral:
            continue
        if req.status in _SKIP_STATUSES:
            continue
        if not req.scenario:
            continue
        # A feature is emitted ONLY when its pytest-bdd steps are wired. An un-wired req gets no
        # feature — that is the root fix for features that generate but never execute.
        if req.id not in wired:
            continue
        path = FEATURES_DIR / f"{req.id}.feature"
        files[path] = feature_content(req.id, req.category, req.description, req.scenario)
    return files


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--check", action="store_true", help="Exit 1 if output differs from committed files"
    )
    args = parser.parse_args()

    try:
        rf = RequirementsFile.load(YAML_PATH)
    except Exception as e:
        print(f"ERROR: {e}", file=sys.stderr)
        return 2

    files = generate_all(rf)
    # A generated feature is an orphan iff NO step file wires it — then it can never execute and is
    # pruned. A feature that IS wired is kept even when this generator can no longer regenerate its
    # content (e.g. its req became structural/no-scenario, or was removed) — deleting it would break
    # the step file that references it.
    wired = _wired_stems()
    orphans = {f for f in _generated_on_disk() if f.stem not in wired}
    errors: list[str] = []

    if args.check:
        for path, content in files.items():
            if not path.exists():
                errors.append(f"MISSING: {path}")
            elif path.read_text() != content:
                errors.append(f"DRIFT: {path}")
        for path in sorted(orphans):
            errors.append(f"ORPHAN: {path} — generated feature with no wired steps")
        if errors:
            for e in errors:
                print(e, file=sys.stderr)
            print("Run: python scripts/gen_features.py", file=sys.stderr)
            return 1
        print(f"OK: {len(files)} feature files up to date, no orphans")
        return 0

    FEATURES_DIR.mkdir(parents=True, exist_ok=True)
    for path, content in files.items():
        path.write_text(content)
    for path in orphans:
        path.unlink()
    suffix = f", pruned {len(orphans)} orphan(s)" if orphans else ""
    print(f"Written: {len(files)} feature files in {FEATURES_DIR}/{suffix}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
