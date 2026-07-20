#!/usr/bin/env python3
# Copyright (c) 2026 Kenneth Stott
# Canary: f3a1d892-7b4e-4c09-a215-6e8f0d3c1b47
#
# This source code is licensed under the Business Source License 1.1
"""Generate docs/arch/requirements.md from docs/arch/requirements.yaml.

Usage:
  python scripts/gen_requirements_md.py            # write file
  python scripts/gen_requirements_md.py --check    # diff only, exit 1 if drift
"""

from __future__ import annotations

import argparse
import re
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))
from provisa.tools.req_schema import RequirementsFile  # type: ignore[import]

YAML_PATH = Path("docs/arch/requirements.yaml")
MD_PATH = Path("docs/arch/requirements.md")

STATUS_LABEL = {
    "complete": "✅ complete",
    "in-progress": "⚙ in-progress",
    "proposed": "💡 proposed",
    "accepted": "✓ accepted",
    "rejected": "✗ rejected",
}

# Inline cross-references like "(REQ-266)" in prose. Each REQ renders as an
# anchored section (`{#REQ-NNN}`), so a bare mention becomes a deep-link to that
# section on the same page.
_REQ_REF = re.compile(r"REQ-\d+")


def _linkify(text: str, self_id: str, known: set[str]) -> str:
    """Turn inline REQ-NNN mentions into anchor links.

    Skips self-references and any id that has no requirement (so no dangling
    anchors) — those are left as plain text.
    """

    def repl(m: re.Match[str]) -> str:
        ref = m.group(0)
        if ref == self_id or ref not in known:
            return ref
        return f"[{ref}](#{ref})"

    return _REQ_REF.sub(repl, text)


def _code_list(items: list[str] | None) -> str:
    return ", ".join(f"`{i}`" for i in items) if items else "—"


def generate(rf: RequirementsFile) -> str:
    lines = [
        "# Requirements",
        "",
        "> Generated from `docs/arch/requirements.yaml`. Do not hand-edit.",
        "",
        "Each requirement is anchored — link to one directly with "
        "`#REQ-NNN` (e.g. [REQ-001](#REQ-001)). Inline `REQ-NNN` mentions "
        "deep-link to their section, and the docs search box indexes every "
        "requirement by id, category, and description.",
    ]

    known = {req.id for req in rf.requirements}
    current_group: str | None = None
    for req in rf.requirements:
        if req.group != current_group:
            current_group = req.group
            lines += ["", f"## {req.group}"]

        status = STATUS_LABEL.get(req.status.value, req.status.value)
        desc = _linkify(req.description.replace("\n", " ").strip(), req.id, known)
        use_case = _linkify((req.use_case or "").replace("\n", " ").strip(), req.id, known)

        lines += [
            "",
            f"### {req.id} · {req.category} {{#{req.id}}}",
            "",
            f"**Status:** {status} · **Priority:** {req.priority.value} · "
            f"**Type:** {req.type.value}",
            "",
            desc,
        ]
        if use_case:
            lines += ["", f"**Use case:** {use_case}"]
        lines += [
            "",
            f"**Code:** {_code_list(req.code)}",
            "",
            f"**Tests:** {_code_list(req.tests)}",
        ]

    return "\n".join(lines) + "\n"


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--check", action="store_true", help="Exit 1 if output differs from committed file"
    )
    args = parser.parse_args()

    try:
        rf = RequirementsFile.load(YAML_PATH)
    except Exception as e:
        print(f"ERROR: {e}", file=sys.stderr)
        return 2

    output = generate(rf)

    if args.check:
        if not MD_PATH.exists():
            print(f"FAIL: {MD_PATH} does not exist", file=sys.stderr)
            return 1
        committed = MD_PATH.read_text()
        if output != committed:
            print(f"FAIL: {MD_PATH} is out of sync with {YAML_PATH}", file=sys.stderr)
            print("Run: python scripts/gen_requirements_md.py", file=sys.stderr)
            return 1
        print(f"OK: {MD_PATH} is up to date")
        return 0

    MD_PATH.write_text(output)
    print(f"Written: {MD_PATH} ({len(rf.requirements)} requirements)")
    return 0


if __name__ == "__main__":
    sys.exit(main())
