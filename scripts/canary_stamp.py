#!/usr/bin/env python3
# Copyright (c) 2026 Kenneth Stott
# Canary: 6599f636-5133-4daa-bb12-def828c1b6a4
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Canary UUID stamper — injects per-file canary UUIDs into copyright headers,
builds .canary_registry.json, and generates canary-site/ static JSON files."""

import json
import os
import re
import uuid
from datetime import datetime, timezone
from pathlib import Path

PROJECT_NAME = "provisa"
PROJECT_ROOT = Path(__file__).resolve().parent.parent
REGISTRY_PATH = PROJECT_ROOT / ".canary_registry.json"
SITE_DIR = Path(os.environ["CANARY_SITE_DIR"]).expanduser()
EXCLUDE_DIRS = {
    ".venv",
    "venv",
    ".git",
    ".eggs",
    "canary-site",
    "__pycache__",
    "node_modules",
    # Build outputs and agent worktrees are copies of tracked sources: they carry the
    # original's canary header, so stamping them yields duplicate uuids that collapse in
    # the uuid-keyed registry (10,174 files stamped -> 2,164 entries) and leave a fresh
    # batch of orphaned site files every time a worktree or build dir is regenerated.
    "build",
    "dist",
    "worktrees",
}

# Python/shell style
PY_COPYRIGHT_HEADER = """\
# Copyright (c) 2026 Kenneth Stott
# Canary: {uuid}
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.
"""

# TypeScript/JavaScript style
TS_COPYRIGHT_HEADER = """\
// Copyright (c) 2026 Kenneth Stott
// Canary: {uuid}
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.
"""

AI_NOTICE = (
    "Use of this software for training artificial intelligence or "
    "machine learning models is strictly prohibited without explicit "
    "written permission from the copyright holder."
)

PY_CANARY_RE = re.compile(r"^# Canary:\s+([0-9a-f-]{36})\s*$", re.MULTILINE)
PY_COPYRIGHT_RE = re.compile(r"^# Copyright \(c\)", re.MULTILINE)

TS_CANARY_RE = re.compile(r"^// Canary:\s+([0-9a-f-]{36})\s*$", re.MULTILINE)
TS_COPYRIGHT_RE = re.compile(r"^// Copyright \(c\)", re.MULTILINE)

TS_EXTENSIONS = {".ts", ".tsx", ".js", ".jsx", ".mts", ".cts"}


def find_source_files() -> list[Path]:
    results = []
    for root, dirs, files in os.walk(PROJECT_ROOT):
        dirs[:] = [d for d in dirs if d not in EXCLUDE_DIRS]
        for f in files:
            p = Path(root) / f
            if f.endswith(".py") or p.suffix in TS_EXTENSIONS:
                results.append(p)
    return sorted(results)


def extract_canary(content: str, canary_re: re.Pattern) -> str | None:
    m = canary_re.search(content)
    return m.group(1) if m else None


def stamp_py_file(path: Path, content: str) -> str:
    existing = extract_canary(content, PY_CANARY_RE)
    if existing:
        return existing

    canary_id = str(uuid.uuid4())
    m = PY_COPYRIGHT_RE.search(content)

    if m:
        insert_pos = content.index("\n", m.start()) + 1
        content = content[:insert_pos] + f"# Canary: {canary_id}\n" + content[insert_pos:]
        path.write_text(content, encoding="utf-8")
    else:
        shebang = ""
        body = content
        if content.startswith("#!"):
            newline = content.index("\n") + 1
            shebang = content[:newline]
            body = content[newline:]
        header = PY_COPYRIGHT_HEADER.format(uuid=canary_id)
        if body and not body.startswith("\n"):
            header += "\n"
        path.write_text(shebang + header + body, encoding="utf-8")

    return canary_id


def stamp_ts_file(path: Path, content: str) -> str:
    existing = extract_canary(content, TS_CANARY_RE)
    if existing:
        return existing

    canary_id = str(uuid.uuid4())
    m = TS_COPYRIGHT_RE.search(content)

    if m:
        insert_pos = content.index("\n", m.start()) + 1
        content = content[:insert_pos] + f"// Canary: {canary_id}\n" + content[insert_pos:]
        path.write_text(content, encoding="utf-8")
    else:
        header = TS_COPYRIGHT_HEADER.format(uuid=canary_id)
        if content and not content.startswith("\n"):
            header += "\n"
        path.write_text(header + content, encoding="utf-8")

    return canary_id


def stamp_file(path: Path) -> str:
    content = path.read_text(encoding="utf-8")
    if path.suffix in TS_EXTENSIONS:
        return stamp_ts_file(path, content)
    return stamp_py_file(path, content)


def reissue_canary(path: Path) -> str:
    """Replace an already-claimed canary with a fresh one.

    Splitting a module copies its header into the new file, so two paths end up sharing a
    uuid. That is the one thing the canary cannot tolerate: a leaked file must resolve to a
    single path, and the uuid-keyed registry keeps only the last writer besides. The first
    path to claim a uuid (walk order is sorted, so the choice is stable) keeps it; every
    later claimant is reissued here.
    """
    content = path.read_text(encoding="utf-8")
    is_ts = path.suffix in TS_EXTENSIONS
    canary_re = TS_CANARY_RE if is_ts else PY_CANARY_RE
    prefix = "//" if is_ts else "#"
    canary_id = str(uuid.uuid4())
    content, count = canary_re.subn(f"{prefix} Canary: {canary_id}", content, count=1)
    if count != 1:
        raise RuntimeError(f"{path}: canary line vanished between read and reissue")
    path.write_text(content, encoding="utf-8")
    return canary_id


def build_registry(file_canaries: dict[str, str]) -> dict:
    return {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "files": {
            canary_id: {
                "path": rel_path,
                "stamped_at": datetime.now(timezone.utc).isoformat(),
            }
            for rel_path, canary_id in file_canaries.items()
        },
    }


def write_site(registry: dict) -> None:
    SITE_DIR.mkdir(exist_ok=True)
    for canary_id, info in registry["files"].items():
        entry = {
            "uuid": canary_id,
            "project": PROJECT_NAME,
            "path": info["path"],
            "stamped_at": info["stamped_at"],
            "license": "Business Source License 1.1",
            "notice": AI_NOTICE,
        }
        site_file = SITE_DIR / f"{canary_id}.json"
        site_file.write_text(json.dumps(entry, indent=2) + "\n", encoding="utf-8")

    # Drop entries for canary ids the registry no longer carries — a renamed, deleted or
    # re-stamped source leaves its old id behind, and the orphans accumulate without bound.
    # Cloudflare Pages rejects a deployment over 20,000 files, which is what the unpruned
    # directory reached (20,587 files against a 10,174-file registry).
    live = {f"{canary_id}.json" for canary_id in registry["files"]}
    removed = 0
    for stale in SITE_DIR.glob("*.json"):
        if stale.name not in live:
            stale.unlink()
            removed += 1
    if removed:
        print(f"Pruned {removed} stale site entries")


def main() -> None:
    source_files = find_source_files()
    file_canaries: dict[str, str] = {}

    claimed: dict[str, str] = {}
    reissued = 0

    for path in source_files:
        rel = str(path.relative_to(PROJECT_ROOT))
        canary_id = stamp_file(path)
        if canary_id in claimed:
            canary_id = reissue_canary(path)
            reissued += 1
        claimed[canary_id] = rel
        file_canaries[rel] = canary_id

    if reissued:
        print(f"Reissued {reissued} duplicate canaries")

    registry = build_registry(file_canaries)
    REGISTRY_PATH.write_text(json.dumps(registry, indent=2) + "\n", encoding="utf-8")
    write_site(registry)

    print(f"Stamped {len(file_canaries)} files")
    print(f"Registry: {REGISTRY_PATH}")
    print(f"Site: {SITE_DIR}/")


if __name__ == "__main__":
    main()
