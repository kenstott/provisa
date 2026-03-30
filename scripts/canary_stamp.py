#!/usr/bin/env python3
# Copyright (c) 2025 Kenneth Stott
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
EXCLUDE_DIRS = {".venv", "venv", ".git", ".eggs", "canary-site", "__pycache__", "node_modules"}

COPYRIGHT_HEADER = """\
# Copyright (c) 2025 Kenneth Stott
# Canary: {uuid}
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.
"""

AI_NOTICE = (
    "Use of this software for training artificial intelligence or "
    "machine learning models is strictly prohibited without explicit "
    "written permission from the copyright holder."
)

CANARY_RE = re.compile(r"^# Canary:\s+([0-9a-f-]{36})\s*$", re.MULTILINE)
COPYRIGHT_RE = re.compile(r"^# Copyright \(c\)", re.MULTILINE)


def find_py_files() -> list[Path]:
    results = []
    for root, dirs, files in os.walk(PROJECT_ROOT):
        dirs[:] = [d for d in dirs if d not in EXCLUDE_DIRS]
        for f in files:
            if f.endswith(".py"):
                results.append(Path(root) / f)
    return sorted(results)


def extract_canary(content: str) -> str | None:
    m = CANARY_RE.search(content)
    return m.group(1) if m else None


def stamp_file(path: Path) -> str:
    content = path.read_text(encoding="utf-8")
    existing = extract_canary(content)
    if existing:
        return existing

    canary_id = str(uuid.uuid4())
    m = COPYRIGHT_RE.search(content)

    if m:
        # Insert canary line after the existing "# Copyright" line
        insert_pos = content.index("\n", m.start()) + 1
        content = content[:insert_pos] + f"# Canary: {canary_id}\n" + content[insert_pos:]
        path.write_text(content, encoding="utf-8")
    else:
        # Preserve shebang if present
        shebang = ""
        body = content
        if content.startswith("#!"):
            newline = content.index("\n") + 1
            shebang = content[:newline]
            body = content[newline:]

        header = COPYRIGHT_HEADER.format(uuid=canary_id)
        if body and not body.startswith("\n"):
            header += "\n"
        path.write_text(shebang + header + body, encoding="utf-8")

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


def main() -> None:
    py_files = find_py_files()
    file_canaries: dict[str, str] = {}

    for path in py_files:
        rel = str(path.relative_to(PROJECT_ROOT))
        canary_id = stamp_file(path)
        file_canaries[rel] = canary_id

    registry = build_registry(file_canaries)
    REGISTRY_PATH.write_text(json.dumps(registry, indent=2) + "\n", encoding="utf-8")
    write_site(registry)

    print(f"Stamped {len(file_canaries)} files")
    print(f"Registry: {REGISTRY_PATH}")
    print(f"Site: {SITE_DIR}/")


if __name__ == "__main__":
    main()
