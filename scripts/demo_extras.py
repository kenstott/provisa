#!/usr/bin/env python3
# Copyright (c) 2026 Kenneth Stott
# Canary: e7e4adb9-0bee-4bc1-a438-c8746cee7fa5
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.
"""The packages a named demo needs beyond a plain install (REQ-2016).

A demo names the optional dependency groups of pyproject.toml it needs in its demo.yaml
(``extras: [vector]``); this prints the requirement of each package those groups list, one a
line. With --groups it prints the group names instead, one a line, for start-ui-install.sh to
sync from uv.lock for that demo alone (the lock takes torch from PyTorch's CPU-only index on Linux,
so a demo never installs a CUDA build). A demo that names none prints nothing. A group
pyproject.toml does not have is refused by name.

Usage: demo_extras.py [--groups] <demo dir>
"""

from __future__ import annotations

import sys
import tomllib
from pathlib import Path

import yaml

_ROOT = Path(__file__).resolve().parents[1]


def _optional_dependencies() -> dict[str, list[str]]:
    return tomllib.loads((_ROOT / "pyproject.toml").read_text())["project"]["optional-dependencies"]


def extras(demo_dir: Path) -> list[str]:
    """The optional dependency groups the demo's demo.yaml names, each one pyproject.toml has."""
    spec = yaml.safe_load((demo_dir / "demo.yaml").read_text()) or {}
    named = spec.get("extras") or []
    groups = _optional_dependencies()
    for extra in named:
        if extra not in groups:
            raise SystemExit(
                f"{demo_dir.name}/demo.yaml names extra {extra!r}, which pyproject.toml does not "
                f"have (it has {', '.join(sorted(groups))})"
            )
    return list(named)


def requirements(demo_dir: Path) -> list[str]:
    groups = _optional_dependencies()
    out: list[str] = []
    for extra in extras(demo_dir):
        out.extend(r for r in groups[extra] if r not in out)
    return out


if __name__ == "__main__":
    args = sys.argv[1:]
    if args[:1] == ["--groups"] and len(args) == 2:
        lines = extras(Path(args[1]))
    elif len(args) == 1 and args[0] != "--groups":
        lines = requirements(Path(args[0]))
    else:
        raise SystemExit("usage: demo_extras.py [--groups] <demo dir>")
    for line in lines:
        print(line)
