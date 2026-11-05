#!/usr/bin/env python3
# Copyright (c) 2026 Kenneth Stott
# Canary: a82e413a-d585-4efb-8ef7-8d463e69cb4e
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Check a built provisa wheel carries provisa._encoders (REQ-2194).

Usage: check-provisa-wheel.py WHEEL [--tag REGEX] [--import]

Exactly one wheel is named. It is a platform wheel (never py3-none-any), it holds the compiled
module (provisa/_encoders.<suffix>.so or .pyd) and the module's third-party notices, and with
--tag its platform tag matches REGEX. --import installs it, without its dependencies, into a
throwaway directory and imports provisa._encoders.pgwire from there with this interpreter, so run
it on the wheel's own platform.
"""

from __future__ import annotations

import argparse
import re
import subprocess
import sys
import tempfile
import zipfile
from pathlib import Path

MODULE = re.compile(r"provisa/_encoders(\.[\w-]+)?\.(so|pyd)")
NOTICES = "provisa/_encoders.THIRD_PARTY_NOTICES"
PROBE = (
    "import provisa._encoders as e, provisa._encoders.pgwire as p; "
    "assert callable(p.encode_batch) and callable(p.encode_stream); print(e.__file__)"
)


def check(wheel: Path, tag: str | None) -> list[str]:
    """What is wrong with ``wheel``; empty when nothing is."""
    problems = []
    name = wheel.name
    if not re.fullmatch(r"provisa-[^-]+-cp312-cp312-[\w.]+\.whl", name):
        problems.append(f"{name} is not a cp312 platform wheel")
    if tag is not None and not re.fullmatch(rf"provisa-[^-]+-cp312-cp312-{tag}\.whl", name):
        problems.append(f"{name}'s platform tag is not {tag}")
    names = zipfile.ZipFile(wheel).namelist()
    modules = [n for n in names if MODULE.fullmatch(n)]
    if len(modules) != 1:
        problems.append(f"expected one compiled provisa._encoders module, found {modules}")
    if NOTICES not in names:
        problems.append(f"{NOTICES} is missing")
    return problems


def import_check(wheel: Path) -> None:
    with tempfile.TemporaryDirectory() as target:
        subprocess.run(
            [
                sys.executable,
                "-m",
                "pip",
                "install",
                "--quiet",
                "--no-deps",
                "--target",
                target,
                str(wheel),
            ],
            check=True,
        )
        subprocess.run(
            [sys.executable, "-I", "-c", f"import sys; sys.path.insert(0, {target!r}); {PROBE}"],
            check=True,
        )


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Check a built provisa wheel carries provisa._encoders."
    )
    parser.add_argument("wheels", nargs="+", type=Path)
    parser.add_argument("--tag")
    parser.add_argument("--import", dest="do_import", action="store_true")
    args = parser.parse_args()
    if len(args.wheels) != 1 or not args.wheels[0].is_file():
        print(f"expected one provisa wheel, got {[str(w) for w in args.wheels]}", file=sys.stderr)
        return 1
    wheel = args.wheels[0]
    problems = check(wheel, args.tag)
    if problems:
        print("\n".join(problems), file=sys.stderr)
        return 1
    if args.do_import:
        import_check(wheel)
    print(f"OK: {wheel.name}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
