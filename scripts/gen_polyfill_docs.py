#!/usr/bin/env python3
# Copyright (c) 2026 Kenneth Stott
# Canary: 24f26baf-546c-4e61-b89b-328d445563a6
#
# This source code is licensed under the Business Source License 1.1
"""Generate docs/conformance/polyfill/ from the polyfill grid (REQ-2236).

Usage:
  python scripts/gen_polyfill_docs.py            # write the pages
  python scripts/gen_polyfill_docs.py --check    # diff only, exit 1 if a page drifted
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from provisa.transpiler.polyfill import GRID  # noqa: E402
from provisa.transpiler.polyfill.docs import pages  # noqa: E402


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--check", action="store_true", help="exit 1 if a page differs")
    args = parser.parse_args()
    drifted = []
    for relative, text in pages(GRID).items():
        path = ROOT / relative
        if args.check:
            if not path.exists() or path.read_text() != text:
                drifted.append(relative)
            continue
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(text)
        print(f"wrote {relative}")
    if drifted:
        print("polyfill pages drifted from the grid; run scripts/gen_polyfill_docs.py:")
        print("\n".join(f"  {p}" for p in drifted))
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
