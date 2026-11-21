#!/usr/bin/env python3
# Copyright (c) 2026 Kenneth Stott
# Canary: a7c3e108-9b2d-4d55-bc61-34f0e8a7d292
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The release's block step (REQ-2225): writes what ``provisa connector build`` printed into the
platform connector block every Provisa package ships, provisa/core/platform_connectors.yaml.

A block that does not read as connectors, or names one twice, stops the release here rather than
ship a package whose Sources page fails.

    python scripts/fill_platform_connectors.py <built block> [<target>]
"""

# Requirements: REQ-2225

from __future__ import annotations

import argparse
import sys
from pathlib import Path

import yaml
from pydantic import TypeAdapter, ValidationError

from provisa.core.models import ConnectorConfig, repeated_connector_names

_TARGET = Path(__file__).resolve().parents[1] / "provisa" / "core" / "platform_connectors.yaml"
_HEADER = (
    "# REQ-2225: the connectors the platform offers every organization, written by the release's\n"
    "# block step (scripts/fill_platform_connectors.py).\n"
)


def fill(block: Path, target: Path) -> list[str]:
    """``target`` holding the connectors of ``block``; their names."""
    raw = yaml.safe_load(block.read_text())
    if not isinstance(raw, dict) or not raw.get("connectors"):
        raise SystemExit(f"{block} holds no connectors")
    try:
        connectors = TypeAdapter(list[ConnectorConfig]).validate_python(raw["connectors"])
    except ValidationError as exc:
        wrong = "; ".join(f"{'.'.join(map(str, e['loc']))}: {e['msg']}" for e in exc.errors())
        raise SystemExit(f"{block}: {wrong}") from None
    repeated = repeated_connector_names(connectors)
    if repeated:
        raise SystemExit(f"{block}: connector names used more than once: {repeated}")
    dumped = [c.model_dump(mode="json", exclude_defaults=True) for c in connectors]
    target.write_text(_HEADER + yaml.safe_dump({"connectors": dumped}, sort_keys=False))
    return [c.name for c in connectors]


def main() -> int:
    parser = argparse.ArgumentParser(description="Fill the platform connector block.")
    parser.add_argument("block", type=Path, help="What `provisa connector build` printed")
    parser.add_argument("target", type=Path, nargs="?", default=_TARGET)
    args = parser.parse_args()
    print(f"platform connectors: {', '.join(fill(args.block, args.target))}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
