#!/usr/bin/env python3
# Copyright (c) 2026 Kenneth Stott
# Canary: 8c2e7a51-4d93-4b06-a1f8-27e5b90c3d64
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Bake the shipped spec of a branded OpenAPI source (REQ-1923).

Fetches the spec the brand's vendor publishes and writes
provisa/openapi/_known_specs/<brand>.json.gz. Run by hand to move the brand to a newer version of
its API; the spec's own ``info.version`` says which one it is.

Usage:
    .venv/bin/python3 scripts/bake_openapi_brand_spec.py stripe
"""

import gzip
import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from provisa.openapi.brands import _SPEC_DIR, BRANDS  # noqa: E402
from provisa.openapi.loader import load_spec  # noqa: E402
from provisa.openapi.mapper import parse_spec  # noqa: E402


def main(brand_id: str) -> None:
    brand = BRANDS[brand_id]
    spec = load_spec(brand.spec_url)
    tables, commands = parse_spec(spec)
    out = _SPEC_DIR / f"{brand_id}.json.gz"
    out.parent.mkdir(parents=True, exist_ok=True)
    # mtime=0 so an unchanged spec bakes to the same bytes.
    with gzip.GzipFile(out, "wb", mtime=0) as fh:
        fh.write(json.dumps(spec, sort_keys=True, separators=(",", ":")).encode())
    print(
        f"Wrote {out} (version {spec['info']['version']}, {len(tables)} tables and "
        f"{len(commands)} commands on offer, {out.stat().st_size} bytes)"
    )


if __name__ == "__main__":
    if len(sys.argv) != 2 or sys.argv[1] not in BRANDS:
        sys.exit(f"usage: {sys.argv[0]} <{'|'.join(BRANDS)}>")
    main(sys.argv[1])
