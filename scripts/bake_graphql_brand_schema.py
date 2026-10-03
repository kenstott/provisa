# Copyright (c) 2026 Kenneth Stott
# Canary: 5e1b8d27-3c64-4a9f-b0e2-7d9c1f4a6b83
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Regenerate the shipped schema of a branded GraphQL source (REQ-1923, REQ-1875).

Introspects the brand's endpoint and writes provisa/graphql_remote/_known_schemas/<brand>.json.gz.
Run by hand when the system's public schema changes; compare ``schema_fingerprint`` to see
whether it has.

The credential is read from BRAND_TOKEN. Introspection needs no particular grant, only a
credential the endpoint accepts. An endpoint that introspects for anonymous callers (GitLab)
is baked with BRAND_TOKEN unset.

Usage:
    BRAND_TOKEN=$(gh auth token) .venv/bin/python3 scripts/bake_graphql_brand_schema.py github
    .venv/bin/python3 scripts/bake_graphql_brand_schema.py gitlab
"""

import asyncio
import gzip
import hashlib
import json
import os
import sys
from datetime import datetime, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from provisa.graphql_remote.brands import _SCHEMA_DIR, BRANDS  # noqa: E402
from provisa.graphql_remote.introspect import introspect_schema  # noqa: E402


async def main(brand_id: str) -> None:
    brand = BRANDS[brand_id]
    token = os.environ.get("BRAND_TOKEN")
    schema = await introspect_schema(brand.url, brand.auth(token) if token else None)
    canonical = json.dumps(schema, sort_keys=True, separators=(",", ":"))
    artifact = {
        "url": brand.url,
        "schema_fingerprint": f"sha256:{hashlib.sha256(canonical.encode()).hexdigest()}",
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "schema": schema,
    }
    out = _SCHEMA_DIR / f"{brand_id}.json.gz"
    out.parent.mkdir(parents=True, exist_ok=True)
    # mtime=0 so an unchanged schema bakes to the same bytes apart from generated_at.
    with gzip.GzipFile(out, "wb", mtime=0) as fh:
        fh.write(json.dumps(artifact, sort_keys=True, separators=(",", ":")).encode())
    print(f"Wrote {out} ({len(schema['types'])} types, {out.stat().st_size} bytes)")


if __name__ == "__main__":
    asyncio.run(main(sys.argv[1]))
