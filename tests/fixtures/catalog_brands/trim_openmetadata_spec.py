# Copyright (c) 2026 Kenneth Stott
# Canary: 0b6f2d8e-41c7-4a9e-b35d-8e1f7c2a9d40
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Trim a recorded OpenMetadata /swagger.json to the operations the catalog tests read.

OpenMetadata serves its spec from the running instance (REQ-2265), so nothing is shipped; the
tests read a recording. The whole spec is 3.7 MB and 1,400 paths, so the recording keeps the
GET operations below and every component they reach, unchanged.

    curl -s https://<instance>/swagger.json -o swagger.json
    .venv/bin/python tests/fixtures/catalog_brands/trim_openmetadata_spec.py swagger.json
"""

import gzip
import json
import sys
from pathlib import Path

PATHS = (
    "/v1/tables",
    "/v1/glossaryTerms",
    "/v1/domains",
    "/v1/dataProducts",
    "/v1/classifications",
    "/v1/tags",
    "/v1/users",
    "/v1/teams",
    "/v1/users/loggedInUser",
    "/v1/dataQuality/testCases",
    "/v1/dataQuality/testCases/testCaseResults/search/list",
)
OUT = Path(__file__).parent / "openmetadata_swagger.json.gz"


def _refs(node, found: set[str]) -> None:
    if isinstance(node, dict):
        ref = node.get("$ref")
        if isinstance(ref, str) and ref.startswith("#/components/schemas/"):
            found.add(ref.rsplit("/", 1)[1])
        for v in node.values():
            _refs(v, found)
    elif isinstance(node, list):
        for v in node:
            _refs(v, found)


def trim(spec: dict) -> dict:
    paths = {p: {"get": spec["paths"][p]["get"]} for p in PATHS}
    schemas = spec["components"]["schemas"]
    kept: set[str] = set()
    todo: set[str] = set()
    _refs(paths, todo)
    while todo:
        name = todo.pop()
        if name in kept:
            continue
        kept.add(name)
        _refs(schemas[name], todo)
    components = {"schemas": {n: schemas[n] for n in sorted(kept)}}
    if "securitySchemes" in spec["components"]:
        components["securitySchemes"] = spec["components"]["securitySchemes"]
    return {
        **{k: v for k, v in spec.items() if k not in ("paths", "components", "tags")},
        "paths": paths,
        "components": components,
    }


if __name__ == "__main__":
    recorded = json.loads(Path(sys.argv[1]).read_text())
    with gzip.open(OUT, "wt", encoding="utf-8") as fh:
        json.dump(trim(recorded), fh, sort_keys=True)
    print(OUT, OUT.stat().st_size)
