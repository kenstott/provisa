# Copyright (c) 2026 Kenneth Stott
# Canary: 91ecd636-7384-4949-8369-62379573f307
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Trim a recorded DataHub OpenAPI v3 spec to the entity lists the catalog tests read.

DataHub's GMS generates the spec from its entity registry (REQ-2265). The recording is taken
from the test stack's GMS (docker-compose.test.yml, datahub-gms) by
tests/integration/test_catalog_brands_live.py, which fails with the trimmed recording when the
shipped one differs from it:

    curl -s http://<gms>/openapi/v3/api-docs -o api-docs.json
    .venv/bin/python -m tests.fixtures.catalog_brands.trim_datahub_spec api-docs.json
"""

import json
import sys
from pathlib import Path

from tests.fixtures.catalog_brands.trim_spec import trim, write

ENTITIES = (
    "dataset",
    "glossaryterm",
    "domain",
    "dataproduct",
    "tag",
    "corpuser",
    "corpgroup",
)
PATHS = tuple(f"/openapi/v3/entity/{e}" for e in ENTITIES)
OUT = Path(__file__).parent / "datahub_api_docs.json.gz"


def trimmed(recorded: dict) -> dict:
    return trim(recorded, PATHS)


if __name__ == "__main__":
    write(trimmed(json.loads(Path(sys.argv[1]).read_text())), OUT)
    print(OUT, OUT.stat().st_size)
