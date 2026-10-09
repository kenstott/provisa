#!/usr/bin/env python3
# Copyright (c) 2026 Kenneth Stott
# Canary: 25d00254-e0bf-4db3-a2ab-c6c7e630412b
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Record the schemas the pinned pgwire-govdata bundle serves (REQ-540, REQ-541).

Writes provisa/govdata/bundle_schemas.json from the bundle's own model/model.json, with the
release it was read from. Run it whenever the bundle pin moves
(provisa/runtime_deps/pgwire_bundles.py); a unit test refuses a record of another release.
The bundle is resolved through the product's own resolver, so it is downloaded if not cached.

Usage: .venv/bin/python3 scripts/record_govdata_bundle_schemas.py
"""

from __future__ import annotations

import json
from pathlib import Path

from provisa.runtime_deps import BundleResolver, bundle_spec_for

#: Written into the record: this script is its only writer.
GENERATED_BY = (
    "scripts/record_govdata_bundle_schemas.py, from the pinned bundle's own model. "
    "The script is the only writer of this file: do not edit it; run the script."
)
OUT_PATH = Path(__file__).resolve().parent.parent / "provisa" / "govdata" / "bundle_schemas.json"


def main() -> None:
    spec = bundle_spec_for("govdata")
    bundle_dir = BundleResolver().resolve(spec)
    model = json.loads((Path(bundle_dir) / "model" / "model.json").read_text())
    record = {
        "generated_by": GENERATED_BY,
        "release": spec.version,
        "schemas": sorted(s["name"] for s in model["schemas"]),
    }
    OUT_PATH.write_text(json.dumps(record, indent=2) + "\n")
    print(f"{OUT_PATH}: {len(record['schemas'])} schemas of {spec.version}")


if __name__ == "__main__":
    main()
