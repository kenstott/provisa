# Copyright (c) 2026 Kenneth Stott
# Canary: f4f48bfa-c5bd-4dd0-bfe2-3516528de8b4
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Write the source-type descriptor (provisa/api/admin/source_types.py) for the UI.

The Sources picker, its brand carriers, aliases, brand credentials and connection-field families
are read from this file (provisa-ui/src/pages/sources/constants.ts), so the picker and Polly's
``source_types`` tool offer the same types. tests/unit/test_source_types_generated.py fails when
the file and the descriptor disagree.

    PYTHONPATH=. .venv/bin/python3 scripts/gen_source_types.py
"""

from __future__ import annotations

import json
from pathlib import Path

from provisa.api.admin import source_types as st

OUT = Path(__file__).resolve().parents[1] / "provisa-ui/src/generated/sourceTypes.json"


def render() -> str:
    descriptors = st.source_types()
    document = {
        "types": [st.as_json(d) for d in descriptors],
        "typeAlias": st.TYPE_ALIAS,
        "brandCarrier": st.brand_carriers(),
        "brandAuth": {b: st.brand_auth(b) for b in st.brand_carriers()},
        "families": st.families(),
    }
    return json.dumps(document, indent=2, sort_keys=False) + "\n"


if __name__ == "__main__":
    OUT.write_text(render(), encoding="utf-8")
    print(f"wrote {OUT}")
