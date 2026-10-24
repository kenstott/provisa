# Copyright (c) 2026 Kenneth Stott
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Write the metadata-export providers, and which are beta, for the UI.

The beta flag is the provider class's own (``MetadataExport.beta``). Copy that names a provider
outside the admin page -- a form hint, the tour -- reads this file rather than the admin API,
which only org settings may call. tests/unit/test_export_providers_generated.py fails when the
file and the registry disagree.

    PYTHONPATH=. .venv/bin/python3 scripts/gen_export_providers.py
"""

from __future__ import annotations

import json
from pathlib import Path

import provisa.api.metadata_export  # noqa: F401  -- registers every provider
from provisa.api.metadata_export.registry import beta_providers, registered_providers

OUT = Path(__file__).resolve().parents[1] / "provisa-ui/src/generated/exportProviders.json"


def render() -> str:
    return (
        json.dumps({"providers": registered_providers(), "beta": beta_providers()}, indent=2) + "\n"
    )


if __name__ == "__main__":
    OUT.write_text(render(), encoding="utf-8")
    print(f"wrote {OUT}")
