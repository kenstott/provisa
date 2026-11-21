# Copyright (c) 2026 Kenneth Stott
# Canary: 0fe8337d-0d51-4dcb-9802-d0f5b5ed08f8
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.

"""Write the naming vectors provisa-ui/src/naming.ts is held to (REQ-194).

naming.ts mirrors provisa/compiler/naming.py where the UI needs a name. This runs naming.py's own
functions over :data:`INPUTS` and every convention it accepts and writes the results to
provisa-ui/src/__fixtures__/naming-vectors.json. src/__tests__/namingVectors.test.ts holds naming.ts
to the file; tests/unit/test_naming_vectors.py holds the file to naming.py, so neither side can
change alone. Run it again after changing a rule in naming.py or an input here.

Usage: .venv/bin/python3 scripts/gen_naming_vectors.py
"""

from __future__ import annotations

import json
from pathlib import Path

from provisa.compiler import naming

FIXTURE = Path(__file__).resolve().parents[1] / "provisa-ui/src/__fixtures__/naming-vectors.json"

INPUTS: tuple[str, ...] = (
    "Order_ID",
    "orderID",
    "userID",
    "col_1x",
    "HTTPServer",
    "a__b",
    "already_snake",
    "CamelCase",
    "x",
    "digits_2nd",
    "",
    "_",
    "-",
    "_leading",
    "trailing_",
    "__x__",
    "kebab-case-name",
    "order-id_Total",
    "foo_123",
    "foo123",
    "snake_42_x",
    "x_1_2",
    "v2_api",
    "a1B2c3",
    "ID",
    "Id",
    "A",
    "ABC",
    "XMLHttpRequest",
    "getHTTPResponseCode",
    "HTTP2Server",
    "iPhone",
    "created_by",
    "CREATED_BY",
    "createdBy",
    "CreatedBy",
    "mixed_Case_Words",
    "UPPER_snake",
    "name with space",
    "café_au_lait",
    "ÄpfelSaft",
    "user_ÉTAT",
    "日本_語",
)

#: Names that are neither a preset nor a Hasura/DDN literal: naming.py refuses each.
UNKNOWN_CONVENTIONS: tuple[str, ...] = (
    "snake_case",
    "camelCase",
    "PascalCase",
    "camel",
    "",
    "SNAKE",
)


def _raises(fn, *args) -> bool:
    try:
        fn(*args)
    except ValueError:
        return True
    return False


def vectors() -> dict:
    conventions = sorted(naming.VALID_CONVENTIONS)
    for c in UNKNOWN_CONVENTIONS:
        assert _raises(naming.apply_convention, "x", c), c
        assert _raises(naming.preset_convention, c), c
    return {
        "presets": list(naming.PRESET_CONVENTIONS),
        "valid_conventions": conventions,
        "preset_of": {c: naming.preset_convention(c) for c in conventions},
        "unknown_conventions": list(UNKNOWN_CONVENTIONS),
        "names": [
            {
                "input": s,
                "snake": naming._to_snake_case(s),
                "camel": naming._to_camel_case(s),
                "pascal": naming._to_pascal_case(s),
                "convention": {c: naming.apply_convention(s, c) for c in conventions},
            }
            for s in INPUTS
        ],
    }


def render() -> str:
    return json.dumps(vectors(), indent=2, ensure_ascii=False) + "\n"


if __name__ == "__main__":
    FIXTURE.write_text(render(), encoding="utf-8")
    print(f"wrote {FIXTURE}")
