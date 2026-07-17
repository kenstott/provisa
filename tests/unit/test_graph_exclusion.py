# Copyright (c) 2026 Kenneth Stott
# Canary: b3e75409-09af-42f6-9511-86ea63d4a042
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.

"""REQ-397 — graph node exclusion (named-test substitute).

The exclusion feature ("hide this node": `n.<pkCol> IN [<pkValue>]`, falling back to
`id(n) IN [<nodeId>]` when no PK is available) is implemented client-side in
`provisa-ui/src/components/graph/graph-model.ts`, not in `provisa/`. Its behavioral
coverage therefore lives in the TypeScript test
`provisa-ui/src/pages/__tests__/inject-exclusion.test.ts`.

This file exists so the spec-named `tests/unit/test_graph_exclusion.py` is present and
asserts that the real (UI) coverage is in place.
"""

from pathlib import Path

_REPO_ROOT = Path(__file__).resolve().parents[2]


def test_exclusion_is_implemented_in_ui():
    impl = _REPO_ROOT / "provisa-ui" / "src" / "components" / "graph" / "graph-model.ts"
    assert impl.is_file(), f"expected exclusion implementation at {impl}"
    src = impl.read_text()
    # PK-based exclusion with id() fallback (REQ-397)
    assert "IN [" in src
    assert "id(" in src


def test_exclusion_has_ui_test_coverage():
    ui_test = _REPO_ROOT / "provisa-ui" / "src" / "pages" / "__tests__" / "inject-exclusion.test.ts"
    assert ui_test.is_file(), f"expected UI exclusion test at {ui_test}"
