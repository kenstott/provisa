#!/usr/bin/env python3
# Copyright (c) 2026 Kenneth Stott
# Canary: 80777f51-ae8f-4529-9205-a4499f14dd97
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Print a path suitable for DUCKDB_FIREBIRD_CLIENT_LIBRARY, or exit 1 with a reason on stderr.

DuckDB's `firebird` community extension needs the native libfbclient client library at ATTACH
time — the extension itself has no bundled copy. tests/integration/test_firebird_source_e2e.py
already self-provisions this on macOS (downloads Firebird's official .pkg once, expands it with
pkgutil, caches it under ~/.cache/provisa-fdw/firebird-client-<version>-<arch>/, and patches one
bare-@rpath dependency with install_name_tool) and checks a short list of common Linux install
paths otherwise. This script reuses that exact logic (imported, not reimplemented) so the
provisa-ui e2e harness (which needs DUCKDB_FIREBIRD_CLIENT_LIBRARY set in the shell that invokes
`npx playwright test`, before the webServer process boots — Playwright only inherits env, it
can't set it after the fact) can resolve the same path a Python test run would.

Usage: .venv/bin/python scripts/resolve_firebird_client_lib.py
"""

from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))


def main() -> int:
    import pytest

    from tests.integration.test_firebird_source_e2e import _ensure_firebird_client_lib

    try:
        print(_ensure_firebird_client_lib())
        return 0
    except pytest.skip.Exception as e:
        print(str(e), file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
