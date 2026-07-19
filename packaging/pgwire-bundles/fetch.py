#!/usr/bin/env python3
# Copyright (c) 2026 Kenneth Stott
# Canary: f08db7a4-05e3-4b19-8c90-6d73c3c7593a
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
"""Fetch the pinned Calcite pgwire bundles into provisa_pgwire_bundles/_bundles for the wheel.

Pins (release tag, repo, connector set) come from provisa.runtime_deps.pgwire_bundles — the runtime's
single source of truth — so the wheel can never drift from what resolution expects. Each connector's
release asset is downloaded and extracted into ``_bundles/<version>/<connector>/`` (the tree that
holds ``bin/pgwire-<connector>``). Fails loud if any launcher is missing after extraction (REQ-956).

Usage: python fetch.py
"""

from __future__ import annotations

import sys
import tarfile
import urllib.request
from pathlib import Path

from provisa.runtime_deps.pgwire_bundles import BUNDLE_CONNECTOR, BundleSpec

_DEST = Path(__file__).resolve().parent / "provisa_pgwire_bundles" / "_bundles"


def main() -> int:
    connectors = sorted(set(BUNDLE_CONNECTOR.values()))
    for connector in connectors:
        spec = BundleSpec(connector)
        dest = _DEST / spec.version / spec.artifact_name
        dest.mkdir(parents=True, exist_ok=True)
        url = spec.download_url
        if not url.startswith("https://"):
            print(f"[fetch] ERROR: refusing non-https URL {url}", file=sys.stderr)
            return 1
        print(f"[fetch] {connector}: {url}")
        with urllib.request.urlopen(url) as resp:  # noqa: S310  # nosec B310 - https validated above
            with tarfile.open(fileobj=resp, mode="r|gz") as tar:
                tar.extractall(dest, filter="data")
        launcher = dest / "bin" / spec.artifact_name
        if not launcher.is_file():
            print(f"[fetch] ERROR: {launcher} missing after extraction", file=sys.stderr)
            return 1
        print(f"[fetch] staged {connector} -> {dest}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
