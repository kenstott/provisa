#!/usr/bin/env python
# Copyright (c) 2026 Kenneth Stott
# Canary: 9d4c27e8-5b61-4f03-a8e7-2c1b6f90d3a4
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.

"""Build step: fetch every pgwire connector bundle into the runtime-deps cache (REQ-294).

Run once on a connected machine: in an image build, an installer build, or on a source checkout. The
runtime never does this: it locates a staged bundle, and refuses by name one that is missing.
Set PROVISA_RUNTIME_DEPS_CACHE to stage into a bundled directory; the runtime reads the same variable.
A bundle already staged is not fetched again. Exit code is non-zero if a bundle could not be staged.
"""

from __future__ import annotations

import sys
from pathlib import Path

from provisa.runtime_deps.pgwire_bundles import (
    BUNDLE_CONNECTOR,
    BundleResolver,
    BundleUnavailable,
    Downloader,
    bundle_spec_for,
    default_cache_root,
    download_release_asset,
    verify_staged,
)


def stage(cache_root: Path, *, downloader: Downloader = download_release_asset) -> list[str]:
    """Fetch each connector's bundle not yet staged under ``cache_root``; return the names fetched."""
    resolver = BundleResolver(cache_root=cache_root, downloader=downloader)
    fetched: list[str] = []
    for source_type in sorted(BUNDLE_CONNECTOR):
        spec = bundle_spec_for(source_type)
        if resolver.is_cached(spec) or spec.artifact_name in fetched:
            continue
        print(f"staging {spec.artifact_name} {spec.version} -> {resolver.cached_path(spec)}")
        resolver.resolve(spec)
        fetched.append(spec.artifact_name)
    return fetched


def main() -> int:
    root = default_cache_root()
    print(f"Staging pgwire bundles into: {root}")
    try:
        stage(root)
        verify_staged(root)
    except BundleUnavailable as refused:
        print(f"FAIL: {refused}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
