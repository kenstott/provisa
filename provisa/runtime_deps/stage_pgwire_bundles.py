# Copyright (c) 2026 Kenneth Stott
# Canary: e31af7cb-7863-4e0c-946b-741d66b8cd2b
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.

"""Stage the pgwire connector bundles from the configured Maven repository (REQ-294).

Run once at install or first launch, through ``python -m provisa.runtime_deps.stage_assets``. The
runtime never does this: it locates a staged bundle, and refuses by name one that is missing. A bundle
already staged is not fetched again. ``--only <connector>`` fetches just that one. ``--github-release``
reads the pinned GitHub release instead, for a CI lane on a connector not yet promoted to Maven Central.
Exit code is non-zero if a bundle could not be staged.
"""

from __future__ import annotations

import argparse
import sys
from collections.abc import Iterable, Sequence
from pathlib import Path

from provisa.runtime_deps.maven_bundles import download_maven_bundle
from provisa.runtime_deps.pgwire_bundles import (
    BUNDLE_CONNECTOR,
    BundleResolver,
    BundleUnavailable,
    Downloader,
    bundle_spec_for,
    checked_connectors,
    default_cache_root,
    download_release_asset,
    verify_staged,
)


def stage(
    cache_root: Path,
    *,
    downloader: Downloader = download_maven_bundle,
    excepted: Iterable[str] = (),
) -> list[str]:
    """Fetch each connector's bundle not yet staged under ``cache_root``, less ``excepted``; return
    the names fetched."""
    resolver = BundleResolver(cache_root=cache_root, downloader=downloader)
    wanted = set(checked_connectors(excepted))
    fetched: list[str] = []
    for source_type in sorted(BUNDLE_CONNECTOR):
        if BUNDLE_CONNECTOR[source_type] not in wanted:
            continue
        spec = bundle_spec_for(source_type)
        if resolver.is_cached(spec) or spec.artifact_name in fetched:
            continue
        print(f"staging {spec.artifact_name} {spec.version} -> {resolver.cached_path(spec)}")
        resolver.resolve(spec)
        fetched.append(spec.artifact_name)
    return fetched


def _excepted(only: Sequence[str]) -> list[str]:
    """What this run leaves out: everything but ``--only``, or nothing."""
    if not only:
        return []
    every = set(checked_connectors())
    unknown = sorted(set(only) - every)
    if unknown:
        raise BundleUnavailable(
            f"--only names no connector: {', '.join(unknown)} (known: {sorted(every)})"
        )
    return sorted(every - set(only))


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Stage the pgwire connector bundles.")
    parser.add_argument("--only", action="append", default=[], metavar="CONNECTOR")
    parser.add_argument("--github-release", action="store_true")
    args = parser.parse_args(argv)
    root = default_cache_root()
    print(f"Staging pgwire bundles into: {root}")
    downloader = download_release_asset if args.github_release else download_maven_bundle
    try:
        excepted = _excepted(args.only)
        stage(root, downloader=downloader, excepted=excepted)
        verify_staged(root, excepted=excepted)
    except BundleUnavailable as refused:
        print(f"FAIL: {refused}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
