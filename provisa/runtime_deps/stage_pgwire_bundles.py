# Copyright (c) 2026 Kenneth Stott
# Canary: 2b6e71d9-84c0-4a35-91fe-7d13c5a0e628
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.

"""Build step: fetch the pgwire connector bundles into the runtime-deps cache (REQ-294).

Run once on a connected machine: in an image build, an installer build, or on a source checkout. The
runtime never does this: it locates a staged bundle, and refuses by name one that is missing.
Set PROVISA_RUNTIME_DEPS_CACHE to stage into a bundled directory; the runtime reads the same variable.
A bundle already staged is not fetched again. ``--except <connector>`` leaves a bundle out and
``--only <connector>`` fetches just that one. Exit code is non-zero if a bundle could not be staged.
"""

from __future__ import annotations

import argparse
import sys
from collections.abc import Iterable, Sequence
from pathlib import Path

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
    downloader: Downloader = download_release_asset,
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


def _excepted(args: argparse.Namespace) -> list[str]:
    """What this run leaves out: ``--except``, or everything but ``--only``."""
    if not args.only:
        return args.excepted
    every = set(checked_connectors())
    unknown = sorted(set(args.only) - every)
    if unknown:
        raise BundleUnavailable(
            f"--only names no connector: {', '.join(unknown)} (known: {sorted(every)})"
        )
    return sorted(every - set(args.only))


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Stage the pgwire connector bundles.")
    group = parser.add_mutually_exclusive_group()
    group.add_argument(
        "--except", dest="excepted", action="append", default=[], metavar="CONNECTOR"
    )
    group.add_argument("--only", action="append", default=[], metavar="CONNECTOR")
    args = parser.parse_args(argv)
    root = default_cache_root()
    print(f"Staging pgwire bundles into: {root}")
    try:
        excepted = _excepted(args)
        stage(root, excepted=excepted)
        verify_staged(root, excepted=excepted)
    except BundleUnavailable as refused:
        print(f"FAIL: {refused}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
