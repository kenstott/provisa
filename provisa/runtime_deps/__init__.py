# Copyright (c) 2026 Kenneth Stott
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Runtime dependency system: on-demand, version-pinned, locally-cached external artifacts (REQ-956).

Some connector runtimes are NOT shipped inside the Provisa distribution — they are fetched on demand
from a pinned upstream release and cached locally. This package owns that resolve+cache contract. The
first (and currently only) consumer is the Calcite pgwire connector bundles used by the replica
strategy (``provisa.federation.pgwire_replica``): pgwire-file / pgwire-sharepoint / pgwire-splunk from
the pinned github.com/kenstott/calcite release.
"""

from __future__ import annotations

from provisa.runtime_deps.pgwire_bundles import (
    BUNDLE_CONNECTOR,
    GITHUB_REPO,
    RELEASE_TAG,
    BundleResolver,
    BundleSpec,
    BundleUnavailable,
    bundle_spec_for,
    default_cache_root,
    download_release_asset,
)

__all__ = [
    "BUNDLE_CONNECTOR",
    "GITHUB_REPO",
    "RELEASE_TAG",
    "BundleResolver",
    "BundleSpec",
    "BundleUnavailable",
    "bundle_spec_for",
    "default_cache_root",
    "download_release_asset",
]
