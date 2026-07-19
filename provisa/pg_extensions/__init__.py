# Copyright (c) 2026 Kenneth Stott
# Canary: 3d627be3-8eaa-4f82-ab2d-da59e607a789
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Curated prebuilt PostgreSQL extension catalog + resolver (REQ-898)."""

from provisa.pg_extensions.catalog import (
    CatalogEntry,
    ExtensionArtifactUnavailable,
    ExtensionBuild,
    ExtensionCatalog,
    NAMED_EXTENSIONS,
    PLATFORM_TAGS,
    current_platform,
    load_catalog,
)
from provisa.pg_extensions.staging import (
    BundledPgExtensionsMissing,
    bundle_platform,
    stage_bundled_pg_extensions,
)

__all__ = [
    "NAMED_EXTENSIONS",
    "PLATFORM_TAGS",
    "BundledPgExtensionsMissing",
    "CatalogEntry",
    "ExtensionArtifactUnavailable",
    "ExtensionBuild",
    "ExtensionCatalog",
    "bundle_platform",
    "current_platform",
    "load_catalog",
    "stage_bundled_pg_extensions",
]
