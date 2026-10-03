# Copyright (c) 2026 Kenneth Stott
# Canary: a7c15e38-4b62-4f90-8d3e-05f9b6c1d274
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Result redirect as operator settings (REQ-1913, REQ-029).

Part of the settings declarations (``provisa/core/settings_catalog.py`` lists them all): where
large results are written and when. Read by ``provisa/executor/redirect.RedirectConfig.from_env``.
"""

# Requirements: REQ-1913, REQ-029, REQ-137, REQ-142, REQ-687

from __future__ import annotations

from provisa.core.settings_registry import Setting


def _redirect_local_dir() -> str:
    """Where a redirected result is written when no object store is reachable (desktop use)."""
    import os
    import tempfile

    return os.path.join(tempfile.gettempdir(), "provisa-redirect-results")


DECLARED: list[Setting] = [
    # --- Redirect: large results written to the deployment's object store (REQ-029) --------------
    # enabled / threshold / ttl / default_format are the deployment's values; an org may narrow
    # its own (REQ-1349). Where results land is the deployment's alone, and changing it sends
    # results somewhere else, so the store's address and credentials are guarded.
    Setting(
        key="redirect.enabled",
        card="redirect",
        type="bool",
        effect="live",
        req="REQ-029",
        env="PROVISA_REDIRECT_ENABLED",
        default=False,
    ),
    Setting(
        key="redirect.threshold",
        card="redirect",
        type="int",
        effect="live",
        req="REQ-029",
        env="PROVISA_REDIRECT_THRESHOLD",
        default=1000,
        min=0,
        unit="rows",
    ),
    Setting(
        key="redirect.ttl",
        card="redirect",
        type="int",
        effect="live",
        req="REQ-137",
        env="PROVISA_REDIRECT_TTL",
        default=3600,
        min=1,
        unit="seconds",
    ),
    Setting(
        key="redirect.default_format",
        card="redirect",
        type="str",
        effect="live",
        req="REQ-142",
        env="PROVISA_REDIRECT_FORMAT",
        default="parquet",
    ),
    Setting(
        key="redirect.bucket",
        card="redirect",
        type="str",
        effect="live",
        req="REQ-029",
        env="PROVISA_REDIRECT_BUCKET",
        default="provisa-results",
        guard="confirm",
    ),
    Setting(
        key="redirect.endpoint",
        card="redirect",
        type="str",
        effect="live",
        req="REQ-029",
        env="PROVISA_REDIRECT_ENDPOINT",
        nullable=True,
        guard="confirm",
    ),
    Setting(
        key="redirect.region",
        card="redirect",
        type="str",
        effect="live",
        req="REQ-029",
        env="PROVISA_REDIRECT_REGION",
        default="us-east-1",
    ),
    Setting(
        # REQ-1194: a Snowflake engine reaches the results bucket through a storage integration
        # (Snowflake's own grant), so no credential is in the statement Snowflake keeps.
        key="redirect.snowflake_storage_integration",
        card="redirect",
        type="str",
        effect="live",
        req="REQ-1194",
        env="PROVISA_REDIRECT_SNOWFLAKE_STORAGE_INTEGRATION",
        nullable=True,
    ),
    Setting(
        key="redirect.encrypt",
        card="redirect",
        type="bool",
        effect="live",
        req="REQ-687",
        env="PROVISA_REDIRECT_ENCRYPT",
        default=False,
    ),
    Setting(
        key="redirect.local_dir",
        card="redirect",
        type="str",
        effect="live",
        req="REQ-029",
        env="PROVISA_REDIRECT_LOCAL_DIR",
        default_fn=_redirect_local_dir,
    ),
    Setting(
        key="redirect.access_key",
        card="redirect",
        type="str",
        effect="live",
        req="REQ-029",
        env="PROVISA_REDIRECT_ACCESS_KEY",
        nullable=True,
        secret=True,
        guard="confirm",
    ),
    Setting(
        key="redirect.secret_key",
        card="redirect",
        type="str",
        effect="live",
        req="REQ-029",
        env="PROVISA_REDIRECT_SECRET_KEY",
        nullable=True,
        secret=True,
        guard="confirm",
    ),
]
