# Copyright (c) 2026 Kenneth Stott
# Canary: 7b3e1a2c-9d4f-4e6a-8c1b-2f5a6d7e8c9a
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Shared test helper: build the in-process app (create_app) with auth disabled.

The integration/e2e tiers exercise the API by passing a `role` in the request
(no bearer token), so the app must be wired with `auth.provider: none`. The
default config (config/provisa.yaml) ships `provider: firebase`, which would
reject every tokenless request with HTTP 401. This generates a faithful copy of
the active config with only `auth.provider` forced to `none` and points
PROVISA_CONFIG at it for the test session, restoring the prior value after.
"""

from __future__ import annotations

import copy
import os
from collections.abc import Iterator
from pathlib import Path

import yaml

# The in-process app runs on the HOST and reaches the dev-stack MinIO/S3 (otel
# Iceberg catalog + Parquet results) over the published localhost port; the config
# default `http://minio:9000` is a docker-internal name that does not resolve from
# the host, so app startup fails with a boto3 NameResolutionError that cascades to
# a tenant_db assertion. Trino, on the compose network, still reaches the bucket by
# service name. Set here (imported at collection by the integration/e2e conftests,
# before any app starts) with setdefault so an explicit outer value — or the e2e
# lane's own ephemeral endpoint — still wins.
os.environ.setdefault("PROVISA_OTEL_S3_ENDPOINT", "http://localhost:9000")


def pin_no_auth_config(tmp_dir: Path) -> Iterator[None]:
    src = os.environ.get("PROVISA_CONFIG", "config/provisa.yaml")
    previous = os.environ.get("PROVISA_CONFIG")
    try:
        raw = Path(src).read_text()
    except OSError:
        # No config to rewrite — leave the environment untouched.
        yield
        return

    cfg = yaml.safe_load(raw) or {}
    auth = cfg.get("auth")
    if isinstance(auth, dict) and auth.get("provider") not in (None, "none"):
        cfg = copy.deepcopy(cfg)
        cfg["auth"]["provider"] = "none"
        out = tmp_dir / "provisa-noauth.yaml"
        out.write_text(yaml.safe_dump(cfg, sort_keys=False))
        os.environ["PROVISA_CONFIG"] = str(out)

    try:
        yield
    finally:
        if previous is None:
            os.environ.pop("PROVISA_CONFIG", None)
        else:
            os.environ["PROVISA_CONFIG"] = previous
