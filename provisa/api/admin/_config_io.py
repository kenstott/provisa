# Copyright (c) 2026 Kenneth Stott
# Canary: 7c3f9a1e-4b2d-4e8f-9c0d-5a6b7c8d9e0f
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.

"""Shared helpers for reading and writing the provisa config YAML."""

# Requirements: REQ-164

import os
from pathlib import Path

import yaml


def read_config() -> dict:  # REQ-164
    config_path = os.environ.get("PROVISA_CONFIG", "config/provisa.yaml")
    try:
        with open(config_path) as f:
            return yaml.safe_load(f) or {}
    except Exception:
        return {}


def config_path() -> Path:
    return Path(os.environ.get("PROVISA_CONFIG", "config/provisa.yaml"))


def read_config_for_setup() -> dict:  # REQ-164, REQ-120
    """Config the setup wizard layers ``auth`` onto. ``ProvisaConfig`` requires
    ``sources``/``domains``/``tables``/``roles``, so a fileless first-run install (empty
    config) has nothing valid for ``_load_and_build`` to parse after the wizard writes.
    Start from the shipped minimal skeleton (``provisa-install-base.yaml``: system
    sources/domains + the built-in ``admin`` role) so the wizard always produces a valid
    config. An existing config is used as-is."""
    cfg = read_config()
    if cfg:
        return cfg
    from provisa.cli import _resolve_base_config

    base = _resolve_base_config()
    with open(base) as f:
        return yaml.safe_load(f) or {}


def write_config(path: Path, cfg: dict) -> None:  # REQ-164
    # First-run setup creates the config for the first time — there is nothing to
    # back up and the config dir may not exist yet. Only snapshot an existing file.
    path.parent.mkdir(parents=True, exist_ok=True)
    if path.exists():
        path.with_suffix(".yaml.bak").write_text(path.read_text())
    with open(path, "w") as f:
        yaml.dump(cfg, f, default_flow_style=False, sort_keys=False)
