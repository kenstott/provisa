# Copyright (c) 2026 Kenneth Stott
# Canary: 7c3f9a1e-4b2d-4e8f-9c0d-5a6b7c8d9e0f
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.

"""Shared helpers for reading and writing the provisa config YAML."""

import os
from pathlib import Path

import yaml


def read_config() -> dict:
    config_path = os.environ.get("PROVISA_CONFIG", "config/provisa.yaml")
    try:
        with open(config_path) as f:
            return yaml.safe_load(f) or {}
    except Exception:
        return {}


def config_path() -> Path:
    return Path(os.environ.get("PROVISA_CONFIG", "config/provisa.yaml"))


def write_config(path: Path, cfg: dict) -> None:
    backup = path.with_suffix(".yaml.bak")
    backup.write_text(path.read_text())
    with open(path, "w") as f:
        yaml.dump(cfg, f, default_flow_style=False, sort_keys=False)
