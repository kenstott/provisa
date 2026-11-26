# Copyright (c) 2026 Kenneth Stott
# Canary: 3f6b2a1e-9c4d-4e7a-8b15-6d2e0c9a7f41
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The installed-host smoke configuration (REQ-1989, REQ-2269)."""

from __future__ import annotations

from pathlib import Path

import yaml

from provisa.core.config_loader import _merge_fragment

REPO = Path(__file__).resolve().parents[2]
CONFIG = "tests/smoke/smoke.yaml"


def installed_smoke_config(installed: dict) -> dict:
    """The smoke configuration as one file, on the engine the install's provisa.yaml names.

    smoke.yaml includes the demo's file, which names duckdb; an include never overrides a scalar
    the including file sets (REQ-1669), so the engine cannot be added to smoke.yaml. The install's
    own configuration selects the engine (REQ-2269): its federation_engine keys replace the demo's
    here, and the demo's file is then merged into the smoke file by the loader's own rule.
    """
    engine = {k: v for k, v in installed.items() if k.startswith("federation_engine")}
    assert engine.get("federation_engine"), installed
    smoke_path = REPO / CONFIG
    smoke = yaml.safe_load(smoke_path.read_text())
    for include in smoke.pop("includes"):
        fragment_path = (smoke_path.parent / include).resolve()
        fragment = yaml.safe_load(fragment_path.read_text())
        assert "includes" not in fragment, f"{fragment_path} includes further files"
        fragment = {k: v for k, v in fragment.items() if not k.startswith("federation_engine")}
        _merge_fragment(smoke, fragment, fragment_path)
    assert not any(k.startswith("federation_engine") for k in smoke), smoke_path
    return {**smoke, **engine}


def write_installed_smoke_config(root: Path, installed: dict) -> Path:
    """Write :func:`installed_smoke_config` under ``root`` at smoke.yaml's own relative path, with
    no includes; returns its path."""
    path = root / CONFIG
    path.parent.mkdir(parents=True)
    path.write_text(yaml.safe_dump(installed_smoke_config(installed), sort_keys=False))
    return path
