# Copyright (c) 2026 Kenneth Stott
# Canary: 7c3f9a1e-4b2d-4e8f-9c0d-5a6b7c8d9e0f
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.

"""Shared helpers for reading and writing the provisa config YAML."""

# Requirements: REQ-164

from pathlib import Path

import yaml

from provisa.core.config_location import config_path as _config_path

# Profiled live (REQ-1730 engine-swap investigation): a schema rebuild's landing loop calls
# platform_config() -> read_config() once per materialized table (17-90+ times per rebuild,
# depending on registered-table count), each a full disk read + yaml.safe_load of the SAME file.
# Cached here, keyed by mtime, so a rebuild's N calls cost one real read — a config that changes
# mid-process (PUT /admin/config writes the file, then always calls _rebuild_schemas itself) is
# picked up on the next call because the mtime changed, so this never serves stale content.
_read_config_cache: dict[str, tuple[float, dict]] = {}


def read_config() -> dict:  # REQ-164
    path = _config_path()
    try:
        mtime = path.stat().st_mtime
    except OSError:
        return {}
    cached = _read_config_cache.get(str(path))
    if cached is not None and cached[0] == mtime:
        return cached[1]
    try:
        with open(path) as f:
            parsed = yaml.safe_load(f) or {}
    except Exception:
        return {}
    _read_config_cache[str(path)] = (mtime, parsed)
    return parsed


def config_path() -> Path:
    return _config_path()


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
