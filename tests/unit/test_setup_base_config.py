# Copyright (c) 2026 Kenneth Stott
# Canary: 5b8c1e2a-9d4f-4a7b-8c3e-1f6a2d9b0c47
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""First-run setup wizard base-config guarantee (REQ-120).

The setup wizard layers an ``auth`` section onto a pre-existing base config; it is
not a from-scratch config builder. ``ProvisaConfig`` requires
``sources``/``domains``/``tables``/``roles`` (no defaults), so a fileless first-run
install has nothing valid for ``_load_and_build`` -> ``parse_config_dict`` to parse
after the wizard writes. ``read_config_for_setup`` must fall back to the shipped
minimal skeleton (``provisa-install-base.yaml``) so the wizard always produces a
config that parses into a valid ``ProvisaConfig``.

Source coverage:
  - provisa/api/admin/_config_io.py — read_config_for_setup
  - provisa/cli.py                  — _resolve_base_config
  - provisa/core/config_loader.py   — parse_config_dict
"""

from __future__ import annotations

from provisa.api.admin._config_io import read_config_for_setup
from provisa.core.config_loader import parse_config_dict


def test_fileless_setup_base_parses(monkeypatch, tmp_path):
    # Fileless first-run install: PROVISA_CONFIG points at a path that does not exist.
    monkeypatch.setenv("PROVISA_CONFIG", str(tmp_path / "missing.yaml"))

    cfg = read_config_for_setup()
    assert cfg, "fileless install must fall back to the base skeleton, not an empty dict"

    # The wizard layers auth onto the base; the result must be a valid ProvisaConfig.
    cfg["auth"] = {"provider": "none"}
    parsed = parse_config_dict(cfg)

    # Skeleton guarantees the four required lists parse_config_dict enforces.
    assert parsed.sources
    assert parsed.domains
    assert parsed.roles
    assert any(r.id == "admin" for r in parsed.roles)


def test_existing_config_used_as_is(monkeypatch, tmp_path):
    # An existing config file is returned verbatim — the base skeleton is a fallback only.
    cfg_file = tmp_path / "provisa.yaml"
    cfg_file.write_text("sources: []\ndomains: []\ntables: []\nroles: []\nsentinel: present\n")
    monkeypatch.setenv("PROVISA_CONFIG", str(cfg_file))

    cfg = read_config_for_setup()
    assert cfg.get("sentinel") == "present"
