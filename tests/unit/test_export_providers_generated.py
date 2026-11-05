# Copyright (c) 2026 Kenneth Stott
# Canary: 33e68f20-c0ec-4adf-8708-937d6fdbd8a7
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The UI's list of metadata-export providers, and which are beta, is the registry's."""

from __future__ import annotations

import importlib.util
from pathlib import Path

_REPO = Path(__file__).resolve().parents[2]


def _generator():
    spec = importlib.util.spec_from_file_location(
        "gen_export_providers", _REPO / "scripts/gen_export_providers.py"
    )
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_the_generated_provider_list_matches_the_registry():
    gen = _generator()
    assert gen.OUT.read_text(encoding="utf-8") == gen.render(), (
        "provisa-ui/src/generated/exportProviders.json is stale: run "
        "PYTHONPATH=. .venv/bin/python3 scripts/gen_export_providers.py"
    )


def test_atlan_is_beta_until_a_tenant_proves_it():
    from provisa.api.metadata_export.atlan import AtlanExport
    from provisa.api.metadata_export.registry import beta_providers

    assert AtlanExport.beta is True
    assert beta_providers() == ["atlan"]
