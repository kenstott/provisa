# Copyright (c) 2026 Kenneth Stott
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""REQ-1934: the standard demo ships a Data Profiler whose members are the demo's own tables."""

from __future__ import annotations

from pathlib import Path

from provisa.core.config_loader import parse_config_dict, read_config_with_includes
from provisa.profiler.registration import validate_config

DEMO_CONFIG = Path(__file__).resolve().parents[2] / "config" / "provisa-install.yaml"


def test_the_demo_config_has_a_profiler_with_members_and_validates(monkeypatch):
    monkeypatch.setenv("PROVISA_DEMO_DIR", "./demo/files")
    cfg = parse_config_dict(read_config_with_includes(str(DEMO_CONFIG)))
    validate_config(cfg)
    profilers = [s.id for s in cfg.sources if s.type.value == "data_profiler"]
    assert profilers == ["demo-profiler"]
    members = sorted(t.table_name for t in cfg.tables if t.profiler_source_id == "demo-profiler")
    assert members == ["inquiries", "pet_visits", "pets", "vets"]
