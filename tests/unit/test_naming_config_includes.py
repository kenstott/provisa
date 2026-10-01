# Copyright (c) 2026 Kenneth Stott
# Canary: 6a1f3c8e-2d4b-4f7a-9e5c-8b0d1a3f7c92
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A rebuild with no config in hand re-reads naming from disk through the includes-aware loader."""

# Requirements: REQ-1669

from pathlib import Path


def test_domain_prefix_is_read_through_an_includes_wrapper(tmp_path: Path, monkeypatch):
    from provisa.api import app_schema_build

    (tmp_path / "base.yaml").write_text("naming:\n  domain_prefix: true\n")
    wrapper = tmp_path / "wrapper.yaml"
    wrapper.write_text("includes:\n  - base.yaml\n")
    monkeypatch.setattr(app_schema_build, "config_path_str", lambda: str(wrapper))

    domain_prefix, raw = app_schema_build._resolve_naming_config(None)

    assert domain_prefix is True
    assert raw is not None and "includes" not in raw
