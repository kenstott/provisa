# Copyright (c) 2026 Kenneth Stott
# Canary: 7e2a9c41-5b3d-4f8e-9a1c-6d0b2e4f7a83
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Config ``includes:`` (REQ-1669): fragments splice into the including file at parse."""

from __future__ import annotations

from pathlib import Path

import pytest
import yaml

from provisa.core.config_loader import load_control_plane, parse_config, read_config_with_includes

_BASE = {
    "sources": [{"id": "pg", "type": "postgresql", "host": "h", "port": 5432}],
    "domains": [{"id": "core"}],
    "tables": [],
    "roles": [],
    "naming": {"domain_prefix": False, "rules": []},
    "control_plane": {},
}


def _write(path: Path, data: dict) -> Path:
    path.write_text(yaml.safe_dump(data))
    return path


class TestReadConfigWithIncludes:
    def test_list_sections_append_after_the_including_file(self, tmp_path: Path):
        _write(
            tmp_path / "frag.yaml",
            {
                "sources": [{"id": "extra", "type": "sqlite", "path": "x.db"}],
                "domains": [{"id": "more"}],
            },
        )
        root = _write(tmp_path / "root.yaml", {**_BASE, "includes": ["frag.yaml"]})
        raw = read_config_with_includes(root)
        assert [s["id"] for s in raw["sources"]] == ["pg", "extra"]
        assert [d["id"] for d in raw["domains"]] == ["core", "more"]
        assert "includes" not in raw

    def test_paths_resolve_relative_to_the_including_file(self, tmp_path: Path):
        sub = tmp_path / "fragments"
        sub.mkdir()
        _write(sub / "frag.yaml", {"domains": [{"id": "sub"}]})
        root = _write(tmp_path / "root.yaml", {**_BASE, "includes": ["fragments/frag.yaml"]})
        assert [d["id"] for d in read_config_with_includes(root)["domains"]] == ["core", "sub"]

    def test_wrapper_with_only_includes_takes_scalars_from_the_fragment(self, tmp_path: Path):
        base = _write(tmp_path / "base.yaml", {**_BASE, "federation_engine": "duckdb"})
        wrapper = _write(tmp_path / "wrapper.yaml", {"includes": [str(base)]})
        raw = read_config_with_includes(wrapper)
        assert raw["federation_engine"] == "duckdb"
        assert raw["naming"] == {"domain_prefix": False, "rules": []}

    def test_includes_nest(self, tmp_path: Path):
        _write(tmp_path / "leaf.yaml", {"domains": [{"id": "leaf"}]})
        _write(tmp_path / "mid.yaml", {"includes": ["leaf.yaml"], "domains": [{"id": "mid"}]})
        root = _write(tmp_path / "root.yaml", {**_BASE, "includes": ["mid.yaml"]})
        assert [d["id"] for d in read_config_with_includes(root)["domains"]] == [
            "core",
            "mid",
            "leaf",
        ]

    def test_conflicting_scalar_is_refused(self, tmp_path: Path):
        _write(tmp_path / "frag.yaml", {"federation_engine": "trino"})
        root = _write(
            tmp_path / "root.yaml",
            {**_BASE, "federation_engine": "duckdb", "includes": ["frag.yaml"]},
        )
        with pytest.raises(ValueError, match="federation_engine.*conflicts"):
            read_config_with_includes(root)

    def test_identical_scalar_is_not_a_conflict(self, tmp_path: Path):
        _write(tmp_path / "frag.yaml", {"federation_engine": "duckdb"})
        root = _write(
            tmp_path / "root.yaml",
            {**_BASE, "federation_engine": "duckdb", "includes": ["frag.yaml"]},
        )
        assert read_config_with_includes(root)["federation_engine"] == "duckdb"

    def test_cycle_is_refused(self, tmp_path: Path):
        _write(tmp_path / "a.yaml", {"includes": ["b.yaml"]})
        _write(tmp_path / "b.yaml", {"includes": ["a.yaml"]})
        with pytest.raises(ValueError, match="cycle"):
            read_config_with_includes(tmp_path / "a.yaml")

    def test_includes_must_be_a_list_of_paths(self, tmp_path: Path):
        root = _write(tmp_path / "root.yaml", {**_BASE, "includes": "frag.yaml"})
        with pytest.raises(ValueError, match="list of paths"):
            read_config_with_includes(root)

    def test_missing_fragment_raises(self, tmp_path: Path):
        root = _write(tmp_path / "root.yaml", {**_BASE, "includes": ["nope.yaml"]})
        with pytest.raises(FileNotFoundError):
            read_config_with_includes(root)


class TestParseConfigWithIncludes:
    def test_parse_config_validates_the_merged_document(self, tmp_path: Path):
        _write(
            tmp_path / "frag.yaml",
            {"sources": [{"id": "lite", "type": "sqlite", "path": "x.db"}]},
        )
        root = _write(tmp_path / "root.yaml", {**_BASE, "includes": ["frag.yaml"]})
        cfg = parse_config(root)
        assert [s.id for s in cfg.sources] == ["pg", "lite"]
        assert cfg.includes == []

    def test_control_plane_section_reaches_through_a_wrapper(self, tmp_path: Path):
        base = _write(
            tmp_path / "base.yaml",
            {**_BASE, "control_plane": {"tenant_url": "sqlite+aiosqlite:///cp.db"}},
        )
        wrapper = _write(tmp_path / "wrapper.yaml", {"includes": [str(base)]})
        assert load_control_plane(wrapper).tenant_url == "sqlite+aiosqlite:///cp.db"
