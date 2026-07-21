# Copyright (c) 2026 Kenneth Stott
# Canary: 9aafad37-e908-4e4e-b966-ca29477f4b3f
# Canary: placeholder
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""REQ-1174 / REQ-417: the Hasura importer maps api_limits.yaml to per-role limits.

Hasura's ``api_limits.yaml`` (rate_limit / depth_limit / node_limit / time_limit, each with a
``global`` and a ``per_role`` map) is now parsed and mapped onto Provisa's ``Role.rate_limit`` —
so a migration preserves both request-rate and query-complexity governance instead of dropping it.
"""

from __future__ import annotations

from pathlib import Path

import yaml

from provisa.hasura_v2.mapper import convert_metadata
from provisa.hasura_v2.models import (
    HasuraMetadata,
    HasuraPermission,
    HasuraSource,
    HasuraTable,
)
from provisa.hasura_v2.parser import parse_metadata_dir


def _md_with_limits(api_limits: dict) -> HasuraMetadata:
    tbl = HasuraTable(
        name="orders",
        schema_name="public",
        select_permissions=[HasuraPermission(role="user", columns=["id"])],
    )
    return HasuraMetadata(
        sources=[HasuraSource(name="default", kind="postgres", tables=[tbl])],
        api_limits=api_limits,
    )


def _user_limits(md: HasuraMetadata):
    cfg = convert_metadata(md)
    return next(r for r in cfg.roles if r.id == "user").rate_limit


def test_per_role_wins_and_units_convert():
    rl = _user_limits(
        _md_with_limits(
            {
                "rate_limit": {"per_role": {"user": {"max_reqs_per_min": 120}}, "global": {"max_reqs_per_min": 600}},
                "depth_limit": {"per_role": {"user": 5}, "global": 10},
                "node_limit": {"per_role": {"user": 500}},
                "time_limit": {"per_role": {"user": 3}},  # seconds
            }
        )
    )
    assert rl is not None
    assert rl.requests_per_second == 2  # 120/min -> 2/s
    assert rl.max_query_depth == 5
    assert rl.max_query_nodes == 500
    assert rl.max_query_time_ms == 3000  # 3s -> ms


def test_global_fallback_when_no_per_role():
    rl = _user_limits(
        _md_with_limits({"depth_limit": {"global": 8}, "node_limit": {"global": 1000}})
    )
    assert rl.max_query_depth == 8 and rl.max_query_nodes == 1000
    assert rl.requests_per_second is None and rl.max_query_time_ms is None


def test_no_api_limits_leaves_rate_limit_unset():
    rl = _user_limits(_md_with_limits({}))
    assert rl is None


def test_rate_floor_is_one_per_second():
    # 30 reqs/min rounds to 0.5/s → floored to 1 (never silently 0 = "block everything").
    rl = _user_limits(
        _md_with_limits({"rate_limit": {"per_role": {"user": {"max_reqs_per_min": 30}}}})
    )
    assert rl.requests_per_second == 1


def test_parse_api_limits_yaml_from_dir(tmp_path: Path):
    md_dir = tmp_path / "metadata"
    md_dir.mkdir()
    (md_dir / "version.yaml").write_text("version: 3\n")
    (md_dir / "api_limits.yaml").write_text(
        yaml.dump(
            {
                "disabled": False,
                "depth_limit": {"global": 10, "per_role": {"user": 5}},
                "rate_limit": {"per_role": {"user": {"max_reqs_per_min": 60, "unique_params": "IP"}}},
            }
        )
    )
    metadata = parse_metadata_dir(md_dir)
    assert metadata.api_limits["depth_limit"]["per_role"]["user"] == 5
    assert metadata.api_limits["rate_limit"]["per_role"]["user"]["max_reqs_per_min"] == 60
