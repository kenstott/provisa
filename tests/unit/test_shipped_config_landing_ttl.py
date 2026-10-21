# Copyright (c) 2026 Kenneth Stott
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Every shipped config holds under the landing-TTL rule on every engine it is run on (REQ-1907).

A table lands when its config says so or when the engine cannot read its source in place, which
the read path judges (query_residency.ensure_resident); a landed ttl / ttl_probe / probe table needs
a cache_ttl on itself or its source (role_ttl.require_landing_ttl). Config load checks only the
first half, so a config that loads can still refuse its tables at the first read: cloud-dev's node,
pinned to Trino, refused the install config's SQLite tables, which DuckDB reads in place. Each
shipped config is judged here on the engine it names and, for the configs the image ships, on every
engine packaging/linux/first-launch.sh pins."""

# Requirements: REQ-1907

from __future__ import annotations

import re
from pathlib import Path

import pytest

from provisa.core.config_loader import parse_config_dict, read_config_with_includes
from provisa.federation.engine import build_engine
from provisa.federation.replica_converge import whole_copy
from provisa.federation.replica_routing import owns_replica
from provisa.federation.role_ttl import lands_from_config, missing_landing_ttl
from provisa.federation.strategy import engine_attaches

pytestmark = pytest.mark.unit

_REPO = Path(__file__).resolve().parents[2]
# What the image bakes (Dockerfile's COPY of config/): the bootstrap org's demo config.
_IMAGE = ["config/provisa-install.yaml", "config/provisa-install-base.yaml"]
_DEMOS = sorted(str(p.relative_to(_REPO)) for p in (_REPO / "demo" / "named").glob("*/config.yaml"))
_PINNED = sorted(
    set(
        re.findall(
            r'PROVISA_ENGINE: "([\w-]+)"',
            (_REPO / "packaging" / "linux" / "first-launch.sh").read_text(),
        )
    )
)
# A placeholder with no default is the deployment's or the demo's to set; a value of its shape
# parses (a region's store is a PostgreSQL URL). A ${secret:...} belongs to an org, which no
# parse here is bound to; the rule judged reads none.
_REQUIRED_ENV = re.compile(r"\$\{env:(\w+)\}")
_SECRET = re.compile(r"\$\{secret:\w+\}")


def _stub(name: str) -> str:
    if name.endswith("_PG_URL"):
        return "postgresql://u:p@h:5432/d"
    if name.endswith("_URL"):
        return "redis://h:6379"
    return "1"


def _unsecret(value):
    if isinstance(value, dict):
        return {k: _unsecret(v) for k, v in value.items()}
    if isinstance(value, list):
        return [_unsecret(v) for v in value]
    return _SECRET.sub("x", value) if isinstance(value, str) else value


def _config(path: str, monkeypatch):
    text = (_REPO / path).read_text()
    for name in set(_REQUIRED_ENV.findall(text)):
        monkeypatch.setenv(name, _stub(name))
    return parse_config_dict(_unsecret(read_config_with_includes(_REPO / path)))


def _refused(config, engine_key: str) -> list[str]:
    """Each table the read path would land on ``engine_key`` (query_residency.ensure_resident's
    ``_lands`` and ``whole_copy``) with no landing TTL."""
    engine = build_engine(engine_key)
    sources = {s.id: s for s in config.sources}
    refused = []
    for table in config.tables:
        source = sources.get(table.source_id)
        if source is None or not owns_replica(source):
            continue
        lands = lands_from_config(
            materialize=table.materialize,
            row_materialize=table.row_materialize,
            table_replicate=table.replicate,
            source_replicate=source.replicate,
            table_load_protected=table.load_protected,
            source_load_protected=source.load_protected,
        ) or not engine_attaches(engine, str(getattr(source.type, "value", source.type)))
        if not lands or not whole_copy(source, table, engine):
            continue
        err = missing_landing_ttl(
            table.change_signal,
            source.change_signal,
            table.cache_ttl,
            source.cache_ttl,
            gated=source.freshness_gate,
        )
        if err is not None:
            refused.append(f"{table.table_name} (source {source.id}): {err}")
    return refused


def _cases() -> list[tuple[str, str | None]]:
    cases: list[tuple[str, str | None]] = [(path, None) for path in _IMAGE + _DEMOS]
    cases += [(path, engine) for path in _IMAGE for engine in _PINNED]
    return cases


def test_the_image_s_configs_are_judged_on_every_engine_a_node_is_pinned_to():
    assert "trino" in _PINNED and "duckdb" in _PINNED, _PINNED


@pytest.mark.parametrize(("path", "engine"), _cases())
def test_no_table_a_shipped_config_lands_lacks_a_landing_ttl(path, engine, monkeypatch):
    config = _config(path, monkeypatch)
    on = engine or config.federation_engine or "duckdb"
    assert _refused(config, on) == [], f"{path} on {on}"
