# Copyright (c) 2026 Kenneth Stott
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Architecture-principle invariants (REQ-830, REQ-889, REQ-894, REQ-896).

These are STRUCTURAL/CONSTRAINT requirements — architectural invariants that must hold
end-to-end. Each test LOCKS the observable consequence of the invariant so an engine swap,
a tier promotion, or a store change cannot silently break the design.
"""

from __future__ import annotations

import inspect
from pathlib import Path

import pytest

from provisa.compiler.stage2 import GovernanceContext, apply_governance
from provisa.core.desktop_profile import (
    _COMPUTE_ONLY_TIER_ADDITIONS,
    load_profile,
)
from provisa.core.models import ControlPlaneConfig, ProvisaConfig
from provisa.federation.engine import (
    DriverClass,
    build_duckdb_engine,
    build_pg_engine,
    build_snowflake_engine,
    build_trino_engine,
    configured_materialize_url,
)
from provisa.security.masking import MaskingRule, MaskType
from provisa.transpiler.transpile import transpile

_CAPS = Path(__file__).resolve().parents[2] / "config" / "capabilities.yaml"


# --------------------------------------------------------------------------- #
# REQ-830: five pluggable stateful components, each config/URI-resolved        #
# --------------------------------------------------------------------------- #
class TestReq830StatefulTopology:
    """A deployment holds FIVE independently-swappable stateful components (orthogonal to the
    stateless per-query flow, REQ-825). Each MUST resolve via its own config/URI abstraction — never
    a hardcoded backend. This enumerates the five and asserts each is pluggable behind config."""

    def test_five_components_each_resolve_via_config_or_uri(self):
        cfg = ProvisaConfig(sources=[], domains=[], tables=[], roles=[])
        cp: ControlPlaneConfig = cfg.control_plane

        # (1) PLATFORM CONTROL PLANE REGISTRY — SQLAlchemy URI (env PLATFORM_DATABASE_URL, REQ-837).
        assert "${env:PLATFORM_DATABASE_URL}" in cp.platform_url
        assert hasattr(cp, "resolved_platform_url")

        # (2) TENANT CONTROL PLANE REGISTRY — SQLAlchemy URI (env TENANT_DATABASE_URL, REQ-828).
        assert "${env:TENANT_DATABASE_URL" in cp.tenant_url
        assert hasattr(cp, "resolved_tenant_url")

        # (3) TENANT MATERIALIZATION STORE — a DSN config field + resolver (REQ-826/989).
        assert "materialize_store_url" in ProvisaConfig.model_fields
        assert callable(configured_materialize_url)

        # (4) TENANT HOT CACHE — a Redis URI config field (REQ-829/544).
        assert "hot_tables" in ProvisaConfig.model_fields

        # (5) FEDERATION ENGINE — a registry-key config field selecting the builder (REQ-840/916).
        assert "federation_engine" in ProvisaConfig.model_fields
        assert "federation_engine_url" in ProvisaConfig.model_fields

    def test_each_component_is_independently_swappable(self):
        # The five are ORTHOGONAL config knobs — changing one does not touch another. Pinning distinct
        # values on each proves no component is hardwired to another's backend.
        cfg = ProvisaConfig(
            sources=[],
            domains=[],
            tables=[],
            roles=[],
            federation_engine="duckdb",
            federation_engine_url="clickhouse://h/db",
            materialize_store_url="postgresql://h/mat",
        )
        assert cfg.federation_engine == "duckdb"
        assert cfg.federation_engine_url == "clickhouse://h/db"
        assert cfg.materialize_store_url == "postgresql://h/mat"
        # Control-plane URIs are a separate object entirely — not derived from the engine.
        assert cfg.control_plane.platform_url != cfg.federation_engine_url

    def test_desktop_preset_collapses_all_to_embedded(self):
        # On a developer desktop the components collapse to a zero-infra embedded stack (REQ-830):
        # duckdb engine, embedded materialize store, sqlite control plane, in-memory cache.
        prof = load_profile("demo", ephemeral=True, capabilities_path=_CAPS)
        assert prof.env["PROVISA_ENGINE"] == "duckdb"
        assert prof.env["PROVISA_REDIS_EMBEDDED"] == "1"  # hot cache -> fakeredis
        assert prof.env["PROVISA_MATERIALIZE_URL"].startswith(("duckdb", "sqlite"))
        assert prof.env["PLATFORM_DATABASE_URL"].startswith("sqlite")
        assert prof.env["TENANT_DATABASE_URL"].startswith("sqlite")


# --------------------------------------------------------------------------- #
# REQ-889: metadata store home is tier-invariant; compute never metadata home  #
# --------------------------------------------------------------------------- #
class TestReq889MetadataHomeTierInvariant:
    """The metadata/config/roles store is the EMBEDDED single source of truth in every tier. Promoting
    to the container tier adds Trino + observability strictly as COMPUTE — it MUST NOT relocate the
    metadata home. Tier changes are additive + reversible."""

    def _base(self) -> dict:
        return dict(ephemeral=True, capabilities_path=_CAPS)

    def test_promoting_to_container_tier_does_not_move_metadata_home(self):
        base = load_profile("native", **self._base())
        # Container-tier promotion: add Trino compute + redirect observability to an external collector.
        promoted = load_profile(
            "native",
            engine="trino",
            trino_endpoint=("trino-host", 8080),
            otlp_endpoint="http://collector:4317",
            **self._base(),
        )
        # The metadata home (control-plane store identity + URIs) is IDENTICAL across tiers.
        assert promoted.control_plane_store == base.control_plane_store
        assert promoted.env["PLATFORM_DATABASE_URL"] == base.env["PLATFORM_DATABASE_URL"]
        assert promoted.env["TENANT_DATABASE_URL"] == base.env["TENANT_DATABASE_URL"]
        # Compute WAS added (proving this is a real tier change), yet metadata did not move.
        assert promoted.env["PROVISA_ENGINE"] == "trino"
        assert promoted.env["OTEL_EXPORTER_OTLP_ENDPOINT"] == "http://collector:4317"

    def test_toggling_trino_is_reversible_without_catalog_migration(self):
        with_trino = load_profile("native", engine="trino", **self._base())
        without = load_profile("native", engine="duckdb", **self._base())
        # Toggling the compute engine on/off never touches the metadata catalog location.
        assert with_trino.env["PLATFORM_DATABASE_URL"] == without.env["PLATFORM_DATABASE_URL"]
        assert with_trino.env["TENANT_DATABASE_URL"] == without.env["TENANT_DATABASE_URL"]

    def test_metadata_home_is_never_a_compute_only_backend(self):
        # Enforcement (fail loud): a preset that names a compute-only tier addition as its metadata
        # home is rejected — Trino/observability can never become the metadata store.
        for compute in _COMPUTE_ONLY_TIER_ADDITIONS:
            caps = _CAPS.read_text()
            import tempfile
            import yaml

            spec = yaml.safe_load(caps)
            spec["presets"]["native"]["control_plane_store"] = compute
            with tempfile.NamedTemporaryFile("w", suffix=".yaml", delete=False) as fh:
                yaml.safe_dump(spec, fh)
                bad = Path(fh.name)
            with pytest.raises(ValueError, match="REQ-889"):
                load_profile("native", ephemeral=True, capabilities_path=bad)


# --------------------------------------------------------------------------- #
# REQ-894: DuckDB is a desktop/edge/dev federator, not a production MPP backend #
# --------------------------------------------------------------------------- #
class TestReq894DuckdbEdgeTier:
    """DuckDB is the DESKTOP/EDGE/DEV embedded federator — single-process, single-writer. Its DECLARED
    trait profile (REQ-897) must NOT claim mpp/production-multiuser. Trino is the only MPP federator;
    DuckDB and Postgres are the single-node federators."""

    def test_duckdb_traits_are_edge_dev_not_mpp(self):
        t = build_duckdb_engine().traits
        assert t.mpp is False  # single-node embedded — NOT an MPP production backend
        assert t.reach is DriverClass.PARTIAL  # reaches a subset, not a broad MPP federator
        assert t.pooled is False  # single-writer embedded connection, no server-side pool
        assert t.file_native is True  # scans files in place (edge/dev zero-infra)
        assert t.transactional is True  # ACID single-writer

    def test_postgres_is_single_node_too(self):
        # The single-node federators (DuckDB + Postgres) are non-MPP; only Trino is MPP (REQ-894 note).
        assert build_pg_engine().traits.mpp is False
        assert build_trino_engine().traits.mpp is True

    def test_only_broad_mpp_engine_declares_broad_reach(self):
        # DuckDB never claims BROAD reach (which implies MPP); Trino does.
        assert build_duckdb_engine().driver_class() is DriverClass.PARTIAL
        assert build_trino_engine().driver_class() is DriverClass.BROAD


# --------------------------------------------------------------------------- #
# REQ-896: governance is engine-independent — same governed IR across engines   #
# --------------------------------------------------------------------------- #
class TestReq896EngineIndependentGovernance:
    """Provisa's defensible value is the GOVERNED-FEDERATION MODEL, not any compute engine. Governance
    (RLS/masking/visibility) is applied in the semantic→IR layer BEFORE any engine sees the query, so
    swapping the federation engine MUST NOT change the governance decision."""

    def _gov(self) -> GovernanceContext:
        return GovernanceContext(
            rls_rules={7: "region = 'US'"},
            masking_rules={
                (7, "email"): (
                    MaskingRule(mask_type=MaskType.regex, pattern=".", replace="*"),
                    "varchar",
                )
            },
            visible_columns={7: frozenset({"email", "region"})},  # ssn hidden
            table_map={"users": 7, "public.users": 7},
            all_columns={7: [("email", "varchar"), ("region", "varchar"), ("ssn", "varchar")]},
        )

    def test_governance_takes_no_engine(self):
        # Structural proof: the governance transformer has NO engine parameter — it cannot depend on
        # the engine. Governance sits strictly above the engine seam (REQ-896 design consequence).
        params = inspect.signature(apply_governance).parameters
        assert "engine" not in params
        assert set(params) == {"sql", "gov_ctx"}

    def test_same_governed_ir_regardless_of_engine(self):
        # The governed IR (semantic layer output) is produced before engine lowering and is identical
        # no matter which engine will later execute it.
        sql = "SELECT * FROM public.users"
        governed = {
            e().name: apply_governance(sql, self._gov())
            for e in (build_duckdb_engine, build_trino_engine, build_pg_engine)
        }
        assert len(set(governed.values())) == 1  # one governed IR across all engines

    def test_governance_decision_survives_lowering_to_every_engine_dialect(self):
        # Swapping the engine changes only the physical dialect of the LOWERED SQL — the governance
        # decision (RLS predicate applied, email masked, ssn hidden) holds across every engine.
        governed = apply_governance("SELECT * FROM public.users", self._gov())
        for build in (
            build_duckdb_engine,
            build_trino_engine,
            build_pg_engine,
            build_snowflake_engine,
        ):
            engine = build()
            lowered = transpile(governed, engine.dialect)
            up = lowered.upper()
            assert "REGION" in up and "'US'" in up, engine.name  # RLS predicate present
            assert "REGEXP_REPLACE" in up, engine.name  # email mask present
            assert "SSN" not in up, engine.name  # hidden column never emitted
