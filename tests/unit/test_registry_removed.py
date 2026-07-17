# Copyright (c) 2026 Kenneth Stott
# Canary: 1de67863-d015-4e66-97c0-a53303b28d3a
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.

"""Regression guards for the approved-query/GPQ registry removal (REQ-001/003).

These pin the Phase-3 removal so the deprecated registry cannot creep back: no schema
table, no governance code reading it, and the GPQ-by-id execution paths gone. Apollo
APQ (REQ-288-291, Redis) is a separate feature and intentionally untouched.
"""

from __future__ import annotations

import pathlib

ROOT = pathlib.Path(__file__).resolve().parents[2]
PROVISA = ROOT / "provisa"


def _provisa_py_files():
    return [p for p in PROVISA.rglob("*.py") if "/tests/" not in str(p)]


class TestRegistrySchemaRemoved:
    def test_schema_does_not_create_persisted_queries(self):
        schema = (PROVISA / "core" / "schema.sql").read_text()
        assert "CREATE TABLE IF NOT EXISTS persisted_queries" not in schema
        assert "CREATE TABLE IF NOT EXISTS approval_log" not in schema

    def test_apq_is_untouched(self):
        # Apollo APQ (Redis) is a separate, retained feature.
        assert (PROVISA / "apq" / "cache.py").exists()


class TestNoGovernanceReadsRegistry:
    def test_no_code_reads_persisted_queries(self):
        offenders = [
            str(p.relative_to(ROOT))
            for p in _provisa_py_files()
            if "persisted_queries" in p.read_text()
        ]
        assert offenders == [], f"registry still read by: {offenders}"


class TestGpqExecutionPathsGone:
    def test_cypher_query_id_returns_410(self):
        src = (PROVISA / "api" / "rest" / "cypher_router.py").read_text()
        assert "_route_approved_query" not in src
        assert "status_code=410" in src

    def test_sse_query_id_routes_to_live_engine(self):
        src = (PROVISA / "api" / "data" / "subscribe.py").read_text()
        # The GPQ-by-stable-id subscription path is gone (no persisted_queries read).
        # query_id now routes to the live engine, not the removed GPQ registry.
        assert "persisted_queries" not in src
        assert "live_engine" in src

    def test_schema_builder_has_no_approved_query_fields(self):
        src = (PROVISA / "compiler" / "schema_gen.py").read_text()
        assert "approved_queries" not in src
