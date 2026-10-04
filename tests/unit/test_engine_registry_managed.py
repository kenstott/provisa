# Copyright (c) 2026 Kenneth Stott
# Canary: 3b7e1f92-6c4d-4a85-9e20-d1f5a8c3b764
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The engine picker names the hosted services that are the PostgreSQL engine, so someone on
Neon, RDS and the like finds it; the registry carries them to the admin UI."""

from __future__ import annotations

from provisa.federation.engine import engine_registry


def test_the_pg_engine_names_the_managed_postgresql_services():
    pg = next(e for e in engine_registry() if e["key"] == "pg")
    assert pg["label"] == "PostgreSQL"
    assert pg["managed"] == "RDS, Aurora, Cloud SQL, AlloyDB, Azure, Supabase, Neon"


def test_an_engine_without_managed_services_carries_none():
    duckdb = next(e for e in engine_registry() if e["key"] == "duckdb")
    assert "managed" not in duckdb
