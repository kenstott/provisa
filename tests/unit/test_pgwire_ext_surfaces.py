# Copyright (c) 2026 Kenneth Stott
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""REQ-892 — pluggable pgwire extension surfaces over the federation engine.

Opt-in-per-deployment surfaces (pgvector / postgis / json-ops / compat-fns):
each enabled surface advertises in pg_extension, declares its types/OIDs in the
single normalization module, and maps operators/functions to engine equivalents
OR rejects loudly. No index-acceleration capability it does not implement."""

from __future__ import annotations

import duckdb
import pytest

from provisa.pgwire import catalog_data
from provisa.pgwire.ext_surfaces import (
    _ENV_VAR,
    enabled_surface_keys,
    extension_rows,
    rewrite_surface_operators,
    surface_typeinfo,
)
from provisa.pgwire.catalog_populate import _populate_pg_extension
from provisa.pgwire.system_tables import _populate_empty_system_tables


def _enable(monkeypatch, value: str) -> None:
    monkeypatch.setenv(_ENV_VAR, value)


# --- opt-in-per-deployment ---------------------------------------------------


def test_no_env_means_no_surfaces(monkeypatch) -> None:
    monkeypatch.delenv(_ENV_VAR, raising=False)
    assert enabled_surface_keys() == frozenset()
    assert extension_rows() == []
    assert surface_typeinfo() == {}


def test_unknown_surface_rejected_loudly(monkeypatch) -> None:
    _enable(monkeypatch, "pgvector,bogus")
    with pytest.raises(ValueError, match="unknown surface"):
        enabled_surface_keys()


# --- (a) advertises itself in pg_extension -----------------------------------


def test_enabled_surface_appears_in_pg_extension(monkeypatch) -> None:
    _enable(monkeypatch, "pgvector")
    names = {r[1] for r in extension_rows()}
    assert "vector" in names


def test_disabled_surface_is_absent_from_pg_extension(monkeypatch) -> None:
    _enable(monkeypatch, "json-ops")  # postgis NOT enabled
    names = {r[1] for r in extension_rows()}
    assert "postgis" not in names
    assert "vector" not in names


def test_pg_extension_table_populated_from_surfaces(monkeypatch) -> None:
    _enable(monkeypatch, "pgvector,compat-fns")
    db = duckdb.connect(":memory:")
    _populate_empty_system_tables(db)
    _populate_pg_extension(db)
    names = {row[0] for row in db.execute("SELECT extname FROM _pg_extension").fetchall()}
    db.close()
    assert {"vector", "pg_trgm", "pgcrypto"} <= names


# --- (b) declares its types/OIDs in the single normalization module ----------


def test_surface_type_declared_in_normalization_module(monkeypatch) -> None:
    _enable(monkeypatch, "pgvector")
    ti = catalog_data.typeinfo()
    # built-in types still present …
    assert ti[16][1] == "bool"
    # … and the surface vector type/OID now resolves through the one module.
    vector_oids = [oid for oid, info in ti.items() if info[1] == "vector"]
    assert vector_oids, "vector type not declared in normalization module"
    rows = catalog_data.pg_type_rows()
    assert any(r[1] == "vector" for r in rows)


def test_surface_types_absent_when_disabled(monkeypatch) -> None:
    monkeypatch.delenv(_ENV_VAR, raising=False)
    ti = catalog_data.typeinfo()
    assert not [oid for oid, info in ti.items() if info[1] == "vector"]


# --- (c) pgvector: distance operators + ORDER BY similarity ------------------


def test_pgvector_l2_distance_maps_to_engine_fn(monkeypatch) -> None:
    _enable(monkeypatch, "pgvector")
    out = rewrite_surface_operators("SELECT emb <-> $1 FROM t").upper()
    assert "ARRAY_DISTANCE" in out
    assert "<->" not in out


def test_pgvector_cosine_and_inner_product_map(monkeypatch) -> None:
    _enable(monkeypatch, "pgvector")
    assert "ARRAY_COSINE_DISTANCE" in rewrite_surface_operators("SELECT a <=> b FROM t").upper()
    ip = rewrite_surface_operators("SELECT a <#> b FROM t").upper()
    assert "ARRAY_NEGATIVE_INNER_PRODUCT" in ip


def test_order_by_similarity_search_maps(monkeypatch) -> None:
    _enable(monkeypatch, "pgvector")
    out = rewrite_surface_operators("SELECT id FROM t ORDER BY emb <-> $1 LIMIT 5").upper()
    assert "ORDER BY ARRAY_DISTANCE(EMB, $1)" in out


# --- convert-or-reject-loudly: NO index acceleration -------------------------


@pytest.mark.parametrize("am", ["hnsw", "ivfflat"])
def test_ann_index_rejected_loudly(monkeypatch, am: str) -> None:
    _enable(monkeypatch, "pgvector")
    with pytest.raises(ValueError, match="does not implement"):
        rewrite_surface_operators(f"CREATE INDEX ON t USING {am} (emb vector_l2_ops)")


def test_ann_access_methods_not_advertised() -> None:
    # pg_am must never gain ivfflat/hnsw — index acceleration is not implemented.
    from provisa.pgwire import catalog_populate

    db = duckdb.connect(":memory:")
    catalog_populate._populate_pg_tables_and_am(db, catalog_populate.CatalogIndex())
    ams = {row[0] for row in db.execute("SELECT amname FROM _pg_am").fetchall()}
    db.close()
    assert "hnsw" not in ams and "ivfflat" not in ams


# --- json-ops: -> ->> #> #>> → JSON_QUERY / JSON_VALUE -----------------------


def test_json_operators_map_to_json_value_query(monkeypatch) -> None:
    _enable(monkeypatch, "json-ops")
    out = rewrite_surface_operators(
        "SELECT j -> 'k', j ->> 'k', j #> '{a,b}', j #>> '{a,b}' FROM t"
    ).upper()
    assert out.count("JSON_QUERY") == 2  # ->  and  #>
    assert out.count("JSON_VALUE") == 2  # ->> and  #>>
    assert "'$.K'" in out and "'$.A.B'" in out


# --- compat-fns: similarity(), gen_random_uuid(), digest() -------------------


def test_gen_random_uuid_answered(monkeypatch) -> None:
    _enable(monkeypatch, "compat-fns")
    assert "UUID()" in rewrite_surface_operators("SELECT gen_random_uuid()").upper()


def test_similarity_answered(monkeypatch) -> None:
    _enable(monkeypatch, "compat-fns")
    assert "JACCARD" in rewrite_surface_operators("SELECT similarity(a, b) FROM t").upper()


def test_digest_known_algo_maps(monkeypatch) -> None:
    _enable(monkeypatch, "compat-fns")
    assert "SHA256" in rewrite_surface_operators("SELECT digest(x, 'sha256') FROM t").upper()


def test_digest_unknown_algo_rejected_loudly(monkeypatch) -> None:
    _enable(monkeypatch, "compat-fns")
    with pytest.raises(ValueError, match="not implemented"):
        rewrite_surface_operators("SELECT digest(x, 'whirlpool') FROM t")


# --- postgis-subset (partial) ------------------------------------------------


def test_postgis_geometry_type_and_operators(monkeypatch) -> None:
    _enable(monkeypatch, "postgis")
    ti = catalog_data.typeinfo()
    assert [oid for oid, info in ti.items() if info[1] == "geometry"]
    # && bbox overlap maps to ST_Intersects (partial: bbox ≈ geometry).
    out = rewrite_surface_operators("SELECT a && b FROM t").upper()
    assert "ST_INTERSECTS" in out


# --- passthrough when disabled -----------------------------------------------


def test_operators_passthrough_when_surface_disabled(monkeypatch) -> None:
    monkeypatch.delenv(_ENV_VAR, raising=False)
    sql = "SELECT emb <-> $1 FROM t"
    assert rewrite_surface_operators(sql) == sql
