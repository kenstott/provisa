# Copyright (c) 2026 Kenneth Stott
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Semantic catalog search for the MCP server (REQ-1008, phase 2 — the "explore").

Small-to-big retrieval over the curated catalog. Three chunk tiers — schema,
table, column — are embedded into an in-process DuckDB VSS (HNSW) index. A
``search_catalog`` hit on a narrow leaf (usually a column) resolves *up* through
deterministic drill-down to the parent table branch, so an agent gets the
authoritative table structure, not a bare chunk.

Design contracts (REQ-1008):
  - ``get_chunk(address, catalog)`` is the SOLE chunk-text contract: address in,
    labeled plain prose out (NOT markdown — markdown chrome is token noise to the
    embedder; describe_table renders the pretty view for a different consumer).
  - ``iter_entities(catalog)`` walks the catalog yielding addresses that drive
    get_chunk, keeping the formatter pure.
  - The address (level, schema, table, column) IS the provenance stored on each
    vector row, so a hit resolves straight back with no chunk-id→entity mapping.
  - One formatter serves both the initial build and any rebuild, so indexed and
    re-indexed text cannot drift.

The index is a server-lifetime artifact: DuckDB VSS's HNSW is memory-resident, so
it is (re)built from the catalog at build time — a cold build is just the full
case. Sizing scales with schema cardinality, not row volume.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from provisa.api.flight.catalog import CatalogTable

# The address of one embeddable entity. Equals the {level, schema, table, column}
# provenance stored on its vector row. table/column are None at coarser tiers.
Address = tuple[str, str, str | None, str | None]  # (level, schema, table, column)


def iter_entities(catalog: list[CatalogTable]) -> list[Address]:
    """Every embeddable entity in the catalog, coarse → fine (schema, table, column).

    All columns are embedded (the detail leaves carry the discriminating signal for
    bottom-up search); schemas and tables are embedded too so a broad query can hit
    the right branch directly.
    """
    schemas: dict[str, None] = {}
    out: list[Address] = []
    for t in catalog:
        schemas.setdefault(t.domain_id, None)
    for sid in schemas:
        out.append(("schema", sid, None, None))
    for t in catalog:
        out.append(("table", t.domain_id, t.table_name, None))
        for c in t.columns:
            out.append(("column", t.domain_id, t.table_name, c.name))
    return out


def _by_schema(catalog: list[CatalogTable]) -> dict[str, list[CatalogTable]]:
    grouped: dict[str, list[CatalogTable]] = {}
    for t in catalog:
        grouped.setdefault(t.domain_id, []).append(t)
    return grouped


def _find_table(catalog: list[CatalogTable], schema: str, table: str) -> CatalogTable | None:
    return next((t for t in catalog if t.domain_id == schema and t.table_name == table), None)


def get_chunk(
    address: Address,
    catalog: list[CatalogTable],
    schema_descriptions: dict[str, str] | None = None,
) -> str:
    """Labeled plain-prose embed text for one address. The sole chunk-text contract.

    Depth is selected by the address tier:
      schema  → schema name + description + its table names
      table   → table name + description + its column names
      column  → column name + description + its type + parent table + schema
    """
    level, schema, table, column = address
    if level == "schema":
        tables = _by_schema(catalog).get(schema, [])
        names = ", ".join(t.table_name for t in tables)
        sdesc = (schema_descriptions or {}).get(schema, "")
        sdesc = f" {sdesc}." if sdesc else ""
        return f"schema {schema}.{sdesc} tables: {names}."
    if level == "table":
        t = _find_table(catalog, schema, table or "")
        if t is None:
            return f"table {schema}.{table}."
        cols = ", ".join(c.name for c in t.columns)
        desc = f" {t.description}." if t.description else ""
        return f"table {schema}.{t.table_name}.{desc} columns: {cols}."
    # column
    t = _find_table(catalog, schema, table or "")
    col = next((c for c in (t.columns if t else []) if c.name == column), None)
    if col is None:
        return f"column {schema}.{table}.{column}."
    desc = f" {col.description}." if col.description else ""
    return f"column {column} of table {table} in schema {schema}.{desc} type {col.data_type}."


@dataclass
class SearchHit:
    """One index hit with its address provenance and cosine distance (lower = closer)."""

    level: str
    schema: str
    table: str | None
    column: str | None
    distance: float


class CatalogSearchIndex:
    """In-process DuckDB VSS (HNSW) index over the catalog's chunk tiers.

    Provider-injectable so tests can embed deterministically without an external
    model. ``build`` is idempotent — it rebuilds from scratch (the incremental-reindex
    fast path is a future optimisation; a full rebuild is always correct).
    """

    def __init__(self, model: Any, provider: Any = None, *, embed_batch: int = 128) -> None:
        self._model = model
        self._provider = provider
        self._batch = embed_batch
        self._dim = int(model.dimensions)
        self._con: Any = None
        self._built = False

    async def _provider_obj(self) -> Any:
        if self._provider is not None:
            return self._provider
        from provisa.vector.providers import get_provider

        return get_provider(self._model.provider)

    async def build(
        self, catalog: list[CatalogTable], schema_descriptions: dict[str, str] | None = None
    ) -> int:
        """Embed every chunk and load it into a fresh HNSW index. Returns chunk count."""
        import duckdb

        addresses = iter_entities(catalog)
        texts = [get_chunk(a, catalog, schema_descriptions) for a in addresses]
        provider = await self._provider_obj()

        vectors: list[list[float]] = []
        for i in range(0, len(texts), self._batch):
            vectors.extend(await provider.embed(texts[i : i + self._batch], self._model))
        if len(vectors) != len(addresses):
            raise RuntimeError(f"embedding count {len(vectors)} != chunk count {len(addresses)}")

        con = duckdb.connect()
        con.execute("INSTALL vss; LOAD vss;")
        con.execute(
            f"CREATE TABLE chunks (level VARCHAR, schema VARCHAR, tbl VARCHAR, "
            f"col VARCHAR, embedding FLOAT[{self._dim}])"
        )
        con.executemany(
            "INSERT INTO chunks VALUES (?, ?, ?, ?, ?)",
            [
                [level, schema, table, column, vec]
                for (level, schema, table, column), vec in zip(addresses, vectors, strict=True)
            ],
        )
        # Cosine HNSW: query with array_cosine_distance; smaller = more similar.
        con.execute(
            "CREATE INDEX chunk_hnsw ON chunks USING HNSW (embedding) WITH (metric = 'cosine')"
        )
        self._con = con
        self._built = True
        return len(addresses)

    @property
    def built(self) -> bool:
        return self._built

    async def search(self, nl_text: str, k: int) -> list[SearchHit]:
        """Embed the query and return the k nearest chunks by cosine distance."""
        if not self._built or self._con is None:
            raise RuntimeError("catalog index is not built")
        from provisa.vector.query import vectorize_text

        provider = await self._provider_obj()
        qvec = await vectorize_text(nl_text, self._model, provider)
        if len(qvec) != self._dim:
            raise ValueError(f"query vector dim {len(qvec)} != model dim {self._dim}")
        rows = self._con.execute(
            "SELECT level, schema, tbl, col, "
            f"array_cosine_distance(embedding, ?::FLOAT[{self._dim}]) AS dist "
            "FROM chunks ORDER BY dist LIMIT ?",
            [qvec, k],
        ).fetchall()
        return [
            SearchHit(level=r[0], schema=r[1], table=r[2], column=r[3], distance=r[4]) for r in rows
        ]
