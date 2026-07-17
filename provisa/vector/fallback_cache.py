# Copyright (c) 2026 Kenneth Stott
# Canary: bd712e1f-1663-476a-825a-7e56772f5cb0
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.

"""pgvector fallback cache (REQ-424).

For sources with no native vector capability, embeddings are materialized into a local
pgvector cache table (PK + embedding), indexed with HNSW. A similarity query runs the
nearest-neighbour search against the cache, then joins the matched PKs back to the
source so the full row is returned.
"""

# Requirements: REQ-424

from __future__ import annotations

import re

from provisa.vector.query import _vector_literal

# Cache/source identifiers are schema-derived, never request-supplied — but these SQL strings are
# built by interpolation (asyncpg has no identifier bind), so guard every interpolated identifier
# against injection: only word chars, the identifier quote, and the schema-qualifier dot are allowed
# (rejects single quotes, semicolons, whitespace, parentheses). A bad value fails loud, never runs.
_IDENT_RE = re.compile(r'^[\w".]+$')


def _safe_ident(name: str) -> str:
    if not _IDENT_RE.match(name):
        raise ValueError(f"unsafe SQL identifier: {name!r}")
    return name


def cache_ddl(cache_table: str, dimensions: int, pk_type: str = "text") -> list[str]:  # REQ-424
    """DDL to create the pgvector cache table and its HNSW cosine index."""
    cache_table = _safe_ident(cache_table)
    pk_type = _safe_ident(pk_type)
    index = f"{cache_table.replace('.', '_').strip('"')}_hnsw"
    return [
        f"CREATE TABLE IF NOT EXISTS {cache_table} "
        f"(pk {pk_type} PRIMARY KEY, embedding vector({dimensions}))",
        f"CREATE INDEX IF NOT EXISTS {index} ON {cache_table} "
        f"USING hnsw (embedding vector_cosine_ops)",
    ]


async def materialize(  # REQ-424
    conn,
    cache_table: str,
    rows: list[dict],
    pk_field: str,
    text_field: str,
    model,
    provider=None,
) -> int:
    """Embed each row's text and upsert (pk, embedding) into the cache. Returns count."""
    if not rows:
        return 0
    cache_table = _safe_ident(cache_table)
    if provider is None:
        from provisa.vector.providers import get_provider

        provider = get_provider(model.provider)
    texts = [str(r[text_field]) for r in rows]
    vectors = await provider.embed(texts, model)
    for row, vec in zip(rows, vectors):
        await conn.execute(
            f"INSERT INTO {cache_table} (pk, embedding) VALUES ($1, $2) "
            f"ON CONFLICT (pk) DO UPDATE SET embedding = EXCLUDED.embedding",
            str(row[pk_field]),
            _vector_literal(vec),
        )
    return len(rows)


def fallback_similarity_sql(  # REQ-424
    source_table: str,
    source_pk: str,
    cache_table: str,
    query_vector: list[float],
    limit: int = 10,
) -> str:
    """Rewrite a similarity query to use the cache, joining matched PKs back to source.

    Runs an HNSW nearest-neighbour scan on the cache (ordered by cosine distance),
    takes the top ``limit``, and joins the PKs back to the source table — so the
    caller gets full source rows ranked by similarity.
    """
    source_table = _safe_ident(source_table)
    source_pk = _safe_ident(source_pk)
    cache_table = _safe_ident(cache_table)
    lit = _vector_literal(query_vector)
    return (
        f"SELECT s.* FROM {source_table} AS s "
        f"JOIN (SELECT pk, (1 - (embedding <=> '{lit}'::vector)) AS _score "
        f"FROM {cache_table} ORDER BY embedding <=> '{lit}'::vector LIMIT {int(limit)}) AS c "
        f"ON s.{source_pk} = c.pk "
        f"ORDER BY c._score DESC"
    )
