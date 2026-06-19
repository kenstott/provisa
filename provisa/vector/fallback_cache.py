# Copyright (c) 2026 Kenneth Stott
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.

"""pgvector fallback cache (REQ-424).

For sources with no native vector capability, embeddings are materialized into a local
pgvector cache table (PK + embedding), indexed with HNSW. A similarity query runs the
nearest-neighbour search against the cache, then joins the matched PKs back to the
source so the full row is returned.
"""

from __future__ import annotations

from provisa.vector.query import _vector_literal


def cache_ddl(cache_table: str, dimensions: int, pk_type: str = "text") -> list[str]:
    """DDL to create the pgvector cache table and its HNSW cosine index."""
    index = f"{cache_table.replace('.', '_').strip('"')}_hnsw"
    return [
        f"CREATE TABLE IF NOT EXISTS {cache_table} "
        f"(pk {pk_type} PRIMARY KEY, embedding vector({dimensions}))",
        f"CREATE INDEX IF NOT EXISTS {index} ON {cache_table} "
        f"USING hnsw (embedding vector_cosine_ops)",
    ]


async def materialize(
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


def fallback_similarity_sql(
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
    lit = _vector_literal(query_vector)
    return (
        f"SELECT s.* FROM {source_table} AS s "
        f"JOIN (SELECT pk, (1 - (embedding <=> '{lit}'::vector)) AS _score "
        f"FROM {cache_table} ORDER BY embedding <=> '{lit}'::vector LIMIT {int(limit)}) AS c "
        f"ON s.{source_pk} = c.pk "
        f"ORDER BY c._score DESC"
    )
