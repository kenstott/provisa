# Copyright (c) 2026 Kenneth Stott
# Canary: b094f0a1-5894-4e5f-87ea-9f1eb7501b7d
# Canary: placeholder
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Semantic schema entity matching for NL query generation (REQ-356 extension).

Extracts type and field names from a GraphQL SDL, embeds them via the configured
vector model, and returns the top-K most relevant entities for a natural language
query.  Exact names are surfaced in the LLM prompt so case/alias mismatches cannot
cause schema-not-found errors at execution time.

If no vector model is configured the matcher degrades gracefully: top_k() returns
all entities in schema order so the prompt still contains exact names even without
ranking.
"""

from __future__ import annotations

import logging
import math
import re
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Callable

if TYPE_CHECKING:
    from provisa.api.app import AppState

log = logging.getLogger(__name__)

# Requirements: REQ-356, REQ-419, REQ-420

# ---------------------------------------------------------------------------
# Entity model
# ---------------------------------------------------------------------------


@dataclass
class SchemaEntity:  # REQ-356
    """A single schema entity extracted from a GraphQL SDL."""

    exact_name: str
    kind: str  # "table" | "field" | "metric"
    parent: str | None  # table name for fields; None for tables and metrics
    embed_text: str  # text sent to the embedding model
    description: str | None = None  # REQ-1319: metric description, surfaced in the prompt


def metric_entities(metrics: list) -> list[SchemaEntity]:  # REQ-1319
    """Build matcher vocabulary entries for registered metrics.

    Name + description + ai_context all participate in the embedding text so a
    metric-shaped question ranks the metric alongside tables/columns.
    """
    out: list[SchemaEntity] = []
    for m in metrics:
        parts = [f"metric {m.name}"]
        if m.description:
            parts.append(m.description)
        if m.ai_context:
            parts.append(m.ai_context)
        out.append(
            SchemaEntity(
                exact_name=m.name,
                kind="metric",
                parent=None,
                embed_text=" — ".join(parts),
                description=m.description,
            )
        )
    return out


# ---------------------------------------------------------------------------
# Extraction
# ---------------------------------------------------------------------------


def extract_entities(schema_sdl: str) -> list[SchemaEntity]:  # REQ-356
    """Parse a GraphQL SDL string and return a flat list of schema entities."""
    try:
        from graphql import build_schema, GraphQLObjectType
    except ImportError:
        return _extract_via_regex(schema_sdl)

    try:
        schema = build_schema(schema_sdl)
    except Exception:
        return _extract_via_regex(schema_sdl)

    entities: list[SchemaEntity] = []
    for type_name, gql_type in schema.type_map.items():
        if type_name.startswith("__"):
            continue
        if not isinstance(gql_type, GraphQLObjectType):
            continue
        # Table-level entity
        entities.append(
            SchemaEntity(
                exact_name=type_name,
                kind="table",
                parent=None,
                embed_text=f"table {type_name}",
            )
        )
        # Field-level entities
        for field_name in gql_type.fields:
            entities.append(
                SchemaEntity(
                    exact_name=field_name,
                    kind="field",
                    parent=type_name,
                    embed_text=f"field {field_name} on table {type_name}",
                )
            )

    return entities


def _extract_via_regex(sdl: str) -> list[SchemaEntity]:
    """Fallback parser for when graphql-core is unavailable."""
    import re

    entities: list[SchemaEntity] = []
    current_type: str | None = None
    for line in sdl.splitlines():
        m = re.match(r"^type\s+(\w+)", line)
        if m:
            name: str = m.group(1)  # group(1) is str: pattern always matches \w+
            if not name.startswith("__"):
                current_type = name
                entities.append(
                    SchemaEntity(
                        exact_name=name,
                        kind="table",
                        parent=None,
                        embed_text=f"table {name}",
                    )
                )
            continue
        if current_type:
            fm = re.match(r"^\s+(\w+)\s*:", line)
            if fm:
                fname: str = fm.group(1)  # group(1) is str: pattern always matches \w+
                entities.append(
                    SchemaEntity(
                        exact_name=fname,
                        kind="field",
                        parent=current_type,
                        embed_text=f"field {fname} on table {current_type}",
                    )
                )
    return entities


# ---------------------------------------------------------------------------
# Cosine similarity (pure Python — no numpy dependency required)
# ---------------------------------------------------------------------------


def _dot(a: list[float], b: list[float]) -> float:
    return sum(x * y for x, y in zip(a, b))


def _norm(v: list[float]) -> float:
    return math.sqrt(sum(x * x for x in v))


def _cosine(a: list[float], b: list[float]) -> float:
    na, nb = _norm(a), _norm(b)
    if na == 0 or nb == 0:
        return 0.0
    return _dot(a, b) / (na * nb)


# ---------------------------------------------------------------------------
# SchemaMatcher
# ---------------------------------------------------------------------------

EmbedFn = Callable[[list[str]], "list[list[float]]"]


@dataclass
class SchemaMatcher:  # REQ-356, REQ-419
    """In-process semantic index of schema entities for one role's SDL."""

    entities: list[SchemaEntity]
    embeddings: list[list[float]]  # parallel to entities; empty = no-vector fallback
    # REQ-1320: table name → modeling role ("fact" | "dimension") for star grounding.
    table_roles: dict[str, str] = field(default_factory=dict)

    def top_k(self, query_embedding: list[float] | None, k: int = 20) -> list[SchemaEntity]:
        """Return the k most relevant entities.

        If query_embedding is None (no vector model configured) returns the first
        k entities in schema order so the prompt still gets exact names.
        """
        if not query_embedding or not self.embeddings:
            return self.entities[:k]
        scored = [
            (entity, _cosine(query_embedding, emb))
            for entity, emb in zip(self.entities, self.embeddings)
        ]
        scored.sort(key=lambda x: x[1], reverse=True)
        return [e for e, _ in scored[:k]]

    def resolve_metric(self, nl_query: str) -> str | None:  # REQ-1319
        """Ground a metric-shaped question to the semantic SQL form.

        When every name token of a registered metric appears in the question, the
        resolution is the semantic addressing form (metric + matched dimension
        columns) — ``SELECT <dims>, value FROM metrics.<name> GROUP BY <dims>`` —
        instead of a raw aggregation the model would otherwise have to invent.
        Dimension columns are matched fields whose parent table has modeling_role
        "dimension" (REQ-1320 star grounding). Returns None when no metric matches.
        """
        from provisa.compiler.metric_expand import metric_semantic_sql

        tokens = set(re.findall(r"[a-z0-9]+", nl_query.lower()))
        metric = next(
            (
                e
                for e in self.entities
                if e.kind == "metric" and set(e.exact_name.lower().split("_")) <= tokens
            ),
            None,
        )
        if metric is None:
            return None
        dims: list[str] = []
        for e in self.entities:
            if e.kind != "field" or e.parent is None:
                continue
            if self.table_roles.get(e.parent) != "dimension":
                continue
            name_tokens = set(re.findall(r"[a-z0-9]+", e.exact_name.lower()))
            if name_tokens <= tokens and e.exact_name not in dims:
                dims.append(e.exact_name)
        return metric_semantic_sql(metric.exact_name, dims)

    @classmethod
    async def build(
        cls,
        schema_sdl: str,
        embed_fn: EmbedFn | None,
        metrics: list | None = None,
        table_roles: dict[str, str] | None = None,
    ) -> "SchemaMatcher":
        """Extract entities from SDL (plus registered metrics) and optionally embed them."""
        entities = extract_entities(schema_sdl) + metric_entities(metrics or [])  # REQ-1319
        roles = table_roles or {}
        if not entities:
            return cls(entities=[], embeddings=[], table_roles=roles)

        if embed_fn is None:
            return cls(entities=entities, embeddings=[], table_roles=roles)

        texts = [e.embed_text for e in entities]
        try:
            embeddings = embed_fn(texts)
        except Exception as exc:
            log.warning("Schema entity embedding failed: %s — falling back to unranked", exc)
            embeddings = []

        return cls(entities=entities, embeddings=embeddings, table_roles=roles)


# ---------------------------------------------------------------------------
# Per-role cache
# ---------------------------------------------------------------------------


@dataclass
class _CacheEntry:
    sdl_hash: int
    matcher: SchemaMatcher


_CACHE: dict[str, _CacheEntry] = {}


async def get_matcher(
    role: str,
    schema_sdl: str,
    embed_fn: EmbedFn | None,
    metrics: list | None = None,
    table_roles: dict[str, str] | None = None,
) -> SchemaMatcher:  # REQ-356
    """Return a cached SchemaMatcher, rebuilding when the SDL, metrics, or roles change."""
    # REQ-1319/REQ-1320: metric definitions and table modeling roles are part of the
    # vocabulary, so they participate in the cache key alongside the SDL.
    h = hash(
        (
            schema_sdl,
            tuple((m.name, m.description, m.ai_context) for m in metrics or []),
            tuple(sorted((table_roles or {}).items())),
        )
    )
    entry = _CACHE.get(role)
    if entry is not None and entry.sdl_hash == h:
        return entry.matcher
    matcher = await SchemaMatcher.build(schema_sdl, embed_fn, metrics, table_roles)
    _CACHE[role] = _CacheEntry(sdl_hash=h, matcher=matcher)
    return matcher


# ---------------------------------------------------------------------------
# Convenience: build embed_fn from app_state
# ---------------------------------------------------------------------------


def make_embed_fn(app_state: AppState) -> EmbedFn | None:  # REQ-419, REQ-420
    """Return a synchronous embed function backed by the first enabled vector model.

    Returns None if no vector model is configured so callers can skip embedding.
    """
    config = getattr(app_state, "config", None)
    if config is None:
        return None
    vector_models = getattr(config, "vector_models", [])
    if not vector_models:
        return None
    first = vector_models[0]
    from provisa.vector.registry import VectorModel
    from provisa.vector.providers import get_provider

    vm = VectorModel(
        id=first.id,
        provider=first.provider,
        dimensions=first.dimensions,
        api_key_env=getattr(first, "api_key_env", None),
        base_url=getattr(first, "base_url", None),
        enabled=getattr(first, "enabled", True),
    )
    if not vm.enabled:
        return None
    provider = get_provider(vm.provider)

    import asyncio

    def _embed(texts: list[str]) -> list[list[float]]:
        return asyncio.get_event_loop().run_until_complete(provider.embed(texts, vm))

    return _embed
