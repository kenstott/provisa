# Copyright (c) 2026 Kenneth Stott
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
from dataclasses import dataclass
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
    kind: str  # "table" | "field"
    parent: str | None  # table name for fields; None for tables
    embed_text: str  # text sent to the embedding model


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

    @classmethod
    async def build(
        cls,
        schema_sdl: str,
        embed_fn: EmbedFn | None,
    ) -> "SchemaMatcher":
        """Extract entities from SDL and optionally embed them."""
        entities = extract_entities(schema_sdl)
        if not entities:
            return cls(entities=[], embeddings=[])

        if embed_fn is None:
            return cls(entities=entities, embeddings=[])

        texts = [e.embed_text for e in entities]
        try:
            embeddings = embed_fn(texts)
        except Exception as exc:
            log.warning("Schema entity embedding failed: %s — falling back to unranked", exc)
            embeddings = []

        return cls(entities=entities, embeddings=embeddings)


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
) -> SchemaMatcher:  # REQ-356
    """Return a cached SchemaMatcher, rebuilding only when the SDL changes."""
    h = hash(schema_sdl)
    entry = _CACHE.get(role)
    if entry is not None and entry.sdl_hash == h:
        return entry.matcher
    matcher = await SchemaMatcher.build(schema_sdl, embed_fn)
    _CACHE[role] = _CacheEntry(sdl_hash=h, matcher=matcher)
    return matcher


# ---------------------------------------------------------------------------
# Convenience: build embed_fn from app_state
# ---------------------------------------------------------------------------


def make_embed_fn(app_state: AppState) -> EmbedFn | None:  # REQ-419, REQ-420
    """Return a synchronous embed function backed by the first enabled vector model.

    Returns None if no vector model is configured so callers can skip embedding.
    """
    try:
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
    except Exception as exc:
        log.debug("Could not build embed_fn: %s", exc)
        return None
