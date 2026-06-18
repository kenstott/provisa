# Copyright (c) 2026 Kenneth Stott
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.

"""Embedding providers (REQ-420).

Pluggable backends that turn text into vectors for a registered VectorModel:
  - openai      — any OpenAI-compatible /embeddings endpoint (OpenAI, vLLM, TEI, …)
  - ollama      — a local/remote Ollama server (/api/embeddings)
  - huggingface — a local sentence-transformers model (no network)
"""

from __future__ import annotations

import os
from abc import ABC, abstractmethod

from provisa.vector.registry import VectorModel


class EmbeddingError(RuntimeError):
    """An embedding request failed or returned an unexpected shape."""


class EmbeddingProvider(ABC):
    """Turn a batch of texts into embedding vectors for a model."""

    @abstractmethod
    async def embed(self, texts: list[str], model: VectorModel) -> list[list[float]]: ...


def _api_key(model: VectorModel) -> str | None:
    return os.environ.get(model.api_key_env) if model.api_key_env else None


class OpenAICompatibleProvider(EmbeddingProvider):
    """Any OpenAI-compatible POST {base_url}/embeddings endpoint."""

    async def embed(self, texts: list[str], model: VectorModel) -> list[list[float]]:
        import httpx

        base = (model.base_url or "https://api.openai.com/v1").rstrip("/")
        headers = {}
        key = _api_key(model)
        if key:
            headers["Authorization"] = f"Bearer {key}"
        async with httpx.AsyncClient(timeout=30) as client:
            resp = await client.post(
                f"{base}/embeddings", headers=headers, json={"model": model.id, "input": texts}
            )
            resp.raise_for_status()
            data = resp.json()
        try:
            return [item["embedding"] for item in data["data"]]
        except (KeyError, TypeError) as exc:
            raise EmbeddingError(f"Unexpected embeddings response: {exc}") from exc


class OllamaProvider(EmbeddingProvider):
    """Ollama embeddings — one POST {base_url}/api/embeddings per text."""

    async def embed(self, texts: list[str], model: VectorModel) -> list[list[float]]:
        import httpx

        base = (model.base_url or "http://localhost:11434").rstrip("/")
        out: list[list[float]] = []
        async with httpx.AsyncClient(timeout=60) as client:
            for text in texts:
                resp = await client.post(
                    f"{base}/api/embeddings", json={"model": model.id, "prompt": text}
                )
                resp.raise_for_status()
                emb = resp.json().get("embedding")
                if not isinstance(emb, list):
                    raise EmbeddingError("Ollama response missing 'embedding'")
                out.append(emb)
        return out


class HuggingFaceLocalProvider(EmbeddingProvider):
    """Local sentence-transformers model — no network."""

    _cache: dict = {}

    async def embed(self, texts: list[str], model: VectorModel) -> list[list[float]]:
        try:
            from sentence_transformers import SentenceTransformer
        except ImportError as exc:
            raise EmbeddingError(
                "huggingface provider requires sentence-transformers: pip install provisa[vector]"
            ) from exc
        st = self._cache.get(model.id)
        if st is None:
            st = SentenceTransformer(model.id)
            self._cache[model.id] = st
        return [list(map(float, v)) for v in st.encode(texts)]


_PROVIDERS: dict[str, type[EmbeddingProvider]] = {
    "openai": OpenAICompatibleProvider,
    "ollama": OllamaProvider,
    "huggingface": HuggingFaceLocalProvider,
}


def get_provider(provider: str) -> EmbeddingProvider:
    """Return an embedding provider instance for the given name."""
    cls = _PROVIDERS.get(provider)
    if cls is None:
        raise EmbeddingError(
            f"Unknown embedding provider {provider!r}; known: {sorted(_PROVIDERS)}"
        )
    return cls()
