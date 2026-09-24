# Copyright (c) 2026 Kenneth Stott
# Canary: 0fd72f36-a1ed-4c7d-aa97-34c9a54a66e6
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

from abc import ABC, abstractmethod

from provisa.vector.registry import VectorModel

# Requirements: REQ-420, REQ-431, REQ-1827


class EmbeddingError(RuntimeError):
    """An embedding request failed or returned an unexpected shape."""


class EmbeddingProvider(ABC):  # REQ-420
    """Turn a batch of texts into embedding vectors for a model."""

    @abstractmethod
    async def embed(self, texts: list[str], model: VectorModel) -> list[list[float]]: ...


async def _api_key(model: VectorModel) -> str | None:  # REQ-1827
    """The vector model's API key from its ``api_key_env`` field — the SAME shared grammar
    (${secret:NAME}/${env:NAME}/${user:NAME}, or a bare env-var name) provisa/api/mcp/chat.py's
    custom AI endpoints use, not a separate bare-env-var-only implementation. Before this, a
    vault-reference value here silently resolved to nothing (a plain os.environ.get on a string
    that is never itself a real env var name), which the OpenAI-compatible provider then treated
    as simply "no key" rather than failing loud."""
    from provisa.core.secrets import resolve_api_key_field

    return await resolve_api_key_field(model.api_key_env)


class OpenAICompatibleProvider(EmbeddingProvider):  # REQ-420
    """Any OpenAI-compatible POST {base_url}/embeddings endpoint."""

    async def embed(self, texts: list[str], model: VectorModel) -> list[list[float]]:
        import httpx

        base = (model.base_url or "https://api.openai.com/v1").rstrip("/")
        headers = {}
        key = await _api_key(model)
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


class OllamaProvider(EmbeddingProvider):  # REQ-420
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


class HuggingFaceLocalProvider(EmbeddingProvider):  # REQ-420
    """Local sentence-transformers model — no network."""

    _cache: dict = {}

    async def embed(self, texts: list[str], model: VectorModel) -> list[list[float]]:
        try:
            from sentence_transformers import SentenceTransformer  # pyright: ignore[reportMissingImports]
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


def get_provider(provider: str) -> EmbeddingProvider:  # REQ-420
    """Return an embedding provider instance for the given name."""
    cls = _PROVIDERS.get(provider)
    if cls is None:
        raise EmbeddingError(
            f"Unknown embedding provider {provider!r}; known: {sorted(_PROVIDERS)}"
        )
    return cls()
