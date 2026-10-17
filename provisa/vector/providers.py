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

import threading

from abc import ABC, abstractmethod
from typing import Any

from provisa.vector.registry import VectorModel

# Requirements: REQ-420, REQ-431, REQ-1827


class EmbeddingError(RuntimeError):
    """An embedding request failed or returned an unexpected shape."""


class EmbeddingProvider(ABC):  # REQ-420
    """Turn a batch of texts into embedding vectors for a model."""

    @abstractmethod
    async def embed(self, texts: list[str], model: VectorModel) -> list[list[float]]: ...

    async def reported_version(self, model: VectorModel) -> str | None:  # REQ-2002
        """The version the provider reports for ``model``, or None where it reports none (a
        model name such as OpenAI's carries no version)."""
        del model
        return None


async def model_version(model: VectorModel, provider: EmbeddingProvider) -> str | None:  # REQ-2002
    """The version recorded for a use of ``model``: the one its provider reports, else the one
    the operator states in the registry, else none (neither knows one)."""
    reported = await provider.reported_version(model)
    return reported if reported is not None else model.version


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

    async def reported_version(self, model: VectorModel) -> str | None:  # REQ-2002
        """The model's digest, as the server's model list gives it."""
        import httpx

        base = (model.base_url or "http://localhost:11434").rstrip("/")
        async with httpx.AsyncClient(timeout=30) as client:
            resp = await client.get(f"{base}/api/tags")
            resp.raise_for_status()
            listed = resp.json().get("models")
        if not isinstance(listed, list):
            raise EmbeddingError("Ollama model list missing 'models'")
        names = {model.id, f"{model.id}:latest"}
        for entry in listed:
            if entry.get("name") in names or entry.get("model") in names:
                return entry.get("digest")
        raise EmbeddingError(f"Ollama does not list model {model.id!r}")


class HuggingFaceLocalProvider(EmbeddingProvider):  # REQ-420
    """Local sentence-transformers model — no network. A model is loaded once per process, and it
    is used by one request at a time: each request runs on its own thread, and two encoding at
    once on Apple's GPU (MPS, where sentence-transformers runs on a Mac) abort the process
    (``failed assertion _status < MTLCommandBufferStatusCommitted``)."""

    _cache: dict = {}
    _in_use = threading.Lock()

    def _loaded(self, model: VectorModel):
        """``model``, loaded once for this process -- fetched into the hub cache first when this
        host has never had it. Called with ``_in_use`` held."""
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
        return st

    def load(self, model: Any) -> None:
        """Load ``model`` (anything with the model's ``id``) for this process now, so that no
        request waits for it: importing sentence-transformers and loading a model took a hosted
        sandbox tier's first search past its 60s request timeout."""
        with self._in_use:
            self._loaded(model)

    async def embed(self, texts: list[str], model: VectorModel) -> list[list[float]]:
        with self._in_use:
            return [list(map(float, v)) for v in self._loaded(model).encode(texts)]

    async def reported_version(self, model: VectorModel) -> str | None:  # REQ-2002
        """The commit of the snapshot the model is loaded from: the hub cache names each snapshot
        by it. The model is loaded first, so a host that has never had it fetches it, as its
        first use would -- the version is asked before any text is embedded."""
        from pathlib import Path

        try:
            from huggingface_hub import snapshot_download  # pyright: ignore[reportMissingImports]
        except ImportError as exc:
            raise EmbeddingError(
                "huggingface provider requires sentence-transformers: pip install provisa[vector]"
            ) from exc
        with self._in_use:
            self._loaded(model)
        return Path(snapshot_download(model.id, local_files_only=True)).name


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
