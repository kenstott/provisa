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


#: Where each network provider is reached when a model names no ``base_url``.
_OPENAI = "https://api.openai.com/v1"
_OLLAMA = "http://localhost:11434"


class EmbeddingError(RuntimeError):
    """An embedding request failed or returned an unexpected shape."""


#: The pyproject extra each local provider's library ships in, and the module the extra installs.
#: Every native installer carries the vector extra (the default config's embedding model needs it);
#: an install without it refuses such a model at startup (:func:`require_provider_libraries`).
PROVIDER_LIBRARIES: dict[str, tuple[str, str]] = {
    "huggingface": ("vector", "sentence_transformers"),
}


class EmbeddingLibraryMissing(EmbeddingError):
    """A configured embedding model's provider library is not installed: refused by name when the
    configuration is loaded, never left to fail at its first embedding in a background task."""

    code = "ai_models.embedding_library_missing"

    def __init__(self, model_id: str, provider: str, extra: str, module: str) -> None:
        self.model_id = model_id
        self.provider = provider
        self.extra = extra
        self.module = module
        super().__init__(
            f"The embedding model {model_id} uses the {provider} provider, which needs the "
            f"{extra} extra (module {module} is not installed): pip install 'provisa[{extra}]'."
        )

    @property
    def params(self) -> dict[str, str]:
        """What the catalogued message of :attr:`code` is filled with."""
        return {
            "model": self.model_id,
            "provider": self.provider,
            "extra": self.extra,
            "module": self.module,
        }


def require_provider_libraries(models: Any) -> None:
    """Refuse the first of ``models`` (each with ``id`` and ``provider``) whose provider's library
    is not installed (:data:`PROVIDER_LIBRARIES`), naming the model and the extra. The module is
    found, not imported: importing sentence-transformers loads torch."""
    import importlib.util

    for model in models:
        if model.provider not in PROVIDER_LIBRARIES:
            continue
        extra, module = PROVIDER_LIBRARIES[model.provider]
        if importlib.util.find_spec(module) is None:
            raise EmbeddingLibraryMissing(model.id, model.provider, extra, module)


class EmbeddingProvider(ABC):  # REQ-420
    """Turn a batch of texts into embedding vectors for a model."""

    @abstractmethod
    async def embed(self, texts: list[str], model: VectorModel) -> list[list[float]]: ...

    async def reported_version(self, model: VectorModel) -> str | None:  # REQ-2002
        """The version the provider reports for ``model``, or None where it reports none (a
        model name such as OpenAI's carries no version)."""
        del model
        return None

    async def embed_with_usage(
        self, texts: list[str], model: VectorModel
    ) -> tuple[list[list[float]], int | None]:  # REQ-2105
        """The vectors, and the input tokens the provider reports for them: None where it
        reports none (a local model)."""
        return await self.embed(texts, model), None


async def model_version(model: VectorModel, provider: EmbeddingProvider) -> str | None:  # REQ-2002
    """The version recorded for a use of ``model``: the one its provider reports, else the one
    the operator states in the registry, else none (neither knows one)."""
    reported = await provider.reported_version(model)
    return reported if reported is not None else model.version


class OpenAICompatibleProvider(EmbeddingProvider):  # REQ-420
    """Any OpenAI-compatible POST {base_url}/embeddings endpoint."""

    async def embed(self, texts: list[str], model: VectorModel) -> list[list[float]]:
        return (await self.embed_with_usage(texts, model))[0]

    async def embed_with_usage(
        self, texts: list[str], model: VectorModel
    ) -> tuple[list[list[float]], int | None]:  # REQ-2105
        import httpx

        from provisa.core.org_secrets import vector_model_address, vector_model_key

        base = (await vector_model_address(model, _OPENAI, local_server=False)).rstrip("/")
        headers = {}
        key = await vector_model_key(model)
        if key:
            headers["Authorization"] = f"Bearer {key}"
        async with httpx.AsyncClient(timeout=30) as client:
            resp = await client.post(
                f"{base}/embeddings", headers=headers, json={"model": model.id, "input": texts}
            )
            resp.raise_for_status()
            data = resp.json()
        try:
            vectors = [item["embedding"] for item in data["data"]]
        except (KeyError, TypeError) as exc:
            raise EmbeddingError(f"Unexpected embeddings response: {exc}") from exc
        usage = data.get("usage") or {}
        return vectors, usage.get("prompt_tokens")


async def _ollama_address(model: VectorModel) -> str:
    from provisa.core.org_secrets import vector_model_address

    return (await vector_model_address(model, _OLLAMA, local_server=True)).rstrip("/")


class OllamaProvider(EmbeddingProvider):  # REQ-420
    """Ollama embeddings — one POST {base_url}/api/embeddings per text."""

    async def embed(self, texts: list[str], model: VectorModel) -> list[list[float]]:
        import httpx

        base = await _ollama_address(model)
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

        base = await _ollama_address(model)
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


#: A hub snapshot's files for frameworks other than torch, which a local model never loads.
_OTHER_FRAMEWORKS = [
    "onnx/*",
    "openvino/*",
    "*.onnx",
    "*.h5",
    "*.msgpack",
    "*.ot",
    "coreml/*",
    "*.mlpackage/*",
]


class HuggingFaceLocalProvider(EmbeddingProvider):  # REQ-420
    """Local sentence-transformers model — no network. A model is loaded once per process, and it
    is used by one request at a time: each request runs on its own thread, and two encoding at
    once on Apple's GPU (MPS, where sentence-transformers runs on a Mac) abort the process
    (``failed assertion _status < MTLCommandBufferStatusCommitted``)."""

    _cache: dict = {}
    _snapshots: dict[str, str] = {}  # model id -> the hub snapshot directory it was loaded from
    _in_use = threading.Lock()

    def _loaded(self, model: VectorModel):
        """``model``, loaded once for this process -- fetched into the hub cache first when this
        host has never had it. Called with ``_in_use`` held."""
        try:
            from huggingface_hub import snapshot_download  # pyright: ignore[reportMissingImports]
            from sentence_transformers import SentenceTransformer  # pyright: ignore[reportMissingImports]
        except ImportError as exc:
            raise EmbeddingError(
                "huggingface provider requires sentence-transformers: pip install provisa[vector]"
            ) from exc
        st = self._cache.get(model.id)
        if st is None:
            from provisa.core.org_secrets import hub_endpoint, hub_token

            # The snapshot is fetched here, from the hub address the operator setting names
            # (hub_endpoint), and the model loaded from it: sentence-transformers takes no address
            # and would fetch from the one huggingface_hub read from HF_ENDPOINT at import. The
            # other frameworks' weights (ONNX, OpenVINO, TensorFlow, Flax, Rust, Core ML) are not
            # fetched: sentence-transformers on torch loads none of them.
            path = snapshot_download(  # nosec B615 -- the hub's current snapshot, as a first use takes
                model.id,
                endpoint=hub_endpoint(),
                token=hub_token(),
                ignore_patterns=_OTHER_FRAMEWORKS,
            )
            st = SentenceTransformer(path, token=hub_token())
            self._snapshots[model.id] = path
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

        with self._in_use:
            self._loaded(model)
            return Path(self._snapshots[model.id]).name


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
