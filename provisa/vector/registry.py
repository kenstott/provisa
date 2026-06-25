# Copyright (c) 2026 Kenneth Stott
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.

"""Embedding-model registry (REQ-419).

The registry is an allowlist: only models registered in config (and enabled) may be
used for embedding generation or similarity queries. Each model fixes its provider
and dimensions so an embedding column can be locked to one model (REQ-429).
"""

from __future__ import annotations

from dataclasses import dataclass

# Requirements: REQ-419, REQ-429, REQ-431


class VectorModelError(ValueError):
    """A vector model is not registered, or is disabled."""


@dataclass(frozen=True)
class VectorModel:  # REQ-419, REQ-429
    """A registered embedding model."""

    id: str  # the model identifier passed to the provider (e.g. "text-embedding-3-small")
    provider: str  # "openai" | "ollama" | "huggingface"
    dimensions: int
    api_key_env: str | None = None  # env var holding the API key (resolved at call time)
    base_url: str | None = None  # provider base URL override
    enabled: bool = True


class VectorModelRegistry:  # REQ-419
    """Allowlist of usable embedding models, keyed by model id."""

    def __init__(self, models: list[VectorModel] | None = None) -> None:
        self._models: dict[str, VectorModel] = {m.id: m for m in (models or [])}

    def get(self, model_id: str) -> VectorModel:  # REQ-500
        """Return the model, or raise if it is not allowlisted / is disabled."""
        model = self._models.get(model_id)
        if model is None:
            raise VectorModelError(f"Vector model {model_id!r} is not registered (allowlist).")
        if not model.enabled:
            raise VectorModelError(f"Vector model {model_id!r} is disabled.")
        return model

    def is_allowed(self, model_id: str) -> bool:  # REQ-500
        model = self._models.get(model_id)
        return model is not None and model.enabled

    def list_enabled(self) -> list[VectorModel]:  # REQ-500
        return [m for m in self._models.values() if m.enabled]

    @classmethod
    def from_config(cls, entries: list[dict]) -> VectorModelRegistry:  # REQ-419
        """Build a registry from raw ``vector_models`` config entries."""
        return cls(
            [
                VectorModel(
                    id=e["id"],
                    provider=e["provider"],
                    dimensions=e["dimensions"],
                    api_key_env=e.get("api_key_env"),
                    base_url=e.get("base_url"),
                    enabled=e.get("enabled", True),
                )
                for e in entries
            ]
        )
