# Copyright (c) 2026 Kenneth Stott
# Canary: 728243cb-3579-4268-bd19-96617b161297
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Qdrant over its own client (REQ-1983): a collection is the table. No federation engine
reaches Qdrant, so this is the source's only reader, on every engine. Synchronous -- callers
run it in a thread.

The source's ``host`` is the instance's URL (``https://…:6333``) and ``password`` its API key
(a secret reference, resolved at use; empty for an instance without one).
``federation_hints.prefer_grpc`` reads over gRPC instead of REST.

A point is read as one document: its payload, with the point beside it under keys no payload
field can collide with -- ``@id``, ``@score`` (a search's) and ``@vector`` (the unnamed vector,
or ``@vector.<name>`` for a named one). A column reads a payload field by its path, and the
point by these.
"""

# Requirements: REQ-1983
from __future__ import annotations

from collections.abc import Iterator
from dataclasses import dataclass
from typing import Any

ID_PATH = "@id"
SCORE_PATH = "@score"
VECTOR_PATH = "@vector"


class QdrantSourceInvalid(ValueError):
    """A Qdrant source whose settings cannot reach it, said naming what is missing."""

    code = "schema.qdrant_source_invalid"

    def __init__(self, source_id: str, reason: str) -> None:
        self.params = {"source": source_id, "reason": reason}
        super().__init__(f"Qdrant source {source_id!r}: {reason}")


@dataclass(frozen=True)
class QdrantConnection:  # REQ-1983
    url: str
    api_key: str | None
    prefer_grpc: bool = False

    @classmethod
    def from_source(cls, source: Any) -> "QdrantConnection":
        """The source's connection, its URL and API key resolved here, at use."""
        from provisa.core.secrets import resolve_secrets

        hints = getattr(source, "federation_hints", None) or {}
        url = resolve_secrets(getattr(source, "host", "") or "")
        if not url.startswith(("https://", "http://")):
            raise QdrantSourceInvalid(source.id, "the host is the instance's URL")
        api_key = resolve_secrets(getattr(source, "password", "") or "") or None
        # A hint is text: the boot resolves each one as a secret reference.
        grpc = str(hints.get("prefer_grpc", "")).lower() == "true"
        return cls(url=url, api_key=api_key, prefer_grpc=grpc)

    def client(self) -> Any:
        from qdrant_client import QdrantClient

        return QdrantClient(url=self.url, api_key=self.api_key, prefer_grpc=self.prefer_grpc)


def document(point: Any) -> dict:
    """One point as the document its columns read: its payload, and the point beside it."""
    doc: dict[str, Any] = dict(point.payload or {})
    doc[ID_PATH] = point.id
    score = getattr(point, "score", None)
    if score is not None:
        doc[SCORE_PATH] = score
    # The unnamed vector is a list; named vectors are a dict, read by ``@vector.<name>``.
    vector = getattr(point, "vector", None)
    if vector is not None:
        doc[VECTOR_PATH] = vector
    return doc


def vector_path(name: str) -> str:
    """Where a vector is read from: the unnamed vector, or a named one."""
    return VECTOR_PATH if not name else f"{VECTOR_PATH}.{name}"


def vectors_read(paths: list[str]) -> bool | list[str]:
    """What a read asks for of the points' vectors: none, the unnamed one, or these named ones."""
    names = [p for p in paths if p == VECTOR_PATH or p.startswith(VECTOR_PATH + ".")]
    if not names:
        return False
    if VECTOR_PATH in names:
        return True
    return [p[len(VECTOR_PATH) + 1 :] for p in names]


def list_collections(conn: QdrantConnection) -> list[str]:
    return sorted(c.name for c in conn.client().get_collections().collections)


@dataclass(frozen=True)
class CollectionShape:
    """What a collection says of itself: its vectors (name → size and distance; the unnamed
    vector under ``""``) and its indexed payload fields (path → Qdrant's payload type)."""

    vectors: dict[str, tuple[int, str]]
    indexed: dict[str, str]


def collection_shape(conn: QdrantConnection, collection: str) -> CollectionShape:
    info = conn.client().get_collection(collection)
    params = info.config.params.vectors
    if params is None:
        vectors: dict[str, tuple[int, str]] = {}
    elif isinstance(params, dict):
        vectors = {n: (p.size, str(p.distance.value)) for n, p in params.items()}
    else:
        vectors = {"": (params.size, str(params.distance.value))}
    indexed = {
        field: str(getattr(index.data_type, "value", index.data_type))
        for field, index in (info.payload_schema or {}).items()
    }
    return CollectionShape(vectors=vectors, indexed=indexed)


def sample(conn: QdrantConnection, collection: str, n: int) -> list[dict]:
    points, _ = conn.client().scroll(
        collection_name=collection, limit=n, with_payload=True, with_vectors=False
    )
    return [document(p) for p in points]


def iter_collection_batches(
    conn: QdrantConnection, collection: str, batch_rows: int, vectors: bool | list[str]
) -> Iterator[list[dict]]:
    """Every point of the collection, a batch at a time (scroll, in id order)."""
    client = conn.client()
    offset = None
    while True:
        points, offset = client.scroll(
            collection_name=collection,
            limit=batch_rows,
            offset=offset,
            with_payload=True,
            with_vectors=vectors,
        )
        if points:
            yield [document(p) for p in points]
        if offset is None:
            return


def retrieve(
    conn: QdrantConnection, collection: str, ids: list[Any], vectors: bool | list[str]
) -> list[dict]:
    points = conn.client().retrieve(
        collection_name=collection, ids=ids, with_payload=True, with_vectors=vectors
    )
    return [document(p) for p in points]


def scroll_matching(
    conn: QdrantConnection,
    collection: str,
    query_filter: dict,
    batch_rows: int,
    vectors: bool | list[str],
) -> list[dict]:
    """Every point a filter matches."""
    from qdrant_client import models

    client = conn.client()
    found: list[dict] = []
    offset = None
    while True:
        points, offset = client.scroll(
            collection_name=collection,
            scroll_filter=models.Filter.model_validate(query_filter),
            limit=batch_rows,
            offset=offset,
            with_payload=True,
            with_vectors=vectors,
        )
        found.extend(document(p) for p in points)
        if offset is None:
            return found


def search(
    conn: QdrantConnection,
    collection: str,
    *,
    query: list[float],
    using: str | None,
    query_filter: dict | None,
    limit: int,
    score_threshold: float | None,
    vectors: bool | list[str],
) -> list[dict]:
    """The points nearest ``query`` (Qdrant's Query API), nearest first, each with its score."""
    from qdrant_client import models

    response = conn.client().query_points(
        collection_name=collection,
        query=query,
        using=using,
        query_filter=None if query_filter is None else models.Filter.model_validate(query_filter),
        limit=limit,
        score_threshold=score_threshold,
        with_payload=True,
        with_vectors=vectors,
    )
    return [document(p) for p in response.points]


def search_each(
    conn: QdrantConnection, requests: list[dict], *, vectors: bool | list[str]
) -> list[list[dict]]:
    """Each of ``requests`` (``table.plan``'s, all of one collection) answered as :func:`search`
    answers one, in one Query API batch request: each request's points, nearest first, in the
    requests' order (REQ-2099)."""
    from qdrant_client import models

    if not requests:
        return []
    [collection] = {r["collection"] for r in requests}
    responses = conn.client().query_batch_points(
        collection_name=collection,
        requests=[
            models.QueryRequest(
                query=r["query"],
                using=r["using"],
                filter=None if r["filter"] is None else models.Filter.model_validate(r["filter"]),
                limit=r["limit"],
                score_threshold=r["score_threshold"],
                with_payload=True,
                with_vector=vectors,
            )
            for r in requests
        ],
    )
    return [[document(p) for p in response.points] for response in responses]
