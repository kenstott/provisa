# Copyright (c) 2026 Kenneth Stott
# Canary: c2ca48e8-b8b4-496d-aa66-7d40b3a48477
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Spike (REQ-1982, maintainer decision 5): does the Cosmos DB Linux emulator serve vector search
and full-text search? Each feature is one test, so the CI result answers per feature. Where a
feature fails here, the Cosmos search tests it would back need a real Cosmos account, and the
maintainer decides whether CI gets one.

The emulator is the ``test`` instance: a container this module starts on a leased port and
removes. It speaks HTTP and takes the emulator's well-known key."""

# Requirements: REQ-1982

from __future__ import annotations

import os
import subprocess
import time

import pytest

pytestmark = [pytest.mark.integration]

# Pinned by digest: the spike records what one emulator build answers, so the image must not move.
# vnext-preview floats; on 2026-11-23 it moved to vnext-EN20261123, which answers FullTextContains
# and FullTextScore with "InternalServerError: Internal error occurred (-2147418113)" instead of the
# parse error asserted below (so does vnext-EN20261117pre, CI 45419867651). This digest is
# vnext-EN20261019, the release vnext-preview served before.
_IMAGE = (
    "mcr.microsoft.com/cosmosdb/linux/azure-cosmos-emulator@sha256:"
    "e2b2c3ca0b99b256ff0781518714592a0f54fefc30d76d70a450d65ead3c96cb"
)
# The emulator's published, well-known account key (not a secret).
_KEY = "C2y6yDjf5/R+ob0N8A7Cgv30VRDJIWEHLM+4QDU5DE2nQ9nDuVTqobD4b8mGGyPMbIZnqyMsEcaGQy67XIw/Jw=="
_DOCS = [
    {
        "id": "1",
        "pk": "a",
        "text": "an old man flies his house with balloons",
        "embedding": [1.0, 0.0, 0.0],
    },
    {
        "id": "2",
        "pk": "a",
        "text": "a race car learns about friendship",
        "embedding": [0.9, 0.1, 0.0],
    },
    {"id": "3", "pk": "b", "text": "a detective chases bank robbers", "embedding": [0.0, 1.0, 0.0]},
]


_VECTOR_POLICY = {
    "vectorEmbeddings": [
        {"path": "/embedding", "dataType": "float32", "distanceFunction": "cosine", "dimensions": 3}
    ]
}
_FULL_TEXT_POLICY = {
    "defaultLanguage": "en-US",
    "fullTextPaths": [{"path": "/text", "language": "en-US"}],
}


def _indexing(**indexes) -> dict:
    return {
        "indexingMode": "consistent",
        "includedPaths": [{"path": "/*"}],
        "excludedPaths": [{"path": '/"_etag"/?'}, {"path": "/embedding/*"}],
        **indexes,
    }


@pytest.fixture(scope="module")
def database():
    from azure.cosmos import CosmosClient

    from tests.port_lease import lease_ports

    (port,) = lease_ports(1)
    name = f"provisa-itest-cosmos-{os.getpid()}"
    subprocess.run(
        ["docker", "run", "-d", "--rm", "--memory", "2g", "--name", name]
        + ["-p", f"127.0.0.1:{port}:8081", _IMAGE],
        check=True,
        capture_output=True,
    )
    try:
        # The client's constructor already asks the account for its endpoints, so it is made inside
        # the wait: until the emulator listens, the port's proxy resets the connection.
        deadline = time.monotonic() + 300
        while True:
            try:
                client = CosmosClient(f"http://127.0.0.1:{port}/", credential=_KEY)
                db = client.create_database_if_not_exists("spike")
                break
            except Exception:  # allow-blind-except: polling the emulator until it answers
                if time.monotonic() > deadline:
                    raise
                time.sleep(3)
        yield db
    finally:
        subprocess.run(["docker", "rm", "-f", name], capture_output=True)


def _container(database, container_id: str, **policies):
    """A container of the documents with ``policies`` -- one per feature, so a policy the
    emulator refuses fails only the tests of that feature."""
    from azure.cosmos import PartitionKey

    made = database.create_container(
        id=container_id, partition_key=PartitionKey(path="/pk"), **policies
    )
    for doc in _DOCS:
        made.upsert_item(doc)
    return made


@pytest.fixture(scope="module")
def plain(database):
    return _container(database, "plain")


@pytest.fixture(scope="module")
def vectors(database):
    # The emulator takes only a diskANN vector index (CI 2026-10-12: "vectorIndexes[0]: only
    # 'diskANN' type is currently supported").
    return _container(
        database,
        "vectors",
        vector_embedding_policy=_VECTOR_POLICY,
        indexing_policy=_indexing(vectorIndexes=[{"path": "/embedding", "type": "diskANN"}]),
    )


@pytest.fixture(scope="module")
def full_text(database):
    return _container(
        database,
        "full_text",
        full_text_policy=_FULL_TEXT_POLICY,
        indexing_policy=_indexing(fullTextIndexes=[{"path": "/text"}]),
    )


@pytest.fixture(scope="module")
def hybrid(database):
    return _container(
        database,
        "hybrid",
        vector_embedding_policy=_VECTOR_POLICY,
        full_text_policy=_FULL_TEXT_POLICY,
        indexing_policy=_indexing(
            vectorIndexes=[{"path": "/embedding", "type": "diskANN"}],
            fullTextIndexes=[{"path": "/text"}],
        ),
    )


def _query(container, sql: str, parameters: list[dict] | None = None) -> list[dict]:
    return list(
        container.query_items(sql, parameters=parameters or [], enable_cross_partition_query=True)
    )


def test_the_emulator_serves_a_parameterized_query(plain):
    rows = _query(plain, "SELECT c.id FROM c WHERE c.pk = @pk", [{"name": "@pk", "value": "a"}])
    assert sorted(r["id"] for r in rows) == ["1", "2"]


def test_the_emulator_serves_vector_search(vectors):
    rows = _query(
        vectors,
        "SELECT TOP 2 c.id, VectorDistance(c.embedding, @v) AS score FROM c "
        "ORDER BY VectorDistance(c.embedding, @v)",
        [{"name": "@v", "value": [1.0, 0.0, 0.0]}],
    )
    assert [r["id"] for r in rows] == ["1", "2"]


# The spike's second answer (CI 2026-10-12): the emulator takes a full-text policy and index but
# knows no full-text function, so full-text and hybrid search need a real Cosmos account.


def test_the_emulator_knows_no_full_text_contains(full_text):
    from azure.cosmos.exceptions import CosmosHttpResponseError

    with pytest.raises(
        CosmosHttpResponseError,
        match="'FullTextContains' is not a recognized built-in function name",
    ):
        _query(full_text, "SELECT c.id FROM c WHERE FullTextContains(c.text, 'car')")


def test_the_emulator_knows_no_full_text_ranking(full_text):
    from azure.cosmos.exceptions import CosmosHttpResponseError

    with pytest.raises(
        CosmosHttpResponseError,
        match="'FullTextScore' is not a recognized built-in function name",
    ):
        _query(
            full_text,
            "SELECT TOP 2 c.id FROM c ORDER BY RANK FullTextScore(c.text, ['old', 'man'])",
        )


def test_the_emulator_knows_no_hybrid_ranking(hybrid):
    from azure.cosmos.exceptions import CosmosHttpResponseError

    with pytest.raises(
        CosmosHttpResponseError,
        match="'FullTextScore' is not a recognized built-in function name",
    ):
        _query(
            hybrid,
            "SELECT TOP 2 c.id FROM c ORDER BY RANK RRF(FullTextScore(c.text, ['car']), "
            "VectorDistance(c.embedding, @v))",
            [{"name": "@v", "value": [0.9, 0.1, 0.0]}],
        )


def test_the_emulator_takes_only_a_diskann_vector_index(database):
    """The spike's first answer (CI 2026-10-12): a flat vector index is refused by name."""
    from azure.cosmos.exceptions import CosmosHttpResponseError

    with pytest.raises(CosmosHttpResponseError, match="only 'diskANN' type is currently supported"):
        _container(
            database,
            "flat_index",
            vector_embedding_policy=_VECTOR_POLICY,
            indexing_policy=_indexing(vectorIndexes=[{"path": "/embedding", "type": "flat"}]),
        )
