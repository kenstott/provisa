# Copyright (c) 2026 Kenneth Stott
# Canary: 3c157225-eb6e-4565-a8a4-a4e4f757da54
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Azure Cosmos DB for NoSQL over its own SDK (REQ-1982): a database is the schema, a container
the table. No federation engine reaches Cosmos, so this is the source's only reader, on every
engine. Synchronous -- callers run it in a thread.

The source's ``host`` is the account endpoint and ``database`` its database. It signs in with the
account key (``password``, a secret reference) or, where ``federation_hints.auth`` is ``entra``,
as a Microsoft Entra service principal: ``federation_hints.tenant_id`` and ``client_id``, its
client secret in ``password``.
"""

from __future__ import annotations

from collections.abc import Iterator
from dataclasses import dataclass
from typing import Any

AUTH_KEY = "key"
AUTH_ENTRA = "entra"
# The fields Cosmos adds to every document. ``_ts`` (the last write, in epoch seconds) is kept: it
# is a watermark the account maintains.
SYSTEM_FIELDS = frozenset({"_rid", "_self", "_etag", "_attachments", "_lsn"})


class CosmosSourceInvalid(ValueError):
    """A Cosmos DB source whose settings cannot sign in, said naming what is missing."""

    code = "schema.cosmosdb_source_invalid"

    def __init__(self, source_id: str, reason: str) -> None:
        self.params = {"source": source_id, "reason": reason}
        super().__init__(f"Cosmos DB source {source_id!r}: {reason}")


@dataclass(frozen=True)
class CosmosConnection:  # REQ-1982
    endpoint: str
    database: str
    auth: str
    secret: str  # the account key, or the service principal's client secret
    tenant_id: str | None = None
    client_id: str | None = None

    @classmethod
    def from_source(cls, source: Any) -> "CosmosConnection":
        """The source's connection, its endpoint and secret resolved here, at use."""
        from provisa.core.secrets import resolve_secrets

        hints = getattr(source, "federation_hints", None) or {}
        auth = hints.get("auth", AUTH_KEY)
        if auth not in (AUTH_KEY, AUTH_ENTRA):
            raise CosmosSourceInvalid(
                source.id, f"auth is {AUTH_KEY!r} or {AUTH_ENTRA!r}, not {auth!r}"
            )
        endpoint = resolve_secrets(getattr(source, "host", "") or "")
        if not endpoint.startswith(("https://", "http://")):
            raise CosmosSourceInvalid(source.id, "the host is the account endpoint URL")
        database = getattr(source, "database", "") or ""
        if not database:
            raise CosmosSourceInvalid(source.id, "the database is required")
        secret = resolve_secrets(getattr(source, "password", "") or "", credential=True)
        if not secret:
            raise CosmosSourceInvalid(
                source.id,
                "the account key is required"
                if auth == AUTH_KEY
                else "the client secret is required",
            )
        if auth == AUTH_ENTRA:
            missing = [k for k in ("tenant_id", "client_id") if not hints.get(k)]
            if missing:
                raise CosmosSourceInvalid(source.id, f"Entra sign-in needs {', '.join(missing)}")
        return cls(
            endpoint=endpoint,
            database=database,
            auth=auth,
            secret=secret,
            tenant_id=hints.get("tenant_id"),
            client_id=hints.get("client_id"),
        )

    def _credential(self) -> Any:
        if self.auth == AUTH_KEY:
            return self.secret
        from azure.identity import ClientSecretCredential

        # from_source refuses an Entra source without either.
        assert self.tenant_id and self.client_id
        return ClientSecretCredential(
            tenant_id=self.tenant_id, client_id=self.client_id, client_secret=self.secret
        )

    def database_client(self) -> Any:
        from azure.cosmos import CosmosClient

        return CosmosClient(self.endpoint, credential=self._credential()).get_database_client(
            self.database
        )

    def container(self, name: str) -> Any:
        return self.database_client().get_container_client(name)


def _clean(doc: dict) -> dict:
    return {k: v for k, v in doc.items() if k not in SYSTEM_FIELDS}


def list_containers(conn: CosmosConnection) -> list[str]:
    return sorted(c["id"] for c in conn.database_client().list_containers())


def partition_key_paths(conn: CosmosConnection, container: str) -> list[str]:
    """The container's partition key paths (``/customerId``; several for a hierarchical key)."""
    return list(conn.container(container).read()["partitionKey"]["paths"])


def iter_query_batches(
    conn: CosmosConnection,
    container: str,
    query: str,
    parameters: list[dict],
    batch_rows: int,
    partition_key: Any = None,
) -> Iterator[list[dict]]:
    """The documents ``query`` returns, page by page, its ``parameters`` sent beside its text.
    With ``partition_key`` the read is one partition's; without, it spans them all."""
    client = conn.container(container)
    options: dict[str, Any] = {"parameters": parameters, "max_item_count": batch_rows}
    if partition_key is None:
        options["enable_cross_partition_query"] = True
    else:
        options["partition_key"] = partition_key
    for page in client.query_items(query, **options).by_page():
        yield [_clean(doc) for doc in page]


def iter_container_batches(
    conn: CosmosConnection, container: str, batch_rows: int, partition_key: Any = None
) -> Iterator[list[dict]]:
    """Every document of ``container`` (of one partition, given ``partition_key``)."""
    return iter_query_batches(conn, container, "SELECT * FROM c", [], batch_rows, partition_key)


def sample(conn: CosmosConnection, container: str, n: int) -> list[dict]:
    """Up to ``n`` documents of ``container``, for a preview."""
    return next(iter_query_batches(conn, container, f"SELECT TOP {int(n)} * FROM c", [], n), [])


def read_points(conn: CosmosConnection, container: str, keys: list[tuple[Any, Any]]) -> list[dict]:
    """The documents of ``keys`` (each ``(id, partition key value)``), by point reads -- the
    cheapest read Cosmos serves. A key with no document is left out."""
    from azure.cosmos.exceptions import CosmosResourceNotFoundError

    client = conn.container(container)
    found: list[dict] = []
    for item_id, partition in keys:
        try:
            found.append(_clean(client.read_item(item=str(item_id), partition_key=partition)))
        except CosmosResourceNotFoundError:
            continue
    return found
