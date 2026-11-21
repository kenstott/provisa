# Copyright (c) 2026 Kenneth Stott
# Canary: 238c6de4-d98b-47bb-9e18-d2821424de2d
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""An Amazon Athena source's catalog, read through Athena's metadata API (REQ-1981).

``ListDatabases``, ``ListTableMetadata`` and ``GetTableMetadata`` run no query, so listing a
source's databases and tables and reading one table's columns costs nothing per byte, unlike a
``SELECT`` on ``information_schema``. A table's columns are read only when it is selected. The
client is the source's own: its region and access key pair (``username``/``password``, each
resolved at use; none for boto3's credential chain) and, when given, the role it assumes.
"""

# Requirements: REQ-1981

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from provisa.core.secrets import resolve_secrets

#: The catalog a source reads when it names none.
DEFAULT_CATALOG = "AwsDataCatalog"
ICEBERG = "iceberg"
HIVE = "hive"


@dataclass(frozen=True)
class AthenaTable:
    name: str
    comment: str | None
    table_format: str  # ICEBERG or HIVE: what writes it takes


def _hints(source: Any) -> dict[str, str]:
    return dict(getattr(source, "federation_hints", None) or {})


def athena_client(source: Any) -> Any:
    """A boto3 Athena client with the source's credential, in its region."""
    import boto3

    hints = _hints(source)
    region = hints.get("region")
    if not region:
        raise ValueError(f"athena source {source.id!r}: requires its region")
    key = resolve_secrets(source.username) if source.username else ""
    secret = resolve_secrets(source.password, credential=True) if source.password else ""
    if bool(key) != bool(secret):
        raise ValueError(
            f"athena source {source.id!r}: an access key ID and its secret are given together, "
            "or neither (for the AWS credential chain)"
        )
    session = (
        boto3.Session(aws_access_key_id=key, aws_secret_access_key=secret, region_name=region)
        if key
        else boto3.Session(region_name=region)
    )
    role_arn = hints.get("role_arn")
    if role_arn:
        assumed = session.client("sts").assume_role(
            RoleArn=role_arn, RoleSessionName=f"provisa-{source.id}"[:64]
        )["Credentials"]
        session = boto3.Session(
            aws_access_key_id=assumed["AccessKeyId"],
            aws_secret_access_key=assumed["SecretAccessKey"],
            aws_session_token=assumed["SessionToken"],
            region_name=region,
        )
    return session.client("athena")


def _catalog(source: Any) -> str:
    return _hints(source).get("catalog") or DEFAULT_CATALOG


def _pages(client: Any, method: str, key: str, **kwargs: Any) -> list[dict]:
    items: list[dict] = []
    for page in client.get_paginator(method).paginate(**kwargs):
        items.extend(page[key])
    return items


def databases(source: Any) -> list[str]:
    """The databases of the source's catalog, by name."""
    client = athena_client(source)
    found = _pages(client, "list_databases", "DatabaseList", CatalogName=_catalog(source))
    return sorted(d["Name"] for d in found)


def table_format(parameters: dict[str, str]) -> str:
    """ICEBERG for a table Athena writes as Iceberg, else HIVE."""
    return ICEBERG if (parameters.get("table_type") or "").lower() == ICEBERG else HIVE


def tables(source: Any, database: str) -> list[AthenaTable]:
    """The tables of one database, each with its comment and format; no column is read."""
    client = athena_client(source)
    found = _pages(
        client,
        "list_table_metadata",
        "TableMetadataList",
        CatalogName=_catalog(source),
        DatabaseName=database,
    )
    return sorted(
        (
            AthenaTable(
                name=t["Name"],
                comment=(t.get("Parameters") or {}).get("comment"),
                table_format=table_format(t.get("Parameters") or {}),
            )
            for t in found
        ),
        key=lambda t: t.name,
    )


def ir_type(athena_type: str) -> str:
    """A column's type as the catalog spells it (``varchar(32)``, ``array<string>``) as an IR
    type; one with none is refused by name (``core.ir_types.to_ir``)."""
    from provisa.core.ir_types import to_ir

    base = athena_type.split("<", 1)[0]
    return to_ir(base, "athena")


def table_metadata(source: Any, database: str, table: str) -> dict:
    client = athena_client(source)
    return client.get_table_metadata(
        CatalogName=_catalog(source), DatabaseName=database, TableName=table
    )["TableMetadata"]


def columns(source: Any, database: str, table: str) -> list[tuple[str, str]]:
    """One table's columns, its partition keys after them, each with its IR type."""
    meta = table_metadata(source, database, table)
    return [
        (c["Name"], ir_type(c["Type"]))
        for c in [*meta.get("Columns", []), *meta.get("PartitionKeys", [])]
    ]
