# Copyright (c) 2026 Kenneth Stott
# Canary: e7a30c58-1f92-4d6b-b4a8-6c9d2e0f7135
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A cloud inventory source's settings (REQ-1947), read by both of its readers: the pgwire
server's model (``pgwire_replica``) and the Trino catalog (``trino_connectors``)."""

from __future__ import annotations

from pathlib import PurePath
from typing import Any

from provisa.core.secrets import resolve_secrets
from provisa.federation.connector_config import MissingConnectorConfig

# REQ-1947: a cloud inventory source's clouds — per cloud, the mapping key that names it (a cloud
# is configured when any of its required keys is set), then (mapping key, model operand) for what
# it requires and for what it may add. One table, read by the pgwire model here and by the Trino
# catalog (``trino_connectors.TrinoCloudopsConnector``, as catalog properties).
CLOUDOPS_CLOUDS: dict[str, dict[str, tuple[tuple[str, str], ...]]] = {
    "azure": {
        "required": (
            ("azure_tenant_id", "azure.tenantId"),
            ("azure_client_id", "azure.clientId"),
            ("azure_client_secret", "azure.clientSecret"),
            ("azure_subscription_ids", "azure.subscriptionIds"),
        ),
        "optional": (),
    },
    "aws": {
        "required": (
            ("aws_access_key_id", "aws.accessKeyId"),
            ("aws_secret_access_key", "aws.secretAccessKey"),
            ("aws_region", "aws.region"),
            ("aws_account_ids", "aws.accountIds"),
        ),
        "optional": (("aws_role_arn", "aws.roleArn"),),
    },
    "gcp": {
        "required": (
            ("gcp_credentials_path", "gcp.credentialsPath"),
            ("gcp_project_ids", "gcp.projectIds"),
        ),
        "optional": (),
    },
}
CLOUDOPS_GCP_CREDENTIALS_KEY = "gcp_credentials_path"


def cloudops_settings(source: Any) -> dict[str, Any]:
    """A cloud inventory source's settings keyed by model operand (REQ-1947): ``providers`` and,
    for each cloud the source names, that cloud's credentials and scope.

    Each cloud is all or nothing: a cloud with some of its required values and not the rest is a
    config error naming what is missing, and so is a source naming no cloud. ``providers`` lists
    exactly the clouds named, because the adapter reads a cloud's credentials from the process
    environment when the model omits them, and this process's environment is not the source's.
    The GCP credential is a file the server opens, so its path must be absolute (the server's
    working directory is its own state directory), as a SharePoint certificate's must."""
    mapping = {
        k: resolve_secrets(v) if isinstance(v, str) else v
        for k, v in (source.mapping or {}).items()
    }
    who = f"cloudops source {source.id!r}"
    settings: dict[str, Any] = {}
    named: list[str] = []
    for cloud, keys in CLOUDOPS_CLOUDS.items():
        required = keys["required"]
        present = [key for key, _ in required if mapping.get(key)]
        if not present:
            continue
        missing = [key for key, _ in required if not mapping.get(key)]
        if missing:
            raise MissingConnectorConfig(
                f"{who}: {cloud} is missing {', '.join('mapping.' + m for m in missing)}"
            )
        named.append(cloud)
        for key, operand_key in (*required, *keys["optional"]):
            if mapping.get(key):
                settings[operand_key] = mapping[key]
    if not named:
        raise MissingConnectorConfig(f"{who}: requires at least one of Azure, AWS or GCP")
    gcp_path = mapping.get(CLOUDOPS_GCP_CREDENTIALS_KEY)
    if gcp_path and not PurePath(gcp_path).is_absolute():
        raise MissingConnectorConfig(
            f"{who}: mapping.{CLOUDOPS_GCP_CREDENTIALS_KEY} must be an absolute path, "
            f"got {gcp_path!r}"
        )
    if mapping.get("cache_ttl_minutes"):
        settings["cache.ttlMinutes"] = int(mapping["cache_ttl_minutes"])
    return {"providers": ",".join(named), **settings}
