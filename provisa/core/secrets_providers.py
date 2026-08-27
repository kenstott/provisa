# Copyright (c) 2026 Kenneth Stott
# Canary: 504543ff-10e3-499d-a84e-6990a0982ecf
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Central secrets services ``${secret:NAME}`` can be pointed at (REQ-1557).

Each of these reads a name out of a system the enterprise ALREADY runs, so a credential Provisa
needs is filed where that org's other credentials are filed and rotated by the same process. None
of them writes: a central store's own tooling owns creation, and Provisa reading a name it was
told to read is the whole of the integration. Provisa's own store (``provisa/core/secrets_store.py``)
is the one that both reads and writes, because when it is in use there is nowhere else to write.

THE BACKEND'S OWN CREDENTIAL IS PROCESS CONFIGURATION. A Vault token or an AWS role reaches these
through ``${env:...}`` or the SDK's ambient credential chain, never through the store it opens --
a secrets service whose credential lives inside itself cannot be opened, so the chain of trust
terminates in the host environment by design (REQ-1557).
"""

# Requirements: REQ-1557

from __future__ import annotations

from provisa.core.secrets import SecretsProvider


class VaultSecretsProvider(SecretsProvider):
    """HashiCorp Vault KV v2. A reference is ``path#key``, or ``path`` when the key is ``value``."""

    def __init__(self, url: str, token: str, mount: str = "secret", namespace: str | None = None):
        import hvac

        self._client = hvac.Client(url=url, token=token, namespace=namespace)
        self._mount = mount

    def resolve(self, reference: str) -> str:
        path, _, key = reference.partition("#")
        read = self._client.secrets.kv.v2.read_secret_version(
            path=path, mount_point=self._mount, raise_on_deleted_version=True
        )
        data = read["data"]["data"]
        wanted = key or "value"
        if wanted not in data:
            raise KeyError(f"Vault secret {path!r} has no key {wanted!r}")
        return data[wanted]


class AwsSecretsManagerProvider(SecretsProvider):
    """AWS Secrets Manager. A reference is the secret id, optionally ``id#json_key``."""

    def __init__(self, region: str | None = None, endpoint_url: str | None = None):
        import boto3

        self._client = boto3.client("secretsmanager", region_name=region, endpoint_url=endpoint_url)

    def resolve(self, reference: str) -> str:
        import json

        secret_id, _, key = reference.partition("#")
        payload = self._client.get_secret_value(SecretId=secret_id)["SecretString"]
        if not key:
            return payload
        document = json.loads(payload)
        if key not in document:
            raise KeyError(f"AWS secret {secret_id!r} has no key {key!r}")
        return document[key]


class GcpSecretManagerProvider(SecretsProvider):
    """GCP Secret Manager. A reference is the secret id, optionally ``id#version``."""

    def __init__(self, project: str):
        # ``import google.cloud.secretmanager``, not ``from google.cloud import secretmanager``:
        # ``google.cloud`` is a namespace package other installed google libraries populate, so the
        # from-form asks for a name inside a package that IS present and resolves to nothing. The
        # module form names this optional distribution outright.
        import google.cloud.secretmanager as secretmanager

        self._client = secretmanager.SecretManagerServiceClient()
        self._project = project

    def resolve(self, reference: str) -> str:
        name, _, version = reference.partition("#")
        path = f"projects/{self._project}/secrets/{name}/versions/{version or 'latest'}"
        return self._client.access_secret_version(name=path).payload.data.decode()


class AzureKeyVaultSecretsProvider(SecretsProvider):
    """Azure Key Vault secrets. A reference is the secret name, optionally ``name#version``."""

    def __init__(self, vault_url: str):
        from azure.identity import DefaultAzureCredential
        from azure.keyvault.secrets import SecretClient

        self._client = SecretClient(vault_url=vault_url, credential=DefaultAzureCredential())

    def resolve(self, reference: str) -> str:
        name, _, version = reference.partition("#")
        return self._client.get_secret(name, version or None).value
