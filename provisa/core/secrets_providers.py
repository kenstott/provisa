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

import re
from collections.abc import Callable

from provisa.core.secrets import SecretsProvider

# REQ-1557: whether the deployment holds many organisations is decided in one place, the running
# deployment's own state. This module asks it each time through the resolver the API layer
# installs at import (``provisa.api.app``), and keeps no copy that could disagree with it.
_many_organisations: "Callable[[], bool] | None" = None

#: The longest name each flat service takes (GCP Secret Manager: a secret id of at most 255
#: characters; Azure Key Vault: an object name of 1-127 characters).
NAME_LIMITS = {"gcp": 255, "azure": 127}

#: The central service each registered backend is (``secrets_registry``); the built-in store is
#: not one.
_KINDS = {
    "hashicorp_vault": "vault",
    "aws_secrets_manager": "aws",
    "gcp_secret_manager": "gcp",
    "azure_key_vault": "azure",
}
_REFERENCE = re.compile(r"\$\{secret:([^}]+)\}")


def set_many_organisations_resolver(resolver: "Callable[[], bool]") -> None:
    """Install the deployment's answer to "does this deployment hold many organisations".
    Called once, at import of the API layer."""
    global _many_organisations
    _many_organisations = resolver


def _scoped() -> bool:
    """Whether a read is made inside the bound organisation's namespace: where the deployment
    holds many organisations. A deployment of one reads a reference as written, since the
    service it is wired to is that organisation's own."""
    if _many_organisations is None:
        raise RuntimeError(
            "A central secrets service cannot be read here: nothing has said whether this "
            "deployment holds many organizations. The API layer says it at import "
            "(provisa.api.app)."
        )
    return _many_organisations()


class SecretNameRefused(KeyError):
    """A reference names a secret outside its organisation, or by a name its service cannot
    hold. ``code`` and ``params`` are what an operator is answered with (HTTP 400)."""

    def __init__(self, code: str, message: str, **params: object) -> None:
        super().__init__(message)
        self.code = code
        self.params = params

    def __str__(self) -> str:  # KeyError shows the repr of its argument
        return str(self.args[0])


#: What an operator calls each central service.
_SERVICE = {
    "vault": "HashiCorp Vault",
    "aws": "AWS Secrets Manager",
    "gcp": "GCP Secret Manager",
    "azure": "Azure Key Vault",
}


def _bound_org(reference: str) -> str:
    """The organisation the running operation is bound to. Every read of a central service is
    made in that organisation's namespace; with none bound there is no namespace to read in."""
    from provisa.core.secrets_store import bound_org_id

    org_id = bound_org_id()
    if org_id is None:
        raise KeyError(
            f"Cannot resolve ${{secret:{reference}}}: no organization is bound to this context. "
            "Secrets belong to an org and are resolved inside its own operations."
        )
    return org_id


def org_namespace(kind: str, org_id: str) -> str:
    """What every name of ``org_id`` begins with in the central service ``kind`` (REQ-1557).

    Vault and AWS Secrets Manager have paths: an organisation's secrets are under
    ``orgs/<org id>/``. GCP Secret Manager and Azure Key Vault have flat names, so the
    organisation is the start of the name; an org id holds letters, digits and underscores only
    (``core.db._validate_org_id``), which is what makes each form unambiguous -- no org id holds
    the ``/`` or ``-`` that ends its part.
    """
    if kind in ("vault", "aws"):
        return f"orgs/{org_id}/"
    if kind == "gcp":
        return f"org-{org_id}--"
    if kind == "azure":
        # Azure names hold letters, digits and dashes: the org id's underscores become dashes,
        # and its length is stated so the end of the org part is known whatever the name holds.
        return f"o{len(org_id)}-{org_id.replace('_', '-')}-"
    raise ValueError(f"unknown central secrets service {kind!r}")


def _own_name(kind: str, reference: str, name: str) -> str:
    """``name`` as the service holds it: inside the bound organisation's namespace where the
    deployment holds many organisations, as written where it holds one. A name that would step
    outside the namespace -- an absolute path, a ``..`` segment, a path in a service whose names
    are flat -- is refused, as is one the service cannot hold once prefixed."""
    if not _scoped():
        return name
    return _inside(kind, reference, name, _bound_org(reference))


def _inside(kind: str, reference: str, name: str, org_id: str) -> str:
    """``name`` inside ``org_id``'s namespace of the service ``kind``, or a refusal by name."""
    flat = kind in ("gcp", "azure")
    if (
        not name
        or name.startswith("/")
        or "\\" in name
        or ".." in name.split("/")
        or (flat and "/" in name)
    ):
        raise SecretNameRefused(
            "secrets.name_refused",
            f"${{secret:{reference}}} is not a name inside this organization's secrets: a name "
            "is relative to the organization and cannot leave it",
            name=reference,
            service=_SERVICE[kind],
        )
    full = org_namespace(kind, org_id) + name
    limit = NAME_LIMITS.get(kind)
    if limit is not None and len(full) > limit:
        raise SecretNameRefused(
            "secrets.name_too_long",
            f"${{secret:{reference}}} is too long a name: with this organization's prefix it is "
            f"{len(full)} characters, and {_SERVICE[kind]} takes at most {limit}",
            name=reference,
            service=_SERVICE[kind],
            length=len(full),
            limit=limit,
        )
    return full


def check_references(value: object, org_id: str) -> None:
    """Refuse a value about to be stored for ``org_id`` when a ``${secret:...}`` in it could
    never be read: on a deployment of many organisations wired to a central service, a name
    that would leave the organisation's namespace or that the service cannot hold once
    prefixed. ``value`` may be text or a structure holding text."""
    if isinstance(value, dict):
        for item in value.values():
            check_references(item, org_id)
        return
    if isinstance(value, (list, tuple)):
        for item in value:
            check_references(item, org_id)
        return
    if not isinstance(value, str) or "${secret:" not in value:
        return
    from provisa.core.secrets_runtime import secrets_backend_spec

    spec = secrets_backend_spec()
    kind = None if spec is None else _KINDS.get(spec.key)
    if kind is None or not _scoped():
        return
    for reference in _REFERENCE.findall(value):
        _inside(kind, reference, reference.partition("#")[0], org_id)


class VaultSecretsProvider(SecretsProvider):
    """HashiCorp Vault KV v2. A reference is ``path#key``, or ``path`` when the key is ``value``."""

    def __init__(self, url: str, token: str, mount: str = "secret", namespace: str | None = None):
        import hvac

        self._client = hvac.Client(url=url, token=token, namespace=namespace)
        self._mount = mount

    def resolve(self, reference: str) -> str:
        path, _, key = reference.partition("#")
        path = _own_name("vault", reference, path)  # REQ-1557
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
        secret_id = _own_name("aws", reference, secret_id)  # REQ-1557
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
        name = _own_name("gcp", reference, name)  # REQ-1557
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
        name = _own_name("azure", reference, name)  # REQ-1557
        return self._client.get_secret(name, version or None).value


def check_stored(value: object) -> None:
    """:func:`check_references` for a value about to be stored, against the organisation it is
    stored for: the one whose vault is bound, else the one the request runs as. A write made
    with neither is not one an organisation's member makes, and the read still refuses such a
    name."""
    from provisa.core.request_context import current_org
    from provisa.core.secrets_store import bound_org_id

    org_id = bound_org_id() or current_org.get()
    if org_id is not None:
        check_references(value, org_id)
