# Copyright (c) 2026 Kenneth Stott
# Canary: b96f56f4-f0ba-4bdd-ba8f-ea5a9bb8af9e
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Python client for Provisa.

Every name is imported when first used, so ``import provisa_client`` works in the browser
(Pyodide), whose pyarrow has no Arrow Flight: there, :func:`query` reads over HTTP and the Flight
client is never loaded.
"""

from importlib import import_module
from typing import Any

# name -> the module that defines it
_EXPORTS = {
    "ProvisaClient": "provisa_client.client",
    "connect": "provisa_client.dbapi",
    "adbc_connect": "provisa_client.adbc",
    "ClientEncryptionService": "provisa_client.encryption",
    "DecryptionError": "provisa_client.encryption",
    "build_client_encryption": "provisa_client.encryption",
    "build_kms_provider": "provisa_client.encryption",
    "GraphQLDecryptClient": "provisa_client.graphql_decrypt",
    "encrypted_fields_from_sdl": "provisa_client.graphql_decrypt",
    "ENCRYPTED_DIRECTIVE_SDL": "provisa_client.graphql_decrypt",
    "query": "provisa_client.reader",
    "aquery": "provisa_client.reader",
    "sign_in": "provisa_client.reader",
}

__all__ = [
    "ProvisaClient",
    "connect",
    "adbc_connect",
    "ClientEncryptionService",
    "DecryptionError",
    "build_client_encryption",
    "build_kms_provider",
    "GraphQLDecryptClient",
    "encrypted_fields_from_sdl",
    "ENCRYPTED_DIRECTIVE_SDL",
    "query",
    "aquery",
    "sign_in",
]


def __getattr__(name: str) -> Any:
    if name not in _EXPORTS:
        raise AttributeError(f"module 'provisa_client' has no attribute {name!r}")
    return getattr(import_module(_EXPORTS[name]), name)


def load_ipython_extension(ipython: Any) -> None:
    """``%load_ext provisa_client``: the %%sql, %%graphql and %%cypher cell magics."""
    from provisa_client.magics import load_ipython_extension as load

    load(ipython)
