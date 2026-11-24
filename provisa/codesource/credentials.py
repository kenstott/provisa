# Copyright (c) 2026 Kenneth Stott
# Canary: f2e0419d-52fb-4e10-b2a1-d8c63b0e9fa7
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A built-in code source's credential (REQ-2267, amended 2026-11-22): stored as a secret
reference and resolved through Provisa's secrets service at each call, never written to the code
directory.

The source's auth, as registered, names its credential in one field (a bearer token, a password,
an API key, a client secret). That field must be a reference (``${secret:NAME}``,
``${env:NAME}``): the reference is declared in the source's mapping as the secret
:data:`CREDENTIAL` (``mapping["secrets"]``, provisa/codesource/services.py), and the code
directory holds the auth with the field naming that declared secret (``{"$secret": "credential"}``).
The code reads it with ``provisa_services.secrets.get`` when it calls the API, so a rotated secret
is read on the next call. A credential given as a value is refused by name: it would be written
where the code is.
"""

# Requirements: REQ-2267, REQ-2104
from __future__ import annotations

from typing import Any

#: The name the source's credential is declared under in its mapping's ``secrets``.
CREDENTIAL = "credential"
#: How the code directory's auth names a declared secret in place of its value.
SECRET_KEY = "$secret"


def held_as_reference(
    auth: dict | None, field: str | None, source: str
) -> tuple[dict | None, dict[str, str]]:
    """``auth`` as the code directory holds it -- its credential ``field`` naming the declared
    secret -- and the ``secrets`` the source's mapping declares for it. ``field`` None: the auth
    carries no credential. A credential that is not a reference is refused by name (``source``
    names the source), its value never in the refusal."""
    from provisa.codesource.bundle import BundleRefused
    from provisa.codesource.services import is_reference

    if auth is None or field is None:
        return auth, {}
    if field not in auth:
        raise BundleRefused(
            "credential_missing",
            f"{source}: its auth of type {auth.get('type')!r} gives no {field}",
            source=source,
            field=field,
        )
    reference = auth[field]
    if not (isinstance(reference, str) and is_reference(reference)):
        raise BundleRefused(
            "credential_not_reference",
            f"{source}: its {field} is a reference to the vault or the environment "
            "(${secret:NAME}, ${env:NAME}); a credential is never written where its code is",
            source=source,
            field=field,
        )
    return {**auth, field: {SECRET_KEY: CREDENTIAL}}, {CREDENTIAL: reference}


def resolved(auth: dict | None) -> dict | None:
    """``auth`` as the code calls with it: each field naming a declared secret given that secret's
    value, read from Provisa's secrets service now (provisa_services.secrets.get)."""
    if auth is None:
        return None
    import provisa_services  # pyright: ignore[reportMissingImports] -- on the host's path

    out: dict[str, Any] = {}
    for key, value in auth.items():
        if isinstance(value, dict) and set(value) == {SECRET_KEY}:
            out[key] = provisa_services.secrets.get(value[SECRET_KEY])
        else:
            out[key] = value
    return out
