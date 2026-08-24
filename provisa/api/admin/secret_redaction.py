# Copyright (c) 2026 Kenneth Stott
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""REQ-1575: the deployment's own secrets go in and never come back out.

The platform settings surfaces render forms over real credentials -- a Vault token, a KMS
credential, an identity provider's client secret, a JWT signing secret, an S3 secret key -- and a
GET that returns the stored value hands every one of them to the browser. This module is the one
place that decides what a settings response may carry: a field the registry marks ``secret: True``
is dropped, and what goes in its place is the single bit a form actually needs -- whether a value
is stored -- so the UI can say "set, replace it?" instead of showing an empty box that misstates
the state.

Nothing here masks. A masked value (``sk-****abcd``) is still a disclosure -- length, prefix, last
characters -- and worse, it looks enough like a value that a form saves it back over the real one.
The value is absent or it is not there at all.
"""

# Requirements: REQ-1575

from __future__ import annotations

from typing import Iterable, Mapping


def secret_keys(config_fields: Iterable[Mapping]) -> set[str]:
    """The config keys a registry entry declares secret."""
    return {f["config_key"] for f in config_fields if f.get("secret")}


def redact(config: Mapping, config_fields: Iterable[Mapping]) -> tuple[dict, dict[str, bool]]:
    """Split one config block into (what may be returned, which secrets are set).

    A secret key is removed outright rather than emptied: an empty string is a legitimate stored
    value meaning "cleared", and returning one for a set credential would make the two states
    indistinguishable to the form and to anyone reading the response.
    """
    secrets = secret_keys(config_fields)
    safe = {k: v for k, v in config.items() if k not in secrets}
    return safe, {k: bool(config.get(k)) for k in sorted(secrets)}


def redact_per_provider(
    configs: Mapping[str, Mapping], specs: Iterable[Mapping]
) -> tuple[dict[str, dict], dict[str, dict[str, bool]]]:
    """The same, for the ``{provider: {field: value}}`` shape the settings GETs return."""
    safe: dict[str, dict] = {}
    is_set: dict[str, dict[str, bool]] = {}
    for spec in specs:
        key = spec["key"]
        safe[key], is_set[key] = redact(configs.get(key) or {}, spec.get("config_fields") or [])
    return safe, is_set


def redact_url_password(url: str | None) -> str | None:
    """Strip the password out of a DSN, keeping everything an operator reads it for.

    A URL field looks like an address and carries a credential: ``trino://user:pw@host:443/cat``.
    The host, port, path and user are what tell an operator what they are pointed at, so those stay
    and only the password goes. Anything that does not parse as a URL is returned untouched --
    there is no password to find in it, and rewriting it would corrupt a value the form must save
    back.
    """
    if not url or "@" not in url:
        return url
    from urllib.parse import urlsplit, urlunsplit

    parts = urlsplit(url)
    if not parts.hostname or parts.password is None:
        return url
    netloc = parts.hostname if parts.username is None else f"{parts.username}@{parts.hostname}"
    if parts.port is not None:
        netloc = f"{netloc}:{parts.port}"
    return urlunsplit((parts.scheme, netloc, parts.path, parts.query, parts.fragment))


def restore_url_password(submitted: str | None, stored: str | None) -> str | None:
    """Put back the password a redacted DSN lost on the way out, when it is the SAME DSN.

    The form receives the URL with its password stripped, so saving the page unchanged would post
    that stripped URL back and destroy the credential. Same address (scheme, user, host, port,
    path) and no password submitted means the operator did not touch the field, so the stored
    password stands. Change the address and the stored password does NOT follow -- it belonged to
    the old target, and carrying it to a new one is a credential sent somewhere it was never issued
    for. A submitted password always wins, which is how a password is changed.
    """
    if not submitted or not stored:
        return submitted
    from urllib.parse import urlsplit

    new, old = urlsplit(submitted), urlsplit(stored)
    if new.password is not None or old.password is None:
        return submitted
    same = (new.scheme, new.username, new.hostname, new.port, new.path) == (
        old.scheme,
        old.username,
        old.hostname,
        old.port,
        old.path,
    )
    return stored if same else submitted
