# Copyright (c) 2026 Kenneth Stott
# Canary: c0c53a0c-092e-44b5-84db-fdf7e6313950
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Where a notebook's connection to Provisa comes from, without a token pasted into a cell.

Each setting is read from the first of these that names it:

1. the keyword given to :func:`resolve` (or :func:`provisa_client.sign_in`);
2. the environment: ``PROVISA_URL``, ``PROVISA_TOKEN``, ``PROVISA_USERNAME``,
   ``PROVISA_PASSWORD``, ``PROVISA_ROLE``, ``PROVISA_ORG``, ``PROVISA_FLIGHT_PORT``,
   ``PROVISA_TRANSPORT``;
3. a section of the profile file ``~/.provisa/credentials`` (INI; ``[default]`` unless
   ``profile=`` or ``PROVISA_PROFILE`` names another), with the same keys in lower case without
   the prefix (``url``, ``token``, ``username``, ...).

A ``token`` is a personal access token (``provisa_pat_...``) or a session token. A ``username``
and ``password`` are exchanged for a session token at ``POST /auth/login``, which a deployment
offers when its provider signs users in with a password (basic, simple, ldap). The server offers
no flow for a client to sign in through a browser, so none is attempted here.
"""

from __future__ import annotations

import configparser
import os
from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path

import httpx

PROFILE_PATH = Path.home() / ".provisa" / "credentials"
_KEYS = ("url", "token", "username", "password", "role", "org", "flight_port", "transport")
_TRANSPORTS = ("flight", "http")
_LOGIN_TIMEOUT = 30.0


@dataclass(frozen=True)
class Credentials:
    url: str
    token: str | None
    role: str | None
    org: str | None
    flight_port: int
    transport: str  # "flight" (Arrow Flight) or "http" (/data/query)


def resolve(
    *,
    profile: str | None = None,
    environ: Mapping[str, str] = os.environ,
    path: Path = PROFILE_PATH,
    **given: str | int | None,
) -> Credentials:
    """The connection from ``given``, then ``environ``, then the profile file (module docstring)."""
    unknown = set(given) - set(_KEYS)
    if unknown:
        raise TypeError(f"unknown connection settings: {sorted(unknown)}")
    section = _profile_section(profile or environ.get("PROVISA_PROFILE"), path)
    settings: dict[str, str] = {}
    for key in _KEYS:
        value = given.get(key)
        if value is None:
            value = environ.get(f"PROVISA_{key.upper()}")
        if value is None:
            value = section.get(key)
        if value is not None:
            settings[key] = str(value)
    if "url" not in settings:
        raise ValueError(
            f"no Provisa URL: pass url=, set PROVISA_URL, or put url in a profile of {path}"
        )
    url = settings["url"].rstrip("/")
    token = settings.get("token")
    if token is None and "username" in settings:
        if "password" not in settings:
            raise ValueError(f"a username ({settings['username']!r}) is given with no password")
        token = login(url, settings["username"], settings["password"])
    transport = settings.get("transport", "flight")
    if transport not in _TRANSPORTS:
        raise ValueError(f"transport is one of {_TRANSPORTS}, not {transport!r}")
    return Credentials(
        url=url,
        token=token,
        role=settings.get("role"),
        org=settings.get("org"),
        # The Arrow Flight server's port when none is named (ProvisaClient's default).
        flight_port=int(settings.get("flight_port", 8815)),
        transport=transport,
    )


def login(url: str, username: str, password: str) -> str:
    """Exchange a username and password for a session token at ``POST /auth/login``."""
    r = httpx.post(
        f"{url}/auth/login",
        json={"username": username, "password": password},
        timeout=_LOGIN_TIMEOUT,
    )
    if r.status_code != 200:
        raise PermissionError(f"sign-in to {url} refused: HTTP {r.status_code}: {r.text}")
    return r.json()["access_token"]


def _profile_section(name: str | None, path: Path) -> Mapping[str, str]:
    """The named section of the profile file; ``default`` when none is named. A named profile
    must exist; the default may be absent (settings then come from the keywords or environment)."""
    parser = configparser.ConfigParser(interpolation=None)
    if path.exists():
        parser.read(path)
    if name is None:
        return parser["default"] if parser.has_section("default") else {}
    if not parser.has_section(name):
        raise ValueError(f"no profile [{name}] in {path}")
    return parser[name]
