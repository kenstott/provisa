# Copyright (c) 2026 Kenneth Stott
# Canary: 8a1ac613-2906-4e5f-af0f-1661ccd16c50
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""What an environment is named and which schema it occupies (REQ-1488, REQ-1523).

An environment is a named copy of an org's governed model, and REQ-1488 makes it physically a
schema of its own rather than a discriminator column on forty tables — so every existing
repository query is already correct in an environment, and one environment's rows cannot reach
another's read by a forgotten predicate.

The org's default environment is ``prod`` and it KEEPS THE EXISTING SCHEMA NAME. An org that
never creates an environment is byte-for-byte unchanged: ``org_schema("acme")`` is ``org_acme``,
which is what every site building that name by hand already produces.

WHY ``_env_`` AND NOT ``__``. The compiler already spends ``org_<id>__<catalog>`` on Trino catalog
names (``provisa/compiler/naming.py``). Those are catalogs and these are PostgreSQL schemas, so
they cannot actually collide, but a reader cannot tell which one they are holding. ``_env_`` is
unambiguous for a stronger reason: REQ-1309 forbids an underscore in an org id, so the first
``_env_`` after the ``org_`` prefix always splits the name into exactly one org and one
environment, whatever either is called.

WHY THE LENGTH CHECK IS PER-ORG. PostgreSQL truncates an identifier over 63 bytes rather than
refusing it, which would silently point two environments at one schema. The longest identifier an
environment derives is its API cache schema, so the cap on a name is whatever is left after this
org's own id — a short id affords a long environment name and a 40-character id affords almost
none. Checking it against the real id at creation is the only version that cannot be wrong.
"""

# Requirements: REQ-1487, REQ-1488, REQ-1491, REQ-1523

from __future__ import annotations

import re

#: The environment every org has from creation. It cannot be created, renamed or deleted
#: (REQ-1487), and it is the environment a request naming none is served by.
PROD = "prod"

#: The per-org store schemas derived from one environment's base name (REQ-1046 meters these).
#: Enumerated rather than matched with ``LIKE 'org_<id>%'``: an org id is user-chosen text, so a
#: prefix match would bill "acme" for every byte belonging to "acmeeu".
SCHEMA_SUFFIXES: tuple[str, ...] = ("", "_mv_cache", "_api_cache", "_gql_cache")

#: PostgreSQL's identifier limit. Over it, PostgreSQL truncates silently.
MAX_IDENTIFIER_BYTES = 63

# A legal unquoted SQL identifier segment. Hyphens are excluded for the same reason REQ-1309
# excludes them from an org id: every schema DDL site interpolates the name unquoted (``SET
# search_path TO ...``), and a hyphen there is a syntax error surfacing during background
# provisioning. An environment is not a DNS label — REQ-1487 carries the selection in the
# ``x-provisa-env`` header — so the SQL rule is the only rule it has to satisfy.
ENV_NAME_PATTERN = re.compile(r"[a-z][a-z0-9_]{1,31}")

# Refused outright: PostgreSQL reserves the ``pg_`` schema prefix, and ``org`` would compose into
# a name a reader would take for an org's own schema.
_RESERVED_PREFIXES = ("pg_",)
_RESERVED_NAMES = ("org",)


class EnvironmentNameError(ValueError):
    """A name that cannot be an environment, with the reason it cannot."""


class EnvironmentPlaneError(RuntimeError):
    """The control plane cannot hold an environment at all.

    REQ-1488 makes an environment a SCHEMA of its own, which is what keeps one environment's rows
    out of another's read. A backend without schemas — SQLite, which is what demo mode runs — has
    nowhere to put a second environment, and provisioning one there would land both models in the
    org's single namespace. Refused rather than approximated.
    """


def is_env_name(value: str | None) -> bool:
    """Whether ``value`` could be an environment name at all — pattern only, no org context."""
    return bool(value) and ENV_NAME_PATTERN.fullmatch(value) is not None  # type: ignore[arg-type]


def org_schema(org_id: str, env: str | None = None, suffix: str = "") -> str:
    """The schema an org's ``env`` occupies, optionally one of its ``SCHEMA_SUFFIXES`` stores.

    ``prod`` and ``None`` both give the pre-environment name, which is what makes an org that
    holds only prod indistinguishable from an org that predates environments entirely.
    """
    if suffix not in SCHEMA_SUFFIXES:
        raise ValueError(f"unknown org store suffix: {suffix!r}")
    if env is None or env == PROD:
        return f"org_{org_id}{suffix}"
    return f"org_{org_id}_env_{env}{suffix}"


def env_schemas(org_id: str, env: str | None = None) -> list[str]:
    """Every store schema whose bytes belong to one environment of one org."""
    return [org_schema(org_id, env, suffix) for suffix in SCHEMA_SUFFIXES]


#: The longest name ``ENV_NAME_PATTERN`` itself admits, before any org-specific budget applies.
MAX_ENV_NAME = 32


def max_env_name_length(org_id: str) -> int:
    """How long an environment name this org may carry.

    Whichever binds first: the pattern's own 32 characters, or what is left of PostgreSQL's
    identifier limit after this org's id and the longest store suffix an environment derives.
    """
    longest = max(len(s) for s in SCHEMA_SUFFIXES)
    budget = MAX_IDENTIFIER_BYTES - len(org_schema(org_id, PROD, "")) - len("_env_") - longest
    return min(MAX_ENV_NAME, budget)


def validate_env_name(org_id: str, name: str) -> None:
    """Raise unless ``name`` is a creatable environment name for ``org_id`` (REQ-1523).

    Creation-time only. ``prod`` is refused here because it is never created — REQ-1487 gives it
    to the org at creation and forbids deleting or renaming it.
    """
    if name == PROD:
        raise EnvironmentNameError(
            f"{PROD!r} exists from the organization's creation and cannot be created again"
        )
    if not is_env_name(name):
        raise EnvironmentNameError(
            f"environment name {name!r} must be 2 to 32 characters of lowercase letters, digits "
            "and underscores, starting with a letter"
        )
    if name in _RESERVED_NAMES or name.startswith(_RESERVED_PREFIXES):
        raise EnvironmentNameError(f"environment name {name!r} is reserved")
    allowed = max_env_name_length(org_id)
    if len(name) > allowed:
        raise EnvironmentNameError(
            f"environment name {name!r} is {len(name)} characters; organization {org_id!r} affords "
            f"at most {allowed} before a derived schema name would exceed PostgreSQL's "
            f"{MAX_IDENTIFIER_BYTES}-byte identifier limit"
        )
