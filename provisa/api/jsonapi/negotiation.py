# Copyright (c) 2026 Kenneth Stott
# Canary: 3f1c7a52-8e4d-4b9a-a6c2-0d5e9b7f2a18
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""JSON:API 1.1 content negotiation and query parameter families (REQ-257).

Content negotiation (https://jsonapi.org/format/1.1/#content-negotiation-servers): a request
whose Content-Type is the JSON:API media type with a parameter other than ``ext`` or ``profile``,
or naming an extension, is answered 415; an Accept naming the JSON:API media type is answered 406
when every instance of it is modified by a parameter other than ``ext`` or ``profile``, or names
an extension. Provisa serves no extension.

Query parameters (https://jsonapi.org/format/1.1/#query-parameters-families): a parameter whose
family name is all lowercase a-z is reserved by the specification, so one the server does not
process is answered 400.
"""

# Requirements: REQ-257

from __future__ import annotations

import re

MEDIA_TYPE = "application/vnd.api+json"

#: The media type parameters JSON:API allows on its media type.
_ALLOWED_PARAMETERS = frozenset({"ext", "profile"})

#: The all-lowercase query parameter families a collection or resource read processes: the
#: specification's (sort, include, fields, page, filter), Provisa's aggregate (REQ-1359), params
#: (REQ-1986) and the acting role.
KNOWN_LOWERCASE_FAMILIES = frozenset(
    {"sort", "include", "fields", "page", "filter", "aggregate", "params", "role"}
)

_LOWERCASE = re.compile(r"^[a-z]+$")


def _media_ranges(header: str) -> list[tuple[str, dict[str, str]]]:
    """Each media range of an Accept or Content-Type header: its type, lowercased, and its
    parameters by lowercased name."""
    ranges: list[tuple[str, dict[str, str]]] = []
    for item in header.split(","):
        parts = [p.strip() for p in item.split(";")]
        if not parts[0]:
            continue
        params: dict[str, str] = {}
        for p in parts[1:]:
            if not p:
                continue
            name, _, value = p.partition("=")
            params[name.strip().lower()] = value.strip().strip('"')
        ranges.append((parts[0].lower(), params))
    return ranges


def _acceptable(params: dict[str, str]) -> bool:
    """An instance of the JSON:API media type this server serves: no parameter beyond ext and
    profile, and no extension (Provisa serves none)."""
    return set(params) <= _ALLOWED_PARAMETERS and not params.get("ext")


def content_type_refusal(content_type: str | None) -> str | None:
    """Why a request's Content-Type is answered 415, or None."""
    if not content_type:
        return None
    for kind, params in _media_ranges(content_type):
        if kind != MEDIA_TYPE:
            continue
        other = sorted(set(params) - _ALLOWED_PARAMETERS)
        if other:
            return f"Content-Type {MEDIA_TYPE} with parameter(s) {', '.join(other)}"
        if params.get("ext"):
            return f"Content-Type names extension(s) this server does not support: {params['ext']}"
    return None


def accept_refusal(accept: str | None) -> str | None:
    """Why a request's Accept is answered 406, or None. An Accept with no instance of the
    JSON:API media type is served when it accepts any type (``*/*``, ``application/*``)."""
    if not accept:
        return None
    ranges = _media_ranges(accept)
    ours = [params for kind, params in ranges if kind == MEDIA_TYPE]
    if ours:
        if any(_acceptable(p) for p in ours):
            return None
        return (
            f"every {MEDIA_TYPE} instance in Accept is modified by a parameter other than ext or "
            "profile, or names an extension this server does not support"
        )
    if any(kind in ("*/*", "application/*") for kind, _ in ranges):
        return None
    return f"This endpoint requires Accept: {MEDIA_TYPE}"


def unknown_reserved_parameter(names: list[str]) -> str | None:
    """The first query parameter whose family name is all lowercase a-z (reserved by the
    specification) and which a read does not process, or None."""
    for name in names:
        family = name.split("[", 1)[0]
        if _LOWERCASE.match(family) and family not in KNOWN_LOWERCASE_FAMILIES:
            return name
    return None
