# Copyright (c) 2026 Kenneth Stott
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Subject IRIs (REQ-2135): ``{public URL}/id/{domain}/{table}/{k1};{k2}``.

A query's SQL never builds an IRI. It returns a subject as its table's identity and its key values
(a node term, :mod:`provisa.gql.terms`), and the answer's writer builds the IRI here, each segment
percent-encoded (RFC 3986), so a key holding ``/``, ``;``, a space or any non-ASCII character still
makes a valid IRI. A query that names a subject by its IRI has it parsed back here, at compile time,
to the table and the key values it was built from.
"""

# Requirements: REQ-2135

from __future__ import annotations

from dataclasses import dataclass
from urllib.parse import quote, unquote

from provisa.core import settings_registry
from provisa.gql.translator_types import CypherTranslateError

# Every character but RFC 3986's unreserved ones is encoded, so '/' and ';' inside a value never
# read as the separators around it.
_SAFE = ""


class IriBaseUnset(CypherTranslateError):
    """A subject's IRI was asked for and server.public_url, its base, is unset."""

    def __init__(self) -> None:
        super().__init__(
            "A subject's IRI starts with the deployment's public address, and server.public_url "
            "is not set; set it in Deployment settings"
        )


@dataclass(frozen=True)
class SubjectRef:
    """A subject as its IRI names it: its table's domain and name, and its key values as text."""

    domain: str
    table: str
    keys: tuple[str, ...]


def iri_base() -> str:
    """The base every subject IRI starts with: server.public_url, without a trailing '/'."""
    base = settings_registry.value("server.public_url")
    if not base:
        raise IriBaseUnset()
    return str(base).rstrip("/")


def _segment(value: object) -> str:
    return quote(str(value), safe=_SAFE)


def subject_iri(base: str, domain: str, table: str, keys: list | tuple) -> str:
    """The IRI of the row of ``domain``.``table`` with these key values, in pk_columns order."""
    if not keys:
        raise ValueError(f"A subject of {domain}.{table} needs its key values")
    key = ";".join(_segment(k) for k in keys)
    return f"{base}/id/{_segment(domain)}/{_segment(table)}/{key}"


def parse_subject_iri(base: str, iri: str) -> SubjectRef | None:
    """The subject a subject IRI names, or None when ``iri`` is not one under ``base``."""
    prefix = f"{base}/id/"
    if not iri.startswith(prefix):
        return None
    parts = iri[len(prefix) :].split("/")
    if len(parts) != 3 or not all(parts):
        return None
    domain, table, key = parts
    return SubjectRef(
        domain=unquote(domain),
        table=unquote(table),
        keys=tuple(unquote(k) for k in key.split(";")),
    )
