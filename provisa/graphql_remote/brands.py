# Copyright (c) 2026 Kenneth Stott
# Canary: 7a2c9e14-5b3d-4f80-9c6a-1e8d4b7f2a35
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Branded sources carried by the remote GraphQL source (REQ-1923).

A brand is a named system whose API is GraphQL -- GitHub is the first. It adds to the generic
remote GraphQL source only what is particular to that system: where its endpoint is, how a
credential is presented and checked, its schema (public and the same for every customer, so it
ships with Provisa rather than being introspected at registration), and which of its errors mean
"this credential may not read that field". Everything else -- mapping the schema to tables,
reading them, governing them -- is the generic source's.

Adding a branded source registers no tables. Every table the schema offers is available, and
the steward registers the ones wanted, as for any source.
"""

# Requirements: REQ-1923, REQ-1875
from __future__ import annotations

import gzip
import json
from dataclasses import dataclass
from functools import lru_cache
from pathlib import Path

from provisa.graphql_remote.executor import ErrorPolicy
from provisa.graphql_remote.mapper import map_schema

_SCHEMA_DIR = Path(__file__).parent / "_known_schemas"

# Where a source row records its brand and namespace (``sources.federation_hints``).
BRAND_HINT = "brand"
NAMESPACE_HINT = "namespace"


@dataclass(frozen=True)
class Brand:
    id: str  # the value the source picker sends and the source row records
    label: str  # what the user sees
    url: str
    namespace: str  # default prefix of the source's table names
    verify_query: str  # a minimal query that succeeds only with a working credential
    # Error types the system returns, before running a query, for a field outside the
    # credential's grant. Such a field is left out of a table when it is registered
    # (provisa.graphql_remote.probe).
    refused_error_types: frozenset[str]
    # What the system's errors mean for a read (provisa.graphql_remote.executor.ErrorPolicy).
    error_policy: ErrorPolicy
    # How deep a nested object column is selected. A branded schema is large and densely
    # cross-referenced; the deployment-wide graphql_remote.max_object_depth is sized for a
    # customer's own schema and selects more than these systems will serve in one query.
    max_object_depth: int
    # How the system's message starts when it rejects a query as costing too much or being too
    # large. A system that prices a query and caps the price cannot serve a wide table whole:
    # a table whose query is so rejected at registration is refused with the system's message,
    # and the steward registers it with fewer columns. What a column costs depends on its kind
    # and on the page size, so the system's own answer is the measure, not a count of columns.
    too_complex_messages: tuple[str, ...] = ()

    # How far lists nested in an object column are followed, and how many of their items asked for.
    max_list_depth: int = 2
    max_list_items: int = 100
    field_overrides: dict[str, str] | None = None  # a brand's schema is mapped as shipped

    def auth(self, token: str) -> dict:
        return {"type": "bearer", "token": token}

    def schema(self) -> dict:
        return brand_schema(self.id)

    def table_index(self, namespace: str) -> dict[str, dict]:
        return _table_index(self.id, namespace)


@dataclass(frozen=True, eq=False)
class LiveSchema:
    """What a plain remote GraphQL source offers: the schema its endpoint answered with, mapped
    under the deployment's traversal settings. It declares none of what a brand declares about
    its remote's errors, so a table of such a source is registered as mapped."""

    label: str
    introspected: dict
    max_object_depth: int
    max_list_depth: int
    max_list_items: int
    refused_error_types: frozenset[str] = frozenset()
    too_complex_messages: tuple[str, ...] = ()
    # REQ-597: fields the steward reclassified between query and mutation.
    field_overrides: dict[str, str] | None = None

    def schema(self) -> dict:
        return self.introspected

    def table_index(self, namespace: str) -> dict[str, dict]:
        tables, _, _ = map_schema(
            self.introspected, namespace, "", "", field_overrides=self.field_overrides, only=set()
        )
        return {t["sql_name"]: t for t in tables}


# What a remote GraphQL source's tables are offered from: a brand's shipped schema, or a plain
# source's own.
SchemaOffer = Brand | LiveSchema


BRANDS: dict[str, Brand] = {
    "github": Brand(
        id="github",
        label="GitHub",
        url="https://api.github.com/graphql",
        namespace="gh",
        verify_query="query { viewer { login } }",
        refused_error_types=frozenset({"INSUFFICIENT_SCOPES"}),
        error_policy=ErrorPolicy(
            # FORBIDDEN: the token may not see this field on this row (a repository's
            # collaborators need push access to that repository). NOT_ORG_OWNED_REPO: the field
            # exists only for repositories an organization owns.
            row_field=frozenset({"FORBIDDEN", "NOT_ORG_OWNED_REPO"}),
            # A page whose rows together cost more than GitHub computes in one query.
            overload=frozenset({"RESOURCE_LIMITS_EXCEEDED"}),
            # GitHub's answer when a query runs past its ten-second limit carries no type.
            overload_messages=("Something went wrong while executing your query",),
        ),
        max_object_depth=0,
    ),
    "gitlab": Brand(
        id="gitlab",
        label="GitLab",
        url="https://gitlab.com/api/graphql",
        namespace="gl",
        # GitLab also serves anonymous callers and answers this with null, not an error, for a
        # token it does not recognize; the registration treats a null answer as a refusal.
        verify_query="query { currentUser { username } }",
        # GitLab answers a field the token may not see with null and no error.
        refused_error_types=frozenset(),
        error_policy=ErrorPolicy(),
        max_object_depth=0,
        # GitLab prices every query (200 points for an anonymous caller, 250 with a token) and
        # refuses one over 10,000 characters.
        too_complex_messages=("Query has complexity of", "Query too large"),
    ),
}


def brand_of(federation_hints: dict | None) -> Brand | None:
    """The brand a source row records, or None for a plain remote GraphQL source. A row naming
    a brand this build does not carry is an error, not a plain source."""
    brand_id = (federation_hints or {}).get(BRAND_HINT)
    if not brand_id:
        return None
    if brand_id not in BRANDS:
        raise ValueError(f"source records brand {brand_id!r}, which this build does not carry")
    return BRANDS[brand_id]


@lru_cache(maxsize=None)
def brand_schema(brand_id: str) -> dict:
    """The brand's introspected schema, as shipped (scripts/bake_graphql_brand_schema.py)."""
    path = _SCHEMA_DIR / f"{brand_id}.json.gz"
    if not path.exists():
        raise FileNotFoundError(f"schema for brand {brand_id!r} is not shipped: {path}")
    with gzip.open(path, "rt", encoding="utf-8") as fh:
        return json.load(fh)["schema"]


@lru_cache(maxsize=None)
def _table_index(brand_id: str, namespace: str) -> dict[str, dict]:
    """Every table the brand's schema offers, by sql name, without columns."""
    tables, _, _ = map_schema(brand_schema(brand_id), namespace, "", "", only=set())
    return {t["sql_name"]: t for t in tables}


def available_tables(offer: SchemaOffer, namespace: str) -> list[dict]:
    """Every table the source offers under ``namespace``: how it is read, without its columns."""
    return list(offer.table_index(namespace).values())


def table_spec(offer: SchemaOffer, namespace: str, sql_name: str) -> dict | None:
    """How one of the source's tables is read (root field, row path, required arguments, page
    arguments), without its columns. None when the schema offers no such table."""
    return offer.table_index(namespace).get(sql_name)


def map_table(
    offer: SchemaOffer, namespace: str, source_id: str, domain_id: str, sql_name: str
) -> dict:
    """One of the source's tables in full, columns included."""
    spec = table_spec(offer, namespace, sql_name)
    if spec is None:
        raise KeyError(f"{offer.label} offers no table {sql_name!r}")
    tables, _, _ = map_schema(
        offer.schema(),
        namespace,
        source_id,
        domain_id,
        max_object_depth=offer.max_object_depth,
        max_list_depth=offer.max_list_depth,
        max_list_items=offer.max_list_items,
        field_overrides=offer.field_overrides,
        only={spec["name"]},
    )
    return next(t for t in tables if t["name"] == spec["name"])


def offered_mutation_count(schema: dict) -> int:
    """How many commands a GraphQL schema offers: the fields of its mutation root."""
    root = (schema.get("mutationType") or {}).get("name")
    if root is None:
        return 0
    for t in schema.get("types") or []:
        if t.get("name") == root:
            return len(t.get("fields") or [])
    return 0
