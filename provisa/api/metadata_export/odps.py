# Copyright (c) 2026 Kenneth Stott
# Canary: 3b8e1d74-6a29-4f05-9c13-0e7d5a2f8b46
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A data product as the open standards write it (REQ-2118): an ODPS 1.0 document for the product,
naming an ODCS 3.1 contract for each of its members.

A catalog that imports either standard (Collibra, DataHub, OpenMetadata, Alation) reads the
product from these, with no adapter of its own. Both are built from the same snapshot every other
target is published from, so they say what the catalogs are told:

* the product's ODPS fields -- purpose, limitations and usage as its description, its status,
  version, domain, team and support;
* an output port per member, each its declared ODCS contract (REQ-2158): the columns it promises
  with their types and descriptions, the governance tags on them (masked, hidden from some roles,
  row-filtered; never a rule body), its terms, its declared service levels beside its freshness
  (the cache_ttl it is kept at) and the quality checks registered against it.

A contract has its own version and status, as ODPS versions each port by its contract. The
product's version is its own; a product with no version says so -- ``unversioned`` -- since ODPS
requires the field and inventing a number would claim a release that never happened.
"""

# Requirements: REQ-2118

from __future__ import annotations

import re
from typing import TYPE_CHECKING, Any

from provisa.core.cache_ttl import NEVER_EXPIRES

if TYPE_CHECKING:
    from provisa.api.metadata_export.model import (
        DataProductAsset,
        MetadataSnapshot,
        TableAsset,
    )

ODPS_API_VERSION = "v1.0.0"
ODCS_API_VERSION = "v3.1.0"
#: A product (or its member's contract) with no version of its own.
UNVERSIONED = "unversioned"

_EMAIL = re.compile(r"^[^@\s]+@[^@\s]+\.[^@\s]+$")

# A column's declared type as ODCS's logical types name it. A type outside these is published by
# its physical name alone: guessing a logical type would misstate it.
_LOGICAL = (
    (re.compile(r"\[\]$|^array"), "array"),
    (re.compile(r"^(small|big)?int(eger)?\d*$|^(tiny|medium)int$|^serial|^long$"), "integer"),
    (re.compile(r"^(numeric|decimal|number|double|float|real)"), "number"),
    (re.compile(r"^bool"), "boolean"),
    (re.compile(r"^timestamp|^datetime"), "timestamp"),
    (re.compile(r"^date$"), "date"),
    (re.compile(r"^time(\b|$)"), "time"),
    (re.compile(r"^(json|jsonb|object|struct|map|variant)"), "object"),
    (re.compile(r"^(text|string|varchar|char|character|uuid|citext|nvarchar|nchar)"), "string"),
)


def logical_type(data_type: str) -> str | None:
    """The ODCS logical type of a declared column type, or None when it has none to name."""
    lowered = data_type.strip().lower()
    return next((logical for pattern, logical in _LOGICAL if pattern.search(lowered)), None)


def product_status(product: "DataProductAsset") -> str:
    """The product's status; one with none set is ``active`` when published and ``draft`` when not
    (``DataProduct.publish``), the same reading every listing gives it."""
    if product.status:
        return product.status
    return "active" if product.publish else "draft"


def contract_id(table: "TableAsset") -> str:
    """A member's contract id: its Provisa URN, unique and stable across renames of nothing else."""
    return table.semantic_uri


def _support(product: "DataProductAsset") -> list[dict[str, Any]]:
    channels: list[dict[str, Any]] = []
    contact = product.support_contact
    if contact:
        email = bool(_EMAIL.match(contact))
        channels.append(
            {
                "channel": "support",
                "url": f"mailto:{contact}" if email else contact,
                "tool": "email" if email else "other",
                **({"description": product.support} if product.support else {}),
            }
        )
    elif product.support:
        channels.append({"channel": "support", "description": product.support})
    return channels


def _team(product: "DataProductAsset") -> dict[str, Any] | None:
    """The product's team as its roles: Provisa names roles, not the people holding them."""
    roles = [
        (role, purpose)
        for role, purpose in (
            (product.owner.id if product.owner is not None else None, "owner"),
            (product.team_role, "team"),
        )
        if role
    ]
    if not roles:
        return None
    return {
        "name": product.team_role or roles[0][0],
        "customProperties": [
            {"property": f"{purpose}Role", "value": role} for role, purpose in roles
        ],
    }


def _description(product: "DataProductAsset") -> dict[str, str]:
    return {
        k: v
        for k, v in (
            ("purpose", product.description),
            ("limitations", product.limitations),
            ("usage", product.usage),
        )
        if v
    }


def _custom_properties(product: "DataProductAsset") -> list[dict[str, Any]]:
    props = [{"property": k, "value": v} for k, v in sorted(product.custom_properties.items())]
    if product.documentation_url:
        props.append({"property": "documentation", "value": product.documentation_url})
    return props


def _members(snapshot: "MetadataSnapshot", product: "DataProductAsset") -> list["TableAsset"]:
    by_fqn = {table.ref.fqn(): table for table in snapshot.tables}
    return [by_fqn[member.fqn()] for member in product.members]


def _ports(snapshot: "MetadataSnapshot", product: "DataProductAsset") -> list["TableAsset"]:
    """The product's output ports: each member with a contract (REQ-2158). A checker table the
    product owns is no output of it, and has none."""
    return [table for table in _members(snapshot, product) if table.contract is not None]


def odps_document(snapshot: "MetadataSnapshot", product: "DataProductAsset") -> dict[str, Any]:
    """The product's ODPS 1.0 document: its fields, and an output port per member naming the
    member's ODCS contract."""
    version = product.version or UNVERSIONED
    document: dict[str, Any] = {
        "apiVersion": ODPS_API_VERSION,
        "kind": "DataProduct",
        "id": product.semantic_uri,
        "name": product.name,
        "version": version,
        "status": product_status(product),
        "domain": product.domain_id,
        "outputPorts": [
            {
                "name": table.name,
                "version": table.contract.version,
                "type": "view" if table.is_view else "table",
                "contractId": contract_id(table),
                **({"description": table.description} if table.description else {}),
            }
            for table in _ports(snapshot, product)
            if table.contract is not None
        ],
    }
    if description := _description(product):
        document["description"] = description
    if support := _support(product):
        document["support"] = support
    if team := _team(product):
        document["team"] = team
    if custom := _custom_properties(product):
        document["customProperties"] = custom
    return document


def _service_levels(table: "TableAsset") -> list[dict[str, Any]]:
    """The member's freshness, then the service levels its contract declares (REQ-2158)."""
    assert table.contract is not None  # only an output port has a contract document
    declared = [
        {k: v for k, v in sla.model_dump().items() if v is not None} for sla in table.contract.sla
    ]
    return _freshness(table) + declared


def _freshness(table: "TableAsset") -> list[dict[str, Any]]:
    """The member's freshness as an ODCS SLA: how long a read may trail its source."""
    if table.cache_ttl is None:
        return []
    if table.cache_ttl == NEVER_EXPIRES:
        return [
            {
                "property": "frequency",
                "value": None,
                "description": "Loaded once; never refreshed",
            }
        ]
    return [
        {
            "property": "latency",
            "value": table.cache_ttl,
            "unit": "s",
            "description": "A read may trail its source by at most this long",
        }
    ]


def _quality(snapshot: "MetadataSnapshot", table: "TableAsset") -> list[dict[str, Any]]:
    """The quality checks registered against the member, as the checker that runs them wrote
    them."""
    prefix = table.ref.fqn()
    return [
        {
            "type": "custom",
            "engine": "great-expectations"
            if assertion.checker == "great_expectations"
            else assertion.checker,
            "name": assertion.check_type,
            "implementation": assertion.definition,
            "severity": assertion.severity,
        }
        for assertion in snapshot.assertions
        if assertion.asset.fqn() == prefix or assertion.asset.fqn().startswith(f"{prefix}.")
    ]


def _column_tags(snapshot: "MetadataSnapshot") -> dict[str, list[str]]:
    """Each governed asset's governance signals -- that a rule applies, never the rule."""
    tags: dict[str, list[str]] = {}
    for tag in snapshot.governance_tags:
        tags.setdefault(tag.asset.fqn(), []).append(tag.signal.value)
    return {fqn: sorted(set(signals)) for fqn, signals in tags.items()}


def odcs_contract(
    snapshot: "MetadataSnapshot", product: "DataProductAsset", table: "TableAsset"
) -> dict[str, Any]:
    """A member's ODCS 3.1 contract as it is declared (REQ-2158): the columns it promises, their
    governance tags, its terms, its service levels and its quality checks."""
    declared = table.contract
    assert declared is not None  # only an output port has a contract document
    tags = _column_tags(snapshot)
    by_name = {column.name: column for column in table.columns}
    properties = []
    for promised in declared.columns:
        column = by_name.get(promised.name)
        prop: dict[str, Any] = {"name": promised.name, "physicalType": promised.type}
        if logical := logical_type(promised.type):
            prop["logicalType"] = logical
        if description := promised.description or (column.description if column else None):
            prop["description"] = description
        if promised.required:
            prop["required"] = True
        if promised.name in table.primary_key:
            prop["primaryKey"] = True
            prop["primaryKeyPosition"] = table.primary_key.index(promised.name) + 1
        if column is not None and (column_tags := tags.get(column.ref.fqn())):
            prop["tags"] = column_tags
        properties.append(prop)
    schema_object: dict[str, Any] = {
        "name": table.name,
        "logicalType": "object",
        "physicalType": "view" if table.is_view else "table",
        "properties": properties,
    }
    if table.description:
        schema_object["description"] = table.description
    if table_tags := tags.get(table.ref.fqn()):
        schema_object["tags"] = table_tags
    if quality := _quality(snapshot, table):
        schema_object["quality"] = quality
    contract: dict[str, Any] = {
        "apiVersion": ODCS_API_VERSION,
        "kind": "DataContract",
        "id": contract_id(table),
        "name": table.name,
        "version": declared.version,
        "status": declared.status,
        "domain": product.domain_id,
        "schema": [schema_object],
        "customProperties": [{"property": "dataProduct", "value": product.semantic_uri}],
    }
    terms = declared.terms
    description = {
        k: v
        for k, v in (
            ("purpose", declared.description),
            ("usage", terms.usage),
            ("limitations", terms.limitations),
        )
        if v
    }
    if description:
        contract["description"] = description
    if terms.notice_period:
        contract["customProperties"].append(
            {"property": "noticePeriod", "value": terms.notice_period}
        )
    if sla := _service_levels(table):
        contract["slaProperties"] = sla
    if support := _support(product):
        contract["support"] = support
    if team := _team(product):
        contract["team"] = team
    return contract


def product_documents(
    snapshot: "MetadataSnapshot", product: "DataProductAsset"
) -> tuple[dict[str, Any], dict[str, dict[str, Any]]]:
    """``(the product's ODPS document, {member: its ODCS contract})``, each member by its address
    (source, schema, table): two members of one name in different sources stay apart."""
    return odps_document(snapshot, product), {
        table.ref.fqn(): odcs_contract(snapshot, product, table)
        for table in _ports(snapshot, product)
    }
