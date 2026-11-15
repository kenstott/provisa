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
naming an ODCS 3.1 contract for each of its offers on each of its members.

A catalog that imports either standard (Collibra, DataHub, OpenMetadata, Alation) reads the
product from these, with no adapter of its own. Both are built from the same snapshot every other
target is published from, so they say what the catalogs are told:

* the product's ODPS fields -- purpose, limitations and usage as its description, its status,
  version, domain, team and support;
* an output port per offer and member, each the ODCS contract the offer gives on it (REQ-2180):
  its schema, the member's columns its role reads with their types and descriptions, the
  governance tags on them (masked, hidden from some roles, row-filtered; never a rule body), the
  terms the product writes once for all its contracts, its freshness (the product's, else the
  cache_ttl the member is kept at) and the quality checks registered against it;
* an input port per input another product promises, naming the contract that product's default
  offer gives on it;
* each field's grounded term (REQ-2238), as an authoritative definition of type ``semantics``
  naming the term's URI, name and definition, and each output port the same links for its
  member's columns. Only a term an agent may see is named: approved, or a defined draft on a path
  holding an approved term, marked draft.

REQ-2180: access to a product is by offers, and its contracts are derived from them. Each offer's
contract on each dataset is an output port, and its ODCS document is the dataset's as the offer's
role sees it (:func:`offer_contract`); the document lists the offers (``customProperties``,
``offers``). Nothing of a contract is written by hand but what all of a product's contracts share,
written once on the product. The offers are the snapshot's (``MetadataSnapshot.offers``).

A contract has its own version and status, as ODPS versions each port by its contract. The
product's version is its own; a product with no version says so -- ``unversioned`` -- since ODPS
requires the field and inventing a number would claim a release that never happened.
"""

# Requirements: REQ-2118, REQ-2238

from __future__ import annotations

import json
import re
from collections.abc import Sequence
from typing import TYPE_CHECKING, Any, Literal

from provisa.api.metadata_export.model import OfferExport
from provisa.core.cache_ttl import NEVER_EXPIRES
from provisa.core.models import DEFAULT_OFFER, TERMS_OF_USE

if TYPE_CHECKING:
    from datetime import datetime

    from provisa.core.models import OfferContract

    from provisa.api.metadata_export.model import (
        ColumnAsset,
        DataProductAsset,
        DataQualityAssertion,
        GroundedTerm,
        MetadataSnapshot,
        TableAsset,
    )

ODPS_API_VERSION = "v1.0.0"
ODCS_API_VERSION = "v3.1.0"
#: A product with no version of its own.
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


#: REQ-1634 STATUS: Provisa's lifecycle in ODPS 1.0's status vocabulary (draft, active,
#: deprecated, retired; the schema's examples, not a closed enum).
_ODPS_STATUS = {
    "draft": "draft",
    "live": "active",
    "deprecated": "deprecated",
    "retired": "retired",
}


def product_status(product: "DataProductAsset") -> str:
    """The product's ODPS status."""
    return _ODPS_STATUS[product.status]


def contract_id(table: "TableAsset") -> str:
    """A member's URN, unique and stable across renames of nothing else: what each offer's
    contract id on it extends (:func:`offer_contract_id`)."""
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
    """The product's team in ODPS's own fields: named for its team role (else its owner role),
    each member a holder of one of the two roles by address, with that role as its role."""
    owner_role = product.owner.id if product.owner is not None else None
    if not (owner_role or product.team_role):
        return None
    members = [
        {"username": email, "role": role}
        for role, emails in (("owner", product.owner_emails), ("team", product.team_emails))
        for email in emails
    ]
    named = "; ".join(
        f"{purpose} role {role}"
        for purpose, role in (("owner", owner_role), ("team", product.team_role))
        if role
    )
    return {
        "name": product.team_role or owner_role,
        "description": named[0].upper() + named[1:],
        **({"members": members} if members else {}),
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


def _custom_properties(
    product: "DataProductAsset", without_contract: list[str], inputs_without_contract: list[str]
) -> list[dict[str, Any]]:
    props: list[dict[str, Any]] = [
        {"property": k, "value": v} for k, v in sorted(product.custom_properties.items())
    ]
    # A member no offer gives a contract on, or an input no product's default offer gives one on,
    # is named, never left out silently: it is no port, since ODPS versions a port by its
    # contract.
    if without_contract:
        props.append({"property": "membersWithoutContract", "value": without_contract})
    if inputs_without_contract:
        props.append({"property": "inputsWithoutContract", "value": inputs_without_contract})
    return props


def _authoritative_definitions(product: "DataProductAsset") -> list[dict[str, str]]:
    """The product's page, where it is documented and edited (REQ-1659)."""
    if not product.documentation_url:
        return []
    return [
        {
            "type": "businessDefinition",
            "url": product.documentation_url,
            "description": "The data product's page",
        }
    ]


#: REQ-2238: the authoritative definition type naming a field's grounded term.
SEMANTICS = "semantics"


def term_definition(term: "GroundedTerm") -> dict[str, str]:
    """A field's grounded term as an authoritative definition of type semantics (REQ-2238): its
    URI, and its name and definition, a draft marked so."""
    label = term.name if term.status == "approved" else f"{term.name} (draft)"
    return {
        "type": SEMANTICS,
        "url": term.semantic_uri,
        "description": f"{label}: {term.definition}" if term.definition else str(label),
    }


def _port_terms(snapshot: "MetadataSnapshot", table: "TableAsset") -> list[dict[str, str]]:
    """The grounded terms of a member's columns, each once, as its output port's authoritative
    definitions (REQ-2238): the links its contract's fields carry."""
    by_column = snapshot.column_terms()
    terms = {
        term.term_id: term
        for column in table.columns
        if (term := by_column.get(column.ref.fqn())) is not None
    }
    return [term_definition(term) for term in sorted(terms.values(), key=lambda t: str(t.name))]


def _management_ports(product: "DataProductAsset") -> list[dict[str, str]]:
    """The endpoints that observe and control the product: its page, its lineage and the admin
    GraphQL API that edits it."""
    ports = (
        ("page", "discoverability", product.documentation_url, "The data product's page"),
        ("lineage", "observability", product.lineage_url, "The data product's lineage"),
        ("admin", "control", product.admin_api_url, "The admin GraphQL API that manages it"),
    )
    return [
        {"name": name, "content": content, "type": "rest", "url": url, "description": text}
        for name, content, url, text in ports
        if url
    ]


def _tags(snapshot: "MetadataSnapshot", product: "DataProductAsset") -> list[str]:
    """The model tags on the product's member tables, each once."""
    members = {member.parts for member in product.members}
    return sorted(
        {
            tag.tag_id
            for tag in snapshot.model_tags
            if tag.asset is not None and tag.asset.parts in members
        }
    )


def _checker_tables(snapshot: "MetadataSnapshot") -> set[tuple[str, ...]]:
    """The tables a check's outcomes land in: a member by product, never an output of it."""
    return {assertion.results_table.parts for assertion in snapshot.assertions}


def _upstream(snapshot: "MetadataSnapshot") -> dict[tuple[str, ...], set[tuple[str, ...]]]:
    """Each table's direct upstream tables, from the snapshot's column lineage."""
    upstream: dict[tuple[str, ...], set[tuple[str, ...]]] = {}
    for edge in snapshot.lineage:
        down, up = edge.downstream.parts[:3], edge.upstream.parts[:3]
        if down != up:
            upstream.setdefault(down, set()).add(up)
    return upstream


def _inputs(
    snapshot: "MetadataSnapshot", product: "DataProductAsset", start: set[tuple[str, ...]]
) -> list["TableAsset"]:
    """The nearest inputs of ``start`` (member tables): upstream through views and the
    product's own members to the first table that is neither -- another product's member, or a
    table read from a source. A member is never an input of its own product; a view that is
    another product's member is an input, as a table is."""
    by_parts = {table.ref.parts: table for table in snapshot.tables}
    members = {member.parts for member in product.members}
    upstream = _upstream(snapshot)
    found: dict[tuple[str, ...], "TableAsset"] = {}
    seen: set[tuple[str, ...]] = set()
    pending = list(start)
    while pending:
        for parts in upstream.get(pending.pop(), ()):
            if parts in seen:
                continue
            seen.add(parts)
            table = by_parts[parts]
            if parts in members or (table.is_view and not table.product_ids):
                pending.append(parts)
            else:
                found[parts] = table
    return [found[parts] for parts in sorted(found)]


def _expectations(
    snapshot: "MetadataSnapshot", product: "DataProductAsset", table: "TableAsset"
) -> list[dict[str, Any]]:
    """The product's own checks on an input (REQ-2157): its expectations of it."""
    return [
        {
            "checkType": assertion.check_type,
            "definition": assertion.definition,
            "severity": assertion.severity,
            **({"column": assertion.asset.parts[3]} if len(assertion.asset.parts) > 3 else {}),
            "outcome": assertion.outcome.status,
        }
        for assertion in snapshot.assertions
        if product.id in assertion.owner_product_ids
        and assertion.asset.parts[:3] == table.ref.parts
    ]


def _input_contracts(
    snapshot: "MetadataSnapshot", table: "TableAsset"
) -> list[tuple[str, OfferExport]]:
    """``(product id, its default offer)`` for each product ``table`` is a member of whose default
    offer gives a contract on it (REQ-2180): what a product reading the table reads it under."""
    found: list[tuple[str, OfferExport]] = []
    for product_id in table.product_ids:
        for offer in snapshot.offers.get(product_id, ()):
            if offer.name == DEFAULT_OFFER.name and table.ref.fqn() in offer.contracts:
                found.append((product_id, offer))
    return found


def _input_port(
    snapshot: "MetadataSnapshot",
    product: "DataProductAsset",
    table: "TableAsset",
    upstream: str,
    offer: OfferExport,
    *,
    named: bool,
) -> dict[str, Any]:
    """An input another product promises: the contract its default offer gives on it, and this
    product's expectations of it. ``named``: the input is read under more than one product's
    contract, so the port names the product too."""
    expected = _expectations(snapshot, product, table)
    return {
        "name": f"{table.name} ({upstream})" if named else table.name,
        "version": offer.contracts[table.ref.fqn()].version,
        "contractId": offer_contract_id(offer, table),
        **(
            {"customProperties": [{"property": "expectations", "value": expected}]}
            if expected
            else {}
        ),
    }


def _input_ports(
    snapshot: "MetadataSnapshot", product: "DataProductAsset", table: "TableAsset"
) -> list[dict[str, Any]]:
    contracts = _input_contracts(snapshot, table)
    return [
        _input_port(snapshot, product, table, upstream, offer, named=len(contracts) > 1)
        for upstream, offer in contracts
    ]


def _members(snapshot: "MetadataSnapshot", product: "DataProductAsset") -> list["TableAsset"]:
    by_fqn = {table.ref.fqn(): table for table in snapshot.tables}
    return [by_fqn[member.fqn()] for member in product.members]


def _offers_of(snapshot: "MetadataSnapshot", product: "DataProductAsset") -> Sequence[OfferExport]:
    return snapshot.offers.get(product.id, ())


def odps_document(snapshot: "MetadataSnapshot", product: "DataProductAsset") -> dict[str, Any]:
    """The product's ODPS 1.0 document (REQ-2118): its fields; an input port per input another
    product promises, naming the contract that product's default offer gives on it and the
    product's expectations of it; an output port per offer and dataset naming the ODCS contract
    the offer gives on it (REQ-2180) and the input contracts it reads; its offers; and the
    endpoints that observe and control it."""
    offers = _offers_of(snapshot, product)
    members = _members(snapshot, product)
    checkers = _checker_tables(snapshot)
    inputs = _inputs(snapshot, product, {table.ref.parts for table in members})
    document: dict[str, Any] = {
        "apiVersion": ODPS_API_VERSION,
        "kind": "DataProduct",
        "id": product.semantic_uri,
        "name": product.name,
        "version": product.version or UNVERSIONED,
        "status": product_status(product),
        "domain": product.domain_id,
        "tenant": snapshot.org_id,
        "inputPorts": [port for table in inputs for port in _input_ports(snapshot, product, table)],
        "outputPorts": [
            _offer_port(snapshot, product, table, offer)
            for offer in offers
            for table in _offered(snapshot, product, offer)
        ],
    }
    if product.created_at:
        document["productCreatedTs"] = product.created_at
    if description := _description(product):
        document["description"] = description
    if definitions := _authoritative_definitions(product):
        document["authoritativeDefinitions"] = definitions
    if tags := _tags(snapshot, product):
        document["tags"] = tags
    if support := _support(product):
        document["support"] = support
    if team := _team(product):
        document["team"] = team
    if management := _management_ports(product):
        document["managementPorts"] = management
    offered = {fqn for offer in offers for fqn in offer.contracts}
    without_contract = [
        table.name
        for table in members
        if table.ref.fqn() not in offered and table.ref.parts not in checkers
    ]
    inputs_without = [table.name for table in inputs if not _input_contracts(snapshot, table)]
    if custom := _custom_properties(product, without_contract, inputs_without):
        document["customProperties"] = custom
    if offers:
        document.setdefault("customProperties", []).append(
            {
                "property": "offers",
                "value": [_offer_summary(snapshot, product, offer) for offer in offers],
            }
        )
    return document


def _freshness(cache_ttl: int | None) -> list[dict[str, Any]]:
    """A contract's freshness as an ODCS SLA: how long a read may trail its source."""
    if cache_ttl is None:
        return []
    if cache_ttl == NEVER_EXPIRES:
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
            "value": cache_ttl,
            "unit": "s",
            "description": "A read may trail its source by at most this long",
        }
    ]


#: REQ-2118: an ODCS library metric's data-quality dimension.
_DIMENSION = {
    "nullValues": "completeness",
    "missingValues": "completeness",
    "rowCount": "completeness",
    "duplicateValues": "uniqueness",
    "invalidValues": "conformity",
}
#: A Soda check type that is an ODCS library metric, and what it holds to when it names no
#: threshold (Soda's own default: none missing, duplicate or invalid; at least one row).
_SODA_METRIC = {
    "missing": ("missingValues", {"mustBe": 0}),
    "duplicate": ("duplicateValues", {"mustBe": 0}),
    "invalid": ("invalidValues", {"mustBe": 0}),
    "row_count": ("rowCount", {"mustBeGreaterThan": 0}),
}
#: Soda's threshold operators by their ODCS names.
_SODA_OPERATOR = {
    "must_be": "mustBe",
    "must_not_be": "mustNotBe",
    "must_be_greater_than": "mustBeGreaterThan",
    "must_be_greater_than_or_equal": "mustBeGreaterOrEqualTo",
    "must_be_less_than": "mustBeLessThan",
    "must_be_less_than_or_equal": "mustBeLessOrEqualTo",
}


def _camel(name: str) -> str:
    head, *rest = name.split("_")
    return head + "".join(part.title() for part in rest)


#: Soda's threshold metric: the units its comparator reads, by ODCS unit.
_SODA_UNIT = {"count": "rows", "percent": "percent"}


def _soda_operator(threshold: Any) -> dict[str, Any] | None:
    """A Soda threshold as one ODCS operator and its unit, or None when it is not one. Its level
    is the check's severity, published beside it."""
    if not isinstance(threshold, dict):
        return None
    rest = {k: v for k, v in threshold.items() if k not in ("level", "metric")}
    unit = threshold.get("metric")
    if len(rest) != 1 or (unit is not None and unit not in _SODA_UNIT):
        return None
    ((key, value),) = rest.items()
    units = {"unit": _SODA_UNIT[unit]} if unit is not None else {}
    if key in _SODA_OPERATOR:
        return {_SODA_OPERATOR[key]: value, **units}
    if (
        key == "must_be_between"
        and isinstance(value, dict)
        and set(value) == {"greater_than_or_equal", "less_than_or_equal"}
    ):
        return {
            "mustBeBetween": [value["greater_than_or_equal"], value["less_than_or_equal"]],
            **units,
        }
    return None


def _soda_library(check_type: str, definition: str) -> dict[str, Any] | None:
    import yaml

    if check_type not in _SODA_METRIC:
        return None
    metric, default = _SODA_METRIC[check_type]
    body = yaml.safe_load(definition) if definition else None
    body = dict(body) if isinstance(body, dict) else {}
    threshold = body.pop("threshold", None)
    # A threshold naming only its level holds the check to Soda's default.
    comparing = isinstance(threshold, dict) and set(threshold) - {"level", "metric"}
    operator = _soda_operator(threshold) if comparing else dict(default)
    if operator is None:
        return None
    arguments = {_camel(k): v for k, v in body.items()}
    return {"metric": metric, **operator, **({"arguments": arguments} if arguments else {})}


def _gx_library(check_type: str, definition: str) -> dict[str, Any] | None:
    kwargs = {k: v for k, v in json.loads(definition or "{}").items() if k != "column"}
    if check_type in ("expect_column_values_to_not_be_null", "expect_column_values_to_be_unique"):
        if kwargs:  # a 'mostly' tolerance is GX's own, with no library operator to hold it
            return None
        metric = "nullValues" if check_type.endswith("null") else "duplicateValues"
        return {"metric": metric, "mustBe": 0}
    if check_type == "expect_column_values_to_be_in_set" and set(kwargs) == {"value_set"}:
        return {
            "metric": "invalidValues",
            "mustBe": 0,
            "arguments": {"validValues": kwargs["value_set"]},
        }
    if check_type == "expect_table_row_count_to_equal" and set(kwargs) == {"value"}:
        return {"metric": "rowCount", "mustBe": kwargs["value"]}
    if check_type == "expect_table_row_count_to_be_between":
        low, high = kwargs.get("min_value"), kwargs.get("max_value")
        if set(kwargs) - {"min_value", "max_value"}:
            return None
        if low is not None and high is not None:
            return {"metric": "rowCount", "mustBeBetween": [low, high]}
        if low is not None:
            return {"metric": "rowCount", "mustBeGreaterOrEqualTo": low}
        if high is not None:
            return {"metric": "rowCount", "mustBeLessOrEqualTo": high}
    return None


def _engine(checker: str) -> str:
    return "great-expectations" if checker == "great_expectations" else checker


def _quality_rule(assertion: "DataQualityAssertion") -> dict[str, Any]:
    """One check as an ODCS quality rule (REQ-2118): typed by the ODCS library, with its
    dimension, where the check is one of its metrics and its threshold one of its operators;
    custom, as the checker that runs it wrote it, otherwise."""
    library = (
        _gx_library(assertion.check_type, assertion.definition)
        if assertion.checker == "great_expectations"
        else _soda_library(assertion.check_type, assertion.definition)
    )
    if library is None:
        return {
            "type": "custom",
            "engine": _engine(assertion.checker),
            "name": assertion.check_type,
            "implementation": assertion.definition,
            "severity": assertion.severity,
        }
    return {
        "type": "library",
        **library,
        "dimension": _DIMENSION[library["metric"]],
        "name": assertion.check_type,
        "severity": assertion.severity,
        "customProperties": [
            {"property": "engine", "value": _engine(assertion.checker)},
            {"property": "implementation", "value": assertion.definition},
        ],
    }


def _promises(
    snapshot: "MetadataSnapshot", product: "DataProductAsset", table: "TableAsset"
) -> list["DataQualityAssertion"]:
    """The product's own checks on the member: its promises (REQ-2157). A check another product
    owns, or one no product owns, is no promise of this one, and is in none of its contracts."""
    return [
        assertion
        for assertion in snapshot.assertions
        if product.id in assertion.owner_product_ids
        and assertion.asset.parts[:3] == table.ref.parts
    ]


def _column_tags(snapshot: "MetadataSnapshot") -> dict[str, list[str]]:
    """Each governed asset's governance signals -- that a rule applies, never the rule."""
    tags: dict[str, list[str]] = {}
    for tag in snapshot.governance_tags:
        tags.setdefault(tag.asset.fqn(), []).append(tag.signal.value)
    return {fqn: sorted(set(signals)) for fqn, signals in tags.items()}


_RESTRICTING = {"masked", "visibility_restricted"}


def _sensitive_columns(snapshot: "MetadataSnapshot") -> set[tuple[str, ...]]:
    """The columns carrying a tag whose Sensitive data option is set (REQ-1943)."""
    return {
        tag.asset.parts
        for tag in snapshot.model_tags
        if tag.asset is not None and tag.tag_id in snapshot.sensitive_tags
    }


def _classification(sensitive: bool, signals: list[str]) -> str | None:
    """A column's ODCS classification from its sensitivity and governance signals: confidential
    when it carries sensitive data, restricted when it is masked or hidden from some readers,
    and none to state otherwise."""
    if sensitive:
        return "confidential"
    if _RESTRICTING & set(signals):
        return "restricted"
    return None


_INTEGER_FORMAT = {
    "tinyint": "i8",
    "smallint": "i16",
    "int2": "i16",
    "int": "i32",
    "integer": "i32",
    "int4": "i32",
    "bigint": "i64",
    "int8": "i64",
    "long": "i64",
}
_NUMBER_FORMAT = {
    "real": "f32",
    "float4": "f32",
    "double": "f64",
    "double precision": "f64",
    "float8": "f64",
}
_SIZED = re.compile(r"^([a-z][a-z ]*?)\s*\(\s*(\d+)\s*(?:,\s*(\d+)\s*)?\)")
_LENGTHED = {"varchar", "character varying", "char", "character", "nvarchar", "nchar"}
_DECIMAL = {"numeric", "decimal", "number"}


def logical_type_options(data_type: str, subtype: str | None) -> dict[str, Any]:
    """A column type's ODCS logicalTypeOptions (REQ-2118): a string's length and format (its
    subtype, a uuid), an integer's or float's width, a timestamp's time zone. ODCS 3.1 names no
    precision or scale, so a decimal's are stated in its own terms: its scale as the multiple
    every value is of, its precision as the bound its magnitude stays under."""
    lowered = data_type.strip().lower()
    logical = logical_type(lowered)
    sized = _SIZED.match(lowered)
    name = sized.group(1).strip() if sized else lowered
    options: dict[str, Any] = {}
    if logical == "string":
        if sized and name in _LENGTHED:
            options["maxLength"] = int(sized.group(2))
        if fmt := subtype or ("uuid" if name == "uuid" else None):
            options["format"] = fmt
    elif logical == "integer" and name in _INTEGER_FORMAT:
        options["format"] = _INTEGER_FORMAT[name]
    elif logical == "number" and sized and name in _DECIMAL:
        precision, scale = int(sized.group(2)), int(sized.group(3) or 0)
        bound = 10 ** (precision - scale)
        options.update(
            multipleOf=10**-scale if scale else 1, exclusiveMaximum=bound, exclusiveMinimum=-bound
        )
    elif logical == "number" and name in _NUMBER_FORMAT:
        options["format"] = _NUMBER_FORMAT[name]
    elif logical == "timestamp":
        options["timezone"] = "with time zone" in lowered or lowered.startswith("timestamptz")
    return options


def _transforms(snapshot: "MetadataSnapshot", column: "ColumnAsset") -> tuple[list[str], list[str]]:
    """``(source columns, operations)`` a view's column is derived from and by, from lineage."""
    sources: set[str] = set()
    operations: set[str] = set()
    for edge in snapshot.lineage:
        if edge.downstream.parts == column.ref.parts:
            sources.add(edge.upstream.fqn())
            operations.update(edge.transforms)
    return sorted(sources), sorted(operations)


def _relationships(snapshot: "MetadataSnapshot", table: "TableAsset") -> list[dict[str, Any]]:
    """The member's foreign keys: each direct relationship from it, column to column. A
    junction-backed one joins through a third table, so it is no key of the member's."""
    return [
        {
            "type": "foreignKey",
            "from": f"{table.name}.{edge.source_column}",
            "to": f"{edge.target.parts[-1]}.{edge.target_column}",
        }
        for edge in snapshot.relationships
        if edge.source.parts == table.ref.parts
        and edge.kind == "direct"
        and edge.target is not None
    ]


def _roles(product: "DataProductAsset", table: "TableAsset") -> list[dict[str, Any]]:
    """The roles that read or write the member, each approved by the owner role's holders."""
    approvers = ", ".join(product.owner_emails)
    return [
        {
            "role": role,
            "access": "write" if role in table.writers else "read",
            **({"firstLevelApprovers": approvers} if approvers else {}),
        }
        for role in sorted({*table.readers, *table.writers})
    ]


def _api_server(server: str, description: str, location: str) -> dict[str, Any]:
    return {"server": server, "type": "api", "description": description, "location": location}


def _servers(
    snapshot: "MetadataSnapshot", product: "DataProductAsset", table: "TableAsset"
) -> list[dict[str, Any]]:
    """The endpoints a consumer reads the member through: pgwire, Flight SQL, gRPC, GraphQL, REST
    and its OpenAPI description narrowed to the member, JSON:API and SPARQL. A listener not started or a surface not
    mounted is no server, and neither is a surface that does not serve the table: a table SQL
    alone serves has only the SQL listeners (REQ-2163), and a declared junction, an edge of the
    graph, is no class of the SPARQL vocabulary (REQ-1586)."""
    from provisa.compiler.naming import domain_to_sql_name
    from provisa.grpc.descriptor import SERVICE_NAME
    from provisa.security.rights import SQL_ONLY_TABLES

    endpoints = snapshot.endpoints
    if endpoints is None:
        return []
    servers: list[dict[str, Any]] = []
    if endpoints.pgwire_port is not None:
        servers.append(
            {
                "server": "pgwire",
                "type": "postgres",
                "description": "PostgreSQL wire protocol",
                "host": endpoints.hostname,
                "port": endpoints.pgwire_port,
                "database": endpoints.database,
                "schema": domain_to_sql_name(product.domain_id),
            }
        )
    if endpoints.flight_port is not None:
        servers.append(
            {
                "server": "flight-sql",
                "type": "custom",
                "description": "Arrow Flight SQL",
                "host": endpoints.hostname,
                "port": endpoints.flight_port,
                "database": endpoints.database,
                "schema": domain_to_sql_name(product.domain_id),
            }
        )
    if (product.domain_id, table.name) in SQL_ONLY_TABLES:
        return servers
    if endpoints.grpc_port is not None:
        servers.append(
            {
                "server": "grpc",
                "type": "custom",
                "description": "gRPC",
                "host": endpoints.hostname,
                "port": endpoints.grpc_port,
                "serviceName": SERVICE_NAME,
            }
        )
    origin, paths = endpoints.http_origin, endpoints.http_paths
    member = f"{product.domain_id}/{table.name}"
    if path := paths.get("GraphQL"):
        servers.append(_api_server("graphql", "GraphQL", f"{origin}{path}"))
    if path := paths.get("REST"):
        servers.append(_api_server("rest", "REST", f"{origin}{path}/{member}"))
        servers.append(
            _api_server(
                "openapi",
                "OpenAPI description of the REST endpoints",
                f"{origin}{path}/openapi.json?domains={product.domain_id}&tables={table.name}",
            )
        )
    if path := paths.get("JSON:API"):
        servers.append(_api_server("jsonapi", "JSON:API", f"{origin}{path}/{member}"))
    junction = any(
        edge.via is not None and edge.via.table.parts == table.ref.parts
        for edge in snapshot.relationships
    )
    if (path := paths.get("SPARQL")) and not junction:
        servers.append(_api_server("sparql", "SPARQL 1.1 Protocol", f"{origin}{path}"))
    return servers


def _property(
    snapshot: "MetadataSnapshot",
    table: "TableAsset",
    column: "ColumnAsset",
    signals: list[str],
    sensitive: set[tuple[str, ...]],
    quality: list["DataQualityAssertion"],
    term: "GroundedTerm | None",
) -> dict[str, Any]:
    """One of the member's columns as its contract's schema property (REQ-2158), naming its
    grounded term (REQ-2238)."""
    prop: dict[str, Any] = {"name": column.name, "physicalType": column.data_type}
    if logical := logical_type(column.data_type):
        prop["logicalType"] = logical
        if options := logical_type_options(column.data_type, column.subtype):
            prop["logicalTypeOptions"] = options
    if column.description:
        prop["description"] = column.description
    if (column.name,) in table.unique_keys or table.primary_key == (column.name,):
        prop["unique"] = True
    if column.name in table.primary_key:
        prop["primaryKey"] = True
        prop["primaryKeyPosition"] = table.primary_key.index(column.name) + 1
    if classification := _classification(column.ref.parts in sensitive, signals):
        prop["classification"] = classification
    if table.is_view:
        sources, operations = _transforms(snapshot, column)
        if sources:
            prop["transformSourceObjects"] = sources
        if operations:
            prop["transformLogic"] = "; ".join(operations)
    if quality:
        prop["quality"] = [_quality_rule(assertion) for assertion in quality]
    if term is not None:
        prop["authoritativeDefinitions"] = [term_definition(term)]
    return prop


def _contract_document(
    snapshot: "MetadataSnapshot",
    product: "DataProductAsset",
    table: "TableAsset",
    derived: "OfferContract",
    freshness: int | None,
) -> dict[str, Any]:
    """The ODCS 3.1 document of a contract derived on ``table`` (REQ-2118, REQ-2180) at its
    version, and every element Provisa already knows of the member, derived -- its address, its
    schema (the member's columns, each one's key, uniqueness, type options, classification,
    derivation and checks), its foreign keys, the product's promises on it, the roles that read
    and write it, the endpoints it is read through, the product's tags, and the terms all of the
    product's contracts share, written once on the product. ``freshness``: seconds a read may
    trail its source, the product's where it states one, else the member's."""
    tags = _column_tags(snapshot)
    sensitive = _sensitive_columns(snapshot)
    promises = _promises(snapshot, product, table)
    on_column: dict[str, list["DataQualityAssertion"]] = {}
    on_table: list["DataQualityAssertion"] = []
    for assertion in promises:
        if len(assertion.asset.parts) > 3:
            on_column.setdefault(assertion.asset.parts[3], []).append(assertion)
        else:
            on_table.append(assertion)
    terms = snapshot.column_terms()
    # REQ-2158: a contract's schema is its member's columns; it selects none of its own.
    properties = [
        _property(
            snapshot,
            table,
            column,
            tags.get(column.ref.fqn(), []),
            sensitive,
            on_column.get(column.name, []),
            terms.get(column.ref.fqn()),
        )
        for column in table.columns
    ]
    schema_object: dict[str, Any] = {
        "name": table.name,
        "physicalName": table.ref.fqn(),
        "logicalType": "object",
        "physicalType": "view" if table.is_view else "table",
        "properties": properties,
    }
    if table.description:
        schema_object["description"] = table.description
    if table_tags := tags.get(table.ref.fqn()):
        schema_object["tags"] = table_tags
    if relationships := _relationships(snapshot, table):
        schema_object["relationships"] = relationships
    if on_table:
        schema_object["quality"] = [_quality_rule(assertion) for assertion in on_table]
    contract: dict[str, Any] = {
        "apiVersion": ODCS_API_VERSION,
        "kind": "DataContract",
        "id": contract_id(table),
        "name": table.name,
        "version": derived.version,
        "status": contract_status(product),
        "domain": product.domain_id,
        "dataProduct": product.name,
        "schema": [schema_object],
        "contractCreatedTs": _utc(derived.created_at),
    }
    if description := _description(product):
        contract["description"] = description
    if product_tags := _tags(snapshot, product):
        contract["tags"] = product_tags
    if sla := _freshness(freshness):
        contract["slaProperties"] = sla
    if support := _support(product):
        contract["support"] = support
    if team := _team(product):
        contract["team"] = team
    if roles := _roles(product, table):
        contract["roles"] = roles
    if servers := _servers(snapshot, product, table):
        contract["servers"] = servers
    return contract


def _utc(at: "datetime") -> str:
    from datetime import timezone

    return at.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


def product_documents(
    snapshot: "MetadataSnapshot", product: "DataProductAsset"
) -> tuple[dict[str, Any], dict[str, dict[str, Any]]]:
    """``(the product's ODPS document, {contract key: its ODCS contract})``: one contract per offer
    and dataset (REQ-2180), each keyed ``<offer>/<source.schema.table>`` (:func:`offer_key`), so
    two members of one name in different sources stay apart."""
    return odps_document(snapshot, product), product_contracts(snapshot, product)


def product_contracts(
    snapshot: "MetadataSnapshot", product: "DataProductAsset"
) -> dict[str, dict[str, Any]]:
    """Every ODCS contract the product gives (REQ-2180): one per offer and dataset, keyed
    ``<offer>/<source.schema.table>`` (:func:`offer_key`)."""
    return {
        offer_key(offer, table): offer_contract(snapshot, product, table, offer)
        for offer in _offers_of(snapshot, product)
        for table in _offered(snapshot, product, offer)
    }


# -- REQ-2180: the contracts derived from the offers ----------------------------------------------


#: A contract's ODCS status as its product's lifecycle says it.
_OFFER_CONTRACT_STATUS: dict[str, Literal["draft", "active", "deprecated", "retired"]] = {
    "draft": "draft",
    "live": "active",
    "deprecated": "deprecated",
    "retired": "retired",
}


def contract_status(product: "DataProductAsset") -> str:
    """The ODCS status of each of the product's contracts, as its lifecycle says it."""
    return _OFFER_CONTRACT_STATUS[product.status]


def offer_key(offer: OfferExport, table: "TableAsset") -> str:
    """The key of an offer's contract on a dataset: the offer, then the dataset's address."""
    return f"{offer.name}/{table.ref.fqn()}"


def offer_contract_id(offer: OfferExport, table: "TableAsset") -> str:
    """An offer's contract id on a dataset: the dataset's URN, then the offer."""
    return f"{contract_id(table)}/offers/{offer.name}"


def _offered(
    snapshot: "MetadataSnapshot", product: "DataProductAsset", offer: OfferExport
) -> list["TableAsset"]:
    """The members ``offer`` gives a contract on: its datasets, in the product's member order."""
    return [table for table in _members(snapshot, product) if table.ref.fqn() in offer.contracts]


def _freshness_of(offer: OfferExport, table: "TableAsset") -> int | None:
    """The freshness an offer's contract on ``table`` promises: the product's, written once on
    it, else the member's own."""
    return offer.freshness if offer.freshness is not None else table.cache_ttl


def _offer_terms(offer: OfferExport, table: "TableAsset") -> list[dict[str, Any]]:
    """What the offer gives on the dataset, as ODCS and ODPS custom properties."""
    terms = offer.contracts[table.ref.fqn()].terms
    props: list[dict[str, Any]] = [
        {"property": "offer", "value": offer.name},
        {"property": "role", "value": offer.role_id},
        {"property": "dataLevel", "value": terms.data_level},
        {"property": "dataQualityResults", "value": terms.dq_results},
    ]
    if terms.sample_views:
        props.append({"property": "sampleViews", "value": terms.sample_views})
    if terms.row_rules:
        props.append({"property": "rowRules", "value": terms.row_rules})
    if terms.profiles:
        props.append({"property": "profiles", "value": terms.profiles})
    if terms.duration_days is not None:
        props.append({"property": "durationDays", "value": terms.duration_days})
    return props


def _offer_port(
    snapshot: "MetadataSnapshot",
    product: "DataProductAsset",
    table: "TableAsset",
    offer: OfferExport,
) -> dict[str, Any]:
    """The output port an offer's contract on a dataset is: the member's port, versioned by and
    naming that contract, with the offer's terms and the input contracts it reads -- each the
    contract an upstream product's default offer gives (REQ-2180)."""
    reads = [
        {
            "id": offer_contract_id(upstream, read),
            "version": upstream.contracts[read.ref.fqn()].version,
        }
        for read in _inputs(snapshot, product, {table.ref.parts})
        for _, upstream in _input_contracts(snapshot, read)
    ]
    return {
        "name": f"{table.name} ({offer.name})",
        "version": offer.contracts[table.ref.fqn()].version,
        "type": "view" if table.is_view else "table",
        "contractId": offer_contract_id(offer, table),
        **({"description": table.description} if table.description else {}),
        **({"inputContracts": reads} if reads else {}),
        **({"authoritativeDefinitions": terms} if (terms := _port_terms(snapshot, table)) else {}),
        "customProperties": _offer_terms(offer, table),
    }


def _offer_summary(
    snapshot: "MetadataSnapshot", product: "DataProductAsset", offer: OfferExport
) -> dict[str, Any]:
    return {
        "name": offer.name,
        "role": offer.role_id,
        "dataLevel": offer.data_level,
        "dataQualityResults": offer.dq_results,
        "profiles": offer.profiles,
        "open": offer.open,
        "durationDays": offer.duration_days,
        "contracts": [
            {
                "id": offer_contract_id(offer, table),
                "version": offer.contracts[table.ref.fqn()].version,
            }
            for table in _offered(snapshot, product, offer)
        ],
    }


def offer_contract(
    snapshot: "MetadataSnapshot",
    product: "DataProductAsset",
    table: "TableAsset",
    offer: OfferExport,
) -> dict[str, Any]:
    """The ODCS 3.1 contract ``offer`` gives on ``table`` (REQ-2180): the member's contract as the
    offer's role sees it -- only the columns it reads, a masked one stated as masked, its row
    rules in words -- at the offer contract's version, with the offer's data level, its data
    quality promises (always), and its profiles where it includes them."""
    derived = offer.contracts[table.ref.fqn()]
    terms = derived.terms
    contract = _contract_document(snapshot, product, table, derived, _freshness_of(offer, table))
    contract["id"] = offer_contract_id(offer, table)
    fields = {f.name: f for f in terms.fields}
    for schema_object in contract["schema"]:
        kept = []
        for prop in schema_object["properties"]:
            field = fields.get(prop["name"])
            if field is None:
                continue
            if field.masked is not None:
                prop["classification"] = "restricted"
                prop.setdefault("customProperties", []).append(
                    {"property": "masked", "value": field.masked}
                )
            kept.append(prop)
        schema_object["properties"] = kept
    contract["customProperties"] = [
        *contract.get("customProperties", []),
        *_offer_terms(offer, table),
    ]
    # Amended 2026-11-11: the offer's terms of use are its contract's; a term it leaves unstated
    # is the product's, written once on it.
    stated = {k: getattr(terms, k) for k in TERMS_OF_USE if getattr(terms, k)}
    if stated:
        contract["description"] = {**contract.get("description", {}), **stated}
    return contract
