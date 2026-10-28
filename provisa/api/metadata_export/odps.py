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
* an output port per member, each its declared ODCS contract (REQ-2158): its schema, the member's
  columns with their types and descriptions, the governance tags on them (masked, hidden from some roles,
  row-filtered; never a rule body), its terms, its declared service levels beside its freshness
  (the cache_ttl it is kept at) and the quality checks registered against it.

A contract has its own version and status, as ODPS versions each port by its contract. The
product's version is its own; a product with no version says so -- ``unversioned`` -- since ODPS
requires the field and inventing a number would claim a release that never happened.
"""

# Requirements: REQ-2118

from __future__ import annotations

import json
import re
from typing import TYPE_CHECKING, Any

from provisa.core.cache_ttl import NEVER_EXPIRES

if TYPE_CHECKING:
    from datetime import datetime

    from provisa.api.metadata_export.model import (
        ColumnAsset,
        DataProductAsset,
        DataQualityAssertion,
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
    # A member, or an input, with no declared contract is named, never left out silently: it
    # is no port, since ODPS versions a port by its contract.
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
    table read from a source. A member is never an input of its own product."""
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
            if parts in members or (table.is_view and table.contract is None):
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
        if assertion.owner_product_id == product.id and assertion.asset.parts[:3] == table.ref.parts
    ]


def _input_port(
    snapshot: "MetadataSnapshot", product: "DataProductAsset", table: "TableAsset"
) -> dict[str, Any]:
    """An input another product promises: its contract, and the product's expectations of it."""
    assert table.contract is not None  # an input port names the contract it reads
    expected = _expectations(snapshot, product, table)
    return {
        "name": table.name,
        "version": table.contract.version,
        "contractId": contract_id(table),
        **(
            {"customProperties": [{"property": "expectations", "value": expected}]}
            if expected
            else {}
        ),
    }


def _output_port(
    snapshot: "MetadataSnapshot", product: "DataProductAsset", table: "TableAsset"
) -> dict[str, Any]:
    assert table.contract is not None  # an output port is its member's declared contract
    reads = [
        {"id": contract_id(upstream), "version": upstream.contract.version}
        for upstream in _inputs(snapshot, product, {table.ref.parts})
        if upstream.contract is not None
    ]
    return {
        "name": table.name,
        "version": table.contract.version,
        "type": "view" if table.is_view else "table",
        "contractId": contract_id(table),
        **({"description": table.description} if table.description else {}),
        **({"inputContracts": reads} if reads else {}),
    }


def _members(snapshot: "MetadataSnapshot", product: "DataProductAsset") -> list["TableAsset"]:
    by_fqn = {table.ref.fqn(): table for table in snapshot.tables}
    return [by_fqn[member.fqn()] for member in product.members]


def _ports(snapshot: "MetadataSnapshot", product: "DataProductAsset") -> list["TableAsset"]:
    """The product's output ports: each member with a contract (REQ-2158). A checker table the
    product owns is no output of it, and has none."""
    return [table for table in _members(snapshot, product) if table.contract is not None]


def odps_document(snapshot: "MetadataSnapshot", product: "DataProductAsset") -> dict[str, Any]:
    """The product's ODPS 1.0 document (REQ-2118): its fields; an input port per input another
    product promises, naming that contract and the product's expectations of it; an output port
    per member naming the member's ODCS contract and the input contracts it reads; and the
    endpoints that observe and control it."""
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
        "inputPorts": [
            _input_port(snapshot, product, table) for table in inputs if table.contract is not None
        ],
        "outputPorts": [
            _output_port(snapshot, product, table) for table in _ports(snapshot, product)
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
    without_contract = [
        table.name
        for table in members
        if table.contract is None and table.ref.parts not in checkers
    ]
    inputs_without = [table.name for table in inputs if table.contract is None]
    if custom := _custom_properties(product, without_contract, inputs_without):
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
        if assertion.owner_product_id == product.id and assertion.asset.parts[:3] == table.ref.parts
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


def _servers(
    snapshot: "MetadataSnapshot", product: "DataProductAsset", table: "TableAsset"
) -> list[dict[str, Any]]:
    """The endpoints a consumer reads the member through: pgwire, Flight SQL, GraphQL, REST."""
    from provisa.compiler.naming import domain_to_sql_name

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
    servers += [
        {
            "server": "graphql",
            "type": "api",
            "description": "GraphQL",
            "location": f"{endpoints.http_origin}/data/graphql",
        },
        {
            "server": "rest",
            "type": "api",
            "description": "REST",
            "location": f"{endpoints.http_origin}/data/rest/{product.domain_id}/{table.name}",
        },
    ]
    return servers


def _property(
    snapshot: "MetadataSnapshot",
    table: "TableAsset",
    column: "ColumnAsset",
    signals: list[str],
    sensitive: set[tuple[str, ...]],
    quality: list["DataQualityAssertion"],
) -> dict[str, Any]:
    """One of the member's columns as its contract's schema property (REQ-2158)."""
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
    return prop


def odcs_contract(
    snapshot: "MetadataSnapshot", product: "DataProductAsset", table: "TableAsset"
) -> dict[str, Any]:
    """A member's ODCS 3.1 contract (REQ-2158, REQ-2118): what it declares, and every element
    Provisa already knows of it, derived -- its address, its schema (the member's columns, each
    one's key, uniqueness, type options, classification, derivation and checks), its foreign keys, the product's promises on
    it, the roles that read and write it, the endpoints it is read through, and the product's
    tags."""
    declared = table.contract
    assert declared is not None  # only an output port has a contract document
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
    # REQ-2158: a contract's schema is its member's columns; it selects none of its own.
    properties = [
        _property(
            snapshot,
            table,
            column,
            tags.get(column.ref.fqn(), []),
            sensitive,
            on_column.get(column.name, []),
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
        "version": declared.version,
        "status": declared.status,
        "domain": product.domain_id,
        "dataProduct": product.name,
        "schema": [schema_object],
    }
    if declared.created_at is not None:
        contract["contractCreatedTs"] = _utc(declared.created_at)
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
    if product_tags := _tags(snapshot, product):
        contract["tags"] = product_tags
    if terms.notice_period:
        contract["customProperties"] = [{"property": "noticePeriod", "value": terms.notice_period}]
    if sla := _service_levels(table):
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
    """``(the product's ODPS document, {member: its ODCS contract})``, each member by its address
    (source, schema, table): two members of one name in different sources stay apart."""
    return odps_document(snapshot, product), {
        table.ref.fqn(): odcs_contract(snapshot, product, table)
        for table in _ports(snapshot, product)
    }
