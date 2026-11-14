# Copyright (c) 2026 Kenneth Stott
# Canary: 356a1da6-fcac-4ea9-87a1-2dc4a0bf266f
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The model URI grammar (REQ-1385): ``provisa://<org>/<domain-path>/tables/<table>`` and its kin.

An object's identifier is its model name, and this is the one place that writes it as a URI. The
metadata export (:mod:`provisa.api.metadata_export.refs`) publishes these URIs and the model's
history names each changed object by one (REQ-1524), so both read them from here: the core holds
the grammar, and the export layer is not imported to reach it.
"""

# Requirements: REQ-1385, REQ-1524

from __future__ import annotations

from urllib.parse import quote

from provisa.core.models import Table

# REQ-1385: kind keywords are reserved path segments; a domain with one of these names
# would make its URIs unparseable, so domain creation refuses them.
#
# REQ-1526 widened the list from the four kinds that appear in a URI to every kind the serialized
# model gives a DIRECTORY to. In a URI a kind is only ever preceded by domain segments, so an
# unreserved kind was merely unparseable in the cases that reached it; in a directory tree the kind
# and the domain are siblings on disk, and a domain named for one would have its own file written
# into the kind's directory. Reserving the whole set costs an org the use of about a dozen domain
# names and removes the collision entirely.
RESERVED_KIND_KEYWORDS = (
    "tables",
    "sources",
    "api-sources",
    "kafka-sources",
    "connectors",  # REQ-2221: the connectors an org offers
    "tags",
    "roles",
    "commands",
    "terms",
    "metrics",
    "views",
    "calendars",
    "naming",
    "domain",
    "data-products",
    "stores",  # REQ-1922: an org's stores
    "regions",  # REQ-1921: the platform regions an org selects
    "erd-layouts",  # REQ-2189: the saved ERD layouts
)

_URI_SCHEME = "provisa"


class NotAKindKeyword(ValueError):
    """A root-kind URI asked for with a keyword the grammar does not reserve."""

    def __init__(self, keyword: str) -> None:
        super().__init__(
            f"{keyword!r} is not a reserved kind keyword (REQ-1385): {RESERVED_KIND_KEYWORDS}"
        )


def _segment(name: str) -> str:
    return quote(name, safe="")


def _domain_segments(domain_id: str | None) -> str:
    """The domain path portion of a URI. Hierarchical domains arrive as slash-separated ids;
    the unassigned ('') bucket contributes no segments."""
    if not domain_id:
        return ""
    return "/".join(_segment(part) for part in domain_id.split("/")) + "/"


def _org_segment(org_id: str | None) -> str:
    """The org path portion of a URI. ``org_id`` is ``None`` for a single-tenant deployment
    (REQ-697 ``multitenancy=False``): there is only ever one org, so naming it in every URI
    would expose config plumbing (the "default" ORG_ID fallback) rather than business identity."""
    return f"{_segment(org_id)}/" if org_id else ""


def kind_uri(org_id: str | None, keyword: str, name: str) -> str:  # REQ-1385
    """An object of a root kind -- one addressed by ``<keyword>/<name>`` under the org rather than
    under a domain (sources, tags, roles, commands, terms, metrics, stores, regions and the rest of
    :data:`RESERVED_KIND_KEYWORDS`)."""
    if keyword not in RESERVED_KIND_KEYWORDS:
        raise NotAKindKeyword(keyword)
    return f"{_URI_SCHEME}://{_org_segment(org_id)}{keyword}/{_segment(name)}"


def domain_uri(org_id: str | None, domain_id: str) -> str:  # REQ-1385
    return f"{_URI_SCHEME}://{_org_segment(org_id)}{_domain_segments(domain_id)}".rstrip("/")


def source_uri(org_id: str | None, source_id: str) -> str:  # REQ-1385
    return kind_uri(org_id, "sources", source_id)


def tag_uri(org_id: str | None, tag_id: str) -> str:  # REQ-1385
    return kind_uri(org_id, "tags", tag_id)


def role_uri(org_id: str | None, role_id: str) -> str:  # REQ-1385
    return kind_uri(org_id, "roles", role_id)


def data_product_uri(
    org_id: str | None, domain_id: str, product_id: str
) -> str:  # REQ-1385, REQ-1634
    # Domain-segmented like table_uri: a data product lives under its one owning domain.
    return (
        f"{_URI_SCHEME}://{_org_segment(org_id)}"
        f"{_domain_segments(domain_id)}data-products/{_segment(product_id)}"
    )


def command_uri(org_id: str | None, command_name: str) -> str:  # REQ-1385
    """A governed command (tracked function/webhook) by its registered name."""
    return kind_uri(org_id, "commands", command_name)


def named_table_uri(org_id: str | None, domain_id: str | None, business_name: str) -> str:
    """A table by its domain and business name (its alias when it has one, else its table name):
    :func:`table_uri` for a caller holding a registered row rather than a :class:`Table`."""
    return (
        f"{_URI_SCHEME}://{_org_segment(org_id)}"
        f"{_domain_segments(domain_id)}tables/{_segment(business_name)}"
    )


def table_uri(org_id: str | None, table: Table) -> str:  # REQ-1385
    """Business-identity address: alias when present, else table name — never the physical
    (source, schema) coordinates, which export separately as the binding."""
    return named_table_uri(org_id, table.domain_id, table.alias or table.table_name)


def field_at(table_address: str, column_name: str, column_alias: str | None) -> str:
    # REQ-1385: #field:<business name> — a column is an attribute of the concept.
    return f"{table_address}#field:{_segment(column_alias or column_name)}"


def column_uri(org_id: str | None, table: Table, column_name: str, column_alias: str | None) -> str:
    return field_at(table_uri(org_id, table), column_name, column_alias)


def named_column_uri(
    org_id: str | None,
    domain_id: str | None,
    table_name: str,
    column_name: str,
    column_alias: str | None,
) -> str:
    """:func:`column_uri` for a caller holding its table's domain and business name
    (:func:`named_table_uri`) rather than a :class:`Table`."""
    return field_at(named_table_uri(org_id, domain_id, table_name), column_name, column_alias)


def metric_uri(org_id: str | None, metric_name: str) -> str:  # REQ-1385, REQ-1592
    """A governed metric by its (config-unique) name.

    Org-level rather than domain-segmented because a metric has no grain and no domain of its own:
    its dataset binding is implicit in the semantic references inside its expression (REQ-1317), so
    there are no domain segments to place it under. ``metrics`` is already a reserved kind keyword.
    """
    return kind_uri(org_id, "metrics", metric_name)


def term_uri(org_id: str | None, term_name: str) -> str:  # REQ-1385, REQ-1387
    """A business-glossary term by its (unique) normalized name."""
    return kind_uri(org_id, "terms", term_name)


def relationship_at(table_address: str, alias: str | None, rel_id: str) -> str:
    # REQ-1385: a relationship is a navigational field anchored at its source concept.
    # Its business name is the alias when the edge is named; the registry id otherwise.
    return f"{table_address}#rel:{_segment(alias or rel_id)}"


def relationship_uri(
    org_id: str | None, source_table: Table, alias: str | None, rel_id: str
) -> str:
    return relationship_at(table_uri(org_id, source_table), alias, rel_id)


def named_relationship_uri(
    org_id: str | None, domain_id: str | None, table_name: str, alias: str | None, rel_id: str
) -> str:
    """:func:`relationship_uri` for a caller holding its source table's domain and business name
    (:func:`named_table_uri`) rather than a :class:`Table`."""
    return relationship_at(named_table_uri(org_id, domain_id, table_name), alias, rel_id)
