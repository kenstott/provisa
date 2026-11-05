# Copyright (c) 2026 Kenneth Stott
# Canary: 2e7b9d14-6a30-4c58-9f1e-8d0c3b5a7e26
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The links a junction implies: from each of its rows to either end (REQ-1586).

A junction-backed relationship (pets -> pets through ``pet_companions``) declares that
``via_table.via_source_column`` holds a key of the source and ``via_table.via_target_column`` a
key of the target. The two ends get the edge; the junction table itself gained nothing, so a
junction row could not be followed to the pet or the companion it names. Each such declaration
implies two many-to-one links, from the junction to the source and to the target, and this is
the one place they are derived: the schema build (fields on the junction's type) and the
relationships API (which marks them derived from the junction relationships) read these. In the
graph schema a declared junction is an edge, not a node (label_map._drop_junction_nodes), so these
links are dropped there with the junction's other foreign-key edges.

Several typed junctions over the same junction columns (bonded pair, littermate, ...) imply the
same two links: each is given once, naming every junction relationship it comes from. A link a
steward already declared (the same junction table, columns, end table and end columns) is theirs,
and none is implied beside it. Read rights are those of any relationship: a link is served where
both of its tables are.
"""

# Requirements: REQ-1586

from __future__ import annotations

from typing import Any

from provisa.compiler.sql_types import key_list

#: Prefix of an implied link's id; no declared relationship id takes it.
IMPLIED_PREFIX = "junction:"


def _declared(rel: dict[str, Any]) -> tuple:
    return (
        rel["source_table_id"],
        key_list(rel.get("source_column") or ""),
        rel.get("target_table_id"),
        key_list(rel.get("target_column") or ""),
    )


def implied_junction_links(
    relationships: list[dict[str, Any]], keys: set[tuple[int, str]], convention: str
) -> list[dict[str, Any]]:
    """The many-to-one links ``relationships``' junctions imply, as relationship rows of the shape
    the registry's rows take, each with ``derived_from``: the ids of the junction relationships
    it comes from. ``keys`` is every (table id, column) that is a key of its table, primary or
    alternate: a link is implied only to an end matched on its table's key, the one row a junction
    row names there. An end matched on a column that is not a key (a junction from countries to
    the finals each won, through a name the finals repeat) is reached by many rows, and implies
    nothing. ``convention`` is the one the deployment spells GraphQL fields in."""
    from provisa.compiler.naming import apply_convention, junction_link_name

    declared = {_declared(r) for r in relationships}
    links: dict[tuple, dict[str, Any]] = {}
    for rel in relationships:
        via = rel.get("via_table_id")
        if via is None:
            continue
        ends = (
            (
                rel["via_source_column"],
                rel["source_table_id"],
                rel["source_column"],
                rel.get("source_table_name") or "",
            ),
            (
                rel["via_target_column"],
                rel["target_table_id"],
                rel["target_column"],
                rel.get("target_table_name") or "",
            ),
        )
        for via_column, end_table, end_column, end_table_name in ends:
            key = (via, key_list(via_column), end_table, key_list(end_column))
            if key in declared or not all((end_table, c) in keys for c in key[3]):
                continue
            link = links.get(key)
            if link is None:
                name = junction_link_name(key_list(via_column), end_table_name)
                graphql_field = apply_convention(name, convention)
                link = {
                    "id": f"{IMPLIED_PREFIX}{via}:{via_column}",
                    "source_table_id": via,
                    "target_table_id": end_table,
                    "source_column": via_column,
                    "target_column": end_column,
                    "cardinality": "many-to-one",
                    "materialize": False,
                    "refresh_interval": 300,
                    "target_function_name": None,
                    "function_arg": None,
                    "alias": None,
                    "graphql_alias": graphql_field,
                    "graphql_field": graphql_field,
                    # No Cypher type: in the graph schema a junction is its typed edges, and these
                    # links are dropped with its other foreign-key edges (REQ-1586).
                    "cypher_type": None,
                    "computed_cypher_alias": None,
                    "disable_cypher": True,
                    "same_entity": False,
                    "via_table_id": None,
                    "via_source_column": None,
                    "via_target_column": None,
                    "via_type_column": None,
                    "via_type_value": None,
                    "via_label_source": None,
                    "parameters": {},
                    "source_table_name": rel.get("via_table_name"),
                    "source_domain_id": rel.get("via_domain_id"),
                    "target_table_name": end_table_name,
                    "via_table_name": None,
                    "derived_from": [],
                }
                links[key] = link
            link["derived_from"].append(rel["id"])
    for link in links.values():
        link["derived_from"].sort()
    return sorted(links.values(), key=lambda link: link["id"])


def table_keys(tables: list[dict[str, Any]]) -> set[tuple[int, str]]:
    """Every (table id, column) of ``tables`` that is a key of its table, primary or alternate."""
    return {
        (t["id"], c["column_name"])
        for t in tables
        for c in t.get("columns") or []
        if c.get("is_primary_key") or c.get("is_alternate_key")
    }


def with_junction_links(
    relationships: list[dict[str, Any]], keys: set[tuple[int, str]], convention: str
) -> list[dict[str, Any]]:
    """``relationships`` and the links their junctions imply: what the model serves."""
    return [*relationships, *implied_junction_links(relationships, keys, convention)]
