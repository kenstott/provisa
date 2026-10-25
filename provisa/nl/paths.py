# Copyright (c) 2026 Kenneth Stott
# Canary: 606cb927-15d7-407c-968d-65bb1a12d8af
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The registered path between two tables a generated query joined outside a relationship
(REQ-603): what the NL retry is given back with the refusal, and what a strict "Not available"
says the question needs."""

# Requirements: REQ-603

from __future__ import annotations

from collections.abc import Callable, Iterable
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from provisa.compiler.sql_validator import ValidationViolation
    from provisa.gql.label_map import CypherLabelMap, NodeMapping

#: The longest path offered: past it, a question is not asking to combine the two tables.
MAX_HOPS = 4


def relationship_hints(
    label_map: "CypherLabelMap",
    violations: Iterable["ValidationViolation"],
    sql_domain: Callable[[str | None], str],
) -> list[str]:
    """For each pair of tables a V002 refusal names, once: the shortest registered path between
    them as the JOINs that walk it, or that no relationship relates them. ``sql_domain`` names a
    table's domain as the generated SQL writes it."""
    by_table = {node.table_id: node for node in label_map.nodes.values()}

    def named(node: "NodeMapping") -> str:
        return f"{sql_domain(node.domain_id)}.{node.table_name}"

    hints: list[str] = []
    seen: set[tuple[int, int]] = set()
    for violation in violations:
        if violation.code != "V002" or violation.tables is None or violation.tables in seen:
            continue
        seen.add(violation.tables)
        left, right = (by_table.get(t) for t in violation.tables)
        if left is None or right is None:
            continue  # a table outside the role's graph: there is no path the role can walk
        paths = label_map.find_paths(
            left.type_name, right.type_name, max_hops=MAX_HOPS, bidirectional=True
        )
        if not paths:
            hints.append(
                f"No registered relationship relates {named(left)} and {named(right)}: they "
                "cannot be joined, and the question needs another way to combine them or one "
                "of them alone"
            )
            continue
        joins = []
        for hop in min(paths, key=len):
            source, target = label_map.nodes[hop.source_label], label_map.nodes[hop.target_label]
            joins.append(
                f"JOIN {named(target)} ON {source.table_name}.{hop.join_source_column} = "
                f"{target.table_name}.{hop.join_target_column}"
            )
        hints.append(
            f"{named(left)} and {named(right)} are related along a registered relationship: "
            + " ".join(joins)
        )
    return hints
