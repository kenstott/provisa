# Copyright (c) 2025 Kenneth Stott
# Canary: 45a7a653-85ec-4cc4-8c80-cf9f9ed6507c
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""GraphQL name generation: shortest unique within domain → regex rules → alias.

Names must be valid GraphQL identifiers: [_A-Za-z][_0-9A-Za-z]*.
"""

import re


def _to_pascal_case(name: str) -> str:
    """Convert snake_case or kebab-case to PascalCase."""
    parts = re.split(r"[_\-]+", name)
    return "".join(p.capitalize() for p in parts if p)


def _to_field_name(name: str) -> str:
    """Convert to valid GraphQL field name (snake_case, no hyphens)."""
    return re.sub(r"[^a-zA-Z0-9_]", "_", name).strip("_")


def _apply_naming_rules(name: str, rules: list[dict]) -> str:
    """Apply regex naming rules in order."""
    for rule in rules:
        name = re.sub(rule["pattern"], rule["replacement"], name)
    return name


def _shortest_unique(name: str, all_names: list[str], qualifiers: list[str]) -> str:
    """Find shortest unique name within a set.

    If `name` is unique in `all_names`, return it. Otherwise, prepend qualifiers
    one at a time until unique.

    Args:
        name: base table name (e.g., "orders")
        all_names: all table names in the domain
        qualifiers: ordered prefixes to try (e.g., [schema_name, source_id])
    """
    if all_names.count(name) <= 1:
        return name
    for qualifier in qualifiers:
        candidate = f"{qualifier}_{name}"
        if candidate not in all_names:
            return candidate
    raise ValueError(
        f"Cannot generate unique name for {name!r} within domain. "
        f"All qualifier combinations exhausted."
    )


def generate_name(
    table_name: str,
    schema_name: str,
    source_id: str,
    domain_table_names: list[str],
    naming_rules: list[dict],
    alias: str | None = None,
) -> str:
    """Generate a unique GraphQL-safe name for a table.

    Priority: alias > naming rules > shortest unique name.
    """
    if alias:
        return _to_field_name(alias)

    # Apply naming rules to this name AND all domain names for correct comparison
    name = _apply_naming_rules(table_name, naming_rules)
    transformed_names = [_apply_naming_rules(n, naming_rules) for n in domain_table_names]

    # Find shortest unique within domain (comparing transformed names)
    name = _shortest_unique(name, transformed_names, [schema_name, source_id])

    result = _to_field_name(name)
    if not result:
        raise ValueError(
            f"Naming rules produced empty name for table {table_name!r}. "
            f"Check naming rule configuration."
        )
    return result


def to_type_name(field_name: str) -> str:
    """Convert a field name to a GraphQL type name (PascalCase).

    Preserves the domain separator: sales_analytics__orders → SalesAnalytics_Orders
    """
    if "__" in field_name:
        parts = field_name.split("__", 1)
        return _to_pascal_case(parts[0]) + "_" + _to_pascal_case(parts[1])
    return _to_pascal_case(field_name)
