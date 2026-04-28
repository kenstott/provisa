# Copyright (c) 2026 Kenneth Stott
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

import inflect as _inflect_mod

_inflect = _inflect_mod.engine()


_VERB_PREFIXES = frozenset({
    "find", "get", "list", "create", "add", "update", "delete",
    "remove", "search", "fetch", "query", "retrieve", "read",
})


def rel_field_name(target_field_name: str, cardinality: str) -> str:
    """Build {noun}_{modifiers} relationship field name with library-based pluralization.

    Handles camelCase (OpenAPI operation IDs) and strips leading verb prefixes
    so findPetsByStatus → pet_by_status (many-to-one).
    """
    base = target_field_name.split("__", 1)[-1]
    # Normalise camelCase/PascalCase → snake_case before splitting
    snake = re.sub(r"([A-Z]+)([A-Z][a-z])", r"\1_\2", base)
    snake = re.sub(r"([a-z0-9])([A-Z])", r"\1_\2", snake).lower()
    parts = [p for p in snake.split("_") if p]
    # Strip leading verb prefixes (common in OpenAPI operation IDs)
    original_len = len(parts)
    while len(parts) > 1 and parts[0] in _VERB_PREFIXES:
        parts = parts[1:]
    verb_was_stripped = len(parts) < original_len
    # Compound noun (no verb stripped, multiple parts): last word is head noun
    if not verb_was_stripped and len(parts) > 1:
        noun, modifiers = parts[-1], parts[:-1]
    else:
        noun, modifiers = parts[0], parts[1:]
    if cardinality == "one-to-many":
        singular = _inflect.singular_noun(noun)
        if singular is False:
            # noun is singular — pluralize it
            noun = _inflect.plural_noun(noun) or noun
        elif singular.endswith("s") and not noun.endswith("ies"):
            # inflect returned a false singular ending in 's' (e.g. address→addres) — force plural
            noun = _inflect.plural_noun(noun) or noun
        # else: genuinely plural (e.g. inquiries, orders) — leave as-is
    else:
        singular = _inflect.singular_noun(noun)
        if singular:
            noun = singular
    if not verb_was_stripped and len(parts) > 1:
        return modifiers[0] + "".join(m.capitalize() for m in modifiers[1:]) + noun.capitalize()
    return noun + "".join(m.capitalize() for m in modifiers)


def _to_pascal_case(name: str) -> str:
    """Convert snake_case or kebab-case to PascalCase."""
    parts = re.split(r"[_\-]+", name)
    result = []
    for p in parts:
        if not p:
            continue
        # If part already has internal uppercase (camelCase), just capitalize first letter
        if any(c.isupper() for c in p[1:]):
            result.append(p[0].upper() + p[1:])
        else:
            result.append(p.capitalize())
    return "".join(result)


def _to_camel_case(name: str) -> str:
    """Convert snake_case or kebab-case to camelCase."""
    pascal = _to_pascal_case(name)
    if not pascal:
        return pascal
    return pascal[0].lower() + pascal[1:]


def _to_field_name(name: str) -> str:
    """Convert to valid GraphQL field name (snake_case, no hyphens)."""
    return re.sub(r"[^a-zA-Z0-9_]", "_", name).strip("_")


def domain_to_sql_name(domain_id: str) -> str:
    """Normalize a domain ID to a valid SQL identifier (non-alphanumeric → underscore)."""
    return re.sub(r"[^a-zA-Z0-9]", "_", domain_id).strip("_")


def domain_gql_alias(domain_id: str, stored: str | None = None) -> str:
    """Return stored alias, or compute first-letter acronym from domain id.

    Stored alias is used as-is (lowercase for ops, UPPER for type prefix).
    Computed default: first letter of each word segment, lowercase.
    e.g. 'sales_analytics' → 'sa', 'human-resources' → 'hr'.
    Returns '' for empty domain_id (no prefix).
    """
    if stored:
        return stored.lower()
    if not domain_id:
        return ""
    parts = re.split(r"[^a-zA-Z0-9]+", domain_id)
    acronym = "".join(p[0] for p in parts if p and p[0].isalpha())
    return acronym.lower() if acronym else domain_id[0].lower()


def _to_snake_case(name: str) -> str:
    """Convert camelCase or PascalCase to snake_case."""
    name = re.sub(r"([A-Z]+)([A-Z][a-z])", r"\1_\2", name)
    name = re.sub(r"([a-z0-9])([A-Z])", r"\1_\2", name)
    return name.lower()


# Public alias for use by other modules
to_snake_case = _to_snake_case


def source_to_catalog(source_id: str) -> str:
    """Convert a source ID to a Trino catalog name (hyphens → underscores)."""
    return source_id.replace("-", "_")


VALID_CONVENTIONS = frozenset({"snake", "hasura_graphql", "apollo_graphql"})


def _canonical_convention(convention: str) -> str:
    """Resolve preset convention to field/column naming form."""
    if convention == "snake":
        return "snake_case"
    return "camelCase"  # hasura_graphql, apollo_graphql


def mutation_style(convention: str) -> str:
    """Return 'snake' or 'camel' mutation prefix style for a given convention."""
    if convention in ("snake", "hasura_graphql"):
        return "snake"
    return "camel"  # apollo_graphql


def apply_convention(name: str, convention: str) -> str | None:
    """Apply a naming convention preset to produce an alias.

    snake: PascalCase → snake_case; camelCase names preserved (REQ-157).
    hasura_graphql / apollo_graphql: snake_case → camelCase; camelCase preserved.
    Returns None if no alias needed.
    """
    canon = _canonical_convention(convention)
    if canon == "snake_case":
        if name and name[0].islower() and any(c.isupper() for c in name):
            return None
        result = _to_snake_case(name)
        return result if result != name else None
    if canon == "camelCase":
        if name and name[0].islower() and any(c.isupper() for c in name):
            return None
        result = _to_camel_case(name)
        return result if result != name else None
    return None


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


def _apply_table_convention(name: str, convention: str) -> str:
    """Apply a naming convention preset to a table/field name."""
    safe = _to_field_name(name)
    canon = _canonical_convention(convention)
    if canon == "camelCase":
        return _to_camel_case(safe)
    return safe  # snake_case


def generate_name(
    table_name: str,
    schema_name: str,
    source_id: str,
    domain_table_names: list[str],
    naming_rules: list[dict],
    alias: str | None = None,
    convention: str = "apollo_graphql",
) -> str:
    """Generate a unique GraphQL-safe name for a table.

    Priority: alias > naming rules > shortest unique name.
    Convention controls output casing (default: camelCase).
    """
    if alias:
        return _apply_table_convention(alias, convention)

    # Apply naming rules to this name AND all domain names for correct comparison
    name = _apply_naming_rules(table_name, naming_rules)
    transformed_names = [_apply_naming_rules(n, naming_rules) for n in domain_table_names]

    # Find shortest unique within domain (comparing transformed names)
    name = _shortest_unique(name, transformed_names, [schema_name, source_id])

    result = _apply_table_convention(name, convention)
    if not result:
        raise ValueError(
            f"Naming rules produced empty name for table {table_name!r}. "
            f"Check naming rule configuration."
        )
    return result


def to_type_name(field_name: str) -> str:
    """Convert a field name to a GraphQL type name (PascalCase).

    Handles camelCase input: capitalizes first letter only.
    Preserves the domain separator: sa__userByName → Sa_UserByName
    """
    if "__" in field_name:
        prefix, rest = field_name.split("__", 1)
        rest_pascal = (rest[0].upper() + rest[1:]) if rest else ""
        return prefix.upper() + "__" + rest_pascal
    return (field_name[0].upper() + field_name[1:]) if field_name else ""
