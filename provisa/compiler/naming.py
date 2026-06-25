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

# Requirements: REQ-154, REQ-155, REQ-156, REQ-157, REQ-194, REQ-195, REQ-411, REQ-412, REQ-416

import re
from typing import cast

import inflect as _inflect_mod
from inflect import Word

_inflect = _inflect_mod.engine()


_VERB_PREFIXES = frozenset(
    {
        "find",
        "get",
        "list",
        "create",
        "add",
        "update",
        "delete",
        "remove",
        "search",
        "fetch",
        "query",
        "retrieve",
        "read",
    }
)


def rel_field_name(target_field_name: str, cardinality: str) -> str:  # REQ-194, REQ-415
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
        singular = _inflect.singular_noun(cast(Word, noun))
        if singular is False:
            # noun is singular — pluralize it
            noun = _inflect.plural_noun(cast(Word, noun)) or noun
        elif singular.endswith("s") and not noun.endswith("ies"):
            # inflect returned a false singular ending in 's' (e.g. address→addres) — force plural
            noun = _inflect.plural_noun(cast(Word, noun)) or noun
        # else: genuinely plural (e.g. inquiries, orders) — leave as-is
    else:
        singular = _inflect.singular_noun(cast(Word, noun))
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


def source_to_catalog(source_id: str) -> str:
    """Convert a source ID to a Trino catalog name (hyphens → underscores)."""
    return source_id.replace("-", "_")


# REQ-195: Hasura v2 / DDN literals map to internal presets.
#   hasura-default  → snake_case   graphql-default → camelCase
#   graphql (DDN namingConvention) → camelCase
_CONVENTION_ALIASES = {
    "hasura-default": "hasura_graphql",
    "graphql-default": "apollo_graphql",
    "graphql": "apollo_graphql",
}


def normalize_convention(convention: str) -> str:  # REQ-195, REQ-416
    """Resolve a Hasura/DDN literal convention name to its internal preset."""
    return _CONVENTION_ALIASES.get(convention, convention)


VALID_CONVENTIONS = frozenset(
    {"snake", "hasura_graphql", "apollo_graphql"} | set(_CONVENTION_ALIASES)
)


def validation_error_for_convention(convention: str) -> str | None:  # REQ-416
    """REQ-416: return an error message if `convention` is not a valid preset, else None.

    Only the presets (and their literal aliases in VALID_CONVENTIONS) are accepted; free-form
    strings must be rejected before `configure` is called on the naming-update path.
    """
    if convention in VALID_CONVENTIONS:
        return None
    valid = ", ".join(sorted(VALID_CONVENTIONS))
    return f"Invalid naming convention {convention!r}. Valid conventions: {valid}"


def _canonical_convention(convention: str) -> str:
    """Resolve preset convention to field/column naming form."""
    convention = normalize_convention(convention)
    # REQ-194: hasura_graphql is snake_case; apollo_graphql is camelCase.
    if convention in ("snake", "hasura_graphql"):
        return "snake_case"
    return "camelCase"  # apollo_graphql


def mutation_style(convention: str) -> str:  # REQ-411, REQ-412
    """Return 'snake' or 'camel' mutation prefix style for a given convention."""
    if normalize_convention(convention) in ("snake", "hasura_graphql"):
        return "snake"
    return "camel"  # apollo_graphql


_gql_convention: str = "apollo_graphql"
_sql_convention: str = "snake"


def configure(gql: str = "apollo_graphql", sql: str = "snake") -> None:
    global _gql_convention, _sql_convention
    _gql_convention = normalize_convention(gql)
    _sql_convention = normalize_convention(sql)


def active_gql_convention() -> str:
    return _gql_convention


def apply_gql_name(name: str, override: str | None = None) -> str:  # REQ-194, REQ-411, REQ-412
    return apply_convention(name, override or _gql_convention)


def apply_sql_name(name: str, override: str | None = None) -> str:  # REQ-194
    return apply_convention(name, override or _sql_convention)


def apply_cql_label(name: str) -> str:
    """Cypher node/relationship label — always PascalCase."""
    return _to_pascal_case(name)


def apply_cql_property(name: str) -> str:
    """Cypher property key — follows the configured GQL convention."""
    return apply_gql_name(name)


def apply_convention(name: str, convention: str) -> str:
    """Apply a naming convention preset to a name. Returns name unchanged when already correct."""
    canon = _canonical_convention(convention)
    if canon == "snake_case":
        return _to_snake_case(name)
    if canon == "camelCase":
        return _to_camel_case(name)
    return name


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


def generate_name(  # REQ-154, REQ-155, REQ-157, REQ-194
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
