# Copyright (c) 2026 Kenneth Stott
# Canary: 51c7e2a9-4b1f-4e60-8d37-a2f96c03b781
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The names a statement queries tables and columns by, as SQL over a meta view (REQ-2190): what
provisa.compiler.naming computes, for a view that cannot call it. The expressions are generated
here and held to naming.py by test_metadata_names; the two conventions in force are read from the
statement's settings (provisa.transpiler.metadata_settings), so a change of convention needs no
view rebuilt. A naming rule the two engines would read differently is refused where it is written
(provisa.core.naming_rule_check)."""

# Requirements: REQ-2190

from __future__ import annotations

from provisa.transpiler.metadata_settings import setting

#: A convention's output form, as naming._canonical_convention names it.
_CAMEL = "camelCase"
#: The stored presets whose output form is camelCase (naming._canonical_convention).
_CAMEL_PRESETS = "('apollo_graphql')"


def snake_case_sql(name: str) -> str:
    """naming._to_snake_case."""
    return (
        "LOWER(REGEXP_REPLACE(REGEXP_REPLACE("
        f"{name}, '([A-Z]+)([A-Z][a-z])', '\\1_\\2', 'g'), "
        "'([a-z0-9])([A-Z])', '\\1_\\2', 'g'))"
    )


def _word(part: str) -> str:
    """A word of naming._to_pascal_case: one with inner capitals keeps them."""
    rest = f"SUBSTR({part}, 2)"
    head = f"UPPER(LEFT({part}, 1))"
    return f"CASE WHEN {rest} ~ '[A-Z]' THEN {head} || {rest} ELSE {head} || LOWER({rest}) END"


def pascal_case_sql(name: str) -> str:
    """naming._to_pascal_case: a word after the first that starts with a digit keeps its
    underscore."""
    word = _word("_w.part")
    return (
        "COALESCE((SELECT STRING_AGG(CASE WHEN _w.place > 1 AND _w.part ~ '^[0-9]' THEN '_' "
        f"ELSE '' END || {word}, '' ORDER BY _w.place) FROM (SELECT _p.part, "
        "ROW_NUMBER() OVER (ORDER BY _p.position) AS place "
        f"FROM REGEXP_SPLIT_TO_TABLE({name}, '[_\\-]+') WITH ORDINALITY AS _p(part, position) "
        "WHERE _p.part <> '') _w), '')"
    )


def camel_case_sql(name: str) -> str:
    """naming._to_camel_case."""
    pascal = pascal_case_sql(name)
    return f"LOWER(LEFT({pascal}, 1)) || SUBSTR({pascal}, 2)"


def sql_name_sql(name: str) -> str:
    """naming.apply_sql_name, in the statement's SQL convention."""
    return (
        f"CASE WHEN {setting('sql_convention')} = '{_CAMEL}' THEN {camel_case_sql(name)} "
        f"ELSE {snake_case_sql(name)} END"
    )


def field_name_sql(name: str) -> str:
    """naming._to_field_name."""
    return f"TRIM(BOTH '_' FROM REGEXP_REPLACE({name}, '[^a-zA-Z0-9_]', '_', 'g'))"


def table_convention_sql(name: str, convention: str) -> str:
    """naming._apply_table_convention, ``convention`` a stored preset (SQL)."""
    safe = field_name_sql(name)
    return f"CASE WHEN {convention} IN {_CAMEL_PRESETS} THEN {camel_case_sql(safe)} ELSE {safe} END"


def domain_sql_name_sql(domain: str) -> str:
    """naming.domain_to_sql_name."""
    return f"TRIM(BOTH '_' FROM REGEXP_REPLACE({domain}, '[^a-zA-Z0-9]', '_', 'g'))"


def without_domain_prefix_sql(name: str, domain: str) -> str:
    """A table's name as schema_gen._assign_names reads it where domains prefix a name."""
    prefix = f"{domain_sql_name_sql(domain)} || '__'"
    return (
        f"CASE WHEN {setting('domain_prefix')} = 'true' "
        f"AND STARTS_WITH(LOWER({name}), LOWER({prefix})) "
        f"THEN SUBSTR({name}, LENGTH({prefix}) + 1) ELSE {name} END"
    )


def bare_field_sql(field: str, system_domain: str) -> str:
    """naming.bare_field_name of a field: ``system_domain`` is whether its domain is a system
    schema's (SQL), which has no ``__`` of its own to strip. Where domains prefix a name the
    prefix is all that goes, and :func:`without_domain_prefix_sql` has it already."""
    return (
        f"CASE WHEN {system_domain} OR {setting('domain_prefix')} = 'true' "
        f"OR POSITION('__' IN {field}) = 0 THEN {field} "
        f"ELSE SUBSTR({field}, POSITION('__' IN {field}) + 2) END"
    )
