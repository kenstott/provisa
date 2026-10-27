# Copyright (c) 2026 Kenneth Stott
# Canary: 09a7a919-8450-4462-9aaa-549b9a4c13a8
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""REQ-2163 step 1: the glossary's definitions are the ontology system schema's, not meta's.

The ontology schema holds the model's meaning as meta holds its structure: the terms, their
bindings to columns, the edges between terms, their domains and experts, each a definition table
named with a leading underscore so it never meets a generated Class table. A binding still links
a term to a column in meta, across the two schemas. Read rights are the glossary's own
(readable_term), applied as they were in meta.
"""

from __future__ import annotations

from provisa.api._meta_seed import META_RELATIONSHIPS
from provisa.api._meta_views import _META_TABLE_VIEWS
from provisa.api._ontology_views import (
    ONTOLOGY_TABLE_ALIAS,
    ONTOLOGY_TABLE_VIEWS,
    ONTOLOGY_TABLES,
)
from provisa.api.app_loaders import _META_TABLE_ALIAS, _META_TABLES
from provisa.compiler.schema_gen import _IMPLICIT_TRAVERSAL_DOMAINS
from provisa.core import domain_policy
from provisa.core.db import SEEDED_DOMAIN_IDS
from provisa.security.rights import GLOSSARY_SCOPED_VIEWS, ONTOLOGY_DOMAIN_ID

DEFINITIONS = ("_terms", "_term_refs", "_term_edges", "_domains", "_experts", "_axioms")
_GLOSSARY = (
    "glossary_terms",
    "glossary_term_refs",
    "glossary_term_edges",
    "glossary_term_domains",
    "glossary_term_experts",
)


def test_the_definitions_are_the_ontology_schemas_tables():
    assert ONTOLOGY_DOMAIN_ID == "ontology"
    assert tuple(ONTOLOGY_TABLES) == DEFINITIONS
    # Each is served from its own view, named for the schema as meta's and ops's are.
    assert ONTOLOGY_TABLE_ALIAS == {t: f"{t}_ontology" for t in DEFINITIONS}
    assert set(ONTOLOGY_TABLE_VIEWS) == set(DEFINITIONS)
    for table, ddl in ONTOLOGY_TABLE_VIEWS.items():
        assert f"CREATE OR REPLACE VIEW {table}_ontology AS" in ddl


def test_meta_no_longer_serves_the_glossary():
    assert not set(_GLOSSARY) & set(_META_TABLES)
    assert not set(_GLOSSARY) & set(_META_TABLE_ALIAS)
    assert not set(_GLOSSARY) & set(_META_TABLE_VIEWS)
    for ddl in _META_TABLE_VIEWS.values():
        assert "glossary_term_domains_meta" not in ddl and "glossary_terms_meta" not in ddl


def test_the_ontology_views_read_one_another_not_metas():
    for ddl in ONTOLOGY_TABLE_VIEWS.values():
        assert "_meta " not in ddl and "_meta\n" not in ddl
    # Every view after the first reads each term's domains from _domains.
    assert "_domains_ontology" in ONTOLOGY_TABLE_VIEWS["_terms"]


def test_the_definitions_relate_within_ontology_and_bind_columns_in_meta():
    rows = {r[0]: r for r in META_RELATIONSHIPS}
    assert not [r for r in rows if r.startswith("meta:glossary")]
    # (id, src_source, src_table, src_col, tgt_col, cardinality, tgt_source, tgt_table, ...)
    refs = rows["ontology:_terms:refs"]
    assert (refs[2], refs[7], refs[9]) == ("_terms", "_term_refs", "refs")
    column = rows["ontology:_term_refs:table_columns"]
    assert (column[2], column[3], column[7]) == ("_term_refs", "column_key", "table_columns")
    tables = rows["ontology:_terms:tables"]
    assert (tables[2], tables[7]) == ("_terms", "registered_tables")


def test_ontology_is_a_system_domain_every_role_may_traverse():
    assert ONTOLOGY_DOMAIN_ID in SEEDED_DOMAIN_IDS
    assert ONTOLOGY_DOMAIN_ID in domain_policy.system_domain_ids()
    assert ONTOLOGY_DOMAIN_ID in _IMPLICIT_TRAVERSAL_DOMAINS


def test_a_terms_rows_are_read_by_the_glossarys_own_rule_in_ontology():
    assert GLOSSARY_SCOPED_VIEWS == {
        "_terms_ontology": ("term_domains",),
        "_term_refs_ontology": ("term_domains",),
        "_term_edges_ontology": ("from_term_domains", "to_term_domains"),
        "_domains_ontology": ("term_domains",),
        "_axioms_ontology": ("subject_domains", "object_domains"),
    }
