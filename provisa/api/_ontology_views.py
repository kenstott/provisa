# Copyright (c) 2026 Kenneth Stott
# Canary: 5696debf-9fb6-42d8-b272-88faae48e8c1
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The ontology system schema's definition views (REQ-2163).

The ontology schema holds the model's meaning as meta holds its structure: the glossary's terms,
their bindings to columns, the edges between terms, and their domains and experts. Each is a
definition table named with a leading underscore, so it never meets a generated Class table, and
served from a ``<table>_ontology`` view over the control plane's glossary tables, as meta's tables
are from ``*_meta`` views. Dialect-neutral, as meta's are (provisa.api._meta_views).
"""

from __future__ import annotations

from provisa.sparql_query.vocabulary import OWL, RDF, RDFS, SKOS, xsd_of_sql


#: The definition tables, in the order their views are created: _domains first, since the
#: others read each term's domains from it.
ONTOLOGY_TABLES: list[str] = [
    "_terms",
    "_term_refs",
    "_term_edges",
    "_domains",
    "_experts",
    "_axioms",
    "_closure",
]

#: Each definition table's view.
ONTOLOGY_TABLE_ALIAS: dict[str, str] = {t: f"{t}_ontology" for t in ONTOLOGY_TABLES}

#: The glossary base table each definition table restates (its primary key is that table's); _axioms
#: and _closure restate none, and each is keyed by its own id.
ONTOLOGY_BASE_TABLE: dict[str, str | None] = {
    "_axioms": None,
    "_closure": None,
    "_terms": "glossary_terms",
    "_term_refs": "glossary_term_refs",
    "_term_edges": "glossary_term_edges",
    "_domains": "glossary_term_domains",
    "_experts": "glossary_term_experts",
}

ONTOLOGY_TABLE_VIEWS: dict[str, str] = {
    # REQ-1591: each term's domains by the one rule (provisa.core.repositories.glossary.
    # term_domains): the domains of the tables its refs point at while it holds refs, its declared
    # rows otherwise. "*" is the enterprise scope (REQ-1592). term_domains is the term's whole set
    # as |a|b|, the column every glossary view's readable-term row scope tests (REQ-1593); it is
    # first because the other glossary views read it.
    "_domains": """
        CREATE OR REPLACE VIEW _domains_ontology AS
        WITH d AS (
            SELECT DISTINCT r.term_id, rt.domain_id, r.tenant_id
            FROM (SELECT term_id, table_id, tenant_id FROM glossary_term_refs
                  UNION
                  SELECT class_term_id, id, tenant_id FROM registered_tables
                  WHERE class_term_id IS NOT NULL) r
            JOIN registered_tables rt ON rt.id = r.table_id
            UNION ALL
            SELECT g.term_id, g.domain_id, g.tenant_id
            FROM glossary_term_domains g
            WHERE NOT EXISTS (SELECT 1 FROM glossary_term_refs r WHERE r.term_id = g.term_id)
              AND NOT EXISTS (SELECT 1 FROM registered_tables r WHERE r.class_term_id = g.term_id)
        ),
        s AS (
            SELECT term_id, '|' || STRING_AGG(domain_id, '|') || '|' AS term_domains
            FROM d
            GROUP BY term_id
        )
        SELECT CAST(d.term_id AS TEXT) || ':' || d.domain_id AS id, d.term_id, d.domain_id,
               d.tenant_id, s.term_domains
        FROM d
        JOIN s ON s.term_id = d.term_id
    """,
    # REQ-1584: the business glossary as queryable metadata. `live` restates the export's
    # admission rule (provisa.core.glossary.live_term_ids) in SQL rather than approximating it:
    # in service, defined, and connected over in-service edges to a term holding a physical ref --
    # a column, or a whole table (REQ-1387, GROUNDED TO A TABLE).
    # Grounding is transitive, so it needs the recursive CTE — every control-plane dialect the
    # ontology schema is served from (PostgreSQL, SQLite, MySQL) supports WITH RECURSIVE in a view.
    # `rooted` here counts EVERY ref, which is the repository's reading of it; the exporter's
    # narrower count (refs whose column actually publishes) is a property of the export, not of
    # the glossary, and belongs in the export.
    # retired and export_excluded are COLUMNS, never predicates — ontology is an internal surface and
    # withholding is the export's job; a view that filtered them would disagree with the admin
    # surface it mirrors.
    "_terms": """
        CREATE OR REPLACE VIEW _terms_ontology AS
        WITH RECURSIVE in_service AS (
            SELECT id FROM glossary_terms WHERE retired = FALSE AND deprecated = FALSE
        ),
        conducting_edges AS (
            SELECT e.from_term_id AS a, e.to_term_id AS b
            FROM glossary_term_edges e
            JOIN in_service sa ON sa.id = e.from_term_id
            JOIN in_service sb ON sb.id = e.to_term_id
            UNION ALL
            SELECT e.to_term_id, e.from_term_id
            FROM glossary_term_edges e
            JOIN in_service sa ON sa.id = e.from_term_id
            JOIN in_service sb ON sb.id = e.to_term_id
        ),
        grounded AS (
            SELECT DISTINCT r.term_id AS id
            FROM (SELECT term_id FROM glossary_term_refs
                  UNION
                  SELECT class_term_id FROM registered_tables WHERE class_term_id IS NOT NULL) r
            JOIN in_service s ON s.id = r.term_id
            UNION
            SELECT ce.b
            FROM grounded g
            JOIN conducting_edges ce ON ce.a = g.id
        ),
        -- A class term grounded in several tables is a curation gap until same-entity
        -- relationships join them all -- every one reached from one of them (identity.py's
        -- identity_gap_ids is the same rule).
        same_entity_edges AS (
            SELECT r.source_table_id AS a, r.target_table_id AS b
            FROM relationships r WHERE r.same_entity
            UNION ALL
            SELECT r.target_table_id, r.source_table_id
            FROM relationships r WHERE r.same_entity
        ),
        identity_reach (term_id, table_id) AS (
            SELECT class_term_id, MIN(id) FROM registered_tables
            WHERE class_term_id IS NOT NULL GROUP BY class_term_id
            UNION
            SELECT ir.term_id, se.b
            FROM identity_reach ir
            JOIN same_entity_edges se ON se.a = ir.table_id
            JOIN registered_tables gt ON gt.class_term_id = ir.term_id AND gt.id = se.b
        )
        SELECT t.id, t.name, t.definition,
               CASE WHEN EXISTS (SELECT 1 FROM glossary_term_refs r WHERE r.term_id = t.id)
                      OR EXISTS (SELECT 1 FROM registered_tables r WHERE r.class_term_id = t.id)
                    THEN FALSE ELSE TRUE END AS is_abstract,
               t.deprecated, t.retired,
               t.export_excluded,
               (SELECT COUNT(*) FROM glossary_term_refs r WHERE r.term_id = t.id) AS ref_count,
               CASE WHEN EXISTS (SELECT 1 FROM glossary_term_refs r WHERE r.term_id = t.id)
                      OR EXISTS (SELECT 1 FROM registered_tables r WHERE r.class_term_id = t.id)
                    THEN TRUE ELSE FALSE END AS is_rooted,
               CASE WHEN t.id IN (SELECT id FROM grounded)
                     AND TRIM(COALESCE(t.definition, '')) <> ''
                    THEN TRUE ELSE FALSE END AS live,
               t.tenant_id,
               CASE WHEN t.id IN (SELECT id FROM grounded) THEN TRUE ELSE FALSE END AS grounded,
               CASE WHEN t.retired THEN 'retired'
                    WHEN t.deprecated OR t.deprecation_asserted THEN 'deprecated'
                    WHEN t.id IN (SELECT id FROM grounded)
                     AND TRIM(COALESCE(t.definition, '')) <> '' THEN 'live'
                    ELSE 'draft' END AS status,
               t.status_reason, t.retire_on,
               CASE WHEN (SELECT COUNT(*) FROM registered_tables r WHERE r.class_term_id = t.id)
                         > (SELECT COUNT(*) FROM identity_reach ir WHERE ir.term_id = t.id)
                    THEN TRUE ELSE FALSE END AS identity_gap,
               (SELECT MAX(x.term_domains) FROM _domains_ontology x WHERE x.term_id = t.id) AS term_domains,
               -- A Class term is one a table is grounded in; a Property term one bound
               -- to a column. A term may be both, or neither (abstract).
               CASE WHEN EXISTS (SELECT 1 FROM registered_tables r WHERE r.class_term_id = t.id)
                    THEN TRUE ELSE FALSE END AS is_class,
               CASE WHEN EXISTS (SELECT 1 FROM glossary_term_refs r WHERE r.term_id = t.id)
                    THEN TRUE ELSE FALSE END AS is_property
        FROM glossary_terms t
    """,
    "_term_refs": """
        CREATE OR REPLACE VIEW _term_refs_ontology AS
        SELECT r.id, r.term_id, r.table_id, r.column_name,
               CAST(r.table_id AS TEXT) || ':' || r.column_name AS column_key,
               rt.source_id, rt.schema_name, rt.table_name, rt.domain_id,
               r.tenant_id,
               (SELECT MAX(x.term_domains) FROM _domains_ontology x WHERE x.term_id = r.term_id) AS term_domains
        FROM glossary_term_refs r
        JOIN registered_tables rt ON rt.id = r.table_id
    """,
    "_term_edges": """
        CREATE OR REPLACE VIEW _term_edges_ontology AS
        SELECT e.id, e.from_term_id, e.to_term_id, e.rel_type,
               f.name AS from_term, t.name AS to_term,
               e.tenant_id,
               (SELECT MAX(x.term_domains) FROM _domains_ontology x WHERE x.term_id = e.from_term_id) AS from_term_domains,
               (SELECT MAX(x.term_domains) FROM _domains_ontology x WHERE x.term_id = e.to_term_id) AS to_term_domains
        FROM glossary_term_edges e
        JOIN glossary_terms f ON f.id = e.from_term_id
        JOIN glossary_terms t ON t.id = e.to_term_id
    """,
    # user_id is the only identity data in the glossary set. It is exposed and governed by the
    # ordinary column visibility rule rather than omitted — a governance query that cannot name
    # the expert cannot route a question to them.
    "_experts": """
        CREATE OR REPLACE VIEW _experts_ontology AS
        SELECT x.id, x.term_id, x.user_id, x.kind, t.name AS term_name, x.tenant_id
        FROM glossary_term_experts x
        JOIN glossary_terms t ON t.id = x.term_id
    """,
    # REQ-2163: the ontology stated as RDFS/OWL/SKOS, one triple a row, for SPARQL to lower its
    # RDFS/OWL/SKOS patterns to (SQL's alone: no GraphQL field, no Cypher label). A subject is a
    # term, named by its name (the answer writes its IRI); an object is a term, an IRI or a literal.
    # A retired term states nothing. Class terms are owl:Class and Property terms owl:*Property; a
    # kind-of edge from a Class is rdfs:subClassOf, from a Property rdfs:subPropertyOf. A
    # Property's rdfs:domain is the Class of each table it binds a column of. It is an
    # owl:ObjectProperty when every column it binds is a many-to-one key to a table with a Class,
    # whose Class is then its rdfs:range; otherwise an owl:DatatypeProperty ranging over its
    # columns' xsd types. Every term has its name as skos:prefLabel and its definition, when it
    # has one, as skos:definition.
    "_axioms": f"""
        CREATE OR REPLACE VIEW _axioms_ontology AS
        WITH t AS (
            SELECT id, name, definition, is_class, is_property, term_domains, tenant_id
            FROM _terms_ontology WHERE NOT retired
        ),
        bound AS (
            SELECT r.term_id, rt.class_term_id AS domain_term_id, tc.data_type,
                   (SELECT MIN(tt.class_term_id)
                    FROM relationships rel
                    JOIN registered_tables tt ON tt.id = rel.target_table_id
                    WHERE rel.source_table_id = r.table_id AND rel.source_column = r.column_name
                      AND rel.cardinality = 'many-to-one' AND rel.via_table_id IS NULL
                      AND tt.class_term_id IS NOT NULL) AS range_term_id
            FROM glossary_term_refs r
            JOIN registered_tables rt ON rt.id = r.table_id
            LEFT JOIN table_columns tc
              ON tc.table_id = r.table_id AND tc.column_name = r.column_name
        ),
        object_properties AS (
            SELECT term_id FROM bound GROUP BY term_id HAVING COUNT(*) = COUNT(range_term_id)
        ),
        x (subject_id, predicate, object_id, object_iri, object_literal) AS (
            -- The first branch types the columns: an object term's id, and a literal's text.
            SELECT id, '{RDF}type', CAST(NULL AS INTEGER), '{OWL}Class', CAST(NULL AS TEXT)
            FROM t WHERE is_class
            UNION
            SELECT id, '{RDF}type', NULL,
                   CASE WHEN id IN (SELECT term_id FROM object_properties)
                        THEN '{OWL}ObjectProperty' ELSE '{OWL}DatatypeProperty' END,
                   NULL
            FROM t WHERE is_property
            UNION
            SELECT e.from_term_id,
                   CASE WHEN f.is_class THEN '{RDFS}subClassOf' ELSE '{RDFS}subPropertyOf' END,
                   e.to_term_id, NULL, NULL
            FROM glossary_term_edges e
            JOIN t f ON f.id = e.from_term_id
            WHERE e.rel_type = 'KIND_OF' AND (f.is_class OR f.is_property)
            UNION
            SELECT term_id, '{RDFS}domain', domain_term_id, NULL, NULL
            FROM bound WHERE domain_term_id IS NOT NULL
            UNION
            SELECT term_id, '{RDFS}range', range_term_id, NULL, NULL
            FROM bound WHERE term_id IN (SELECT term_id FROM object_properties)
            UNION
            SELECT term_id, '{RDFS}range', NULL, {xsd_of_sql("data_type")}, NULL
            FROM bound
            WHERE term_id NOT IN (SELECT term_id FROM object_properties) AND data_type IS NOT NULL
            UNION
            SELECT id, '{SKOS}prefLabel', NULL, NULL, name FROM t
            UNION
            SELECT id, '{SKOS}definition', NULL, NULL, definition
            FROM t WHERE TRIM(COALESCE(definition, '')) <> ''
        )
        SELECT CAST(x.subject_id AS TEXT) || ' ' || x.predicate || ' '
                   || COALESCE(CAST(x.object_id AS TEXT), x.object_iri, '"' || x.object_literal)
                   AS id,
               x.subject_id AS subject_term_id, s.name AS subject, x.predicate,
               x.object_id AS object_term_id, o.name AS object, x.object_iri, x.object_literal,
               s.term_domains AS subject_domains, o.term_domains AS object_domains, s.tenant_id
        FROM x
        JOIN t s ON s.id = x.subject_id
        LEFT JOIN t o ON o.id = x.object_id
        WHERE x.object_id IS NULL OR o.id IS NOT NULL
    """,
    # REQ-2163: rdfs:subClassOf and rdfs:subPropertyOf closed over, for SPARQL's * and + paths and
    # the instances of a class's sub-classes (SQL's alone, as _axioms is). One row per (predicate,
    # sub-term, super-term), with hops 0 for a term itself (the zero-length path) and 1 for one or
    # more kind-of steps. A kind-of edge is a sub-class edge from a Class and a sub-property edge
    # from a Property, as in _axioms. The rows are a finite set and UNION drops repeats, so the
    # recursion ends on a cycle too.
    "_closure": f"""
        CREATE OR REPLACE VIEW _closure_ontology AS
        WITH RECURSIVE t AS (
            SELECT id, name, is_class, is_property, term_domains, tenant_id
            FROM _terms_ontology WHERE NOT retired
        ),
        kinds (predicate, sub_id, sup_id) AS (
            SELECT CASE WHEN f.is_class THEN '{RDFS}subClassOf' ELSE '{RDFS}subPropertyOf' END,
                   e.from_term_id, e.to_term_id
            FROM glossary_term_edges e
            JOIN t f ON f.id = e.from_term_id
            JOIN t g ON g.id = e.to_term_id
            WHERE e.rel_type = 'KIND_OF' AND (f.is_class OR f.is_property)
        ),
        reach (predicate, sub_id, sup_id) AS (
            SELECT predicate, sub_id, sup_id FROM kinds
            UNION
            SELECT r.predicate, r.sub_id, k.sup_id
            FROM reach r JOIN kinds k ON k.predicate = r.predicate AND k.sub_id = r.sup_id
        ),
        pairs (predicate, sub_id, sup_id, hops) AS (
            SELECT predicate, sub_id, sup_id, 1 FROM reach
            UNION
            SELECT '{RDFS}subClassOf', id, id, 0 FROM t WHERE is_class
            UNION
            SELECT '{RDFS}subPropertyOf', id, id, 0 FROM t WHERE is_property
            UNION
            SELECT predicate, sup_id, sup_id, 0 FROM kinds
        )
        SELECT x.predicate || ' ' || CAST(x.sub_id AS TEXT) || ' ' || CAST(x.sup_id AS TEXT)
                   || ' ' || CAST(x.hops AS TEXT) AS id,
               x.predicate, x.sub_id AS subject_term_id, s.name AS subject,
               x.sup_id AS object_term_id, o.name AS object, x.hops,
               s.term_domains AS subject_domains, o.term_domains AS object_domains, s.tenant_id
        FROM pairs x
        JOIN t s ON s.id = x.sub_id
        JOIN t o ON o.id = x.sup_id
    """,
}
