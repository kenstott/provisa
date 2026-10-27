# Copyright (c) 2026 Kenneth Stott
# Canary: b994d8ff-89f4-4e9a-a4e3-3747a04445ac
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Meta-domain SQL view definitions (extracted from api/app.py).

These views expose the admin tables to the engine surface. They are dialect-neutral:
the control plane's list-valued columns are ``JSON`` everywhere (portable SQLAlchemy
type — never PG ``TEXT[]``), so no ``array_to_json``/``::text`` normalizers are needed,
and ``DROP VIEW`` takes no ``CASCADE``. Runs verbatim on PostgreSQL, SQLite, MySQL, …
"""

from __future__ import annotations

# REQ-1132/REQ-1134: meta (catalog) CORE vs GOVERNANCE column split + the meta domain id. The single
# source of truth lives in provisa.security.rights so every query surface shares it; re-exported here
# for callers near the view definitions.
from provisa.security.rights import GOVERNANCE_META_COLUMNS, META_DOMAIN_ID  # noqa: E402,F401
from provisa.core.models import DERIVED_SOURCE_ID, DERIVED_TAGS, SYSTEM_TAGS
from provisa.dq.contract import CHECKERS

# REQ-1375/REQ-1443: system and derived tags are code-defined intrinsics with no rows in the tags
# table, so the meta view unions them in from the constants — the registry a query surface sees is
# complete. Built at import time; never hand-edit the literals.
import json as _json

from provisa.compiler.sql_literals import sql_literal


def _system_tag_rows_sql() -> str:
    rows = []
    for tag in SYSTEM_TAGS + DERIVED_TAGS:
        desc = sql_literal(tag.description, "postgres")[1:-1]
        applies = _json.dumps(tag.applies_to).replace(" ", "")
        rows.append(
            f"SELECT '{tag.id}' AS id, '{desc}' AS description, "
            f"'{applies}' AS applies_to, TRUE AS is_system, "
            f"{'TRUE' if tag.derived else 'FALSE'} AS derived, "
            f"'{tag.reason_policy}' AS reason_policy, "
            f"'{tag.expires_policy}' AS expires_policy, "
            f"'{tag.param_policy}' AS param_policy, NULL AS tenant_id"  # REQ-1467
        )
    return "\n        UNION ALL\n        ".join(rows)


# REQ-1443: the checker source types, as a SQL IN-list. Built from the constant so the view and
# provisa.core.derived_tags cannot disagree about what a data-quality source is.
_CHECKER_TYPES_SQL = ", ".join(f"'{name}'" for name in sorted(CHECKERS))


def _sensitive_assignments_sql() -> str:
    """REQ-2091: the sensitive (table, column, tag) assignments, as the one statement
    provisa.security.sensitive.sensitive_assignments defines -- the same reading of sensitivity
    the environment's Test (fake) choice applies (provisa.core.env_data)."""
    from sqlalchemy.dialects import postgresql

    from provisa.core.schema_org import (
        glossary_term_refs,
        glossary_terms,
        registered_tables,
        tag_assignments,
        tags,
    )
    from provisa.security.sensitive import sensitive_assignments

    statement = sensitive_assignments(
        tag_assignments, registered_tables, tags, glossary_term_refs, glossary_terms
    )
    return str(
        statement.compile(dialect=postgresql.dialect(), compile_kwargs={"literal_binds": True})
    )


def _sql_names(names: tuple[str, ...]) -> str:
    return ", ".join(f"'{n}'" for n in names)


def _sensitive_coverage_sql() -> str:
    """REQ-2091: one row per sensitive column -- its tags, its fake and synthetic rule as declared,
    and whether it is covered, has no fake, or has a fake or rule an adequacy check failed (the
    latest run of each check, provisa.fakes.adequacy)."""
    from provisa.fakes.adequacy import COPIES_REAL, DERIVES_FROM_REAL, KIND_MISMATCH, LEAK_RATE

    kind = (
        "CASE WHEN POSITION('(' IN tc.fake) > 0 "
        "THEN SUBSTR(tc.fake, 1, POSITION('(' IN tc.fake) - 1) ELSE tc.fake END"
    )

    def latest(check: str, alias: str) -> str:
        return (
            f"LEFT JOIN latest {alias} ON {alias}.table_id = c.table_id "
            f"AND {alias}.column_name = c.column_name AND {alias}.check_name = '{check}'"
        )

    return f"""
        CREATE OR REPLACE VIEW sensitive_coverage AS
        WITH sens AS ({_sensitive_assignments_sql()}),
        cols AS (
            SELECT table_id, table_name, column_name,
                   STRING_AGG(DISTINCT base_tag_id, ', ' ORDER BY base_tag_id) AS tags
            FROM sens
            GROUP BY table_id, table_name, column_name
        ),
        latest AS (
            SELECT k.table_id, k.column_name, k.check_name, k.status, k.measure, k.detail,
                   k.checked_at
            FROM fake_adequacy_checks k
            WHERE k.checked_at = (
                SELECT MAX(x.checked_at) FROM fake_adequacy_checks x
                WHERE x.table_id = k.table_id AND x.column_name = k.column_name
                  AND x.check_name = k.check_name
            )
        ),
        failed AS (
            SELECT table_id, column_name,
                   STRING_AGG(check_name || ': ' || detail, '; ' ORDER BY check_name) AS reason
            FROM latest
            WHERE status = 'failed'
            GROUP BY table_id, column_name
        )
        SELECT CAST(c.table_id AS TEXT) || ':' || c.column_name AS column_key,
               c.table_id, c.table_name, rt.domain_id, c.column_name, c.tags,
               CASE WHEN tc.fake IS NOT NULL THEN TRUE ELSE FALSE END AS has_fake,
               CASE WHEN tc.synthetic_rule IS NOT NULL THEN TRUE ELSE FALSE END
                   AS has_synthetic_rule,
               tc.fake,
               CASE WHEN tc.fake IS NULL THEN NULL ELSE {kind} END AS fake_kind,
               CASE WHEN tc.fake IS NULL THEN NULL ELSE TRUE END AS fake_deterministic,
               CASE WHEN tc.fake IS NULL THEN NULL ELSE tc.fake_stable END AS fake_stable,
               CASE WHEN tc.fake IS NULL THEN NULL
                    WHEN {kind} IN ({_sql_names(DERIVES_FROM_REAL)}) THEN TRUE
                    ELSE FALSE END AS fake_derives_from_real,
               tc.synthetic_rule,
               CASE WHEN tc.fake IS NULL THEN 'no_fake'
                    WHEN f.reason IS NOT NULL THEN 'inadequate'
                    ELSE 'covered' END AS status,
               CASE WHEN tc.fake IS NULL THEN 'no fake declared' ELSE f.reason END AS reason,
               lr.measure AS leak_rate, lr.status AS leak_rate_status,
               km.status AS kind_mismatch_status, cr.status AS copies_real_status,
               (SELECT MAX(l.checked_at) FROM latest l
                WHERE l.table_id = c.table_id AND l.column_name = c.column_name) AS checked_at,
               CAST(rt.tenant_id AS TEXT) AS tenant_id
        FROM cols c
        JOIN registered_tables rt ON rt.id = c.table_id
        JOIN table_columns tc ON tc.table_id = c.table_id AND tc.column_name = c.column_name
        LEFT JOIN failed f ON f.table_id = c.table_id AND f.column_name = c.column_name
        {latest(LEAK_RATE, "lr")}
        {latest(KIND_MISMATCH, "km")}
        {latest(COPIES_REAL, "cr")}
    """


def _data_product_edges_sql() -> str:
    """REQ-2126: a data product's links to the rest of the model.

    A relationship edge follows the registered relationships, either way along each, from the
    product's member tables through tables that belong to no product. It stops at another
    product's member, and the row targets that product; a table that belongs to no product is a
    target only where nothing reachable through it is another product's member (otherwise it
    supports that product's row). A related edge is a table one declared relationship from a
    member, not one of the product's own. A product's lineage is no row here: it is cut to its
    caller's read rights, so it is served by the product lineage endpoint (REQ-2156).

    Everything is computed from reachability, never by enumerating routes: the path count is the
    number of distinct relationships leaving the members that lead on to the target, the hops are
    the shortest distance, and the aliases and supporting tables are those on any walk from a
    member to the target. Each recursion's rows are bounded by the model's tables, so the cost
    grows with the model, not with the routes through it.

    Every supporting table a row names is named as the model names it: its domain and its alias,
    or its name when it has none."""
    return """
        CREATE OR REPLACE VIEW data_product_edges AS
        WITH RECURSIVE
        -- Each registered table by the name the model gives it: its domain and its alias, or its
        -- name when it has none -- every reference this view returns is a model name.
        model_names AS (
            SELECT id AS table_id, domain_id || '.' || COALESCE(alias, table_name) AS model_name
            FROM registered_tables
        ),
        members AS (
            SELECT id AS table_id, product_id, dq_contract
            FROM registered_tables
            WHERE product_id IS NOT NULL AND NOT draft
        ),
        free AS (
            SELECT id AS table_id FROM registered_tables WHERE product_id IS NULL AND NOT draft
        ),
        links AS (
            SELECT r.id AS rel_id, r.alias, r.source_table_id AS a, r.target_table_id AS b
            FROM relationships r
            JOIN registered_tables s ON s.id = r.source_table_id AND NOT s.draft
            JOIN registered_tables t ON t.id = r.target_table_id AND NOT t.draft
            WHERE r.source_table_id <> r.target_table_id
            UNION ALL
            SELECT r.id, r.alias, r.target_table_id, r.source_table_id
            FROM relationships r
            JOIN registered_tables s ON s.id = r.source_table_id AND NOT s.draft
            JOIN registered_tables t ON t.id = r.target_table_id AND NOT t.draft
            WHERE r.source_table_id <> r.target_table_id
        ),
        -- (product, the relationship leaving its member, a table reached): walking on only
        -- through tables in no product, never into the product's own members.
        reach (product_id, first_rel, node) AS (
            SELECT m.product_id, l.rel_id, l.b
            FROM members m
            JOIN links l ON l.a = m.table_id
            WHERE NOT EXISTS (
                SELECT 1 FROM members own
                WHERE own.table_id = l.b AND own.product_id = m.product_id)
            UNION
            SELECT r.product_id, r.first_rel, l.b
            FROM reach r
            JOIN free f ON f.table_id = r.node
            JOIN links l ON l.a = r.node
            WHERE NOT EXISTS (
                SELECT 1 FROM members own
                WHERE own.table_id = l.b AND own.product_id = r.product_id)
        ),
        -- (product, table reached, hops): every distance the walk reaches a table at, up to the
        -- number of tables; the least is the shortest.
        dist (product_id, node, hops) AS (
            SELECT m.product_id, l.b, 1
            FROM members m
            JOIN links l ON l.a = m.table_id
            WHERE NOT EXISTS (
                SELECT 1 FROM members own
                WHERE own.table_id = l.b AND own.product_id = m.product_id)
            UNION
            SELECT d.product_id, l.b, d.hops + 1
            FROM dist d
            JOIN free f ON f.table_id = d.node
            JOIN links l ON l.a = d.node
            WHERE d.hops < (SELECT COUNT(*) FROM registered_tables)
              AND NOT EXISTS (
                  SELECT 1 FROM members own
                  WHERE own.table_id = l.b AND own.product_id = d.product_id)
        ),
        -- (end table, a table in no product it can be reached from through tables in no product).
        back (end_table, node) AS (
            SELECT l.b, l.a
            FROM links l
            JOIN free f ON f.table_id = l.a
            UNION
            SELECT k.end_table, l.a
            FROM back k
            JOIN links l ON l.b = k.node
            JOIN free f ON f.table_id = l.a
            WHERE l.a <> k.end_table
        ),
        -- Each target and the tables it ends at: another product (its members reached), or a
        -- table in no product from which no other product's member can be reached.
        targets AS (
            SELECT DISTINCT r.product_id, m.product_id AS target_product_id,
                   CAST(NULL AS INTEGER) AS target_table_id, r.node AS end_table
            FROM reach r
            JOIN members m ON m.table_id = r.node
            UNION
            SELECT DISTINCT r.product_id, CAST(NULL AS TEXT), r.node, r.node
            FROM reach r
            JOIN free f ON f.table_id = r.node
            WHERE NOT EXISTS (
                SELECT 1 FROM back k
                JOIN members other ON other.table_id = k.end_table
                WHERE k.node = r.node AND other.product_id <> r.product_id)
        ),
        rel_rows AS (
            SELECT t.product_id, t.target_product_id, t.target_table_id,
                   COUNT(DISTINCT r.first_rel) AS path_count,
                   (SELECT MIN(d.hops) FROM dist d
                    JOIN targets e ON e.product_id = d.product_id AND e.end_table = d.node
                    WHERE e.product_id = t.product_id
                      AND e.target_product_id IS NOT DISTINCT FROM t.target_product_id
                      AND e.target_table_id IS NOT DISTINCT FROM t.target_table_id) AS hops
            FROM targets t
            JOIN reach r ON r.product_id = t.product_id AND r.node = t.end_table
            GROUP BY t.product_id, t.target_product_id, t.target_table_id
        ),
        -- The tables in no product on a walk from a product's member to a target.
        supporting AS (
            SELECT DISTINCT t.product_id, t.target_product_id, t.target_table_id, k.node
            FROM targets t
            JOIN back k ON k.end_table = t.end_table
            JOIN reach r ON r.product_id = t.product_id AND r.node = k.node
            WHERE t.target_table_id IS NULL OR k.node <> t.target_table_id
        ),
        -- The relationships on such a walk: from a member or a supporting table, to a supporting
        -- table or the target's end.
        on_walk AS (
            SELECT t.product_id, t.target_product_id, t.target_table_id, l.rel_id
            FROM targets t
            JOIN links l ON l.b = t.end_table
            JOIN members m ON m.table_id = l.a AND m.product_id = t.product_id
            UNION
            SELECT s.product_id, s.target_product_id, s.target_table_id, l.rel_id
            FROM supporting s
            JOIN links l ON l.b = s.node
            JOIN members m ON m.table_id = l.a AND m.product_id = s.product_id
            UNION
            SELECT s.product_id, s.target_product_id, s.target_table_id, l.rel_id
            FROM supporting s
            JOIN links l ON l.a = s.node
            JOIN supporting s2
              ON s2.product_id = s.product_id
             AND s2.target_product_id IS NOT DISTINCT FROM s.target_product_id
             AND s2.target_table_id IS NOT DISTINCT FROM s.target_table_id
             AND s2.node = l.b
            UNION
            SELECT s.product_id, s.target_product_id, s.target_table_id, l.rel_id
            FROM supporting s
            JOIN links l ON l.a = s.node
            JOIN targets t
              ON t.product_id = s.product_id
             AND t.target_product_id IS NOT DISTINCT FROM s.target_product_id
             AND t.target_table_id IS NOT DISTINCT FROM s.target_table_id
             AND t.end_table = l.b
        ),
        rel_detail AS (
            SELECT w.product_id, w.target_product_id, w.target_table_id,
                   STRING_AGG(DISTINCT r.alias, ', ' ORDER BY r.alias) AS aliases,
                   STRING_AGG(DISTINCT r.id, ', ' ORDER BY r.id) AS relationship_ids
            FROM on_walk w
            JOIN relationships r ON r.id = w.rel_id
            GROUP BY w.product_id, w.target_product_id, w.target_table_id
        ),
        rel_support AS (
            SELECT s.product_id, s.target_product_id, s.target_table_id,
                   STRING_AGG(DISTINCT t.model_name, ', ' ORDER BY t.model_name)
                       AS supporting_tables
            FROM supporting s
            JOIN model_names t ON t.table_id = s.node
            GROUP BY s.product_id, s.target_product_id, s.target_table_id
        ),
        -- Every table one declared relationship from a member that is not one of the product's
        -- own: the panel's Related.
        related AS (
            SELECT m.product_id, l.b AS table_id,
                   COUNT(DISTINCT l.rel_id) AS path_count,
                   STRING_AGG(DISTINCT r.alias, ', ' ORDER BY r.alias) AS aliases,
                   STRING_AGG(DISTINCT r.id, ', ' ORDER BY r.id) AS relationship_ids
            FROM members m
            JOIN links l ON l.a = m.table_id
            JOIN relationships r ON r.id = l.rel_id
            WHERE NOT EXISTS (
                SELECT 1 FROM members own
                WHERE own.table_id = l.b AND own.product_id = m.product_id)
            GROUP BY m.product_id, l.b
        )
        SELECT r.product_id || ':relationship:'
                   || CASE WHEN r.target_product_id IS NOT NULL
                           THEN 'product:' || r.target_product_id
                           ELSE 'table:' || CAST(r.target_table_id AS TEXT) END AS id,
               r.product_id, 'relationship' AS kind,
               r.target_product_id, r.target_table_id,
               CASE WHEN r.hops = 1 THEN TRUE ELSE FALSE END AS direct,
               r.hops, r.path_count, d.aliases, d.relationship_ids, s.supporting_tables
        FROM rel_rows r
        JOIN rel_detail d
          ON d.product_id = r.product_id
         AND d.target_product_id IS NOT DISTINCT FROM r.target_product_id
         AND d.target_table_id IS NOT DISTINCT FROM r.target_table_id
        LEFT JOIN rel_support s
          ON s.product_id = r.product_id
         AND s.target_product_id IS NOT DISTINCT FROM r.target_product_id
         AND s.target_table_id IS NOT DISTINCT FROM r.target_table_id
        UNION ALL
        SELECT rl.product_id || ':related:' || CAST(rl.table_id AS TEXT),
               rl.product_id, 'related', o.product_id, rl.table_id,
               TRUE, 1, rl.path_count, rl.aliases, rl.relationship_ids, NULL
        FROM related rl
        LEFT JOIN members o ON o.table_id = rl.table_id
    """


def _table_inputs_sql() -> str:
    """REQ-2126: each registered table's nearest physical inputs -- an internal control-plane
    view, read by provisa.lineage.inputs, so the walk is
    written once.

    From a table, its lineage (the lineage edge table, REQ-2129) is walked upstream through plain
    views, which are expanded at query time and so are no input, and stops at the first physical
    dataset on each path: a source table or a materialized view. A materialized view's inputs are
    what its refresh reads, found the same way. A relation that is not registered ends a path."""
    return """
        CREATE OR REPLACE VIEW table_inputs AS
        WITH RECURSIVE ledges AS (
            SELECT DISTINCT source_relation AS src, source_table_id AS src_id,
                   target_relation AS tgt, target_table_id AS tgt_id
            FROM lineage_edges
            WHERE source_relation <> target_relation
        ),
        kinds AS (
            SELECT t.id, t.source_id, t.schema_name, t.table_name,
                   CASE WHEN t.source_id <> '{derived}' THEN 'source_table'
                        WHEN t.materialize THEN 'materialized_view'
                        ELSE 'view' END AS kind
            FROM registered_tables t
            WHERE NOT t.draft
        ),
        walk (table_id, relation, relation_id) AS (
            SELECT e.tgt_id, e.src, e.src_id
            FROM ledges e
            WHERE e.tgt_id IS NOT NULL
            UNION
            SELECT w.table_id, e.src, e.src_id
            FROM walk w
            JOIN kinds k ON k.id = w.relation_id AND k.kind = 'view'
            JOIN ledges e ON e.tgt = w.relation
        )
        SELECT DISTINCT w.table_id, w.relation_id AS input_table_id,
               w.relation AS input_relation, k.kind AS input_kind,
               k.source_id || '/' || k.schema_name || '/' || k.table_name AS input_address
        FROM walk w
        JOIN kinds k ON k.id = w.relation_id AND k.kind IN ('source_table', 'materialized_view')
    """.replace("{derived}", DERIVED_SOURCE_ID)


_META_TABLE_VIEWS: dict[str, str] = {
    # REQ-1373/1377: the tag registry and its assignments as queryable metadata, so
    # governance reporting (e.g. expiring deprecations) runs through the governed pipeline.
    # applies_to and tenant_id are CAST to TEXT: the system-tag branches supply applies_to
    # as a string literal and tenant_id as a bare NULL, and PG's pairwise UNION resolution
    # types those as text — refusing to match the table's JSONB/UUID columns. The casts
    # make every branch text, the same shape SQLite (JSON/UUID-as-TEXT) already returns.
    "tags": f"""
        CREATE OR REPLACE VIEW tags_meta AS
        {_system_tag_rows_sql()}
        UNION ALL
        SELECT id, description, CAST(applies_to AS TEXT) AS applies_to, is_system,
               FALSE AS derived, reason_policy, expires_policy, param_policy,
               CAST(tenant_id AS TEXT) AS tenant_id
        FROM tags
    """,
    # REQ-1443: derived tags are computed, never assigned, so they have no rows in
    # tag_assignments and cannot be unioned into it — a governance query that treats a
    # derivation as an assignment would offer the operator an unassign that does nothing.
    # They get their own view, whose WHERE clauses restate provisa.core.derived_tags.
    "derived_tags": f"""
        CREATE OR REPLACE VIEW derived_tags AS
        SELECT rt.modeling_role AS tag_id, 'table' AS object_type, rt.source_id,
               rt.id AS table_id, rt.table_name, rt.domain_id,
               'table:' || CAST(rt.id AS TEXT) AS object_key,
               CAST(rt.tenant_id AS TEXT) AS tenant_id
        FROM registered_tables rt
        WHERE rt.modeling_role IN ('fact', 'dimension') AND NOT rt.draft
        UNION ALL
        SELECT 'data_quality', 'table', rt.source_id, rt.id, rt.table_name, rt.domain_id,
               'table:' || CAST(rt.id AS TEXT), CAST(rt.tenant_id AS TEXT)
        FROM registered_tables rt
        JOIN sources s ON s.id = rt.source_id
        WHERE s.type IN ({_CHECKER_TYPES_SQL}) AND rt.dq_contract IS NOT NULL AND NOT rt.draft
    """,
    # REQ-1443 clause 10: a checker table's rules, one row per check of its contract in the order
    # authored -- parsed once at registration (provisa.dq.registration) and stored on the table.
    "dq_rules": """
        CREATE OR REPLACE VIEW dq_rules AS
        SELECT CAST(rt.id AS TEXT) || ':' || CAST(r.position AS TEXT) AS id,
               rt.id AS table_id, rt.domain_id, s.type AS checker, r.position,
               r.rule ->> 'column_name' AS column_name,
               r.rule ->> 'check_type' AS check_type,
               r.rule ->> 'definition' AS definition,
               rt.tenant_id
        FROM registered_tables rt
        JOIN sources s ON s.id = rt.source_id
        CROSS JOIN LATERAL jsonb_array_elements(rt.dq_rules) WITH ORDINALITY AS r(rule, position)
        WHERE rt.dq_rules IS NOT NULL AND NOT rt.draft
    """,
    # REQ-2091: every sensitive column's coverage by a fake, with the latest adequacy checks.
    "sensitive_coverage": _sensitive_coverage_sql(),
    # REQ-2126: a data product's links to the rest of the model, one row per product, target and
    # kind.
    # REQ-2126: each table's nearest physical inputs; internal, read by the views below and by
    # provisa.lineage.inputs. Registered in no catalog.
    "table_inputs": _table_inputs_sql(),
    "data_product_edges": _data_product_edges_sql(),
    # REQ-2125: product_id is the tagged data product, when the object is one.
    "tag_assignments": """
        CREATE OR REPLACE VIEW tag_assignments_meta AS
        SELECT id, tag_id, base_tag_id, object_type, source_id, table_id, column_name,
               relationship_id, command_name, object_key, reason, expires_on,
               -- the column identity the tag graph joins on, shared with
               -- table_columns and glossary_term_refs. NULL for a non-column assignment,
               -- which is what makes the join select only the column-level tags.
               CAST(table_id AS TEXT) || ':' || column_name AS column_key,
               tenant_id,
               product_id,
               term_id  -- the glossary term, when the object is one
        FROM tag_assignments
    """,
    # REQ-2145: every tag each column carries, its own and its glossary term's, each saying where
    # it comes from -- computed here, never copied into tag_assignments.
    "effective_column_tags": """
        CREATE OR REPLACE VIEW effective_column_tags AS
        SELECT 'column:' || CAST(ta.id AS TEXT) AS id,
               CAST(ta.table_id AS TEXT) || ':' || ta.column_name AS column_key,
               ta.table_id, ta.column_name, ta.tag_id, ta.base_tag_id,
               'column' AS via, CAST(NULL AS INTEGER) AS term_id, CAST(NULL AS TEXT) AS term,
               ta.reason, CAST(ta.tenant_id AS TEXT) AS tenant_id
        FROM tag_assignments ta
        WHERE ta.object_type = 'column'
        UNION ALL
        SELECT 'term:' || CAST(ta.id AS TEXT) || ':' || CAST(r.id AS TEXT),
               CAST(r.table_id AS TEXT) || ':' || r.column_name,
               r.table_id, r.column_name, ta.tag_id, ta.base_tag_id,
               'term', ta.term_id, gt.name, ta.reason, CAST(ta.tenant_id AS TEXT)
        FROM tag_assignments ta
        JOIN glossary_term_refs r ON r.term_id = ta.term_id
        JOIN glossary_terms gt ON gt.id = ta.term_id
        WHERE ta.object_type = 'term'
          -- only a defined, in-service term governs its columns (provisa.core.term_tags.governs)
          AND COALESCE(TRIM(gt.definition), '') <> '' AND NOT gt.retired
    """,
    # REQ-1375: the management report — every assignment with a planned end date, with its
    # state derived at query time. expires_on is an ISO date string, so lexicographic
    # comparison with CURRENT_DATE (cast to text) is correct on every dialect.
    "tag_expiry": """
        CREATE OR REPLACE VIEW tag_expiry AS
        SELECT id, tag_id, object_type, source_id, table_id, column_name,
               relationship_id, command_name, object_key, reason, expires_on,
               CASE WHEN expires_on < CAST(CURRENT_DATE AS TEXT)
                    THEN 'expired' ELSE 'expiring' END AS status,
               CAST(table_id AS TEXT) || ':' || column_name AS column_key,
               tenant_id
        FROM tag_assignments
        WHERE expires_on IS NOT NULL
    """,
    # REQ-1591: each term's domains by the one rule (provisa.core.repositories.glossary.
    # term_domains): the domains of the tables its refs point at while it holds refs, its declared
    # rows otherwise. "*" is the enterprise scope (REQ-1592). term_domains is the term's whole set
    # as |a|b|, the column every glossary view's readable-term row scope tests (REQ-1593); it is
    # first because the other glossary views read it.
    "glossary_term_domains": """
        CREATE OR REPLACE VIEW glossary_term_domains_meta AS
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
    # meta domain is served from (PostgreSQL, SQLite, MySQL) supports WITH RECURSIVE in a view.
    # `rooted` here counts EVERY ref, which is the repository's reading of it; the exporter's
    # narrower count (refs whose column actually publishes) is a property of the export, not of
    # the glossary, and belongs in the export.
    # retired and export_excluded are COLUMNS, never predicates — meta is the internal surface and
    # withholding is the export's job; a view that filtered them would disagree with the admin
    # surface it mirrors.
    "glossary_terms": """
        CREATE OR REPLACE VIEW glossary_terms_meta AS
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
               (SELECT MAX(x.term_domains) FROM glossary_term_domains_meta x WHERE x.term_id = t.id) AS term_domains
        FROM glossary_terms t
    """,
    "glossary_term_refs": """
        CREATE OR REPLACE VIEW glossary_term_refs_meta AS
        SELECT r.id, r.term_id, r.table_id, r.column_name,
               CAST(r.table_id AS TEXT) || ':' || r.column_name AS column_key,
               rt.source_id, rt.schema_name, rt.table_name, rt.domain_id,
               r.tenant_id,
               (SELECT MAX(x.term_domains) FROM glossary_term_domains_meta x WHERE x.term_id = r.term_id) AS term_domains
        FROM glossary_term_refs r
        JOIN registered_tables rt ON rt.id = r.table_id
    """,
    "glossary_term_edges": """
        CREATE OR REPLACE VIEW glossary_term_edges_meta AS
        SELECT e.id, e.from_term_id, e.to_term_id, e.rel_type,
               f.name AS from_term, t.name AS to_term,
               e.tenant_id,
               (SELECT MAX(x.term_domains) FROM glossary_term_domains_meta x WHERE x.term_id = e.from_term_id) AS from_term_domains,
               (SELECT MAX(x.term_domains) FROM glossary_term_domains_meta x WHERE x.term_id = e.to_term_id) AS to_term_domains
        FROM glossary_term_edges e
        JOIN glossary_terms f ON f.id = e.from_term_id
        JOIN glossary_terms t ON t.id = e.to_term_id
    """,
    # user_id is the only identity data in the glossary set. It is exposed and governed by the
    # ordinary column visibility rule rather than omitted — a governance query that cannot name
    # the expert cannot route a question to them.
    "glossary_term_experts": """
        CREATE OR REPLACE VIEW glossary_term_experts_meta AS
        SELECT x.id, x.term_id, x.user_id, x.kind, t.name AS term_name, x.tenant_id
        FROM glossary_term_experts x
        JOIN glossary_terms t ON t.id = x.term_id
    """,
    # REQ-2125: dq_contract tells a product's checker members from its output tables, and
    # checked_table_id names the table a checker's contract scans, as the model build resolved
    # it (checker_targets). profiled_table_id names, for a profile result table, the member table
    # it profiles: the
    # profiler names each result <member>_<member id>_profile_<kind> on the member's profiler
    # source (provisa.profiler.schema.result_table_name). Both are last, so an existing view is
    # replaced in place. REQ-2132: latest_outcome and latest_run_at are a checker's latest scan.
    # REQ-2158: contract, a product member's declared contract, is last too.
    "registered_tables": """
        CREATE OR REPLACE VIEW registered_tables_meta AS
        SELECT rt.id, rt.source_id, rt.domain_id, rt.schema_name, rt.table_name,
               rt.alias, rt.description, rt.cache_ttl, rt.gql_naming_convention,
               rt.watermark_column,
               rt.view_sql, rt.product_id, rt.materialize, rt.mv_refresh_interval,
               rt.l1_cluster, rt.l2_cluster, rt.l3_cluster, rt.clusters_computed_at,
               rt.tenant_id,
               rt.dq_contract,
               (SELECT m.id FROM registered_tables m
                WHERE m.profiler_source_id = rt.source_id
                  AND SUBSTR(rt.table_name, 1,
                             LENGTH(m.table_name || '_' || CAST(m.id AS TEXT) || '_profile_'))
                      = m.table_name || '_' || CAST(m.id AS TEXT) || '_profile_')
                   AS profiled_table_id,
               (SELECT c.checked_table_id FROM checker_targets c WHERE c.table_id = rt.id)
                   AS checked_table_id,
               -- The table's class term, NULL when none grounds it.
               rt.class_term_id,
               -- A checker table's latest scan. Last, like the columns above, so an
               -- existing view is replaced in place.
               (SELECT r.outcome FROM checker_latest_runs r WHERE r.table_id = rt.id)
                   AS latest_outcome,
               (SELECT r.scan_time FROM checker_latest_runs r WHERE r.table_id = rt.id)
                   AS latest_run_at,
               -- A product member's contract, its output port; last, as above.
               rt.contract
        FROM registered_tables rt
        WHERE NOT rt.draft  -- a draft table is in no catalog
    """,
    # REQ-1443 clause 10: a source's identity and type only -- the checker dialect a data-quality
    # table's rules are parsed with. Its connection settings stay out of the catalog.
    "sources": """
        CREATE OR REPLACE VIEW sources_meta AS
        SELECT id, type FROM sources
    """,
    # REQ-2125: one row per data product, its own fields only. Everything one-to-many about a
    # product -- members, commands, tags, terms, profiles -- is reached through meta-domain
    # relationships (provisa.api._meta_seed), never flattened or counted here.
    "data_products": """
        CREATE OR REPLACE VIEW data_products_meta AS
        SELECT id, domain_id, name, owner_role, team_role, purpose, limitations, usage,
               version, status, status_reason, retire_on, successor_id, support,
               support_contact, custom_properties
        FROM data_products
    """,
    "table_columns": """
        CREATE OR REPLACE VIEW table_columns_meta AS
        SELECT id, table_id, domain_id, column_name,
               -- the stable column identity the glossary binds to, and the join key of
               -- the registered glossary_term_refs -> table_columns edge (relationships is
               -- single-column, and table_columns.id is replaced wholesale by the table upsert).
               CAST(table_id AS TEXT) || ':' || column_name AS column_key,
               data_type, is_primary_key,
               alias, description, path, scope,
               masks,
               native_filter_type, is_foreign_key, is_alternate_key,
               object_fields,
               visible_to,
               tenant_id
        FROM table_columns
        -- nor are a draft table's columns.
        WHERE table_id IN (SELECT id FROM registered_tables WHERE NOT draft)
    """,
    # REQ-2080: description is the last column, so an existing view is replaced in place.
    "roles": """
        CREATE OR REPLACE VIEW roles_meta AS
        SELECT id, org_id,
               capabilities,
               tenant_id,
               'meta' AS domain_id,
               description
        FROM roles
    """,
    # REQ-2153: the names of every listed column, for the catalog of a reader outside its domain --
    # names only, by construction; what a reader reaches is in registered_tables/table_columns.
    "listed_names": """
        CREATE OR REPLACE VIEW listed_names AS
        SELECT CAST(tc.table_id AS TEXT) || ':' || tc.column_name AS id,
               rt.domain_id AS listed_domain, rt.table_name, tc.column_name,
               'meta' AS domain_id
        FROM table_columns tc
        JOIN registered_tables rt ON rt.id = tc.table_id
        WHERE tc.scope = 'listed' AND NOT rt.draft
    """,
    # REQ-2155: a role's parents, one row per (role, parent), in declared order.
    "roles_parents": """
        CREATE OR REPLACE VIEW roles_parents AS
        SELECT r.id || ':' || p.parent_role_id AS id,
               r.id AS role_id, p.parent_role_id, p.position::int AS position,
               'meta' AS domain_id
        FROM roles r
        CROSS JOIN LATERAL jsonb_array_elements_text(r.parents)
            WITH ORDINALITY AS p(parent_role_id, position)
    """,
    "roles_domain_access": """
        CREATE OR REPLACE VIEW roles_domain_access AS
        SELECT r.id || ':' || d.id AS id,
               r.id AS role_id, 'meta' AS domain_id, d.id AS accessed_domain_id
        FROM roles r
        CROSS JOIN domains d
        WHERE d.id <> ''
    """,
    "tracked_webhooks": """
        CREATE OR REPLACE VIEW tracked_webhooks_meta AS
        SELECT id, name, url, method, timeout_ms, returns, kind,
               inline_return_type,
               arguments,
               visible_to,
               domain_id, description, created_at, updated_at
        FROM tracked_webhooks
    """,
    # REQ-2125: product_id is the data product the command is a member of.
    "tracked_functions": """
        CREATE OR REPLACE VIEW tracked_functions_meta AS
        SELECT id, name, source_id, schema_name, function_name, returns, kind,
               arguments,
               return_schema,
               visible_to,
               domain_id, description, created_at, updated_at,
               product_id
        FROM tracked_functions
    """,
}

# REQ-884: Internal operational/observability logs exposed as first-class tables in
# the built-in ``ops`` domain, so telemetry is queryable through the governed pipeline
# (pgwire/SQL/GraphQL/Cypher) under role + domain access control — not only via the
# Python export path or raw control-plane JDBC, which bypass governance.
#
# Registry: to expose another internal log, add one ``source_table -> exposed_view``
# entry here plus its view DDL in ``_OPS_LOG_TABLE_VIEWS``. The seed
# (``startup_seed._seed_ops_domain``) and catalog population handle the rest — no new
# subsystem. The encrypted ``query_text_enc`` column is deliberately NOT exposed; its
# plaintext is only reachable via the authorised admin decrypt path (REQ-689).
_OPS_LOG_TABLE_ALIAS: dict[str, str] = {
    "query_audit_log": "query_audit_log_ops",
    "replica_build_log": "replica_build_log_ops",
    "sla_breach_log": "sla_breach_log_ops",
}

_OPS_LOG_TABLE_VIEWS: dict[str, str] = {
    "query_audit_log": """
        CREATE OR REPLACE VIEW query_audit_log_ops AS
        SELECT q.id, q.user_id, ud.display_name AS user_name, q.role_id, q.query_hash,
               q.table_ids, q.table_names, q.source, q.status_code, q.duration_ms, q.logged_at,
               q.route, q.row_count, q.route_reason, q.sources, q.data_age, q.region,
               q.model_stamp, q.model_commit, q.enforced,
               -- The statement's time travel: last, so an existing view is replaced in place.
               q.as_of, q.as_recorded
        FROM query_audit_log q
        LEFT JOIN user_directory ud ON ud.user_id = q.user_id
    """,
    # Every replica build that ended, completed or failed: its last builds a replica
    # (federation/replica_state.REPLICA_BUILD_LOG_KEEP).
    "replica_build_log": """
        CREATE OR REPLACE VIEW replica_build_log_ops AS
        SELECT l.id, l.source_id, l.schema_name, l.table_name, l.started_at, l.finished_at,
               l.outcome, l.method, l.rows_copied, l.error_code, l.error_message
        FROM replica_build_log l
    """,
    # REQ-506: every service-level breach, open (resolved_at empty) or closed, opened by the
    # scheduled evaluation (audit/sla_monitor.py).
    "sla_breach_log": """
        CREATE OR REPLACE VIEW sla_breach_log_ops AS
        SELECT b.id, b.kind, b.observed, b.last_observed, b.limit_value, b.window_start,
               b.window_end, b.started_at, b.last_seen_at, b.resolved_at
        FROM sla_breach_log b
    """,
}


# REQ-1386: helper view unnesting query_audit_log.table_ids (JSONB, AUDIT_SCHEMA_SQL) to one row
# per (audit row, table id) -- the join spine for the report views below. Not registered in the
# catalog -- internal only.
def _ops_table_usage_ddl() -> str:
    return """CREATE OR REPLACE VIEW ops_table_usage AS
        SELECT q.id, q.user_id, q.role_id, q.source,
               q.status_code, q.duration_ms, q.logged_at,
               CAST(j.elem AS INTEGER) AS table_id
        FROM query_audit_log q,
             jsonb_array_elements_text(q.table_ids) AS j(elem)
        """


# REQ-1386: management/operations report views in the built-in ``ops`` domain.
# Each is registered as a first-class ops-domain table (source ``provisa-admin``)
# by ``startup_seed._seed_ops_domain`` — always seeded on install, not demo data.
# Every view exposes an ``id`` column (its primary key at registration time).
# Table-level granularity throughout: the audit log records table_ids and the
# query text is encrypted (REQ-689), so element-level usage, masking outcome,
# and denying-rule id await the REQ-1386 log-write prerequisites.
def _judgment_review_sql() -> str:
    """REQ-2094: every finding of the review queue, joined to its judgment's why -- what was
    relied on, what for, the bases cited, the review-by date, who decided it and when -- and to
    the domain of the table it is on, so audit evidence is queried under role and domain access."""

    def why(field: str) -> str:
        return (
            "CASE f.object_type"
            f" WHEN 'column' THEN tc.fake_why ->> '{field}'"
            f" WHEN 'mask_entry' THEN tc.masks -> CAST(SPLIT_PART(f.object_key, '#', 2) AS INTEGER)"
            f" -> 'why' ->> '{field}'"
            f" WHEN 'row_rule' THEN rr.why ->> '{field}'"
            f" WHEN 'write_grant' THEN wg.why ->> '{field}' END"
        )

    return f"""
        CREATE OR REPLACE VIEW judgment_review AS
        SELECT f.id, f.object_type, f.object_key, f.judgment, f.category, f.finding,
               CAST(f.observed AS TEXT) AS observed, f.first_seen, f.last_seen,
               f.resolved_at, f.resolved_by, f.resolution,
               CASE WHEN f.object_type = 'tag_assignment' THEN ta.reason
                    ELSE {why("reason")} END AS reason,
               CASE WHEN f.object_type = 'tag_assignment' THEN ta.purpose
                    WHEN f.object_type = 'row_rule' THEN rr.description
                    ELSE {why("purpose")} END AS purpose,
               CASE WHEN f.object_type = 'tag_assignment' THEN CAST(ta.bases AS TEXT)
                    ELSE {why("bases")} END AS bases,
               CASE WHEN f.object_type = 'tag_assignment' THEN ta.review_on
                    ELSE {why("review_on")} END AS review_on,
               CASE WHEN f.object_type = 'tag_assignment' THEN ta.decided_by
                    ELSE {why("decided_by")} END AS decided_by,
               CASE WHEN f.object_type = 'tag_assignment' THEN CAST(ta.decided_at AS TEXT)
                    ELSE {why("decided_at")} END AS decided_at,
               rt.domain_id
        FROM judgment_findings f
        LEFT JOIN tag_assignments ta
          ON f.object_type = 'tag_assignment' AND ta.base_tag_id || '@' || ta.object_key = f.object_key
        LEFT JOIN table_columns tc
          ON f.object_type IN ('column', 'mask_entry')
         AND CAST(tc.table_id AS TEXT) || ':' || tc.column_name = SPLIT_PART(f.object_key, '#', 1)
        LEFT JOIN rls_rules rr
          ON f.object_type = 'row_rule' AND CAST(rr.id AS TEXT) = f.object_key
        LEFT JOIN table_write_grants wg
          ON f.object_type = 'write_grant' AND CAST(wg.id AS TEXT) = f.object_key
        LEFT JOIN registered_tables rt
          ON rt.id = COALESCE(ta.table_id, tc.table_id, rr.table_id, wg.table_id)
    """


_OPS_REPORT_VIEWS: dict[str, str] = {
    # Query count and distinct consumers per registered table; never-queried
    # tables surface with query_count = 0 (deprecation candidates).
    "usage_ranking": """
        CREATE OR REPLACE VIEW usage_ranking AS
        SELECT rt.id AS id, rt.source_id, rt.domain_id,
               rt.schema_name, rt.table_name,
               COUNT(u.id) AS query_count,
               COUNT(DISTINCT u.user_id) AS distinct_users,
               MAX(u.logged_at) AS last_queried_at
        FROM registered_tables rt
        LEFT JOIN ops_table_usage u ON u.table_id = rt.id
        GROUP BY rt.id, rt.source_id, rt.domain_id,
                 rt.schema_name, rt.table_name
    """,
    # One row per access to a table carrying (or containing a column carrying)
    # the 'deprecated' system tag — the consumers blocking safe removal.
    "deprecated_usage": """
        CREATE OR REPLACE VIEW deprecated_usage AS
        SELECT CAST(u.id AS TEXT) || ':' || CAST(ta.id AS TEXT) AS id,
               u.table_id, rt.table_name, rt.domain_id,
               ta.object_type, ta.column_name, ta.reason, ta.expires_on,
               u.user_id, ud.display_name AS user_name, u.role_id, u.source, u.logged_at
        FROM ops_table_usage u
        JOIN tag_assignments ta
          ON ta.table_id = u.table_id AND ta.tag_id = 'deprecated'
        JOIN registered_tables rt ON rt.id = u.table_id
        LEFT JOIN user_directory ud ON ud.user_id = u.user_id
    """,
    # One row per access to a table carrying (or containing a column carrying)
    # the 'pii' system tag: who, under which role, over which surface.
    # REQ-2094: the review queue joined to each judgment's why, as audit evidence.
    "judgment_review": _judgment_review_sql(),
    "pii_access": """
        CREATE OR REPLACE VIEW pii_access AS
        SELECT CAST(u.id AS TEXT) || ':' || ta.id AS id,
               u.table_id, rt.table_name, rt.domain_id,
               ta.object_type, ta.column_name AS pii_column,
               u.user_id, ud.display_name AS user_name, u.role_id, u.source, u.status_code,
               u.logged_at
        FROM ops_table_usage u
        JOIN (
            SELECT CAST(id AS TEXT) AS id, table_id, object_type, column_name
            FROM tag_assignments
            WHERE tag_id = 'pii' AND table_id IS NOT NULL
            -- a column carries pii through its glossary term too
            UNION ALL
            SELECT CAST(t.id AS TEXT) || ':' || r.column_name, r.table_id, t.object_type,
                   r.column_name
            FROM tag_assignments t
            JOIN glossary_term_refs r ON r.term_id = t.term_id
            JOIN glossary_terms gt ON gt.id = t.term_id
            WHERE t.object_type = 'term' AND t.base_tag_id = 'pii'
              AND COALESCE(TRIM(gt.definition), '') <> '' AND NOT gt.retired
        ) ta ON ta.table_id = u.table_id
        JOIN registered_tables rt ON rt.id = u.table_id
        LEFT JOIN user_directory ud ON ud.user_id = u.user_id
    """,
    # Access attempts rejected by governance (401 unauthenticated / 403 denied).
    "policy_denials": """
        CREATE OR REPLACE VIEW policy_denials AS
        SELECT q.id, q.user_id, ud.display_name AS user_name, q.role_id, q.query_hash, q.table_ids,
               q.source, q.status_code, q.duration_ms, q.logged_at
        FROM query_audit_log q
        LEFT JOIN user_directory ud ON ud.user_id = q.user_id
        WHERE q.status_code IN (401, 403)
    """,
    # Daily traffic per protocol surface (the audit log's ``source`` column).
    # REQ-1910: one row per statement Provisa executed — the audit row, which is the one home of
    # a statement's user, role, transport (``source``), route, rows, outcome and duration. A statement over
    # several tables names its first registered table (lowest id) and counts them; the full set is
    # ops_table_usage. ``trace_id`` joins to ops ``traces`` (the request's record: stage timings,
    # route, engine, cache result, rows). The statement text is encrypted (REQ-689) and is not a
    # column: it is read per statement through GET /admin/audit/queries/{id}/text.
    "queries": """
        CREATE OR REPLACE VIEW queries AS
        SELECT q.id AS id, q.trace_id, q.logged_at, q.duration_ms, q.status_code,
               q.user_id, q.role_id, q.source, q.route, q.row_count, q.query_hash,
               q.region,
               t.table_id, rt.table_name, rt.domain_id, t.table_count
        FROM query_audit_log q
        LEFT JOIN (SELECT u.id AS audit_id, MIN(u.table_id) AS table_id,
                          COUNT(*) AS table_count
                   FROM ops_table_usage u
                   GROUP BY u.id) t ON t.audit_id = q.id
        LEFT JOIN registered_tables rt ON rt.id = t.table_id
    """,
    "surface_mix": """
        CREATE OR REPLACE VIEW surface_mix AS
        SELECT source || ':' || SUBSTR(CAST(logged_at AS TEXT), 1, 10) AS id,
               source, SUBSTR(CAST(logged_at AS TEXT), 1, 10) AS day,
               COUNT(*) AS query_count,
               COUNT(DISTINCT user_id) AS distinct_users
        FROM query_audit_log
        GROUP BY source, SUBSTR(CAST(logged_at AS TEXT), 1, 10)
    """,
    # Daily latency/error profile per protocol surface. Portable aggregates only
    # (percentile_cont is PG-only); p95 arrives with the REQ-1386 rollup tables.
    "query_health": """
        CREATE OR REPLACE VIEW query_health AS
        SELECT source || ':' || SUBSTR(CAST(logged_at AS TEXT), 1, 10) AS id,
               source, SUBSTR(CAST(logged_at AS TEXT), 1, 10) AS day,
               COUNT(*) AS query_count,
               SUM(CASE WHEN status_code >= 400 THEN 1 ELSE 0 END) AS error_count,
               AVG(duration_ms) AS avg_duration_ms,
               MAX(duration_ms) AS max_duration_ms
        FROM query_audit_log
        GROUP BY source, SUBSTR(CAST(logged_at AS TEXT), 1, 10)
    """,
    # Governance-completeness gaps: tables/columns missing a description,
    # domains missing a steward (REQ-609 PENDING state).
    "stale_metadata": """
        CREATE OR REPLACE VIEW stale_metadata AS
        SELECT 'table:' || CAST(rt.id AS TEXT) AS id, 'table' AS object_type,
               rt.table_name AS object_name, rt.domain_id,
               'missing_description' AS issue
        FROM registered_tables rt
        WHERE rt.description IS NULL OR rt.description = ''
        UNION ALL
        SELECT 'column:' || CAST(tc.id AS TEXT), 'column',
               rt.table_name || '.' || tc.column_name, rt.domain_id,
               'missing_description'
        FROM table_columns tc
        JOIN registered_tables rt ON rt.id = tc.table_id
        WHERE tc.description IS NULL OR tc.description = ''
        UNION ALL
        SELECT 'domain:' || d.id, 'domain', d.id, d.id,
               'missing_steward'
        FROM domains d
        WHERE d.id <> '' AND d.id NOT IN (
            -- a domain under a stewarded one answers to that steward
            WITH RECURSIVE stewarded(id) AS (
                SELECT id FROM domains WHERE steward IS NOT NULL
                UNION
                SELECT c.id FROM domains c JOIN stewarded s ON c.parent_domain_id = s.id
            )
            SELECT id FROM stewarded
        )
    """,
    # Table pairs most often read in the same query — materialization/caching
    # candidates. Pairs ordered table_id_a < table_id_b to count each pair once.
    "join_hotspots": """
        CREATE OR REPLACE VIEW join_hotspots AS
        SELECT CAST(a.table_id AS TEXT) || ':' || CAST(b.table_id AS TEXT) AS id,
               a.table_id AS table_id_a, ra.table_name AS table_name_a,
               b.table_id AS table_id_b, rb.table_name AS table_name_b,
               COUNT(*) AS co_occurrence_count,
               MAX(a.logged_at) AS last_seen_at
        FROM ops_table_usage a
        JOIN ops_table_usage b
          ON b.id = a.id AND b.table_id > a.table_id
        JOIN registered_tables ra ON ra.id = a.table_id
        JOIN registered_tables rb ON rb.id = b.table_id
        GROUP BY a.table_id, ra.table_name, b.table_id, rb.table_name
    """,
    # One row per tag in the registry — how widely it is applied and how much traffic
    # reaches what it marks. Driven from tags_meta, not the tags table, so the code-defined
    # system tags (models.SYSTEM_TAGS, which have no rows) are present; a tag nobody has
    # applied still surfaces, with zeroes. The two aggregates are separate derived tables on
    # purpose: joining assignments to usage in one pass multiplies each assignment by the
    # statements that read it, and the counts stop meaning what their names say. Usage counts
    # DISTINCT statements over DISTINCT tagged tables, so a table carrying the same tag at
    # table level and on three columns still counts one statement once.
    "tag_usage": """
        CREATE OR REPLACE VIEW tag_usage AS
        SELECT t.id AS id, t.id AS tag_id, t.is_system,
               COALESCE(a.assignment_count, 0) AS assignment_count,
               COALESCE(a.sources_tagged, 0) AS sources_tagged,
               COALESCE(a.tables_tagged, 0) AS tables_tagged,
               COALESCE(a.columns_tagged, 0) AS columns_tagged,
               COALESCE(a.relationships_tagged, 0) AS relationships_tagged,
               COALESCE(a.commands_tagged, 0) AS commands_tagged,
               COALESCE(a.expiring_count, 0) AS expiring_count,
               COALESCE(a.expired_count, 0) AS expired_count,
               COALESCE(q.query_count, 0) AS query_count,
               COALESCE(q.distinct_users, 0) AS distinct_users,
               q.last_queried_at AS last_queried_at
        FROM tags_meta t
        LEFT JOIN (
            SELECT tag_id,
                   COUNT(*) AS assignment_count,
                   COUNT(DISTINCT source_id) AS sources_tagged,
                   COUNT(DISTINCT table_id) AS tables_tagged,
                   COUNT(DISTINCT CASE WHEN object_type = 'column'
                                       THEN object_key END) AS columns_tagged,
                   COUNT(DISTINCT relationship_id) AS relationships_tagged,
                   COUNT(DISTINCT command_name) AS commands_tagged,
                   SUM(CASE WHEN expires_on IS NOT NULL THEN 1 ELSE 0 END) AS expiring_count,
                   SUM(CASE WHEN expires_on < CAST(CURRENT_DATE AS TEXT)
                            THEN 1 ELSE 0 END) AS expired_count
            FROM tag_assignments
            GROUP BY tag_id
        ) a ON a.tag_id = t.id
        LEFT JOIN (
            SELECT tt.tag_id,
                   COUNT(DISTINCT u.id) AS query_count,
                   COUNT(DISTINCT u.user_id) AS distinct_users,
                   MAX(u.logged_at) AS last_queried_at
            FROM (SELECT DISTINCT tag_id, table_id FROM tag_assignments
                  WHERE table_id IS NOT NULL) tt
            JOIN ops_table_usage u ON u.table_id = tt.table_id
            GROUP BY tt.tag_id
        ) q ON q.tag_id = t.id
    """,
    # REQ-1915: every replica and its build as it stands -- the record the build runner keeps,
    # one row a replica: requested, building, idle (built), failed, or retired. ``failed_attempts``
    # counts the failures in a row since the last build that completed. The indexes built after
    # it landed (REQ-2015) are counted from its index state.
    "replica_builds": """
        CREATE OR REPLACE VIEW replica_builds AS
        SELECT r.source_id || '.' || r.schema_name || '.' || r.table_name AS id,
               r.source_id, r.schema_name, r.table_name,
               CASE WHEN r.retired_at IS NOT NULL THEN 'retired' ELSE r.build_state END
                   AS status,
               r.requested_reason, r.build_method AS method, r.load_kind,
               r.build_started_at AS started_at, r.completed_at AS finished_at,
               r.rows_copied, r.completed_rows, r.next_refresh_at,
               r.failed_at, r.failed_attempts,
               r.last_error_code AS error_code, r.last_error AS error_message,
               jsonb_array_length(CAST(r.index_state AS JSONB) -> 'wanted') AS indexes_wanted,
               jsonb_array_length(CAST(r.index_state AS JSONB) -> 'built') AS indexes_built,
               CASE WHEN r.index_state IS NULL THEN NULL
                    ELSE (SELECT COUNT(*)
                          FROM jsonb_object_keys(CAST(r.index_state AS JSONB) -> 'failed'))
               END AS indexes_failed
        FROM replica_state r
    """,
    # Replica builds and their failures per source and day, from the build log.
    "replica_build_health": """
        CREATE OR REPLACE VIEW replica_build_health AS
        SELECT l.source_id || ':' || SUBSTR(CAST(l.finished_at AS TEXT), 1, 10) AS id,
               l.source_id, SUBSTR(CAST(l.finished_at AS TEXT), 1, 10) AS day,
               COUNT(*) AS build_count,
               SUM(CASE WHEN l.outcome = 'failed' THEN 1 ELSE 0 END) AS failure_count,
               COUNT(DISTINCT CASE WHEN l.outcome = 'failed'
                                   THEN l.schema_name || '.' || l.table_name END)
                   AS failed_replicas,
               MAX(CASE WHEN l.outcome = 'failed' THEN l.finished_at END) AS last_failed_at
        FROM replica_build_log l
        GROUP BY l.source_id, SUBSTR(CAST(l.finished_at AS TEXT), 1, 10)
    """,
    # REQ-506: the service level of the last hour, from the audit log -- what the scheduled
    # evaluation compares with the limits (audit/sla_monitor.get_sla_summary). A request is
    # available when its status is below the 5xx band. No queries in the hour: no row.
    "service_level": """
        CREATE OR REPLACE VIEW service_level AS
        SELECT 'last_hour' AS id,
               now() - INTERVAL '1 hour' AS window_start, now() AS window_end,
               COUNT(*) AS query_count,
               AVG(q.duration_ms) AS avg_ms,
               percentile_cont(0.50) WITHIN GROUP (ORDER BY q.duration_ms) AS p50_ms,
               percentile_cont(0.95) WITHIN GROUP (ORDER BY q.duration_ms) AS p95_ms,
               percentile_cont(0.99) WITHIN GROUP (ORDER BY q.duration_ms) AS p99_ms,
               100.0 * SUM(CASE WHEN q.status_code < 500 THEN 1 ELSE 0 END) / COUNT(*)
                   AS availability_pct
        FROM query_audit_log q
        WHERE q.logged_at >= now() - INTERVAL '1 hour'
        HAVING COUNT(*) > 0
    """,
}
