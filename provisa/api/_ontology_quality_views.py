# Copyright (c) 2026 Kenneth Stott
# Canary: 4c7e2a19-8b3d-4f60-a1e5-93d0b6c2f714
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The Ontology quality standard report (REQ-1386, ONTOLOGY QUALITY IS A STANDARD REPORT).

Three ops-domain report views measure the glossary from the model, by domain and by steward:

* ``ontology_term_quality`` -- one row per in-service term and domain: its status and grounding,
  its RELATED_TO placeholders, its hops to data, the broad hub terms its paths pass through, the
  terms whose approval alone makes it grounded, and whether it is an undefined term on a path to
  data;
* ``ontology_placeholders`` -- the unresolved placeholders: each RELATED_TO edge, and each term
  whose only links are RELATED_TO;
* ``ontology_quality`` -- the shares and counts of the two above, one row per domain and one per
  steward.

Grounding is REQ-1387's, read from the ontology schema's ``_terms_ontology.grounding`` (the SQL
statement of ``provisa.core.glossary.term_grounding``); every walk here starts from the same edge
definition, ``_ontology_views.GROUNDING_EDGES_CTES``. The approvals that finalise a pending term
restate ``provisa.core.glossary.approvals_that_ground`` (the REQ-1387 path_pending finding) over
those edges. A term's domains are ``_domains_ontology``'s; a domain's steward is its own or, when
it has none, its nearest stewarded parent's (the rule ``stale_metadata`` applies).

Drift (terms that lost or gained grounding since the previous period) is not here: it is computed
from the model's history by ``provisa.core.term_drift`` and shown with the report.
"""

# Requirements: REQ-1386, REQ-1387

from __future__ import annotations

from provisa.api._ontology_views import GROUNDING_EDGES_CTES

#: A broad hub: an in-service term at least this many distinct terms reach directly from above,
#: as a kind, part or valid value of it (REQ-1386 grounding precision).
HUB_MIN_REACHERS = 5

# A domain's steward: its own, else its nearest stewarded parent's. Read after WITH RECURSIVE.
_STEWARD_OF_CTE = """steward_of (domain_id, steward) AS (
            SELECT id, steward FROM domains WHERE steward IS NOT NULL
            UNION
            SELECT d.id, s.steward
            FROM domains d
            JOIN steward_of s ON d.parent_domain_id = s.domain_id
            WHERE d.steward IS NULL
        ),
        term_domain AS (
            SELECT DISTINCT term_id, domain_id FROM _domains_ontology
        ),"""

ONTOLOGY_QUALITY_VIEWS: dict[str, str] = {
    # One row per in-service term (draft or approved) and domain it is scoped to.
    "ontology_term_quality": f"""
        CREATE OR REPLACE VIEW ontology_term_quality AS
        WITH RECURSIVE {GROUNDING_EDGES_CTES}
        {_STEWARD_OF_CTE}
        t AS (
            SELECT id, name, definition, status, grounding
            FROM _terms_ontology
            WHERE status IN ('draft', 'approved')
        ),
        -- Hops to data: the fewest kind, part or valid-value steps down from the term to a term
        -- holding a binding (a synonym is the same meaning, no step). Bounded by the number of
        -- in-service terms, which no shortest path exceeds, so a cycle ends.
        hops_down (id, hops) AS (
            SELECT id, 0 FROM rooted
            UNION
            SELECT ce.b, h.hops + ce.hop
            FROM hops_down h
            JOIN conducting_edges ce ON ce.a = h.id
            WHERE h.hops < (SELECT COUNT(*) FROM in_service)
        ),
        hub AS (
            SELECT ce.a AS id
            FROM conducting_edges ce
            WHERE ce.hop = 1
            GROUP BY ce.a
            HAVING COUNT(DISTINCT ce.b) >= {HUB_MIN_REACHERS}
        ),
        -- The terms with a path down through a hub strictly between them and data: above a hub
        -- that reaches data and holds no binding (a binding ends a path), over terms holding none.
        above_hub (hub_id, id) AS (
            SELECT h.id, ce.b
            FROM hub h
            JOIN t g ON g.id = h.id AND g.grounding <> 'not_grounded'
            JOIN conducting_edges ce ON ce.a = h.id
            WHERE h.id NOT IN (SELECT id FROM rooted)
            UNION
            SELECT v.hub_id, ce.b
            FROM above_hub v
            JOIN conducting_edges ce ON ce.a = v.id
            WHERE v.id NOT IN (SELECT id FROM rooted)
        ),
        -- The terms with a path down that passes through no hub strictly between.
        hub_free (id) AS (
            SELECT id FROM rooted
            UNION
            SELECT ce.b
            FROM hub_free f
            JOIN conducting_edges ce ON ce.a = f.id
            WHERE f.id IN (SELECT id FROM rooted) OR f.id NOT IN (SELECT id FROM hub)
        ),
        -- approvals_that_ground: a draft that reaches data and holds no binding, whose approval
        -- alone gives a grounded_pending term a path with no draft strictly between.
        candidate AS (
            SELECT id, name
            FROM t
            WHERE status = 'draft' AND grounding <> 'not_grounded'
              AND id NOT IN (SELECT id FROM rooted)
        ),
        unlocked (candidate_id, id) AS (
            SELECT c.id, r.id FROM candidate c CROSS JOIN rooted r
            UNION
            SELECT u.candidate_id, ce.b
            FROM unlocked u
            JOIN conducting_edges ce ON ce.a = u.id
            JOIN glossary_terms via ON via.id = u.id
            WHERE via.approved OR u.id = u.candidate_id OR u.id IN (SELECT id FROM rooted)
        ),
        finaliser AS (
            SELECT u.id, STRING_AGG(c.name, ', ' ORDER BY c.name) AS names
            FROM unlocked u
            JOIN candidate c ON c.id = u.candidate_id
            JOIN t p ON p.id = u.id AND p.grounding = 'grounded_pending'
            GROUP BY u.id
        ),
        -- The RELATED_TO edges a term is on, either end (the ontology list's related_to count),
        -- and every edge it is on.
        related AS (
            SELECT term_id, COUNT(*) AS n
            FROM (SELECT from_term_id AS term_id FROM glossary_term_edges
                  WHERE rel_type = 'RELATED_TO'
                  UNION ALL
                  SELECT to_term_id FROM glossary_term_edges WHERE rel_type = 'RELATED_TO') x
            GROUP BY term_id
        ),
        linked AS (
            SELECT term_id, COUNT(*) AS n
            FROM (SELECT from_term_id AS term_id FROM glossary_term_edges
                  UNION ALL
                  SELECT to_term_id FROM glossary_term_edges) x
            GROUP BY term_id
        )
        SELECT CAST(t.id AS TEXT) || ':' || COALESCE(td.domain_id, '') AS id,
               t.id AS term_id, t.name AS term_name, td.domain_id, so.steward,
               t.status, t.grounding,
               CASE WHEN TRIM(COALESCE(t.definition, '')) <> '' THEN TRUE ELSE FALSE END
                   AS defined,
               COALESCE(rel.n, 0) AS related_to,
               CASE WHEN t.grounding = 'not_grounded' AND COALESCE(lk.n, 0) > 0
                         AND lk.n = COALESCE(rel.n, 0)
                    THEN TRUE ELSE FALSE END AS only_related_to,
               hd.hops AS hops_to_data,
               CASE WHEN t.id IN (SELECT id FROM hub) THEN TRUE ELSE FALSE END AS is_hub,
               COALESCE(ah.n, 0) AS hubs_between,
               CASE WHEN t.grounding <> 'not_grounded' AND t.id NOT IN (SELECT id FROM hub_free)
                    THEN TRUE ELSE FALSE END AS only_through_hub,
               f.names AS finalised_by,
               CASE WHEN TRIM(COALESCE(t.definition, '')) = '' AND t.grounding <> 'not_grounded'
                         AND EXISTS (SELECT 1 FROM conducting_edges ce WHERE ce.a = t.id)
                    THEN TRUE ELSE FALSE END AS undefined_on_path
        FROM t
        LEFT JOIN term_domain td ON td.term_id = t.id
        LEFT JOIN steward_of so ON so.domain_id = td.domain_id
        LEFT JOIN related rel ON rel.term_id = t.id
        LEFT JOIN linked lk ON lk.term_id = t.id
        LEFT JOIN (SELECT id, MIN(hops) AS hops FROM hops_down GROUP BY id) hd ON hd.id = t.id
        LEFT JOIN (SELECT id, COUNT(DISTINCT hub_id) AS n FROM above_hub
                   WHERE hub_id <> id GROUP BY id) ah ON ah.id = t.id
        LEFT JOIN finaliser f ON f.id = t.id
    """,
    # The unresolved placeholders: each RELATED_TO edge, once per domain of either end, and each
    # in-service term whose only links are RELATED_TO.
    "ontology_placeholders": f"""
        CREATE OR REPLACE VIEW ontology_placeholders AS
        WITH RECURSIVE {_STEWARD_OF_CTE}
        edge_domain AS (
            SELECT DISTINCT e.id AS edge_id, td.domain_id
            FROM glossary_term_edges e
            LEFT JOIN term_domain td ON td.term_id IN (e.from_term_id, e.to_term_id)
            WHERE e.rel_type = 'RELATED_TO'
        )
        SELECT 'edge:' || CAST(e.id AS TEXT) || ':' || COALESCE(ed.domain_id, '') AS id,
               'related_to_edge' AS placeholder, e.id AS edge_id,
               e.from_term_id AS term_id, f.name AS term_name,
               e.to_term_id AS other_term_id, o.name AS other_term_name,
               ed.domain_id, so.steward,
               e.from_term_id AS related_to_review
        FROM glossary_term_edges e
        JOIN edge_domain ed ON ed.edge_id = e.id
        JOIN glossary_terms f ON f.id = e.from_term_id
        JOIN glossary_terms o ON o.id = e.to_term_id
        LEFT JOIN steward_of so ON so.domain_id = ed.domain_id
        UNION ALL
        SELECT 'term:' || q.id, 'only_related_to', CAST(NULL AS INTEGER),
               q.term_id, q.term_name, CAST(NULL AS INTEGER), CAST(NULL AS TEXT),
               q.domain_id, q.steward, q.term_id
        FROM ontology_term_quality q
        WHERE q.only_related_to
    """,
    # One row per domain and one per steward; a term scoped to several domains of one steward
    # counts once for the steward.
    "ontology_quality": """
        CREATE OR REPLACE VIEW ontology_quality AS
        WITH q AS (
            SELECT 'domain' AS scope, domain_id AS scope_id, term_id, status, grounding,
                   only_related_to, hops_to_data, hubs_between, only_through_hub,
                   finalised_by, undefined_on_path
            FROM ontology_term_quality
            UNION
            SELECT 'steward', steward, term_id, status, grounding,
                   only_related_to, hops_to_data, hubs_between, only_through_hub,
                   finalised_by, undefined_on_path
            FROM ontology_term_quality
        ),
        p AS (
            SELECT 'domain' AS scope, domain_id AS scope_id, edge_id
            FROM ontology_placeholders WHERE placeholder = 'related_to_edge'
            UNION
            SELECT 'steward', steward, edge_id
            FROM ontology_placeholders WHERE placeholder = 'related_to_edge'
        ),
        pc AS (
            SELECT scope, scope_id, COUNT(*) AS n FROM p GROUP BY scope, scope_id
        ),
        g AS (
            SELECT scope, scope_id,
                   COUNT(*) AS in_service_terms,
                   SUM(CASE WHEN grounding = 'grounded' THEN 1 ELSE 0 END) AS grounded_terms,
                   SUM(CASE WHEN grounding = 'grounded_pending' THEN 1 ELSE 0 END)
                       AS grounded_pending_terms,
                   SUM(CASE WHEN status = 'approved' THEN 1 ELSE 0 END) AS approved_terms,
                   SUM(CASE WHEN status = 'approved' AND grounding <> 'not_grounded'
                            THEN 1 ELSE 0 END) AS approved_grounded_terms,
                   SUM(CASE WHEN only_related_to THEN 1 ELSE 0 END) AS only_related_to_terms,
                   AVG(hops_to_data) AS avg_hops,
                   MAX(hops_to_data) AS max_hops_to_data,
                   SUM(hubs_between) AS hub_paths,
                   SUM(CASE WHEN only_through_hub THEN 1 ELSE 0 END) AS only_through_hub_terms,
                   SUM(CASE WHEN grounding = 'grounded_pending' AND finalised_by IS NOT NULL
                            THEN 1 ELSE 0 END) AS pending_finalisable_terms,
                   SUM(CASE WHEN undefined_on_path THEN 1 ELSE 0 END) AS undefined_on_path_terms
            FROM q
            GROUP BY scope, scope_id
        )
        SELECT g.scope || ':' || COALESCE(g.scope_id, '') AS id,
               g.scope, g.scope_id,
               g.in_service_terms,
               g.grounded_terms,
               ROUND(100.0 * g.grounded_terms / g.in_service_terms, 1) AS grounded_pct,
               g.grounded_pending_terms,
               ROUND(100.0 * g.grounded_pending_terms / g.in_service_terms, 1)
                   AS grounded_pending_pct,
               g.approved_terms,
               g.approved_grounded_terms,
               ROUND(100.0 * g.approved_grounded_terms / g.in_service_terms, 1)
                   AS approved_grounded_pct,
               COALESCE(pc.n, 0) AS related_to_edges,
               g.only_related_to_terms,
               ROUND(g.avg_hops, 2) AS avg_hops_to_data,
               g.max_hops_to_data,
               g.hub_paths,
               g.only_through_hub_terms,
               g.pending_finalisable_terms,
               g.undefined_on_path_terms
        FROM g
        LEFT JOIN pc ON pc.scope = g.scope AND pc.scope_id IS NOT DISTINCT FROM g.scope_id
    """,
}
