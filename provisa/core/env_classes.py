# Copyright (c) 2026 Kenneth Stott
# Canary: 9f27b4c1-3ad8-4e60-8c15-7b0e2d94af63
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""What an environment copy carries, stated as an ALLOW-LIST (REQ-1489).

Every table in the org schema falls in exactly one class, and the classification is exhaustive by
construction: :data:`CLASSIFIED` is checked against ``schema_org``'s own metadata by a test that
fails on any table carrying no classification. The failure mode for a table somebody added and
forgot is therefore a red test, not a copied secret.

It is an allow-list and not an exclusion list for the same reason. An exclusion list is safe only
while it is complete, and it stops being complete the moment a table is added — which is exactly
when nobody is looking at it. Under an allow-list a new table travels only when someone writes its
name down here, and the test makes writing it down unavoidable.
"""

# Requirements: REQ-1489, REQ-1491, REQ-1492, REQ-1539

from __future__ import annotations

#: The governed model. This is what an environment IS, and all of it travels.
CARRIED: frozenset[str] = frozenset(
    {
        "domains",
        "naming_rules",
        "registered_tables",
        "table_columns",
        "relationships",
        "metrics",
        # ``roles`` and ``user_role_assignments`` are additionally SEEDED_AT_CREATION: CARRIED,
        # but only by a creation. See that class for why no later copy touches them.
        "roles",
        # REQ-1539: assignments travel with the roles they name. WHAT a role may do is the
        # environment's own answer -- dev's ``developer`` can be unrestricted while prod's holds
        # nothing -- and that answer is worthless if nobody is assigned the role here. Copying the
        # assignment carries no new right: the person arrives holding exactly what they held in the
        # environment this one came from, and every capability behind the name is re-read from THIS
        # environment's ``roles`` row.
        "user_role_assignments",
        "rls_rules",
        "tags",
        "tag_param_values",
        "tag_assignments",
        "glossary_terms",
        "glossary_term_refs",
        "glossary_term_edges",
        "glossary_term_experts",
        "glossary_term_domains",
        "materialized_views",
        "calendars",
        "kafka_topics",
        "api_endpoints",
        "tracked_functions",
        "tracked_webhooks",
        "table_meta_links",
    }
)

#: CARRIED, but only when the environment is being CREATED (REQ-1539). These two say WHO MAY DO
#: WHAT, and a new environment needs them or it opens with nobody able to act -- so they are seeded
#: from the environment it came from. Afterwards they are that environment's own answer and no
#: later copy touches them. A merge is a statement about the MODEL, and prod's ``developer`` row
#: holding nothing is precisely the guarantee a merge from an unrestricted dev must not overwrite;
#: were these to travel on merge, the review path itself would become the escalation route.
SEEDED_AT_CREATION: frozenset[str] = frozenset({"roles", "user_role_assignments"})

#: The source rows carry their id, type and governance fields; WHERE THEY POINT stays behind
#: (REQ-1491). They cannot simply be excluded — registered_tables references a source, and dropping
#: the row would cascade the model away — so they travel stripped, and the environment marks them
#: unbound rather than leaving a blank host the connection builder would read as localhost:5432.
IDENTITY_ONLY: frozenset[str] = frozenset(
    {"sources", "api_sources", "kafka_sources", "kafka_sinks"}
)

#: Never copied: a credential or an identity. ``user_role_assignments`` used to be here, on
#: REQ-1492's reasoning that a copied environment inheriting where it points TOGETHER WITH
#: permission to write there was a compound failure. REQ-1539 answers that where it actually arose:
#: the escalation was REQ-1528 conferring ``developer`` -- ``write`` included -- on whoever created
#: an environment, not the assignment a member already held. Withholding the assignments instead
#: left an environment in which nobody could do anything and made rights look like a property of
#: the environment rather than of the person.
NEVER_SENSITIVE: frozenset[str] = frozenset({"org_secrets", "user_directory"})

#: Never copied: runtime state and evidence, which belong to the environment that PRODUCED them.
#: ``catalog_bindings`` is here rather than with the model because its vendor_ref and physical_key
#: address another environment's external catalog.
NEVER_RUNTIME: frozenset[str] = frozenset(
    {
        "mv_refresh_log",
        "mv_delta_ledger",
        "relationship_candidates",
        "creation_requests",
        "api_endpoint_candidates",
        "live_query_state",
        "file_source_mtimes",
        "node_ids",
        "rel_ids",
        "query_audit_log",
        "query_sla_log",
        "source_catalog_cache",
        "events",
        "event_status",
        "node_freshness_state",
        "preserved_snapshots",
        "admin_audit_log",
        "catalog_bindings",
        "email_send_authority_audit",
    }
)

#: Classified PER KEY, not per table: the settings keys that name an external target or a
#: per-environment runtime stay behind, and the rest of the org's settings travel.
PARTIAL: frozenset[str] = frozenset({"org_settings"})

#: The org_settings key PREFIXES that never travel — each names an external target or a runtime
#: belonging to one environment. Prefixes rather than exact keys because these are namespaces
#: (``metadata_export.endpoint``, ``cache.redis_url``), and an exact list would miss the next key
#: added inside one.
NEVER_SETTING_PREFIXES: tuple[str, ...] = ("metadata_export", "redirect", "cache")

#: Every table name this module classifies. The test asserts it equals ``schema_org``'s metadata.
CLASSIFIED: frozenset[str] = CARRIED | IDENTITY_ONLY | NEVER_SENSITIVE | NEVER_RUNTIME | PARTIAL


def carries_setting(key: str) -> bool:
    """Whether an ``org_settings`` key travels with an environment copy (REQ-1489).

    Default-CARRY within a table that is already classified PARTIAL: the excluded namespaces are
    named, and a governance setting added later travels, which is what an operator expects of a
    setting that is part of the model.
    """
    head = key.split(".", 1)[0]
    return head not in NEVER_SETTING_PREFIXES


#: The column an IDENTITY_ONLY row carries its boundness in (REQ-1491). Never copied: a copy
#: produces an unbound row in the target whatever the source row said.
BOUND_COLUMN = "bound"

#: The columns of an IDENTITY_ONLY table that say WHERE the environment points, per REQ-1491.
#: These stay behind. Everything else on those tables is identity or governance and travels.
#:
#: ``sources.mapping`` is here because REQ-1489's amendment classifies it PER KEY by the connector
#: that owns it, and the declaration is DEFAULT-DENY: until a connector names the mapping keys that
#: are governance, none of its keys are, so the bag stays behind whole. That is the requirement's
#: own stated behaviour for a connector that declares nothing — not a placeholder standing in for
#: the per-key split, which lands with the connector declarations.
#: ``cdc`` is a per-environment runtime fact for the same reason a cache setting is: it names slots
#: and publications in the database this environment happens to point at.
BINDING_COLUMNS: dict[str, frozenset[str]] = {
    "sources": frozenset(
        {
            "host",
            "port",
            "database",
            "username",
            "dialect",
            "path",
            "federation_hints",
            "mapping",
            "cdc",
        }
    ),
    "api_sources": frozenset({"base_url", "spec_url", "auth"}),
    "kafka_sources": frozenset({"bootstrap_servers", "schema_registry_url", "auth_type"}),
    "kafka_sinks": frozenset({"topic"}),
}


def binding_columns(table: str) -> frozenset[str]:
    """The columns of ``table`` that no copy ever supplies, plus its boundness marker.

    Raises for a table that is not IDENTITY_ONLY: asking which of a CARRIED table's columns are
    bindings is a question with no answer, and answering it with an empty set would silently carry
    a connection value the day somebody moved one.
    """
    if table not in IDENTITY_ONLY:
        raise KeyError(f"{table!r} is not an IDENTITY_ONLY table; it has no binding columns")
    return BINDING_COLUMNS[table] | {BOUND_COLUMN}
