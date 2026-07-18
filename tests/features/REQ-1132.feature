# Generated from docs/arch/requirements.yaml. Do not hand-edit.
Feature: REQ-1132 — Semantic-Layer Meta Visibility
  # Metadata visibility is tiered by role. The meta domain exposes two column classes: CORE (table_name, column_name, data_t…

  Scenario: REQ-1132 default behaviour
    Given a role that can query table t1 (connected via semantic relationship to t2 and t3)
    When querying registered_tables and table_columns
    Then sees CORE meta for t1, t2, t3; sees nothing for t4 (no relationship); sees no GOVERNANCE columns unless view_governance capability is granted
