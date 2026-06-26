# Generated from docs/arch/requirements.yaml. Do not hand-edit.
Feature: REQ-613 — Query Governance
  # Every query that touches a domain asset is logged in an append-only audit log (query_audit_log). The log captures: user_…

  Scenario: REQ-613 default behaviour
    Given any query touching a domain asset
    When the query is executed
    Then it is logged in the append-only query_audit_log with all required fields
