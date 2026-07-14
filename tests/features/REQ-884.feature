# Generated from docs/arch/requirements.yaml. Do not hand-edit.
Feature: REQ-884 — Observability
  # All Provisa internal logs—starting with the query audit log (REQ-074) and extending to any other operational/observabili…

  Scenario: REQ-884 default behaviour
    Given a Provisa instance with query_audit_log records and a user with role r that has ops domain access
    When the user queries "SELECT query, user, status FROM ops.query_audit_log WHERE timestamp > ..." over pgwire
    Then the query returns only the audit log rows that satisfy the user's domain access policy, enforced by the governed access control layer
