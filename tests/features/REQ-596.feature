# Generated from docs/arch/requirements.yaml. Do not hand-edit.
Feature: REQ-596 — Audit Logging
  # Every query is recorded in `query_audit_log` with tenant_id, user_id, role_id, SHA-256 query hash, table_ids, source, st…

  Scenario: REQ-596 default behaviour
    Given any query executed against the system
    When the query completes
    Then it is recorded in query_audit_log with required fields and only the SHA-256 hash of the query text
