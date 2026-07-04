# Generated from docs/arch/requirements.yaml. Do not hand-edit.
Feature: REQ-881 — Materialization Store
  # Probe-based freshness gate on the MV REFRESH path, opt-in per MV — the refresh-time application of REQ-855's centralized…

  Scenario: REQ-881 default behaviour
    Given a relationship MV with freshness_mode="probe" over an Iceberg-backed source and a last_input_token equal to the source's current snapshot id
    When the refresh loop processes it
    Then the refresh computes the source token, finds it unchanged, and skips the rebuild (no DELETE/CREATE) — resetting the TTL and keeping the materialized rows FRESH
    And when the source snapshot id later differs, the same MV rebuilds and stores the new token
    And an MV where any source yields no token degrades to plain TTL (never skips on partial signal)
