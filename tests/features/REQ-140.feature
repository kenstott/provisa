# Generated from docs/arch/requirements.yaml. Do not hand-edit.
Feature: REQ-140 — Large Result Redirect & CTAS
  # Threshold-based redirect uses LIMIT threshold+1 probe — no COUNT(*), no double execution for inline results.

  Scenario: REQ-140 default behaviour
    Given a redirect threshold is set
    When the query executes
    Then a LIMIT threshold+1 probe determines redirect without COUNT(*) or re-executing the query
