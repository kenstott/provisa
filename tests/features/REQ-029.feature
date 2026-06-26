# Generated from docs/arch/requirements.yaml. Do not hand-edit.
Feature: REQ-029 — Execution & Routing
  # Large results above threshold redirect to blob storage with presigned URL and TTL.

  Scenario: REQ-029 default behaviour
    Given a query whose result set exceeds the configured size threshold
    When the query completes
    Then the result is stored in blob storage and the client receives a presigned URL with TTL
