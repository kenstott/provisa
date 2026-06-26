# Generated from docs/arch/requirements.yaml. Do not hand-edit.
Feature: REQ-044 — API & Integration
  # Presigned URL redirect for large result consumers with TTL-bounded access.

  Scenario: REQ-044 default behaviour
    Given a consumer requesting a large result set
    When the server generates a presigned URL with a TTL
    Then the consumer can access the result via the URL within the TTL without server-side buffering
