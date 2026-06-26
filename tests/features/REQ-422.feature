# Generated from docs/arch/requirements.yaml. Do not hand-edit.
Feature: REQ-422 — Vector Search
  # Source capability auto-detection at registration time must identify native vector support: pgvector extension for Postgr…

  Scenario: REQ-422 default behaviour
    Given a PostgreSQL source being registered
    When Provisa checks for native vector support
    Then it detects the pgvector extension and marks the source as native-capable, or flags it for fallback
