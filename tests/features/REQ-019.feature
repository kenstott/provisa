# Generated from docs/arch/requirements.yaml. Do not hand-edit.
Feature: REQ-019 — Registration & Governance
  # Cross-source relationships defined manually by steward with cardinality (many-to-one, one-to-many). (Revised 2026-06-18:…

  Scenario: REQ-019 default behaviour
    Given two tables in different registered sources
    When a steward manually defines a cross-source relationship with cardinality
    Then the relationship is persisted and available for query traversal
