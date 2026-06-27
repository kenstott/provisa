# Generated from docs/arch/requirements.yaml. Do not hand-edit.
Feature: REQ-784 — Cypher Graph Analytics
  # Auto-impute endpoint generates relationship edges for visible graph nodes based on their labels and the schema relations…

  Scenario: REQ-784 default behaviour
    Given a set of visible graph nodes with known labels
    When the auto-impute endpoint receives the visible node set with stable integer ids
    Then it queries each relationship pair (src_label)-[rel_type]->(tgt_label) where both endpoints are visible
    And returns all discovered edges merged with the input nodes in standard Cypher response format
