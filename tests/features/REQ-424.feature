# Generated from docs/arch/requirements.yaml. Do not hand-edit.
Feature: REQ-424 — Vector Search
  # For sources without native vector capability, Provisa must transparently materialize the embedding column to an internal…

  Scenario: REQ-424 default behaviour
    Given a source without native vector capability
    When a cosine_similarity query is executed
    Then the embedding column is materialized to the pgvector cache, an HNSW index is built, and results are returned transparently
