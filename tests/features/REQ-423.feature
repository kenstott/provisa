# Generated from docs/arch/requirements.yaml. Do not hand-edit.
Feature: REQ-423 — Vector Search
  # A `cosine_similarity(column, query_vector)` UDF must be available in Provisa SQL. It translates to the native vector ope…

  Scenario: REQ-423 default behaviour
    Given a query using cosine_similarity(column, query_vector)
    When compiled for a native-capable source
    Then the UDF translates to the native vector operator; for non-native sources it routes to the pgvector fallback cache
