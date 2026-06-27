# Generated from docs/arch/requirements.yaml. Do not hand-edit.
Feature: REQ-757 — Cypher Query Frontend (Phase AU)
  # Map projections — `n { .prop1, .prop2 }`, `n { .* }`, `n { key: expr }` — translate to Trino MAP(...) function calls wit…

  Scenario: REQ-757 default behaviour
    Given a Cypher query MATCH (n:Person) RETURN n { .name, .age }
    When the translator processes it
    Then it emits MAP(ARRAY['name','age'], ARRAY[n."name",n."age"])
