# Generated from docs/arch/requirements.yaml. Do not hand-edit.
Feature: REQ-798 — Cypher Mutations
  # Cypher mutations (CREATE/DELETE/UPDATE) must be transpiled through the full semantic SQL write pipeline, applying RLS in…

  Scenario: REQ-798 default behaviour
    Given a Cypher CREATE/DELETE/UPDATE mutation
    When the mutation is transpiled through WriteTranslator and wrapped in MutationResult
    Then RLS is injected via inject_rls_into_mutation
    And the mutation is transpiled to the target dialect
    And the mutation is executed via execute_direct
    And all post-mutation hooks fire (cache invalidation, MV stale marking, Kafka events, hot-table reload)
