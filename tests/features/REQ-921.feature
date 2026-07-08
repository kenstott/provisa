# Generated from docs/arch/requirements.yaml. Do not hand-edit.
Feature: REQ-921 — Promotion Coercion
  # An unmapped promotion target_type in API JSONB promotions (provisa/api_source/promotions.py, generate_promotion_ddl) yie…

  Scenario: REQ-921 default behaviour
    Given a promotion with a target_type that is in _PG_CAST_MAP
    When generate_promotion_ddl generates the coercion SQL
    Then the output includes an explicit CAST expression
    When a promotion with a target_type not in _PG_CAST_MAP
    Then the output omits the CAST and relies on JSONB native representation
