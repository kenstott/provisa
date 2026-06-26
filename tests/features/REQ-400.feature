# Generated from docs/arch/requirements.yaml. Do not hand-edit.
Feature: REQ-400 — Domain Model
  # When a Relationship is saved, the target_column on the target table is marked as `is_primary_key=true` if no other colum…

  Scenario: REQ-400 default behaviour
    Given a relationship being saved where the target table has no existing primary key
    When the relationship is persisted
    Then the target_column is marked is_primary_key=true; if a PK already exists it is marked
    is_alternate_key=true
