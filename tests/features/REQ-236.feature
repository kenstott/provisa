# Generated from docs/arch/requirements.yaml. Do not hand-edit.
Feature: REQ-236 — Hot Table Auto-Detection
  # A table is auto-hot if it is the target of a many-to-one relationship OR its row count is <= `auto_threshold` (SELECT CO…

  Scenario: REQ-236 default behaviour
    Given a table that is the target of a many-to-one relationship or has row count <= auto_threshold
    When schema is built
    Then the table is automatically designated as hot and cached in Redis
