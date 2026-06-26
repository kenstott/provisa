# Generated from docs/arch/requirements.yaml. Do not hand-edit.
Feature: REQ-525 — Compiler & Schema
  # Auto-generated `.proto` from the data schema is generated per role. Each role receives a proto definition reflecting onl…

  Scenario: REQ-525 default behaviour
    Given two roles with different table and column visibility
    When proto definitions are generated
    Then each role receives a proto reflecting only its visible tables and columns
