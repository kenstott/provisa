# Generated from docs/arch/requirements.yaml. Do not hand-edit.
Feature: REQ-542 — Naming & Schema
  # [SUPERSEDED by REQ-1933, 2026-10-04 -- a name is no longer made unique by qualifying it; a name the rules make taken is…

  Scenario: REQ-542 default behaviour
    Given a config with ordered regex naming rules
    When GraphQL field names are generated for table names
    Then each rule is applied in order, and a name the rules make taken is refused
