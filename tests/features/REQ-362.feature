# Generated from docs/arch/requirements.yaml. Do not hand-edit.
Feature: REQ-362 — Tracked Functions & Custom Mutations
  # One-to-many relationships on action result rows must return an array field; many-to-one relationships must return an obj…

  Scenario: REQ-362 default behaviour
    Given an action result with a one-to-many relationship
    When the relationship field is resolved
    Then it returns an array; many-to-one returns an object or null per JoinMeta cardinality
