# Generated from docs/arch/requirements.yaml. Do not hand-edit.
Feature: REQ-009 — Compiler & Schema
  # Query compilation produces PG-style SQL from validated GraphQL AST — single SQL statement, no resolver chain, no N+1. Ex…

  Scenario: REQ-009 default behaviour
    Given a valid GraphQL query AST
    When the compiler processes it
    Then it emits a single PG-style SQL statement with no resolver chain and no N+1 pattern
