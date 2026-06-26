# Generated from docs/arch/requirements.yaml. Do not hand-edit.
Feature: REQ-256 — API & Integration
  # Auto-generated plain REST endpoints for every registered table via `GET /data/rest/{table}` with query string mapping to…

  Scenario: REQ-256 default behaviour
    Given a REST-only client querying GET /data/rest/{table}
    When the query string maps to GraphQL args
    Then the request compiles and executes with the same RLS, masking, and routing as GraphQL
