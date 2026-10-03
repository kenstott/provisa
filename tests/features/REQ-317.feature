# Generated from docs/arch/requirements.yaml. Do not hand-edit.
Feature: REQ-317 — OpenAPI Auto-Registration Connector
  # OpenAPI write operations as commands. [SUPERSEDED by REQ-1924, 2026-10-02 -- a source's write operations are offered and…

  Scenario: REQ-317 default behaviour
    Given an OpenAPI spec with POST/PUT/PATCH/DELETE operations
    When the spec is registered
    Then those operations are auto-registered as tracked functions with request body properties as mutation input arguments
