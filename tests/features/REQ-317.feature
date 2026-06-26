# Generated from docs/arch/requirements.yaml. Do not hand-edit.
Feature: REQ-317 — OpenAPI Auto-Registration Connector
  # All non-GET operations (POST, PUT, PATCH, DELETE) are auto-registered as tracked functions (mutations). Request body sch…

  Scenario: REQ-317 default behaviour
    Given an OpenAPI spec with POST/PUT/PATCH/DELETE operations
    When the spec is registered
    Then those operations are auto-registered as tracked functions with request body properties as mutation input arguments
