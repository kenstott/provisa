# Generated from docs/arch/requirements.yaml. Do not hand-edit.
Feature: REQ-257 — API & Integration
  # Auto-generated JSON:API compliant endpoints for every registered table via `GET /data/jsonapi/{table}` following spec (j…

  Scenario: REQ-257 default behaviour
    Given a client querying GET /data/jsonapi/{table}
    When the request includes sparse fieldsets, includes, filters, sorting, or pagination
    Then a JSON:API compliant response with compound documents is returned
