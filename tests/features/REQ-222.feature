# Generated from docs/arch/requirements.yaml. Do not hand-edit.
Feature: REQ-222 — Hasura v2 Parity: Medium-Complexity Features
  # REST endpoint auto-generation -- for each root query field, generate `GET /data/rest/<table>` FastAPI endpoint. Map quer…

  Scenario: REQ-222 default behaviour
    Given a REST client calling GET /data/rest/<table>?limit=10&where.id.eq=1
    When the endpoint is hit
    Then the request is processed through the GraphQL compilation pipeline and results are returned
