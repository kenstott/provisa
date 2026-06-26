# Generated from docs/arch/requirements.yaml. Do not hand-edit.
Feature: REQ-279 — Federation Performance
  # Provisa-branded comment hint syntax `/*+ hint */` in query text. Supported hints: `BROADCAST(<table>)`, `NO_REORDER`, `B…

  Scenario: REQ-279 default behaviour
    Given a query containing a /*+ BROADCAST(table) */ hint comment
    When the query is compiled
    Then the comment is stripped and translated to the equivalent Trino session property before
    forwarding
