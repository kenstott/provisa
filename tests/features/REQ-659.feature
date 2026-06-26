# Generated from docs/arch/requirements.yaml. Do not hand-edit.
Feature: REQ-659 — JSON:API Remote Schema Connector
  # JSON:API pagination via `links.next` / `links.prev` integrated with Provisa limit/offset — the compiler tracks paginatio…

  Scenario: REQ-659 default behaviour
    Given a client-issued LIMIT/OFFSET query against a paginated JSON:API source
    When results span multiple pages
    Then the compiler follows links.next to fetch all pages and materializes a complete result set
