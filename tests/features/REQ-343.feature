# Generated from docs/arch/requirements.yaml. Do not hand-edit.
Feature: REQ-343 — Phase AT — WebSocket & RSS Sources (REQ-338–344)
  # RSS parser handles both RSS 2.0 (`<item>`) and Atom (`<entry>`) formats. Fields extracted: `title`, `link`, `description…

  Scenario: REQ-343 default behaviour
    Given an RSS 2.0 feed and an Atom feed registered as sources
    When items are parsed
    Then both formats extract title, link, description/summary, published, and id; unparseable dates use datetime.min
