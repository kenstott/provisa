# Generated from docs/arch/requirements.yaml. Do not hand-edit.
Feature: REQ-536 — Execution & Routing
  # All data responses include cache status headers: `X-Provisa-Cache: HIT|MISS` on every response, and `X-Provisa-Cache-Age…

  Scenario: REQ-536 default behaviour
    Given any data response from Provisa
    When the response is returned to the client
    Then it includes X-Provisa-Cache: HIT|MISS and X-Provisa-Cache-Age on cache HITs
