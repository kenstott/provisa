# Generated from docs/arch/requirements.yaml. Do not hand-edit.
Feature: REQ-749 — Security
  # [SUPERSEDED by REQ-749, 2026-11-04 -- there are two modes, not three; see the TWO MODES amendment. Kept here for history…

  Scenario: REQ-749 default behaviour
    Given a config with use_domains mode specified
    When the config is applied
    Then domain_id is stored according to the tri-state mode (legacy/single/namespaced) and reload validates existing domains
