# Generated from docs/arch/requirements.yaml. Do not hand-edit.
Feature: REQ-749 — Security
  # Domain policy tri-state mode (legacy/single-domain/namespaced) — legacy mode (use_domains absent) stores declared domain…

  Scenario: REQ-749 default behaviour
    Given a config with use_domains mode specified
    When the config is applied
    Then domain_id is stored according to the tri-state mode (legacy/single/namespaced) and reload validates existing domains
