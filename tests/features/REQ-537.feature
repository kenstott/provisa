# Generated from docs/arch/requirements.yaml. Do not hand-edit.
Feature: REQ-537 — Compiler & Schema
  # `GET /data/schema-version` returns a string combining a per-boot UUID nonce with a monotonically incrementing rebuild co…

  Scenario: REQ-537 default behaviour
    Given the schema is rebuilt after a naming convention change
    When GET /data/schema-version is called
    Then it returns a new <boot-id>-<counter> string reflecting the rebuild
