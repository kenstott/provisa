# Generated from docs/arch/requirements.yaml. Do not hand-edit.
Feature: REQ-562 — Installer & Packaging
  # In a multi-node deployment, secondary Provisa API instances are stateless and read all configuration (sources, tables, r…

  Scenario: REQ-562 default behaviour
    Given a multi-node deployment with a primary PostgreSQL database
    When a secondary API node starts
    Then it reads all configuration from the primary PostgreSQL without manual sync
