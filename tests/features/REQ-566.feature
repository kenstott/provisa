# Generated from docs/arch/requirements.yaml. Do not hand-edit.
Feature: REQ-566 — Subscriptions
  # When PostgreSQL trigger installation fails (e.g. insufficient privilege), Provisa logs a warning and falls back to water…

  Scenario: REQ-566 default behaviour
    Given a PostgreSQL table where Provisa lacks trigger creation privileges
    When Provisa starts up
    Then it logs a warning and uses watermark-based polling for that table instead of LISTEN/NOTIFY
