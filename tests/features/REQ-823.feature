# Generated from docs/arch/requirements.yaml. Do not hand-edit.
Feature: REQ-823 — Live Delivery Configuration
  # The LiveEngine reconciles poll jobs from the database at startup and after every admin mutation via _rebuild_schemas().…

  Scenario: REQ-823 default behaviour
    Given live config stored in registered_tables.live
    When the LiveEngine starts
    Then it queries the database for all active live configs and rebuilds poll jobs

    Given live config modified via admin GraphQL API
    When the mutation completes
    Then _rebuild_schemas() is called to reconcile the engine immediately
    And the new poll schedule takes effect without restart
