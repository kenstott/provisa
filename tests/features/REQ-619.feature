# Generated from docs/arch/requirements.yaml. Do not hand-edit.
Feature: REQ-619 — Infrastructure
  # `start-ui.sh` manages the full dev lifecycle: Ctrl+C stops the backend, UI dev server, and all Docker Compose services a…

  Scenario: REQ-619 default behaviour
    Given start-ui.sh is running the full dev stack
    When Ctrl+C is pressed without --keep-docker
    Then the backend, UI dev server, and all Docker Compose services stop and Trino patches are reverted
