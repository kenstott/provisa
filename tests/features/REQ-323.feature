# Generated from docs/arch/requirements.yaml. Do not hand-edit.
Feature: REQ-323 — gRPC Remote Schema Connector (REQ-322–329)
  # Query vs mutation classification defaults to a name-prefix rule: methods whose names start with `Get`, `List`, `Find`, `…

  Scenario: REQ-323 default behaviour
    Given a gRPC service with methods named GetUsers, CreateUser, and StreamEvents
    When Provisa auto-classifies them
    Then GetUsers and StreamEvents are classified as queries and CreateUser as a mutation
