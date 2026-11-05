# Generated from docs/arch/requirements.yaml. Do not hand-edit.
Feature: REQ-818 — Cypher Mutations
  # Cypher supports WRITES via the /data/cypher endpoint. CREATE, DELETE, and SET statements execute as direct table writes…

  Scenario: REQ-818 default behaviour
    Given a valid CREATE statement targeting a table with write rights
    When executed via the /data/cypher endpoint
    Then the role's row filter is applied to the translated write by the one write admission
    And it executes as a direct table write, returns affected_rows, and runs the one after-write step

    Given a MERGE of a label the model does not have
    When executed via the /data/cypher endpoint
    Then it matches the session node with those properties, or creates one ephemeral node (REQ-2176)
