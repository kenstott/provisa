# Generated from docs/arch/requirements.yaml. Do not hand-edit.
Feature: REQ-417 — Registration & Governance
  # Hasura v2 migration tool (`provisa/hasura_v2/mapper.py`) maps Hasura Remote Schemas to Provisa `graphql_remote` source r…

  Scenario: REQ-417 default behaviour
    Given a Hasura v2 metadata file containing Remote Schema entries
    When the migration tool runs
    Then each Remote Schema is mapped to a graphql_remote source registration preserving name, URL,
    headers, and auth
