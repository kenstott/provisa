# Generated from docs/arch/requirements.yaml. Do not hand-edit.
Feature: REQ-268 — SQL & Multi-Protocol Client Access
  # Python DB-API 2.0 (PEP 249) interface in `provisa-client`. `provisa_client.connect(url, user, password)` returns a PEP 2…

  Scenario: REQ-268 default behaviour
    Given a Python caller using provisa_client.connect()
    When cursor.execute() is called with GraphQL or SQL
    Then the query executes with the server-assigned role via DB-API 2.0 semantics
