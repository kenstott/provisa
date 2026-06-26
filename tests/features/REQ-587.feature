# Generated from docs/arch/requirements.yaml. Do not hand-edit.
Feature: REQ-587 — pgwire Server
  # Transaction control commands (SET, BEGIN, START TRANSACTION, COMMIT, ROLLBACK, SAVEPOINT, RELEASE, DISCARD, RESET, DEALL…

  Scenario: REQ-587 default behaviour
    Given a JDBC driver or ORM issuing BEGIN, COMMIT, or ROLLBACK
    When the command is received by pgwire
    Then an empty success response is returned with no actual transaction state maintained
