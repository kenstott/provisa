# Generated from docs/arch/requirements.yaml. Do not hand-edit.
Feature: REQ-288 — Automatic Persisted Queries (APQ)
  # Provisa implements the Apollo APQ wire protocol (GraphQL over HTTP with `extensions.persistedQuery.sha256Hash`). Client…

  Scenario: REQ-288 default behaviour
    Given an Apollo client sending only a hash
    When the server has the query cached it executes immediately; when not it returns
      PersistedQueryNotFound
    Then the client resends with full text, server stores and executes without modification
