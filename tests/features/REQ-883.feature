# Generated from docs/arch/requirements.yaml. Do not hand-edit.
Feature: REQ-883 — Federation Engine Abstraction
  # Provisa's pgwire server must satisfy the capability set that DuckDB's `postgres` extension exercises when attaching Prov…

  Scenario: REQ-883 default behaviour
    Given a client issues COPY (SELECT id, name FROM t) TO STDOUT (FORMAT binary) over pgwire
    When the result has an INTEGER id and a VARCHAR name column
    Then the server replies with a binary-mode CopyOutResponse and a byte stream beginning with the PGCOPY signature, encoding id as a 4-byte int and name as UTF-8 text per row, a NULL as field length -1, and terminating with the int16 -1 trailer
    And the same COPY in text/csv format is byte-for-byte unchanged from before
