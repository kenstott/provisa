# Generated from docs/arch/requirements.yaml. Do not hand-edit.
Feature: REQ-585 — pgwire Server
  # pgwire supports `COPY ... TO STDOUT` (export) and `COPY ... FROM STDIN` (import). COPY TO STDOUT supports both table ref…

  Scenario: REQ-585 default behaviour
    Given a psql or JDBC copy manager issuing COPY TO STDOUT or COPY FROM STDIN
    When the command executes
    Then text and csv formats are supported; binary format is rejected
