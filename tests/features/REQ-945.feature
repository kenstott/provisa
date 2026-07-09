# Generated from docs/arch/requirements.yaml. Do not hand-edit.
Feature: REQ-945 — Live Data & Events
  # The openapi adapter fetch is wired into SourceRowLoader via make_openapi_loader, which accepts injectable per-type adapt…

  Scenario: REQ-945 default behaviour
    Given an openapi source with a registered ApiEndpoint and ApiSource in live state
    When SourceRowLoader.load(source, table) is invoked
    Then make_openapi_loader resolves the ApiEndpoint and ApiSource from state
    And calls the operation with default_params via api_source.caller.call_api
    And flattens the response pages via api_source.flattener.flatten_response
    And returns row dicts without issuing an engine SELECT
    Given an openapi source with no registered ApiEndpoint
    When SourceRowLoader.load(source, table) is invoked
    Then UnsupportedSourceFetch is raised
