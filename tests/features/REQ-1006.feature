# Generated from docs/arch/requirements.yaml. Do not hand-edit.
Feature: REQ-1006 — Infrastructure
  # The SPA static server routing is deterministic and driven by the browser Sec-Fetch-Dest request header (never by a maint…

  Scenario: REQ-1006 default behaviour
    Given a browser navigates directly to the SPA deep link /graph/node/42
    When the request includes Sec-Fetch-Dest: document
    Then index.html is served with HTTP 200

    Given the UI calls fetch('/api/setup/status')
    When the API endpoint is unlisted and returns 404
    Then the 404 status is proxied to the browser (never silently replaced with index.html)

    Given a browser makes a XHR request to /api/v1/query
    When the request lacks Sec-Fetch-Dest: document header
    Then the request is proxied to the API and its real HTTP status is returned
