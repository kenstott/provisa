# Generated from docs/arch/requirements.yaml. Do not hand-edit.
Feature: REQ-1017 — SaaS Billing
  # SaaS billing is provided by Lemon Squeezy as Merchant of Record. `provisa/api/billing/` integrates over the Lemon Squeez…

  Scenario: REQ-1017 default behaviour
    Given a tenant initiates checkout with tenant_id in custom_data
    When POST /v1/checkouts is called to Lemon Squeezy API
    Then a checkout URL is returned; the webhook subsequently updates tenant.ls_customer_id and applies plan/source_limit from the subscription variant

    And Given a subscription_created webhook from Lemon Squeezy with valid HMAC-SHA256 signature
    When the webhook signature is verified against LEMONSQUEEZY_SIGNING_SECRET
    Then the tenant's plan and source_limit are updated from the variant name; unrecognized variants reject the webhook
