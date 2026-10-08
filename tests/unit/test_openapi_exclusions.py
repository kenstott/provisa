# Copyright (c) 2026 Kenneth Stott
# Canary: 9b260047-bab2-4038-855e-224546be00fe
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A specification-built source does not offer what its exclusions name (REQ-1957).

The Stripe source offered Stripe's deprecated token/source payment integration as registrable
commands; one was run against a sandbox account and Stripe flagged the account. A source carries
exclusions -- operations, and request arguments -- applied to its specification before anything
reads it: a brand's as part of its curation, an operator's on any OpenAPI source."""

from __future__ import annotations

import copy
import json

import pytest

from provisa.openapi import exclusions as ex
from provisa.openapi.brands import _SPEC_DIR, brand_spec
from provisa.openapi.mapper import parse_spec

_FORM = "application/x-www-form-urlencoded"


def _spec() -> dict:
    def body(properties: dict, required: list[str] | None = None, ref: str | None = None) -> dict:
        schema = (
            {"$ref": f"#/components/schemas/{ref}"}
            if ref
            else {
                "type": "object",
                "properties": properties,
                **({"required": required} if required else {}),
            }
        )
        return {"content": {_FORM: {"schema": schema}}}

    return {
        "openapi": "3.0.0",
        "info": {"title": "t", "version": "1"},
        "components": {
            "schemas": {
                "NewOrder": {
                    "type": "object",
                    "properties": {"sku": {"type": "string"}, "token": {"type": "string"}},
                    "required": ["sku", "token"],
                }
            }
        },
        "paths": {
            "/customers": {
                "get": {
                    "operationId": "ListCustomers",
                    "parameters": [
                        {"name": "limit", "in": "query", "schema": {"type": "integer"}},
                        {"name": "legacy", "in": "query", "schema": {"type": "string"}},
                    ],
                },
                "post": {
                    "operationId": "CreateCustomer",
                    "requestBody": body(
                        {"name": {"type": "string"}, "token": {"type": "string"}}, ["token"]
                    ),
                },
            },
            "/customers/{id}": {
                "parameters": [{"name": "id", "in": "path", "required": True}],
                "post": {
                    "operationId": "UpdateCustomer",
                    "requestBody": body({"name": {"type": "string"}, "token": {"type": "string"}}),
                },
            },
            "/tokens": {"post": {"operationId": "CreateToken", "requestBody": body({})}},
            "/orders": {
                "post": {"operationId": "CreateOrder", "requestBody": body({}, ref="NewOrder")}
            },
        },
    }


def _args(spec: dict, path: str, method: str = "post") -> list[str]:
    schema = spec["paths"][path][method]["requestBody"]["content"][_FORM]["schema"]
    if "$ref" in schema:
        schema = spec["components"]["schemas"][schema["$ref"].rsplit("/", 1)[1]]
    return sorted(schema.get("properties") or {})


def _exclusions(**raw) -> ex.Exclusions:
    return ex.Exclusions.parse(raw, who="test")


# --- what an exclusion removes -------------------------------------------------------------------


def test_an_excluded_operation_is_not_in_the_specification():
    spec = _spec()
    out = ex.apply(spec, _exclusions(operations=[{"method": "POST", "path": "/tokens"}]), who="t")
    assert "/tokens" not in out["paths"]  # its only operation: the path goes with it
    assert "/tokens" in spec["paths"]  # the specification given is not changed
    out = ex.apply(
        spec, _exclusions(operations=[{"method": "post", "path": "/customers"}]), who="t"
    )
    assert list(out["paths"]["/customers"]) == ["get"]  # the path's other operation stays


def test_an_excluded_argument_on_one_operation_leaves_it_elsewhere():
    out = ex.apply(
        _spec(),
        _exclusions(arguments=[{"name": "token", "method": "POST", "path": "/customers"}]),
        who="t",
    )
    assert _args(out, "/customers") == ["name"]
    assert "required" not in out["paths"]["/customers"]["post"]["requestBody"]["content"][_FORM][
        "schema"
    ] or (
        "token"
        not in out["paths"]["/customers"]["post"]["requestBody"]["content"][_FORM]["schema"][
            "required"
        ]
    )
    assert _args(out, "/customers/{id}") == ["name", "token"]


def test_an_excluded_argument_with_no_operation_goes_from_every_operation_that_has_it():
    out = ex.apply(_spec(), _exclusions(arguments=[{"name": "token"}]), who="t")
    assert _args(out, "/customers") == ["name"]
    assert _args(out, "/customers/{id}") == ["name"]
    assert _args(out, "/orders") == ["sku"]  # a body declared by reference
    assert out["components"]["schemas"]["NewOrder"]["required"] == ["sku"]


def test_a_query_argument_is_excluded_and_a_path_parameter_never_is():
    out = ex.apply(
        _spec(),
        _exclusions(arguments=[{"name": "legacy", "method": "GET", "path": "/customers"}]),
        who="t",
    )
    assert [p["name"] for p in out["paths"]["/customers"]["get"]["parameters"]] == ["limit"]
    with pytest.raises(ex.ExclusionError, match="nothing for these exclusions: argument id"):
        ex.apply(_spec(), _exclusions(arguments=[{"name": "id"}]), who="t")


def test_no_exclusions_is_the_specification_itself():
    spec = _spec()
    assert ex.apply(spec, ex.Exclusions(), who="t") is spec
    assert not ex.Exclusions.parse(None, who="t")
    assert not ex.Exclusions.parse({"_about": "a note"}, who="t")


def test_what_is_excluded_is_not_offered_for_registration_and_is_not_an_argument():
    """Through the consumer every surface uses: the excluded operation is no command, and the
    excluded argument is not among a kept command's inputs."""
    out = ex.apply(
        _spec(),
        _exclusions(
            operations=[{"method": "POST", "path": "/tokens"}], arguments=[{"name": "token"}]
        ),
        who="t",
    )
    _tables, commands = parse_spec(out)
    by_id = {c.operation_id: c for c in commands}
    assert "CreateToken" not in by_id
    assert "CreateCustomer" in by_id
    kept = by_id["CreateCustomer"].input_schema
    assert kept is not None and sorted(kept["properties"]) == ["name"]
    assert all("token" not in (c.input_schema or {}).get("properties", {}) for c in commands)


# --- a list that cannot be applied fails by name -------------------------------------------------


def test_an_exclusion_the_specification_has_nothing_for_fails_naming_each():
    stale = _exclusions(
        operations=[{"method": "DELETE", "path": "/tokens"}, {"method": "POST", "path": "/gone"}],
        arguments=[
            {"name": "nope"},
            {"name": "token", "method": "POST", "path": "/tokens"},
        ],
    )
    with pytest.raises(ex.ExclusionError) as refused:
        ex.apply(_spec(), stale, who="source shop")
    message = str(refused.value)
    assert message.startswith("source shop: the specification has nothing for these exclusions")
    for named in (
        "operation DELETE /tokens",
        "operation POST /gone",
        "argument nope on every operation",
        "argument token on POST /tokens",
    ):
        assert named in message


@pytest.mark.parametrize(
    ("raw", "said"),
    [
        (["x"], "must be a mapping"),
        ({"operation": []}, "unknown key(s) operation"),
        ({"operations": [{"path": "/x"}]}, "needs both a method and a path"),
        ({"operations": [{"method": "FETCH", "path": "/x"}]}, "names no HTTP method"),
        ({"arguments": [{"method": "POST", "path": "/x"}]}, "needs a name"),
        ({"arguments": [{"name": "a", "method": "POST"}]}, "needs both a method and a path"),
    ],
)
def test_a_malformed_list_is_refused_naming_whose_it_is(raw, said):
    with pytest.raises(ex.ExclusionError, match=r"^source shop: ") as refused:
        ex.Exclusions.parse(raw, who="source shop")
    assert said in str(refused.value)


def test_operations_an_exclusion_covers_are_named_by_their_ids():
    covered = ex.excluded_operation_ids(
        _spec(), _exclusions(operations=[{"method": "POST", "path": "/tokens"}])
    )
    assert covered == {"CreateToken": "POST /tokens"}


# --- an operator's exclusions on any OpenAPI source ----------------------------------------------


def test_a_sources_own_exclusions_are_applied_when_its_specification_is_loaded(tmp_path):
    path = tmp_path / "spec.json"
    path.write_text(json.dumps(_spec()))
    mapping = {"exclusions": {"operations": [{"method": "POST", "path": "/tokens"}]}}
    loaded = ex.load_source_spec(str(path), mapping, source_id="shop")
    assert "/tokens" not in loaded["paths"]
    assert "/tokens" in ex.load_source_spec(str(path), {}, source_id="shop")["paths"]
    assert "/tokens" in ex.load_source_spec(str(path), None, source_id="shop")["paths"]
    with pytest.raises(ex.ExclusionError, match="^source shop: "):
        ex.load_source_spec(
            str(path),
            {"exclusions": {"operations": [{"method": "POST", "path": "/absent"}]}},
            source_id="shop",
        )


def test_an_operator_excludes_more_from_a_brands_source():
    """A branded source arrives with the brand's exclusions applied; the operator's are added."""
    mapping = {"exclusions": {"operations": [{"method": "POST", "path": "/v1/refunds"}]}}
    loaded = ex.load_source_spec("brand:stripe", mapping, source_id="pay")
    assert "post" not in loaded["paths"]["/v1/refunds"]
    assert "post" in brand_spec("stripe")["paths"]["/v1/refunds"]  # the brand's own is unchanged
    assert "/v1/tokens" not in loaded["paths"]


# --- the Stripe brand ----------------------------------------------------------------------------

# Request arguments that carry a card/bank token or inline card data to the Charges, Customers
# and Invoices APIs: the integration Stripe has deprecated and restricts.
_LEGACY_ARGUMENTS = {
    ("post", "/v1/charges"): {"source", "card"},
    ("post", "/v1/customers"): {"source"},
    ("post", "/v1/customers/{customer}"): {"source", "card", "bank_account", "default_card"},
    ("post", "/v1/invoices/{invoice}/pay"): {"source"},
}
# Operations that exist only to make or change such payment data.
_LEGACY_WRITES = [
    "/v1/tokens",
    "/v1/sources",
    "/v1/sources/{source}",
    "/v1/customers/{customer}/sources",
    "/v1/customers/{customer}/sources/{id}",
    "/v1/customers/{customer}/cards",
    "/v1/customers/{customer}/cards/{id}",
    "/v1/customers/{customer}/bank_accounts",
    "/v1/customers/{customer}/bank_accounts/{id}",
    "/v1/charges",
]


def test_the_stripe_source_offers_no_legacy_token_or_source_input():
    """Fails if a rebake from a newer Stripe specification, or an edit of the brand's list,
    brings the deprecated integration back."""
    paths = brand_spec("stripe")["paths"]
    offered = []
    for (method, path), names in _LEGACY_ARGUMENTS.items():
        operation = (paths.get(path) or {}).get(method)
        if operation is None:
            continue  # the operation itself is not offered
        schema = operation["requestBody"]["content"][_FORM]["schema"]
        offered += [
            f"{method.upper()} {path}: {n}" for n in sorted(names & set(schema["properties"]))
        ]
    offered += [f"POST {p}" for p in _LEGACY_WRITES if "post" in (paths.get(p) or {})]
    assert offered == []


def test_no_stripe_command_on_offer_takes_a_token_or_inline_source():
    """The same, read as a deployment reads it: no registrable command is one of the legacy
    writes, by the names Stripe's specification gives them."""
    _tables, commands = parse_spec(brand_spec("stripe"))
    ids = {c.operation_id for c in commands}
    legacy = {
        "PostTokens",
        "PostSources",
        "PostSourcesSource",
        "PostCustomersCustomerSources",
        "PostCustomersCustomerSourcesId",
        "PostCustomersCustomerCards",
        "PostCustomersCustomerCardsId",
        "PostCustomersCustomerBankAccounts",
        "PostCustomersCustomerBankAccountsId",
        "PostCharges",
    }
    assert sorted(ids & legacy) == []


def test_the_stripe_source_offers_the_payment_intents_family():
    _tables, commands = parse_spec(brand_spec("stripe"))
    ids = {c.operation_id for c in commands}
    for wanted in (
        "PostPaymentIntents",
        "PostPaymentIntentsIntentConfirm",
        "PostPaymentMethods",
        "PostPaymentMethodsPaymentMethodAttach",
        "PostSetupIntents",
        "PostCustomers",  # a customer is still created -- without a payment source
    ):
        assert wanted in ids, wanted


def test_what_stripe_still_offers_around_the_legacy_integration():
    """Reading and removing payment sources an account already has is not the deprecated
    integration; neither are the response fields the brand adds."""
    spec = brand_spec("stripe")
    assert "get" in spec["paths"]["/v1/customers/{customer}/sources"]
    assert "delete" in spec["paths"]["/v1/customers/{customer}/sources/{id}"]
    assert "source" in spec["components"]["schemas"]["charge"]["properties"]  # a READ column


def test_every_entry_of_the_stripe_list_says_why():
    listed = json.loads((_SPEC_DIR / "stripe.exclusions.json").read_text())
    assert listed["_about"]
    for entry in (*listed["operations"], *listed["arguments"]):
        assert entry.get("why"), entry


def test_the_vendors_published_specification_is_baked_unchanged():
    """Exclusions are applied when the shipped specification is loaded: the baked file is the
    vendor's, so a rebake does not need the list and the list is checked against every load."""
    import gzip

    baked = json.load(gzip.open(_SPEC_DIR / "stripe.json.gz"))
    assert "post" in baked["paths"]["/v1/tokens"]
    again = copy.deepcopy(brand_spec("stripe"))
    assert "/v1/tokens" not in again["paths"]


# --- what is already registered and now excluded is reported by name ----------------------------


def test_a_registered_table_or_command_an_exclusion_covers_is_named_with_its_operation():
    """A table registered from an operation is named for it; a command records its operation's
    id. One registered before the exclusion is reported -- it is not silently dropped, and
    nothing else is named."""
    excluded = {"CreateToken": "POST /tokens", "ListCustomers": "GET /customers"}
    reported = ex.covered_registrations(
        excluded,
        tables=["list_customers", "ListCustomers", "orders"],
        commands=["CreateToken", "CreateCustomer"],
    )
    assert reported == [
        "command CreateToken (POST /tokens)",
        "table ListCustomers (GET /customers)",
        "table list_customers (GET /customers)",
    ]
    assert ex.covered_registrations({}, tables=["orders"], commands=["CreateToken"]) == []


def test_a_sources_excluded_operations_are_its_brands_and_the_operators():
    own = {"exclusions": {"operations": [{"method": "POST", "path": "/v1/refunds"}]}}
    excluded = ex.source_excluded_operations("brand:stripe", own, source_id="pay")
    assert excluded["PostCharges"] == "POST /v1/charges"  # the brand's
    assert excluded["PostCustomersCustomerSources"] == "POST /v1/customers/{customer}/sources"
    assert excluded["PostRefunds"] == "POST /v1/refunds"  # the operator's
    assert "PostPaymentIntents" not in excluded
    brand_only = ex.source_excluded_operations("brand:stripe", None, source_id="pay")
    assert "PostRefunds" not in brand_only and len(brand_only) == len(excluded) - 1


def test_a_deployment_that_registered_stripes_legacy_commands_is_told_which(tmp_path):
    """The case that prompted the mechanism: a command registered before the curation."""
    excluded = ex.source_excluded_operations("brand:stripe", None, source_id="pay")
    reported = ex.covered_registrations(
        excluded,
        tables=["GetCustomers"],
        commands=["PostCustomersCustomerSources", "PostPaymentIntents"],
    )
    assert reported == [
        "command PostCustomersCustomerSources (POST /v1/customers/{customer}/sources)"
    ]


def test_a_source_of_the_stewards_own_specification_has_only_its_own_exclusions(tmp_path):
    path = tmp_path / "spec.json"
    path.write_text(json.dumps(_spec()))
    own = {"exclusions": {"operations": [{"method": "POST", "path": "/tokens"}]}}
    assert ex.source_excluded_operations(str(path), own, source_id="shop") == {
        "CreateToken": "POST /tokens"
    }
    assert ex.source_excluded_operations(str(path), {}, source_id="shop") == {}


def test_the_boot_loader_reports_what_an_exclusion_covers():
    """The by-name report is wired where a deployment's OpenAPI specifications are loaded."""
    from pathlib import Path

    import provisa.api.app_loaders as loaders

    source = Path(loaders.__file__).read_text()
    assert "covered_registrations(" in source and "source_excluded_operations(" in source
    assert "no longer offers %s: it is excluded (REQ-1957)" in source
