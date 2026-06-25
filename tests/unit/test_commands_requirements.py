# Copyright (c) 2026 Kenneth Stott
# Canary: placeholder
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
"""Unit tests for commands UI requirements: REQ-242, REQ-243, REQ-244, REQ-245, REQ-248"""

from __future__ import annotations


# ---------------------------------------------------------------------------
# REQ-242: Admin UI "Commands" page listing all registered functions and
# webhooks. Grouped by type (DB Function / Webhook). Shows source, domain,
# exposed_as (mutation/query), governance level, return table, argument count.
# ---------------------------------------------------------------------------


def test_function_model_has_kind_field():
    # REQ-242: Function must have `kind` field (mutation/query) for exposed_as display.
    from provisa.core.models import Function

    fn = Function(
        name="create_order",
        source_id="pg1",
        function_name="create_order",
        returns="pg1.public.orders",
        kind="mutation",
    )
    assert fn.kind == "mutation"


def test_function_model_has_source_id():
    # REQ-242: Function carries source_id for source column in Commands page.
    from provisa.core.models import Function

    fn = Function(
        name="list_products",
        source_id="pg2",
        function_name="list_products",
        returns="pg2.public.products",
    )
    assert fn.source_id == "pg2"


def test_function_model_has_domain_id():
    # REQ-242: Function carries domain_id for domain column in Commands page.
    from provisa.core.models import Function

    fn = Function(
        name="fn",
        source_id="src",
        function_name="fn",
        returns="src.public.t",
        domain_id="sales",
    )
    assert fn.domain_id == "sales"


def test_function_model_has_returns_field():
    # REQ-242: Function carries returns (return table id) for Commands page display.
    from provisa.core.models import Function

    fn = Function(
        name="get_user",
        source_id="pg",
        function_name="get_user",
        returns="pg.public.users",
    )
    assert fn.returns == "pg.public.users"


def test_function_model_has_arguments():
    # REQ-242/243: Function arguments count is displayed on the Commands page.
    from provisa.core.models import Function, FunctionArgument

    fn = Function(
        name="search",
        source_id="pg",
        function_name="search",
        returns="pg.public.results",
        arguments=[
            FunctionArgument(name="query", type="String"),
            FunctionArgument(name="limit", type="Int"),
        ],
    )
    assert len(fn.arguments) == 2


def test_webhook_model_has_kind_field():
    # REQ-242: Webhook must have `kind` field for exposed_as display.
    from provisa.core.models import Webhook

    wh = Webhook(
        name="notify_order",
        url="https://example.com/notify",
        kind="mutation",
    )
    assert wh.kind == "mutation"


def test_webhook_model_has_url_and_method():
    # REQ-242/243: Webhook carries url and method for Commands page and add form.
    from provisa.core.models import Webhook

    wh = Webhook(name="callback", url="https://hooks.example.com/cb", method="POST")
    assert wh.url == "https://hooks.example.com/cb"
    assert wh.method == "POST"


def test_webhook_model_has_timeout_ms():
    # REQ-243: Webhook add form includes timeout_ms field.
    from provisa.core.models import Webhook

    wh = Webhook(name="slow_hook", url="https://example.com", timeout_ms=10000)
    assert wh.timeout_ms == 10000


# ---------------------------------------------------------------------------
# REQ-243: Add command form with type selector: DB Function or Webhook.
# Fields: source, schema, function name, exposed_as, returns, arguments, visible_to.
# ---------------------------------------------------------------------------


def test_function_model_has_schema_name():
    # REQ-243: DB Function form includes schema field.
    from provisa.core.models import Function

    fn = Function(
        name="fn",
        source_id="pg",
        function_name="fn",
        returns="pg.custom.t",
        schema_name="custom",
    )
    assert fn.schema_name == "custom"


def test_function_model_has_visible_to():
    # REQ-243: Functions carry visible_to for governance display.
    from provisa.core.models import Function

    fn = Function(
        name="fn",
        source_id="pg",
        function_name="fn",
        returns="pg.public.t",
        visible_to=["analyst"],
    )
    assert "analyst" in fn.visible_to


def test_function_kind_can_be_query():
    # REQ-243: exposed_as can be "query" as well as "mutation".
    from provisa.core.models import Function

    fn = Function(
        name="get_report",
        source_id="pg",
        function_name="get_report",
        returns="pg.public.reports",
        kind="query",
    )
    assert fn.kind == "query"


def test_webhook_has_visible_to():
    # REQ-243: Webhooks carry visible_to governance field.
    from provisa.core.models import Webhook

    wh = Webhook(
        name="hook",
        url="https://example.com",
        visible_to=["admin"],
    )
    assert "admin" in wh.visible_to


# ---------------------------------------------------------------------------
# REQ-244: Inline type builder for webhook return types — dynamic rows of
# field name + GraphQL type. Used when webhook returns a custom shape not
# backed by a registered table.
# ---------------------------------------------------------------------------


def test_inline_type_model_has_name_and_type():
    # REQ-244: InlineType must have name and type fields for the inline type builder.
    from provisa.core.models import InlineType

    field = InlineType(name="order_id", type="String")
    assert field.name == "order_id"
    assert field.type == "String"


def test_webhook_supports_inline_return_type_list():
    # REQ-244: Webhook.inline_return_type is a list of InlineType entries.
    from provisa.core.models import InlineType, Webhook

    wh = Webhook(
        name="custom_hook",
        url="https://example.com",
        inline_return_type=[
            InlineType(name="result", type="String"),
            InlineType(name="count", type="Int"),
        ],
    )
    assert len(wh.inline_return_type) == 2
    assert wh.inline_return_type[0].name == "result"


def test_webhook_returns_can_be_none_for_inline_type():
    # REQ-244: When using inline return type, returns field is None (no registered table).
    from provisa.core.models import InlineType, Webhook

    wh = Webhook(
        name="hook",
        url="https://example.com",
        returns=None,
        inline_return_type=[InlineType(name="msg", type="String")],
    )
    assert wh.returns is None
    assert len(wh.inline_return_type) > 0


def test_inline_type_accepts_graphql_scalar_types():
    # REQ-244: InlineType.type accepts GraphQL scalar names.
    from provisa.core.models import InlineType

    for gql_type in ("String", "Int", "Float", "Boolean", "DateTime"):
        field = InlineType(name="f", type=gql_type)
        assert field.type == gql_type


# ---------------------------------------------------------------------------
# REQ-245: Test command button — execute function/webhook with sample arguments,
# display result + governance pipeline applied.
# ---------------------------------------------------------------------------


def test_function_argument_has_name_and_type():
    # REQ-245: FunctionArgument carries name and type for test command execution.
    from provisa.core.models import FunctionArgument

    arg = FunctionArgument(name="user_id", type="String")
    assert arg.name == "user_id"
    assert arg.type == "String"


def test_webhook_kind_can_be_query():
    # REQ-245: Webhooks can be exposed as queries (test command also applies to queries).
    from provisa.core.models import Webhook

    wh = Webhook(name="fetch_hook", url="https://example.com", kind="query")
    assert wh.kind == "query"


# ---------------------------------------------------------------------------
# REQ-248: GraphQL Voyager integration uses iframe with React 18 CDN standalone
# bundle — avoids MUI v5/React 19 incompatibility. No component fork planned.
# ---------------------------------------------------------------------------


def test_voyager_uses_iframe_approach():
    # REQ-248: Voyager is integrated via iframe, not a native React component fork.
    # Verify that no voyager React component fork exists in the codebase.
    import os

    ui_src = "/Volumes/main/Users/kennethstott/PycharmProjects/provisa/provisa-ui/src"
    if not os.path.isdir(ui_src):
        return  # UI source not present in this environment

    # No voyager component fork should exist
    for dirpath, _, filenames in os.walk(ui_src):
        for fname in filenames:
            if (
                "voyager" in fname.lower()
                and fname.endswith((".tsx", ".jsx", ".js"))
                and not fname.endswith(".d.ts")
            ):
                # If a voyager file exists, it should be an iframe wrapper, not a full port
                fpath = os.path.join(dirpath, fname)
                with open(fpath) as f:
                    content = f.read()
                # An iframe integration contains "iframe" not a full React port
                assert "iframe" in content.lower() or "cdn" in content.lower(), (
                    f"{fpath} appears to be a voyager component fork, not an iframe integration"
                )
