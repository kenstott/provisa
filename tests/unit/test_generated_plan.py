# Copyright (c) 2026 Kenneth Stott
# Canary: 0a7c3e95-4d18-4b62-9f3e-6c1b8d2a5e47
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""One kept plan per shape of synthesized GraphQL text, values bound (REQ-1877).

``provisa.api.generated_plan`` takes the values out of the text REST and JSON:API synthesize,
keeps one compiled plan per remaining shape, and binds each request's values into it. Pinned here:
what counts as a value, that a bound plan is exactly what compiling the request's own text yields,
and that a shape whose compilation depends on a value is kept per text instead.
"""

# Requirements: REQ-1877

from __future__ import annotations

from types import SimpleNamespace

import pytest

from provisa.api import generated_plan
from provisa.api.generated_plan import compile_generated_graphql, request_shape
from provisa.compiler.compiled_query_cache import CompiledQueryCache
from provisa.compiler.parser import GraphQLValidationError, parse_query
from provisa.compiler.rls import RLSContext
from provisa.compiler.sql_gen import compile_query
from provisa.pgwire import governed_plan
from tests.unit.test_jsonapi import _build_test_schema

_ROLE = "admin"
_FIELDS = "{ id customerId amount region createdAt }"


@pytest.fixture
def env(monkeypatch):
    schema, ctx = _build_test_schema()
    state = SimpleNamespace(
        schemas={_ROLE: schema},
        contexts={_ROLE: ctx},
        rls_contexts={_ROLE: RLSContext.empty()},
        roles={_ROLE: {"id": _ROLE}},
        masking_rules={},
        tables=[],
        schema_boot_id="boot",
        schema_version=1,
        compiled_query_cache=CompiledQueryCache(),
    )
    monkeypatch.setattr(governed_plan, "_rebuild_in_progress", lambda: False)
    parses: list[str] = []

    def _counting_parse(schema_, text, *args, **kwargs):
        parses.append(text)
        return parse_query(schema_, text, *args, **kwargs)

    monkeypatch.setattr(generated_plan, "parse_query", _counting_parse)

    def run(text: str):
        return compile_generated_graphql(state, _ROLE, schema, ctx, text)

    def fresh(text: str):
        return compile_query(parse_query(schema, text), ctx)

    return SimpleNamespace(run=run, fresh=fresh, parses=parses, state=state)


# -- what is a value ------------------------------------------------------------------------------


def test_values_are_taken_out_of_the_shape_in_order():
    shape, values = request_shape(
        "{ orders(limit: 26, offset: 50, where: {id: {gt: -3}, amount: {lt: 9.5}, "
        'region: {in: ["US", "EU"]}}) { id } }'
    )
    assert values == [26, 50, -3, 9.5, "US", "EU"]
    assert [type(v) for v in values] == [int, int, int, float, str, str]
    assert "26" not in "".join(shape) and "US" not in "".join(shape)


@pytest.mark.parametrize(
    "a, b",
    [
        ("{ orders(where: {id: {eq: 1}}) { id } }", "{ orders(where: {id: {eq: 987}}) { id } }"),
        (
            '{ orders(where: {region: {eq: "US"}}) { id } }',
            '{ orders(where: {region: {eq: "a longer value, with: punctuation {}"}}) { id } }',
        ),
        ("{ orders(limit: 5) { id } }", "{ orders(limit: 500) { id } }"),
    ],
)
def test_requests_that_differ_only_in_a_value_have_one_shape(a, b):
    assert request_shape(a)[0] == request_shape(b)[0]


@pytest.mark.parametrize(
    "a, b",
    [
        # an integer and a float, a number and a string
        ("{ orders(where: {id: {eq: 1}}) { id } }", "{ orders(where: {id: {eq: 1.5}}) { id } }"),
        ("{ orders(where: {id: {eq: 1}}) { id } }", '{ orders(where: {id: {eq: "1"}}) { id } }'),
        # another operator, another column, another list length, another selection
        ("{ orders(where: {id: {eq: 1}}) { id } }", "{ orders(where: {id: {gt: 1}}) { id } }"),
        ("{ orders(where: {id: {eq: 1}}) { id } }", "{ orders(where: {amount: {eq: 1}}) { id } }"),
        (
            '{ orders(where: {region: {in: ["a"]}}) { id } }',
            '{ orders(where: {region: {in: ["a", "b"]}}) { id } }',
        ),
        ("{ orders(limit: 5) { id } }", "{ orders(limit: 5) { id region } }"),
        # a boolean and an enum are part of the shape
        (
            "{ orders(where: {id: {is_null: true}}) { id } }",
            "{ orders(where: {id: {is_null: false}}) { id } }",
        ),
        ("{ orders(order_by: {id: asc}) { id } }", "{ orders(order_by: {id: desc}) { id } }"),
    ],
)
def test_requests_that_differ_in_more_than_a_value_have_different_shapes(a, b):
    assert request_shape(a)[0] != request_shape(b)[0]


@pytest.mark.parametrize(
    "text, kept_verbatim",
    [
        # larger than the schema's Int: validated for itself, never bound past the validation
        ("{ orders(where: {id: {eq: 99999999999}}) { id } }", "99999999999"),
        # the compiler writes an ISO date into the statement as a TIMESTAMP literal
        ('{ orders(where: {createdAt: {eq: "2026-01-01"}}) { id } }', '"2026-01-01"'),
        ('{ orders(where: {createdAt: {gt: "2026-01-01T10:00:00Z"}}) { id } }', "T10:00:00Z"),
        # digits that belong to a name
        ("{ orders2024(limit: 5) { col1 x_9 } }", "orders2024"),
        ("{ orders2024(limit: 5) { col1 x_9 } }", "col1 x_9"),
    ],
)
def test_what_is_not_a_value_stays_in_the_shape(text, kept_verbatim):
    shape, values = request_shape(text)
    assert kept_verbatim in "".join(shape)
    assert all(str(v) not in kept_verbatim for v in values)


def test_text_carrying_the_shape_marker_has_no_values():
    text = '{ orders(where: {region: {eq: "a\x00b"}}, limit: 5) { id } }'
    assert request_shape(text) == ((text,), [])


# -- a bound plan is what compiling the text yields --------------------------------------------------


_TEMPLATES = [
    "{{ orders(where: {{id: {{eq: {0}}}}}) {fields} }}",
    "{{ orders(limit: {0}, offset: {1}) {fields} }}",
    "{{ orders(limit: {0}, where: {{id: {{gte: {1}, lt: {2}}}}}, order_by: {{id: desc}}) {fields} }}",
    '{{ orders(where: {{region: {{eq: "r{0}"}}, amount: {{gt: {1}.5}}}}) {fields} }}',
    '{{ orders(where: {{region: {{in: ["a{0}", "b{1}", "c{2}"]}}}}) {fields} }}',
    '{{ orders(where: {{_or: [{{id: {{eq: {0}}}}}, {{region: {{like: "%{1}%"}}}}]}}) {fields} }}',
    "{{ orders(limit: {0}) {{ id customer {{ id name }} }} }}",
    # the same value twice, and values equal to each other
    "{{ orders(limit: {0}, offset: {0}, where: {{id: {{eq: {0}}}}}) {fields} }}",
]


@pytest.mark.parametrize("template", _TEMPLATES)
def test_a_bound_plan_equals_a_fresh_compile_of_the_same_text(env, template):
    texts = [template.format(n, n + 7, n + 13, fields=_FIELDS) for n in (3, 40, 500, 6000, 2)]
    env.run(texts[0])
    parses = len(env.parses)
    for text in texts[1:]:
        assert env.run(text) == env.fresh(text), text
    assert len(env.parses) == parses, "a kept shape was parsed again"
    assert env.run(texts[0]) == env.fresh(texts[0])


def test_each_caller_gets_its_own_parameter_lists(env):
    text = "{ orders(where: {id: {eq: 5}}) { id } }"
    first = env.run(text)
    first[0].params.append("scribble")
    assert env.run(text) == env.fresh(text)


def test_text_with_no_values_is_compiled_once(env):
    text = "{ orders(order_by: {id: desc}) { id region } }"
    assert env.run(text) == env.fresh(text)
    assert len(env.parses) == 1
    assert env.run(text) == env.fresh(text)
    assert len(env.parses) == 1


# -- a shape that is not value-independent is kept per text ----------------------------------------


def test_a_virtual_column_filter_is_compiled_for_each_value(env):
    """``_domain_`` is decided at compile time: TRUE for the table's own domain, FALSE otherwise.
    The value reaches no parameter, so the shape is kept per text."""
    own = '{ orders(where: {_domain_: {eq: "sales"}}) { id } }'
    other = '{ orders(where: {_domain_: {eq: "finance"}}) { id } }'
    for text in (other, own, other, own):
        assert env.run(text) == env.fresh(text), text
    assert "TRUE" in env.run(own)[0].sql and "FALSE" in env.run(other)[0].sql
    parses = len(env.parses)
    env.run(own)
    env.run(other)
    assert len(env.parses) == parses, "a repeated text of a per-text shape was parsed again"


def test_a_rejected_value_is_rejected_every_time_and_keeps_nothing(env):
    env.run("{ orders(where: {id: {eq: 5}}) { id } }")
    kept = len(env.state.compiled_query_cache)
    for _ in range(2):
        with pytest.raises(GraphQLValidationError):
            env.run("{ orders(where: {id: {eq: 99999999999}}) { id } }")
        with pytest.raises(GraphQLValidationError):
            env.run('{ orders(where: {id: {eq: "five"}}) { id } }')
    assert len(env.state.compiled_query_cache) == kept
