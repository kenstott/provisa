# Copyright (c) 2026 Kenneth Stott
# Canary: 5e1c7b93-2a48-4d6f-8b05-9c3e7a1d4f26
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""REQ-2168: command bindings. A command (or function) is offered in every language under its
generated name; a declaration renames it in a language or excludes it from one, SQL never
excluded. Two commands bound to one name in a language, or a binding shadowing a built-in, are
refused naming both. A function declared as a SQL expression template is parsed and transpiled
for every engine when it is saved."""

# Requirements: REQ-2168

from __future__ import annotations

from types import SimpleNamespace

import pytest
import sqlglot
from pydantic import ValidationError

from provisa.core.command_bindings import (
    CommandBindingRefused,
    binding_name,
    bound,
    check_names,
)
from provisa.core.models import Function
from provisa.core.sql_expression import (
    SqlExpressionRefused,
    check_for_engines,
    parse_template,
    render,
)

pytestmark = pytest.mark.unit


def _command(name: str, **bindings) -> dict:
    return {"name": name, "impl_kind": "python", "language_bindings": bindings}


def _function(**overrides) -> dict:
    return {
        "name": "text_join",
        "source_id": "",
        "function_name": "text_join",
        "returns": "",
        "domain_id": "sales",
        "kind": "query",
        "impl_kind": "sql_expression",
        "binding": {"sql": "array_to_string({list}, {sep})"},
        "arguments": [{"name": "list", "type": "json"}, {"name": "sep", "type": "text"}],
        **overrides,
    }


# --- names per language ---------------------------------------------------------------------


def test_a_language_left_out_offers_the_command_under_its_own_name():
    command = _command("enrich")
    assert {binding_name(command, lang) for lang in ("sql", "cypher", "graphql", "mcp")} == {
        "enrich"
    }


def test_a_binding_renames_or_excludes_the_command_in_its_language():
    command = _command(
        "meta_schema", cypher={"name": "apoc.meta.schema"}, graphql={"exclude": True}
    )
    assert binding_name(command, "cypher") == "apoc.meta.schema"
    assert binding_name(command, "graphql") is None
    assert binding_name(command, "sql") == "meta_schema"
    assert bound([command], "cypher") == {"apoc.meta.schema": command}
    assert bound([command], "graphql") == {}


def test_the_sql_binding_cannot_be_excluded():
    with pytest.raises(ValidationError, match="SQL command binding cannot be excluded"):
        Function.model_validate(_function(language_bindings={"sql": {"exclude": True}}))


def test_an_unknown_language_is_refused_naming_the_languages():
    with pytest.raises(ValidationError, match="'cobol' is not a language"):
        Function.model_validate(_function(language_bindings={"cobol": {"name": "x"}}))


def test_a_binding_names_or_excludes_not_both():
    with pytest.raises(ValidationError, match="not both"):
        Function.model_validate(
            _function(language_bindings={"cypher": {"name": "x.y", "exclude": True}})
        )


def test_two_commands_bound_to_one_name_in_a_language_are_refused_naming_both():
    with pytest.raises(CommandBindingRefused, match="'alpha' and 'beta'.*'apoc.x' in cypher"):
        check_names(
            [
                _command("alpha", cypher={"name": "apoc.x"}),
                _command("beta", cypher={"name": "apoc.x"}),
            ]
        )


def test_the_same_name_in_two_languages_is_not_a_collision():
    # alpha is gamma in Cypher; gamma is gamma in SQL and left out of Cypher.
    check_names(
        [_command("alpha", cypher={"name": "gamma"}), _command("gamma", cypher={"exclude": True})]
    )


@pytest.mark.parametrize(
    ("command", "shadowed"),
    [
        (_command("labels", cypher={"name": "db.labels"}), "procedure namespace db"),
        (_command("upper"), "function upper"),
        (
            {
                "name": "lower_it",
                "impl_kind": "sql_expression",
                "language_bindings": {"cypher": {"name": "toLower"}},
            },
            "function tolower",
        ),
    ],
)
def test_a_binding_shadowing_a_built_in_is_refused_naming_both(command, shadowed):
    with pytest.raises(CommandBindingRefused, match=f"{command['name']!r}.*{shadowed}"):
        check_names([command])


def test_the_shipped_apoc_declarations_load_and_collide_with_nothing():
    from provisa.core.builtin_commands import declared

    shipped = declared()
    check_names(shipped)
    cypher = bound(shipped, "cypher")
    assert {"apoc.meta.data", "apoc.meta.schema", "apoc.text.join", "apoc.coll.sum"} <= set(cypher)
    for language in ("graphql", "rest", "mcp", "sparql"):
        assert bound(shipped, language) == {}
    assert set(bound(shipped, "sql")) == {c["name"] for c in shipped}


# --- built-ins --------------------------------------------------------------------------------


def test_a_builtin_command_names_a_builtin_provisa_ships():
    with pytest.raises(ValidationError, match="commands Provisa ships"):
        Function.model_validate(
            _function(name="mine", impl_kind="builtin", binding={"callable": "os:system"})
        )


# --- SQL expression templates -----------------------------------------------------------------


def test_a_sql_expression_function_is_a_query():
    with pytest.raises(ValidationError, match="is a query"):
        Function.model_validate(_function(kind="mutation"))


@pytest.mark.parametrize(
    ("sql", "refusal"),
    [
        ("array_to_string({list}, {other})", r"\{other\}, which is not one of its arguments"),
        ("array_to_string({list}, ',')", "does not use the argument 'sep'"),
        ("array_to_string({list}, {sep}", "is not a SQL expression"),
        ("{list}, {sep}", "is not one SQL expression"),
    ],
)
def test_a_template_that_cannot_be_used_is_refused_at_save(sql, refusal):
    with pytest.raises(ValidationError, match=refusal):
        Function.model_validate(_function(binding={"sql": sql}))


def test_a_call_puts_its_arguments_in_the_template_and_transpiles_per_engine():
    fn = _function()
    call = render(fn, [sqlglot.parse_one("ARRAY['a','b']"), sqlglot.exp.Literal.string("-")])
    marked = " /* provisa:fn text_join */"
    assert call.sql("postgres") == "(ARRAY_TO_STRING(ARRAY['a', 'b'], '-'))" + marked
    assert call.sql("trino") == "(ARRAY_JOIN(ARRAY['a', 'b'], '-'))" + marked


def test_a_call_with_another_number_of_arguments_is_refused_naming_the_signature():
    with pytest.raises(SqlExpressionRefused, match=r"text_join\(list, sep\) takes 2"):
        render(_function(), [sqlglot.exp.Literal.string("x")])


def test_a_template_an_engine_of_the_deployment_cannot_express_is_refused_naming_it():
    binding = {"sql": "regexp_replace({text}, 'a', 'b', 'g')"}
    check_for_engines("strip_a", binding, ["text"], ["postgres", "duckdb", "trino"])
    with pytest.raises(SqlExpressionRefused, match="for the bigquery engine"):
        check_for_engines("strip_a", binding, ["text"], ["duckdb", "bigquery"])


def test_the_deployment_engines_are_the_federation_engine_and_each_source():
    from provisa.core.sql_expression import deployment_dialects

    state = SimpleNamespace(
        federation_engine=SimpleNamespace(dialect="trino"),
        source_dialects={"pg": "postgres", "sheet": ""},
    )
    assert deployment_dialects(state) == {"trino", "postgres"}


def test_a_function_called_on_its_own_selects_its_template_with_bound_parameters():
    from provisa.executor.sql_expression_call import statement

    assert statement(_function()) == (
        'SELECT (ARRAY_TO_STRING($1, $2)) /* provisa:fn text_join */ AS "value"'
    )


def test_the_template_parses_with_its_argument_slots():
    template = parse_template("f", {"sql": "lower({text})"}, ["text"])
    assert template.sql() == 'LOWER("__provisa_arg_text")'


def test_a_stored_command_keeps_its_bindings():
    from provisa.core.repositories.function import function_from_dict

    stored = function_from_dict(
        {
            **_function(),
            "schema_name": "public",
            "language_bindings": {"cypher": {"name": "apoc.text.join"}},
        }
    )
    assert stored.language_bindings["cypher"].name == "apoc.text.join"


# --- SQL expression functions written in SQL ---------------------------------------------------


def _sql_state(**roles) -> SimpleNamespace:
    fn = {**_function(), "language_bindings": {"sql": {"name": "join_text"}}, "visible_to": []}
    return SimpleNamespace(
        tracked_functions={"text_join": fn},
        roles={rid: {"id": rid, "domain_access": access} for rid, access in roles.items()},
        federation_engine=SimpleNamespace(dialect="duckdb"),
    )


def test_a_sql_statement_calls_a_function_by_its_sql_binding_and_reads_its_template():
    from provisa.compiler.sql_expression_expand import expand

    sql = "SELECT join_text(ARRAY[a, upper(join_text(b, '-'))], ', ') AS j FROM t"
    tree = sqlglot.parse_one(sql, read="postgres")
    assert expand(tree, sql, "seller", _sql_state(seller=["sales"]))
    assert tree.sql(dialect="postgres") == (
        "SELECT (ARRAY_TO_STRING(ARRAY[a, UPPER((ARRAY_TO_STRING(b, '-')) /* provisa:fn text_join */)],"
        " ', ')) /* provisa:fn text_join */ AS j FROM t"
    )


def test_a_statement_naming_no_function_is_not_walked(monkeypatch):
    from provisa.compiler import sql_expression_expand

    tree = sqlglot.parse_one("SELECT a FROM t", read="postgres")
    monkeypatch.setattr(tree, "find_all", lambda *_a: pytest.fail("walked"))
    assert not sql_expression_expand.expand(
        tree, "SELECT a FROM t", "seller", _sql_state(seller=["*"])
    )


def test_a_function_the_role_may_not_use_is_left_as_written():
    from provisa.compiler.sql_expression_expand import expand

    sql = "SELECT join_text(b, '-') FROM t"
    tree = sqlglot.parse_one(sql, read="postgres")
    assert not expand(tree, sql, "outsider", _sql_state(outsider=["hr"]))
    assert "JOIN_TEXT" in tree.sql(dialect="postgres").upper()


def test_the_engine_function_anomalies_page_is_what_the_templates_say():
    import subprocess
    import sys

    checked = subprocess.run(
        [sys.executable, "scripts/gen_engine_function_anomalies.py", "--check"],
        capture_output=True,
    )
    assert checked.returncode == 0, (
        "docs/engines.md's function anomalies are stale: run "
        "scripts/gen_engine_function_anomalies.py"
    )
