# Copyright (c) 2026 Kenneth Stott
# Canary: b72aec27-8537-4927-9361-1015be813a0f
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""``provisa query`` and ``provisa explain`` against a real HTTP listener: the door each language
goes through, each flag, the output and every exit code (REQ-2222)."""

# Requirements: REQ-2222

from __future__ import annotations

import io
import json
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from types import SimpleNamespace

import pytest
import yaml

from provisa import cli
from provisa.cli_query import EXPECTATION_FAILED, FAILED, OK, REFUSED, USAGE

REFUSAL = (403, {"detail": "no", "code": "data.governance_refused", "params": {}})


class _Instance:
    """A listener answering what a test queues, recording what it was asked."""

    def __init__(self) -> None:
        self.seen: list[dict] = []
        self.answers: list[tuple] = []
        outer = self

        class Handler(BaseHTTPRequestHandler):
            def _answer(self) -> None:
                length = int(self.headers.get("Content-Length") or 0)
                raw = self.rfile.read(length)
                outer.seen.append(
                    {
                        "method": self.command,
                        "path": self.path,
                        "headers": {k.lower(): v for k, v in self.headers.items()},
                        "raw": raw,
                    }
                )
                status, body, *rest = outer.answers.pop(0)
                payload = body if isinstance(body, bytes) else json.dumps(body).encode()
                self.send_response(status)
                for name, value in (rest[0] if rest else {}).items():
                    self.send_header(name, value)
                self.send_header("Content-Length", str(len(payload)))
                self.end_headers()
                self.wfile.write(payload)

            do_GET = do_POST = _answer

            def log_message(self, *args) -> None:
                pass

        self.server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        self.url = f"http://127.0.0.1:{self.server.server_address[1]}"
        threading.Thread(target=self.server.serve_forever, daemon=True).start()


@pytest.fixture
def instance():
    made = _Instance()
    yield made
    made.server.shutdown()
    made.server.server_close()


def _run(instance, command, language, statement, *flags) -> int:
    argv = [command, language, statement, "--role", "analyst", "--api", instance.url, *flags]
    return cli.main(argv)


def _body(seen: dict) -> dict:
    return json.loads(seen["raw"])


class TestQueryDoors:
    """Each language goes through the door a client of it uses."""

    @pytest.mark.parametrize(
        "language, statement, flags, method, path, body",
        [
            ("sql", "SELECT 1", [], "POST", "/data/sql", {"sql": "SELECT 1"}),
            (
                "graphql",
                "{ orders { id } }",
                ["--arguments", '{"a": 1}'],
                "POST",
                "/data/graphql",
                {"query": "{ orders { id } }", "variables": {"a": 1}},
            ),
            (
                "cypher",
                "MATCH (o) RETURN o",
                ["--arguments", '{"a": 1}'],
                "POST",
                "/data/cypher",
                {"query": "MATCH (o) RETURN o", "params": {"a": 1}},
            ),
            (
                "grpc",
                "Sales_Orders",
                ["--arguments", '{"limit": 5}'],
                "POST",
                "/data/grpc/Sales_Orders",
                {"limit": 5},
            ),
        ],
    )
    def test_json_doors(self, instance, capsys, language, statement, flags, method, path, body):
        instance.answers.append((200, {"rows": []}))
        assert _run(instance, "query", language, statement, *flags) == OK
        assert instance.seen[0]["method"] == method
        assert instance.seen[0]["path"] == path
        assert _body(instance.seen[0]) == body

    def test_sparql_uses_the_protocol(self, instance, capsys):
        instance.answers.append((200, {"results": {}}))
        assert _run(instance, "query", "sparql", "ASK {}") == OK
        seen = instance.seen[0]
        assert seen["path"] == "/data/sparql"
        assert seen["raw"] == b"ASK {}"
        assert seen["headers"]["content-type"] == "application/sparql-query"
        assert seen["headers"]["accept"] == "application/sparql-results+json"

    @pytest.mark.parametrize(
        "statement", ["sales/orders?limit=2", "/data/rest/sales/orders?limit=2"]
    )
    def test_rest_is_a_get_of_the_path(self, instance, capsys, statement):
        instance.answers.append((200, []))
        assert _run(instance, "query", "rest", statement) == OK
        assert instance.seen[0]["method"] == "GET"
        assert instance.seen[0]["path"] == "/data/rest/sales/orders?limit=2"

    def test_mcp_initializes_then_calls_the_tool_under_the_role(self, instance, capsys):
        instance.answers += [
            (200, {"result": {}}, {"Mcp-Session-Id": "s1"}),
            (202, b""),
            (
                200,
                b'event: message\ndata: {"result": {"structuredContent": {"rows": [1]}}}\n\n',
            ),
        ]
        code = _run(
            instance,
            "query",
            "mcp",
            "run_sql",
            "--mcp-url",
            instance.url + "/mcp",
            "--arguments",
            '{"sql": "SELECT 1"}',
        )
        assert code == OK
        assert [_body(s).get("method") for s in instance.seen] == [
            "initialize",
            "notifications/initialized",
            "tools/call",
        ]
        call = instance.seen[2]
        assert call["headers"]["mcp-session-id"] == "s1"
        assert _body(call)["params"] == {
            "name": "run_sql",
            "arguments": {"role": "analyst", "sql": "SELECT 1"},
        }
        assert json.loads(capsys.readouterr().out)["result"] == {"rows": [1]}

    def test_an_mcp_tool_error_is_a_refusal(self, instance, capsys):
        instance.answers += [
            (200, {"result": {}}),
            (202, b""),
            (200, {"result": {"isError": True, "content": [{"text": "denied"}]}}),
        ]
        code = _run(instance, "query", "mcp", "run_sql", "--mcp-url", instance.url + "/mcp")
        assert code == REFUSED
        assert json.loads(capsys.readouterr().out)["code"] == "mcp.tool_refused"

    def test_mcp_without_its_url_is_a_usage_error(self, instance, capsys, monkeypatch):
        monkeypatch.delenv("PROVISA_MCP_URL", raising=False)
        assert _run(instance, "query", "mcp", "run_sql") == USAGE
        assert instance.seen == []


class TestFlags:
    def test_role_org_and_token_are_sent(self, instance, capsys):
        instance.answers.append((200, {}))
        assert _run(instance, "query", "sql", "SELECT 1", "--org", "acme", "--token", "t") == OK
        headers = instance.seen[0]["headers"]
        assert headers["x-provisa-role"] == "analyst"
        assert headers["x-org-provisa"] == "acme"
        assert headers["authorization"] == "Bearer t"

    def test_no_org_flag_sends_no_org(self, instance, capsys):
        instance.answers.append((200, {}))
        _run(instance, "query", "sql", "SELECT 1")
        assert "x-org-provisa" not in instance.seen[0]["headers"]

    def test_the_role_is_required(self, instance, capsys):
        with pytest.raises(SystemExit) as raised:
            cli.main(["query", "sql", "SELECT 1", "--api", instance.url])
        assert raised.value.code == USAGE

    def test_a_person_is_refused_until_entitlements_apply(self, instance, capsys):
        """A run naming a person and answered as the bare role would be read as that person's."""
        assert _run(instance, "query", "sql", "SELECT 1", "--person", "ann") == USAGE
        assert "acting as a person is not available yet" in capsys.readouterr().err
        assert instance.seen == []

    def test_the_statement_comes_from_a_file(self, instance, capsys, tmp_path):
        path = tmp_path / "q.sql"
        path.write_text("SELECT 2")
        instance.answers.append((200, {}))
        code = cli.main(
            ["query", "sql", "--file", str(path), "--role", "analyst", "--api", instance.url]
        )
        assert code == OK
        assert _body(instance.seen[0]) == {"sql": "SELECT 2"}

    def test_the_statement_comes_from_stdin(self, instance, capsys, monkeypatch):
        monkeypatch.setattr("sys.stdin", io.StringIO("SELECT 3"))
        instance.answers.append((200, {}))
        assert _run(instance, "query", "sql", "-") == OK
        assert _body(instance.seen[0]) == {"sql": "SELECT 3"}

    def test_no_statement_is_a_usage_error(self, instance, capsys):
        code = cli.main(["query", "sql", "--role", "analyst", "--api", instance.url])
        assert code == USAGE

    @pytest.mark.parametrize("arguments", ["not json", "[1]"])
    def test_arguments_must_be_a_json_object(self, instance, capsys, arguments):
        assert _run(instance, "query", "sql", "SELECT 1", "--arguments", arguments) == USAGE
        assert instance.seen == []

    def test_there_is_no_interactive_mode(self, instance, capsys):
        with pytest.raises(SystemExit) as raised:
            cli.main(["query", "--role", "analyst", "--api", instance.url])
        assert raised.value.code == USAGE


class TestOutputAndExitCodes:
    def test_an_answer_is_one_sorted_document_and_exit_0(self, instance, capsys):
        instance.answers.append((200, {"z": 1, "a": 2}))
        assert _run(instance, "query", "sql", "SELECT 1") == OK
        out = capsys.readouterr().out
        assert out == '{"result": {"a": 2, "z": 1}, "status": "ok"}\n'

    def test_a_refused_role_exits_1_and_prints_its_code(self, instance, capsys):
        instance.answers.append(REFUSAL)
        assert _run(instance, "query", "sql", "SELECT secret FROM t") == REFUSED
        captured = capsys.readouterr()
        assert json.loads(captured.out) == {
            "code": "data.governance_refused",
            "detail": "no",
            "http_status": 403,
            "status": "refused",
        }
        assert "data.governance_refused" in captured.err

    def test_a_refusal_with_no_code_is_named_by_its_status(self, instance, capsys):
        instance.answers.append((403, {"detail": "no"}))
        assert _run(instance, "query", "sql", "SELECT 1") == REFUSED
        assert json.loads(capsys.readouterr().out)["code"] == "http.403"

    def test_graphql_errors_in_a_200_are_a_refusal(self, instance, capsys):
        error = {"message": "no field", "extensions": {"code": "GRAPHQL_VALIDATION_FAILED"}}
        instance.answers.append((200, {"errors": [error]}))
        assert _run(instance, "query", "graphql", "{ x }") == REFUSED
        assert json.loads(capsys.readouterr().out)["code"] == "GRAPHQL_VALIDATION_FAILED"

    def test_the_expected_refusal_exits_0(self, instance, capsys):
        instance.answers.append(REFUSAL)
        flags = ["--expect-refused", "data.governance_refused"]
        assert _run(instance, "query", "sql", "SELECT 1", *flags) == OK

    def test_a_refusal_under_another_code_fails_the_expectation(self, instance, capsys):
        instance.answers.append(REFUSAL)
        flags = ["--expect-refused", "cypher.refused"]
        assert _run(instance, "query", "sql", "SELECT 1", *flags) == EXPECTATION_FAILED

    def test_an_answer_fails_the_expectation(self, instance, capsys):
        instance.answers.append((200, {}))
        flags = ["--expect-refused", "data.governance_refused"]
        assert _run(instance, "query", "sql", "SELECT 1", *flags) == EXPECTATION_FAILED

    def test_expect_refused_takes_its_code(self, instance, capsys):
        with pytest.raises(SystemExit) as raised:
            cli.main(
                ["query", "sql", "S", "--role", "r", "--api", instance.url, "--expect-refused"]
            )
        assert raised.value.code == USAGE

    def test_a_server_error_exits_4_even_when_a_refusal_was_expected(self, instance, capsys):
        instance.answers.append((500, {"detail": "boom"}))
        flags = ["--expect-refused", "data.governance_refused"]
        assert _run(instance, "query", "sql", "SELECT 1", *flags) == FAILED
        assert json.loads(capsys.readouterr().out)["status"] == "failed"

    def test_an_unreachable_instance_exits_4(self, capsys):
        code = cli.main(["query", "sql", "SELECT 1", "--role", "r", "--api", "http://127.0.0.1:1"])
        assert code == FAILED
        assert json.loads(capsys.readouterr().out)["code"] == "client.unreachable"


class TestExplain:
    @pytest.mark.parametrize(
        "language, statement",
        [
            ("sql", "SELECT 1"),
            ("graphql", "{ a }"),
            ("cypher", "MATCH (n) RETURN n"),
            ("sparql", "ASK {}"),
            ("grpc", "Sales_Orders"),
            ("rest", "sales/orders"),
            ("mcp", "run_sql"),
        ],
    )
    def test_every_language_posts_to_the_one_endpoint(self, instance, capsys, language, statement):
        answer = {"language": language, "statements": [{"route": "DIRECT"}]}
        instance.answers.append((200, answer))
        assert _run(instance, "explain", language, statement) == OK
        assert instance.seen[0]["method"] == "POST"
        assert instance.seen[0]["path"] == "/data/explain"
        assert _body(instance.seen[0]) == {
            "language": language,
            "statement": statement,
            "arguments": {},
            "analyze": False,
        }
        assert json.loads(capsys.readouterr().out)["result"] == answer

    def test_analyze_and_the_flags_are_sent(self, instance, capsys):
        instance.answers.append((200, {"statements": []}))
        code = _run(
            instance,
            "explain",
            "cypher",
            "MATCH (n) RETURN n",
            "--analyze",
            "--org",
            "acme",
            "--arguments",
            '{"a": 1}',
        )
        assert code == OK
        seen = instance.seen[0]
        assert _body(seen)["analyze"] is True
        assert _body(seen)["arguments"] == {"a": 1}
        assert seen["headers"]["x-provisa-role"] == "analyst"
        assert seen["headers"]["x-org-provisa"] == "acme"

    def test_a_refused_role_exits_1_with_its_code(self, instance, capsys):
        instance.answers.append(REFUSAL)
        assert _run(instance, "explain", "sql", "SELECT 1") == REFUSED
        assert "data.governance_refused" in capsys.readouterr().err

    def test_an_expected_refusal_exits_0(self, instance, capsys):
        instance.answers.append(REFUSAL)
        flags = ["--expect-refused", "data.governance_refused"]
        assert _run(instance, "explain", "sql", "SELECT 1", *flags) == OK


_ORDERS = "pg/public/orders"
_CUSTOMERS = "pg/public/customers"
_BIG_ORDERS = "__derived__/views/big_orders"
_TOP_ORDERS = "__derived__/views/top_orders"


def _table(name: str, **extra) -> dict:
    return {
        "source_id": "pg",
        "domain_id": "sales",
        "schema": "public",
        "table": name,
        "columns": [{"name": "id", "visible_to": ["analyst"]}],
        **extra,
    }


def _view(name: str, sql: str) -> dict:
    return _table(name, source_id="__derived__", schema="views", view_sql=sql)


def _model(*tables: dict) -> dict:
    return {
        "sources": [
            {
                "id": "pg",
                "type": "postgresql",
                "host": "db",
                "port": 5432,
                "database": "d",
                "username": "u",
                "password": "${env:J1147_UNSET}",
            }
        ],
        "domains": [{"id": "sales"}],
        "tables": list(tables),
        "roles": [
            {"id": "analyst", "description": "r", "capabilities": [], "domain_access": ["sales"]}
        ],
    }


_BASE = (
    _table("orders"),
    _table("customers"),
    _view("big_orders", "SELECT * FROM sales.orders"),
    _view("top_orders", "WITH t AS (SELECT * FROM big_orders) SELECT * FROM t"),
)
_ORDERS_CHANGED = (_table("orders", description="changed"), *_BASE[1:])


def _explained(*addresses: str) -> tuple:
    tables = [{"address": a, "domain": "sales", "table": a.rsplit("/")[-1]} for a in addresses]
    return (200, {"language": "sql", "statements": [{"route": "DIRECT", "tables": tables}]})


class TestChanged:
    @pytest.fixture
    def tree(self, tmp_path):
        statements = tmp_path / "statements"
        statements.mkdir()
        state = tmp_path / "state.yaml"
        state.write_text(yaml.safe_dump(_model(*_BASE)))

        def run(instance, *flags) -> int:
            argv = ["query", "--changed", "--state", str(state), "--dir", str(statements)]
            argv += ["--role", "analyst", "--org", "acme", "--api", instance.url, *flags]
            return cli.main(argv)

        return SimpleNamespace(statements=statements, state=state, run=run)

    @staticmethod
    def _now(instance, *tables: dict) -> None:
        instance.answers.append((200, yaml.safe_dump(_model(*tables)).encode()))

    @staticmethod
    def _document(capsys) -> dict:
        return json.loads(capsys.readouterr().out)

    def test_only_statements_reading_a_changed_table_or_a_dependent_run(
        self, instance, tree, capsys
    ):
        (tree.statements / "a_orders.sql").write_text("SELECT id FROM sales.orders")
        (tree.statements / "b_big.sql").write_text("SELECT id FROM sales.big_orders")
        (tree.statements / "c_top.sql").write_text("SELECT id FROM sales.top_orders")
        (tree.statements / "d_customers.sql").write_text("SELECT id FROM sales.customers")
        self._now(instance, *_ORDERS_CHANGED)
        instance.answers += [
            _explained(_ORDERS),
            (200, {"rows": ["a"]}),
            _explained(_BIG_ORDERS),
            (200, {"rows": ["b"]}),
            _explained(_TOP_ORDERS),
            (200, {"rows": ["c"]}),
            _explained(_CUSTOMERS),
        ]

        assert tree.run(instance) == OK

        document = self._document(capsys)
        assert document["status"] == "ok"
        assert document["changed"] == [_ORDERS]
        assert document["affected"] == [_BIG_ORDERS, _TOP_ORDERS, _ORDERS]
        outcomes = {s["file"]: s["outcome"] for s in document["statements"]}
        assert outcomes == {
            "a_orders.sql": "ok",
            "b_big.sql": "ok",
            "c_top.sql": "ok",
            "d_customers.sql": "skipped",
        }
        assert document["statements"][0]["result"] == {"rows": ["a"]}
        ran = [json.loads(s["raw"])["sql"] for s in instance.seen if s["path"] == "/data/sql"]
        assert ran == [
            "SELECT id FROM sales.orders",
            "SELECT id FROM sales.big_orders",
            "SELECT id FROM sales.top_orders",
        ]

    def test_the_export_is_read_for_the_org_and_not_as_the_statements_role(self, instance, tree):
        self._now(instance, *_BASE)
        assert tree.run(instance) == OK
        export = instance.seen[0]
        assert export["path"] == "/admin/orgs/acme/config-export"
        assert export["headers"]["x-org-provisa"] == "acme"
        assert "x-provisa-role" not in export["headers"]

    def test_explain_is_asked_as_the_statements_role(self, instance, tree):
        (tree.statements / "a.sql").write_text("SELECT 1")
        self._now(instance, *_BASE)
        instance.answers.append(_explained(_CUSTOMERS))
        assert tree.run(instance) == OK
        asked = instance.seen[1]
        assert asked["path"] == "/data/explain"
        assert asked["headers"]["x-provisa-role"] == "analyst"
        assert _body(asked) == {
            "language": "sql",
            "statement": "SELECT 1",
            "arguments": {},
            "analyze": False,
        }

    def test_nothing_changed_runs_nothing(self, instance, tree, capsys):
        (tree.statements / "a.sql").write_text("SELECT id FROM sales.orders")
        self._now(instance, *_BASE)
        instance.answers.append(_explained(_ORDERS))
        assert tree.run(instance) == OK
        document = self._document(capsys)
        assert document["changed"] == []
        assert document["statements"][0]["outcome"] == "skipped"
        assert [s["path"] for s in instance.seen].count("/data/sql") == 0

    def test_an_edited_view_selects_itself_and_what_reads_it_not_what_it_reads(
        self, instance, tree, capsys
    ):
        (tree.statements / "a_orders.sql").write_text("SELECT id FROM sales.orders")
        (tree.statements / "b_top.sql").write_text("SELECT id FROM sales.top_orders")
        edited = _view("big_orders", "SELECT id FROM sales.orders WHERE id > 1")
        self._now(instance, _BASE[0], _BASE[1], edited, _BASE[3])
        instance.answers += [_explained(_ORDERS), _explained(_TOP_ORDERS), (200, {"rows": []})]
        assert tree.run(instance) == OK
        document = self._document(capsys)
        assert document["changed"] == [_BIG_ORDERS]
        assert document["affected"] == [_BIG_ORDERS, _TOP_ORDERS]
        assert [s["outcome"] for s in document["statements"]] == ["skipped", "ok"]

    def test_a_new_table_counts_as_changed(self, instance, tree, capsys):
        (tree.statements / "a.sql").write_text("SELECT id FROM sales.returns")
        self._now(instance, *_BASE, _table("returns"))
        instance.answers += [_explained("pg/public/returns"), (200, {"rows": []})]
        assert tree.run(instance) == OK
        assert self._document(capsys)["changed"] == ["pg/public/returns"]

    def test_a_removed_table_changes_what_read_it_and_explain_refuses_those(
        self, instance, tree, capsys
    ):
        (tree.statements / "a.sql").write_text("SELECT id FROM sales.customers")
        self._now(instance, _BASE[0], _BASE[2], _BASE[3])
        instance.answers.append(REFUSAL)
        assert tree.run(instance) == REFUSED
        document = self._document(capsys)
        assert document["changed"] == [_CUSTOMERS]
        assert document["status"] == "refused"
        statement = document["statements"][0]
        assert statement["outcome"] == "refused"
        assert statement["code"] == "data.governance_refused"
        assert [s["path"] for s in instance.seen].count("/data/sql") == 0

    def test_a_metric_composed_view_depends_on_the_tables_its_metrics_read(
        self, instance, tree, capsys
    ):
        metric_view = _view("order_totals", "SELECT value FROM metrics.total_orders")
        metrics = [{"name": "total_orders", "expression": "SUM(orders.id)"}]
        then, now = _model(*_BASE, metric_view), _model(*_ORDERS_CHANGED, metric_view)
        tree.state.write_text(yaml.safe_dump({**then, "metrics": metrics}))
        instance.answers.append((200, yaml.safe_dump({**now, "metrics": metrics}).encode()))
        assert tree.run(instance) == OK
        assert "__derived__/views/order_totals" in self._document(capsys)["affected"]

    def test_a_refused_run_and_a_failed_run_set_the_exit(self, instance, tree, capsys):
        (tree.statements / "a.sql").write_text("SELECT id FROM sales.orders")
        (tree.statements / "b.sql").write_text("SELECT id FROM sales.big_orders")
        self._now(instance, *_ORDERS_CHANGED)
        instance.answers += [
            _explained(_ORDERS),
            REFUSAL,
            _explained(_BIG_ORDERS),
            (500, {"detail": "boom"}),
        ]
        assert tree.run(instance) == FAILED
        document = self._document(capsys)
        assert document["status"] == "failed"
        assert [s["outcome"] for s in document["statements"]] == ["refused", "failed"]

    @pytest.mark.parametrize(
        "name, text, language, statement, arguments",
        [
            ("a.graphql", "{ a }", "graphql", "{ a }", {}),
            ("a.gql", "{ a }", "graphql", "{ a }", {}),
            ("a.cypher", "MATCH (n) RETURN n", "cypher", "MATCH (n) RETURN n", {}),
            ("a.cql", "MATCH (n) RETURN n", "cypher", "MATCH (n) RETURN n", {}),
            ("a.sparql", "ASK {}", "sparql", "ASK {}", {}),
            ("a.rq", "ASK {}", "sparql", "ASK {}", {}),
            ("a.rest", "sales/orders?limit=1\n", "rest", "sales/orders?limit=1", {}),
            (
                "a.grpc",
                '{"statement": "Sales_Orders", "arguments": {"limit": 1}}',
                "grpc",
                "Sales_Orders",
                {"limit": 1},
            ),
            (
                "a.mcp",
                '{"statement": "run_sql", "arguments": {"sql": "SELECT 1"}}',
                "mcp",
                "run_sql",
                {"sql": "SELECT 1"},
            ),
        ],
    )
    def test_a_files_extension_names_its_language(
        self, instance, tree, name, text, language, statement, arguments
    ):
        (tree.statements / name).write_text(text)
        self._now(instance, *_BASE)
        instance.answers.append(_explained(_CUSTOMERS))
        assert tree.run(instance) == OK
        assert _body(instance.seen[1]) == {
            "language": language,
            "statement": statement,
            "arguments": arguments,
            "analyze": False,
        }

    def test_files_are_found_below_the_directory_and_others_are_left(self, instance, tree, capsys):
        nested = tree.statements / "reports"
        nested.mkdir()
        (nested / "a.sql").write_text("SELECT 1")
        (tree.statements / "README.md").write_text("not a statement")
        self._now(instance, *_BASE)
        instance.answers.append(_explained(_CUSTOMERS))
        assert tree.run(instance) == OK
        files = [s["file"] for s in self._document(capsys)["statements"]]
        assert files == [str(Path("reports") / "a.sql")]

    def test_a_view_that_will_not_parse_is_refused(self, instance, tree, capsys):
        self._now(instance, *_BASE, _view("broken", "SELECT FROM WHERE ((("))
        assert tree.run(instance) == REFUSED
        document = self._document(capsys)
        assert document["code"] == "config.instance_invalid"
        assert [s["path"] for s in instance.seen].count("/data/explain") == 0

    def test_a_state_file_that_cannot_be_read_is_refused(self, instance, tree, capsys):
        tree.state.unlink()
        assert tree.run(instance) == REFUSED
        assert self._document(capsys)["code"] == "config.unreadable"
        assert instance.seen == []

    def test_a_refused_export_is_reported(self, instance, tree, capsys):
        instance.answers.append(REFUSAL)
        assert tree.run(instance) == REFUSED
        assert self._document(capsys)["code"] == "data.governance_refused"

    def test_a_malformed_statement_file_is_a_usage_error_before_anything_runs(
        self, instance, tree, capsys
    ):
        (tree.statements / "a.grpc").write_text("not json")
        self._now(instance, *_BASE)
        assert tree.run(instance) == USAGE
        assert "a.grpc" in capsys.readouterr().err
        assert [s["path"] for s in instance.seen].count("/data/explain") == 0

    @pytest.mark.parametrize(
        "flags",
        [
            ["query", "--changed", "--dir", "d", "--org", "a"],
            ["query", "--changed", "--state", "s", "--org", "a"],
            ["query", "--changed", "--state", "s", "--dir", "d"],
            ["query", "sql", "SELECT 1", "--changed", "--state", "s", "--dir", "d", "--org", "a"],
            ["query", "--changed", "--state", "s", "--dir", "d", "--org", "a", "--file", "f"],
            [
                "query",
                "--changed",
                "--state",
                "s",
                "--dir",
                "d",
                "--org",
                "a",
                "--expect-refused",
                "c",
            ],
            ["query", "sql", "SELECT 1", "--state", "s"],
            ["query", "sql", "SELECT 1", "--dir", "d"],
        ],
    )
    def test_a_wrong_command_line_exits_2(self, instance, capsys, flags):
        assert cli.main([*flags, "--role", "analyst", "--api", instance.url]) == USAGE
        assert instance.seen == []

    def test_a_directory_that_is_not_one_exits_2(self, instance, tree, capsys):
        missing = str(tree.statements / "missing")
        argv = ["query", "--changed", "--state", str(tree.state), "--dir", missing]
        argv += ["--role", "analyst", "--org", "acme", "--api", instance.url]
        assert cli.main(argv) == USAGE
        assert instance.seen == []

    def test_an_unreachable_instance_exits_4(self, tree, capsys):
        argv = ["query", "--changed", "--state", str(tree.state), "--dir", str(tree.statements)]
        argv += ["--role", "analyst", "--org", "acme", "--api", "http://127.0.0.1:9", "--timeout"]
        assert cli.main([*argv, "2"]) == FAILED
        assert self._document(capsys)["code"] == "client.unreachable"
