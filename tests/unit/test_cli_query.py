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

import pytest

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
