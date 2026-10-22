# Copyright (c) 2026 Kenneth Stott
# Canary: 11cf66b5-6b2f-4511-a079-c6897c432941
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.
"""REQ-2016: a named demo's setup.yaml runs once per control plane, in order, through the API; the
first step that fails stops it, naming the step. A demo's demo.yaml names the optional groups it
needs installed, and start-ui-install.sh installs them for that demo alone."""

# Requirements: REQ-2016

from __future__ import annotations

import importlib.util
import json
from pathlib import Path

import httpx
import pytest
import yaml

_REPO = Path(__file__).resolve().parents[2]
_LAUNCHER = (_REPO / "start-ui-install.sh").read_text()


def _script(name: str):
    spec = importlib.util.spec_from_file_location(name, _REPO / "scripts" / f"{name}.py")
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


setup = _script("run_demo_setup")
extras = _script("demo_extras")


class _Server:
    """The API calls a setup makes, answered as a server answers them, and recorded."""

    def __init__(self, done: list[str], fail: str | None = None) -> None:
        self.done = done
        self.fail = fail
        self.calls: list[str] = []
        self.stored: list | None = None

    def __call__(self, request: httpx.Request) -> httpx.Response:
        path = request.url.path
        if path == "/admin/settings/catalog" and request.method == "GET":
            setting = {"key": setup.SETTING, "value": self.done}
            return httpx.Response(200, json={"cards": [{"id": "bootstrap", "settings": [setting]}]})
        if path == "/admin/settings/catalog":
            self.stored = json.loads(request.content)["values"][setup.SETTING]
            return httpx.Response(200, json={"updated": [setup.SETTING]})
        if path.startswith("/admin/profilers/"):
            self.calls.append(path)
            error = "it broke" if self.fail is not None and self.fail in path else None
            return httpx.Response(
                200,
                json=[
                    {"table": "customers", "run_id": "r1", "error": None},
                    {"table": "orders", "run_id": None if error else "r2", "error": error},
                ],
            )
        query = json.loads(request.content)["query"]
        if "__schema" in query:
            fields = [
                {"name": "sonnets__embedLines", "type": {"kind": "SCALAR", "name": "JSON"}},
                {"name": "sonnets__linkEchoes", "type": {"kind": "SCALAR", "name": "JSON"}},
            ]
            return httpx.Response(
                200, json={"data": {"__schema": {"mutationType": {"fields": fields}}}}
            )
        self.calls.append(query)
        if self.fail is not None and self.fail in query:
            return httpx.Response(200, json={"errors": [{"message": "it broke"}]})
        if "refreshMv" in query:
            return httpx.Response(
                200, json={"data": {"refreshMv": {"success": True, "message": "refreshed"}}}
            )
        field = query.split("{", 1)[1].split()[0]
        return httpx.Response(200, json={"data": {field: [{"rows_affected": 3}]}})


@pytest.fixture
def demo(tmp_path: Path) -> Path:
    home = tmp_path / "sonnets"
    home.mkdir()
    (home / "setup.yaml").write_text(
        yaml.safe_dump(
            {
                "steps": [
                    {"command": "embed_lines"},
                    {"command": "link_echoes"},
                    {"refresh": "theme_time"},
                ]
            }
        )
    )
    return home


def _run(monkeypatch, demo: Path, server: _Server) -> int:
    real = httpx.Client

    def _client(**kw):
        return real(transport=httpx.MockTransport(server), **kw)

    monkeypatch.setattr(setup.httpx, "Client", _client)
    return setup.run(demo, "http://provisa.test")


def test_the_steps_run_in_order_and_the_demo_is_marked_set_up(monkeypatch, demo):
    server = _Server(done=["weblogs"])
    assert _run(monkeypatch, demo, server) == 0
    assert [c.split("{", 2)[1].split()[0] for c in server.calls] == [
        "sonnets__embedLines",
        "sonnets__linkEchoes",
        "refreshMv(mvId:",
    ]
    assert 'refreshMv(mvId: "view-view_theme_time")' in server.calls[2]
    assert server.stored == ["weblogs", "sonnets"]


def test_a_demo_already_set_up_runs_nothing(monkeypatch, demo):
    server = _Server(done=["sonnets"])
    assert _run(monkeypatch, demo, server) == 0
    assert server.calls == [] and server.stored is None


def test_the_first_step_that_fails_stops_the_run_naming_it(monkeypatch, demo, capsys):
    server = _Server(done=[], fail="linkEchoes")
    assert _run(monkeypatch, demo, server) == 1
    assert len(server.calls) == 2  # the view is never built
    assert server.stored is None  # and the demo is not marked set up
    assert "{'command': 'link_echoes'}" in capsys.readouterr().err


def test_a_step_that_is_neither_a_command_nor_a_refresh_is_refused(tmp_path):
    (tmp_path / "setup.yaml").write_text(yaml.safe_dump({"steps": [{"sql": "SELECT 1"}]}))
    with pytest.raises(SystemExit, match="'profile: <profiler>', 'environment.* or 'client"):
        setup.steps(tmp_path)


def _client_demo(tmp_path: Path, script: str) -> Path:
    (tmp_path / "setup.yaml").write_text(yaml.safe_dump({"steps": [{"client": "pulls.py"}]}))
    (tmp_path / "pulls.py").write_text(script)
    return tmp_path


def test_a_client_step_runs_the_demo_s_script_told_where_the_server_is(monkeypatch, tmp_path):
    """REQ-2067: the provisioning demo's setup pulls over Arrow Flight from the server it is set
    up on, and what the client writes to the demo's stores is theirs."""
    out = tmp_path / "told.txt"
    demo = _client_demo(
        tmp_path,
        "import os, pathlib\n"
        f"pathlib.Path({str(out)!r}).write_text("
        "os.environ['PROVISA_URL'] + ' ' + os.environ['PROVISA_FLIGHT'])\n",
    )
    server = _Server(done=[])
    real = httpx.Client
    monkeypatch.setattr(
        setup.httpx, "Client", lambda **kw: real(transport=httpx.MockTransport(server), **kw)
    )
    assert setup.run(demo, "http://provisa.test", flight="grpc://127.0.0.1:8815") == 0
    assert out.read_text() == "http://provisa.test grpc://127.0.0.1:8815"
    assert server.stored == [tmp_path.resolve().name]


def test_a_client_that_fails_fails_its_step_naming_it(monkeypatch, tmp_path, capsys):
    demo = _client_demo(tmp_path, "raise SystemExit(3)\n")
    server = _Server(done=[])
    real = httpx.Client
    monkeypatch.setattr(
        setup.httpx, "Client", lambda **kw: real(transport=httpx.MockTransport(server), **kw)
    )
    assert setup.run(demo, "http://provisa.test", flight="grpc://127.0.0.1:8815") == 1
    assert "pulls.py exited 3" in capsys.readouterr().err
    assert server.stored is None


def test_a_client_step_needs_the_servers_flight_address(monkeypatch, tmp_path, capsys):
    demo = _client_demo(tmp_path, "")
    server = _Server(done=[])
    real = httpx.Client
    monkeypatch.setattr(
        setup.httpx, "Client", lambda **kw: real(transport=httpx.MockTransport(server), **kw)
    )
    assert setup.run(demo, "http://provisa.test") == 1
    assert "--flight" in capsys.readouterr().err


def test_a_client_step_names_a_script_of_the_demo(tmp_path):
    (tmp_path / "setup.yaml").write_text(yaml.safe_dump({"steps": [{"client": "gone.py"}]}))
    with pytest.raises(SystemExit, match="a client step names a script of the demo's"):
        setup.steps(tmp_path)


def test_the_launcher_tells_the_setup_where_its_flight_is():
    set_up = _LAUNCHER[_LAUNCHER.index("scripts/run_demo_setup.py") :][:400]
    assert '--flight "grpc://localhost:${FLIGHT_PORT:-8815}"' in set_up


def test_an_environment_step_creates_it_in_the_active_org(monkeypatch, tmp_path):
    """REQ-2053: a demo's environments are made as the Environments page makes them."""
    home = tmp_path / "testdata"
    home.mkdir()
    (home / "setup.yaml").write_text(
        yaml.safe_dump({"steps": [{"environment": {"name": "dev", "data_mode": "test_fake"}}]})
    )
    made: list[tuple[str, dict]] = []

    class _EnvServer(_Server):
        def __call__(self, request: httpx.Request) -> httpx.Response:
            if request.url.path == "/auth/me":
                return httpx.Response(200, json={"active_org_id": "acme"})
            if request.url.path.endswith("/environments"):
                body = json.loads(request.content)
                made.append((request.url.path, body))
                return httpx.Response(200, json={"environment": body})
            return super().__call__(request)

    server = _EnvServer(done=[])
    assert _run(monkeypatch, home, server) == 0
    assert made == [("/admin/orgs/acme/environments", {"name": "dev", "data_mode": "test_fake"})]
    assert server.stored == ["testdata"]


def test_an_environment_step_names_its_name_and_data_mode(tmp_path):
    (tmp_path / "setup.yaml").write_text(
        yaml.safe_dump({"steps": [{"environment": {"name": "dev"}}]})
    )
    with pytest.raises(SystemExit, match="names its name and data_mode"):
        setup.steps(tmp_path)


def test_environments_only_runs_the_environment_steps_alone(monkeypatch, demo):
    (demo / "setup.yaml").write_text(
        yaml.safe_dump(
            {
                "steps": [
                    {"command": "embed_lines"},
                    {"environment": {"name": "dev", "data_mode": "test_fake"}},
                ]
            }
        )
    )
    made: list[str] = []

    class _EnvServer(_Server):
        def __call__(self, request: httpx.Request) -> httpx.Response:
            if request.url.path == "/auth/me":
                return httpx.Response(200, json={"active_org_id": "acme"})
            if request.url.path.endswith("/environments"):
                made.append(json.loads(request.content)["name"])
                return httpx.Response(200, json={"environment": json.loads(request.content)})
            return super().__call__(request)

    server = _EnvServer(done=[])
    real = httpx.Client
    monkeypatch.setattr(
        setup.httpx, "Client", lambda **kw: real(transport=httpx.MockTransport(server), **kw)
    )
    assert setup.run(demo, "http://provisa.test", environments_only=True) == 0
    assert made == ["dev"] and server.calls == []  # the command ran at the bundle build
    assert server.stored == ["sonnets"]


def test_a_demo_with_no_setup_has_nothing_to_run(tmp_path):
    assert setup.steps(tmp_path) == []


def test_the_sonnets_setup_names_its_own_commands_and_views():
    home = _REPO / "demo" / "named" / "sonnets"
    config = yaml.safe_load((home / "config.yaml").read_text())
    commands = {f["name"] for f in config["functions"]}
    stored = {v["id"] for v in config["views"] if v.get("materialize")}
    for step in setup.steps(home):
        if "command" in step:
            assert step["command"] in commands, step
        else:
            assert step["refresh"] in stored, step


def test_a_demo_s_extras_are_the_packages_of_the_groups_it_names(tmp_path):
    assert extras.requirements(_REPO / "demo" / "named" / "sonnets") == ["sentence-transformers"]
    assert extras.requirements(_REPO / "demo" / "named" / "weblogs") == []
    (tmp_path / "demo.yaml").write_text("extras: [nosuch]\n")
    with pytest.raises(SystemExit, match="'nosuch'"):
        extras.requirements(tmp_path)


def test_the_launcher_installs_a_demo_s_extras_before_the_server_and_sets_it_up_after():
    install = _LAUNCHER.index("scripts/demo_extras.py")
    started = _LAUNCHER.index('"$SCRIPT_DIR/.venv/bin/uvicorn" main:app')
    healthy = _LAUNCHER.index('echo "Backend healthy (PID $BACKEND_PID)"')
    set_up = _LAUNCHER.index("scripts/run_demo_setup.py")
    assert install < started < healthy < set_up


def _sonnets_search() -> dict:
    """The sonnets demo's lines_like search as registered: its columns under their column
    names, its collection without the per-run namespace."""
    config = yaml.safe_load((_REPO / "demo" / "named" / "sonnets" / "config.yaml").read_text())
    [search] = [t for t in config["tables"] if t["table"] == "lines_like"]
    return {
        "table_name": search["table"],
        "query_template": search["query_template"].replace("${env:SONNETS_QDRANT_NS}", ""),
        "columns": [
            {
                "native_filter_type": None,
                "native_filter_required": False,
                **c,
                "column_name": c["name"],
            }
            for c in search["columns"]
        ],
    }


async def _planned(monkeypatch, given: dict) -> dict:
    from provisa.qdrant import table as qdrant_table

    async def embedded(_state, _table, _column, value, model):
        assert model == "sentence-transformers/all-MiniLM-L6-v2"
        return [float(len(value))]

    monkeypatch.setattr(qdrant_table, "_vector", embedded)
    return await qdrant_table.plan(None, _sonnets_search(), given)


@pytest.mark.asyncio
async def test_link_echoes_asks_for_each_line_s_nearest_lines_in_other_sonnets(monkeypatch):
    """link_echoes's join feeds lines_like a line's words and sonnet, and takes 3 (REQ-2099)."""
    request = await _planned(
        monkeypatch,
        {"query": "Then thou alone kingdoms of hearts shouldst owe.", "sonnet": 36, "k": 3},
    )
    assert (request["collection"], request["using"], request["limit"]) == (
        "sonnet_lines",
        "text",
        3,
    )
    assert request["query"] == [48.0]
    assert request["filter"] == {"must_not": [{"key": "sonnet_number", "match": {"value": 36}}]}


@pytest.mark.asyncio
async def test_link_echoes_reads_the_points_embed_lines_wrote_and_a_theme_leaves_no_sonnet_out(
    monkeypatch,
):
    """The points link_echoes searches are the ones embed_lines writes before it; a theme's search,
    which gives no sonnet, leaves none out."""
    steps = yaml.safe_load((_REPO / "demo" / "named" / "sonnets" / "setup.yaml").read_text())[
        "steps"
    ]
    assert steps.index({"command": "embed_lines"}) < steps.index({"command": "link_echoes"})
    request = await _planned(monkeypatch, {"query": "the ravages of time", "k": 20})
    assert request["collection"] == "sonnet_lines"
    assert (request["filter"], request["limit"]) == (None, 20)


def test_a_profile_step_runs_the_profiler_for_every_member_table(monkeypatch, tmp_path):
    """REQ-2052: a demo's profiler is run at its template build, so its sandboxes carry the runs."""
    home = tmp_path / "profiling"
    home.mkdir()
    (home / "setup.yaml").write_text(yaml.safe_dump({"steps": [{"profile": "harbor-profiler"}]}))
    server = _Server(done=[])
    assert _run(monkeypatch, home, server) == 0
    assert server.calls == ["/admin/profilers/harbor-profiler/run"]
    assert server.stored == ["profiling"]


def test_a_profile_run_that_fails_for_a_table_fails_the_step_naming_it(
    monkeypatch, tmp_path, capsys
):
    home = tmp_path / "profiling"
    home.mkdir()
    (home / "setup.yaml").write_text(yaml.safe_dump({"steps": [{"profile": "harbor-profiler"}]}))
    server = _Server(done=[], fail="harbor-profiler")
    assert _run(monkeypatch, home, server) == 1
    assert server.stored is None
    assert "orders: it broke" in capsys.readouterr().err
