# Copyright (c) 2026 Kenneth Stott
# Canary: 9b52bae0-824d-4ee2-96c0-0adc1ef9bdff
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""REQ-2112: the VS Code extension for Node and Python code sources (provisa-vscode/).

Each test drives the extension's own modules on the machine's node, one per rule of the
requirement: what Provisa would serve, read by Provisa's own ``code check``; dev mode on a chosen
instance; the forms marked inline; Build image through the UI's routes; no model editing and no
query editor; and the docs page for the existing query extensions.
"""

# Requirements: REQ-2112
from __future__ import annotations

import json
import shutil
import subprocess
import sys
from pathlib import Path
from typing import Any

import pytest
import yaml

ROOT = Path(__file__).resolve().parents[2]
EXT = ROOT / "provisa-vscode"
PACKAGE = json.loads((EXT / "package.json").read_text()) if (EXT / "package.json").is_file() else {}


def _node(script: str, *args: str) -> Any:
    """Runs ``script`` (CommonJS, the extension's ``src`` as ``SRC``) and returns what it printed
    as JSON."""
    node = shutil.which("node")
    assert node is not None, "the extension's tests run its modules on the machine's node"
    prelude = f"const SRC = {json.dumps(str(EXT / 'src'))};\n"
    done = subprocess.run(  # noqa: S603 -- the machine's node, a fixed script
        [node, "-e", prelude + script, *args],
        capture_output=True,
        text=True,
        timeout=180,
    )
    assert done.returncode == 0, done.stderr
    return json.loads(done.stdout)


# -- A Visual Studio Code extension for Node and Python code sources, shipped as a .vsix ---------


def test_the_package_is_a_vs_code_extension_for_python_and_node_packaged_as_a_vsix():
    assert PACKAGE["engines"]["vscode"].startswith("^")
    assert PACKAGE["publisher"] and PACKAGE["name"] and PACKAGE["displayName"]
    assert PACKAGE["repository"]["url"].startswith("https://")
    assert (EXT / PACKAGE["main"]).is_file()
    assert {"onLanguage:python", "onLanguage:typescript"} <= set(PACKAGE["activationEvents"])
    assert "@vscode/vsce" in PACKAGE["scripts"]["package"]
    assert "dependencies" not in PACKAGE, "the extension ships with nothing to install"
    readme = (EXT / "README.md").read_text()
    assert "Python" in readme and "Node" in readme


# -- It lists the functions Provisa would serve, with the columns and parameters it infers -------

_PYTHON_SOURCE = '''
from dataclasses import dataclass


@dataclass
class Region:
    code: str
    name: str


@dataclass
class RegionInputs:
    continent: str


def regions(inputs: RegionInputs) -> list[Region]:
    """Sales regions.

    @table
    """
    return []


def rename_region(code: str, name: str) -> bool:
    """@command"""
    return True


def helper() -> int:
    return 1
'''


def test_the_workspace_lists_each_served_function_with_the_columns_and_parameters_provisa_infers(
    tmp_path,
):
    (tmp_path / "main.py").write_text(_PYTHON_SOURCE)
    cli = [sys.executable, "-m", "provisa.cli"]
    listed = _node(
        "const { readWorkspace } = require(SRC + '/check.js');"
        "readWorkspace(JSON.parse(process.argv[1]), process.argv[2], {python: process.argv[3]})"
        ".then((r) => console.log(JSON.stringify(r.functions)));",
        json.dumps(cli),
        str(tmp_path),
        sys.executable,
    )
    by_name = {f["name"]: f for f in listed}
    assert set(by_name) == {"regions", "rename_region"}, "an untagged function is not served"
    regions = by_name["regions"]
    assert regions["kind"] == "table"
    assert [(c["name"], c["type"]) for c in regions["columns"]] == [
        ("code", "text"),
        ("name", "text"),
    ]
    assert [(p["name"], p["type"]) for p in regions["parameters"]] == [("continent", "text")]
    rename = by_name["rename_region"]
    assert rename["kind"] == "command"
    assert [p["name"] for p in rename["parameters"]] == ["code", "name"]
    assert rename["columns"] == []


def test_a_refusal_of_the_code_is_shown_as_provisa_gave_it(tmp_path):
    (tmp_path / "main.py").write_text('def broken():\n    """@table"""\n    return 1\n')
    said = _node(
        "const { readWorkspace } = require(SRC + '/check.js');"
        "readWorkspace(JSON.parse(process.argv[1]), process.argv[2], {python: process.argv[3]})"
        ".then(() => console.log(JSON.stringify(null)), (e) => console.log(JSON.stringify(e.message)));",
        json.dumps([sys.executable, "-m", "provisa.cli"]),
        str(tmp_path),
        sys.executable,
    )
    assert isinstance(said, str) and "broken" in said


# -- It points a code source at the workspace folder in dev mode on a chosen instance ------------

_FAKE_INSTANCE = """
const calls = [];
function fakeFetch(existing) {
  return async (url, init) => {
    const body = init.body ? JSON.parse(init.body) : null;
    calls.push({ url, method: init.method, headers: init.headers, body });
    let answer;
    if (body && body.query && body.query.includes('sources')) answer = { data: { sources: existing } };
    else if (body && body.query && body.query.includes('createSource'))
      answer = { data: { createSource: { success: true, message: 'ok' } } };
    return { ok: true, status: 200, text: async () => JSON.stringify(answer) };
  };
}
"""


def test_dev_mode_registers_the_workspace_folder_read_in_place_on_the_chosen_instance():
    out = _node(
        _FAKE_INSTANCE + "const { ProvisaClient } = require(SRC + '/api.js');"
        "const { pointAtWorkspace } = require(SRC + '/flows.js');"
        "const api = new ProvisaClient({url: 'http://dev.example:8000/', org: 'acme'}, 'provisa_pat_x', fakeFetch([]));"
        "pointAtWorkspace(api, 'orders', 'python', '/work/orders')"
        ".then((r) => console.log(JSON.stringify({ r, calls })));"
    )
    assert out["r"] == {"created": True}
    create = out["calls"][-1]
    assert create["url"] == "http://dev.example:8000/admin/graphql"
    assert create["headers"]["Authorization"] == "Bearer provisa_pat_x"
    assert create["headers"]["X-Org-Provisa"] == "acme"
    source = create["body"]["variables"]["input"]
    assert source["id"] == "orders" and source["type"] == "python"
    mapping = json.loads(source["mappingJson"])
    # A server directory (REQ-2121) is read where it is, so an edit is read by the next query; no
    # mode is dev mode.
    assert mapping == {"runtime": "python", "code": {"kind": "directory", "path": "/work/orders"}}


def test_dev_mode_maps_a_node_workspace_to_a_node_source():
    out = _node(
        "const { devSourceInput } = require(SRC + '/flows.js');"
        "console.log(JSON.stringify(devSourceInput('risk', 'typescript', '/work/risk')));"
    )
    assert out["type"] == "node"
    assert json.loads(out["mappingJson"])["runtime"] == "node"


def test_dev_mode_leaves_a_source_already_reading_the_folder_and_refuses_one_reading_other_code():
    out = _node(
        _FAKE_INSTANCE + "const { ProvisaClient } = require(SRC + '/api.js');"
        "const { pointAtWorkspace } = require(SRC + '/flows.js');"
        "const same = [{id: 'orders', type: 'python', mappingJson: JSON.stringify({runtime: 'python', code: {kind: 'directory', path: '/work/orders'}})}];"
        "const other = [{id: 'orders', type: 'python', mappingJson: JSON.stringify({runtime: 'python', code: {kind: 'pypi', package: 'x', version: '1'}})}];"
        "(async () => {"
        "  const kept = await pointAtWorkspace(new ProvisaClient({url: 'http://h', org: 'o'}, 't', fakeFetch(same)), 'orders', 'python', '/work/orders');"
        "  const creates = calls.filter((c) => c.body.query.includes('createSource')).length;"
        "  let refused = null;"
        "  try { await pointAtWorkspace(new ProvisaClient({url: 'http://h', org: 'o'}, 't', fakeFetch(other)), 'orders', 'python', '/work/orders'); }"
        "  catch (e) { refused = e.message; }"
        "  console.log(JSON.stringify({ kept, creates, refused }));"
        "})();"
    )
    assert out["kept"] == {"created": False}
    assert out["creates"] == 0
    assert "orders" in out["refused"] and "already exists" in out["refused"]


def test_no_call_is_made_before_signing_in_to_the_instance():
    said = _node(
        "const { ProvisaClient } = require(SRC + '/api.js');"
        "try { new ProvisaClient({url: 'http://h', org: 'o'}, undefined, null); console.log('null'); }"
        "catch (e) { console.log(JSON.stringify(e.message)); }"
    )
    assert "Sign In" in said


# -- It marks the tag and Annotated forms and the SDK forms inline -------------------------------

_PY_FORMS = '''from typing import Annotated
from provisa_sdk import table as tbl, command
import provisa_sdk as sdk


def regions(continent: Annotated[str, "the continent"]) -> list[Region]:
    """Sales regions.

    @table
    """


@tbl
def orders() -> list[Order]:
    """Orders."""


@sdk.stream
async def ticks() -> AsyncIterator[Tick]:
    """Ticks."""


@command
def rename(code: str) -> bool:
    """Rename."""


@property
def not_provisa(self) -> int:
    """@ mention in prose, not a tag"""
'''

_TS_FORMS = """import { table, command as cmd } from "@provisa/sdk";
import * as provisa from "@provisa/sdk";

/** Churn risk.
 * @table
 */
export async function churnRisk(inputs: RiskInputs): Promise<Risk[]> {
  return [];
}

export const scored = table(async function scored(): Promise<Risk[]> {
  return [];
});

export const close = cmd(async function close(id: string): Promise<boolean> {
  return true;
});

export const live = provisa.stream(async function* live() {});
"""


def _marks(language: str, text: str) -> list[dict]:
    return _node(
        "const { markers } = require(SRC + '/markers.js');"
        "console.log(JSON.stringify(markers(process.argv[1], process.argv[2])));",
        language,
        text,
    )


def test_python_tag_sdk_and_annotated_forms_are_each_marked_at_their_text():
    lines = _PY_FORMS.split("\n")
    found = _marks("python", _PY_FORMS)
    seen = {(m["form"], m["tag"], m["name"]) for m in found}
    assert seen == {
        ("annotated", None, "regions"),
        ("tag", "table", "regions"),
        ("sdk", "table", "orders"),
        ("sdk", "stream", "ticks"),
        ("sdk", "command", "rename"),
    }
    texts = {
        m["form"] + ":" + str(m["tag"]): lines[m["line"]][m["start"] : m["end"]] for m in found
    }
    assert texts["annotated:None"] == 'Annotated[str, "the continent"]'
    assert texts["tag:table"] == "@table"
    assert texts["sdk:stream"] == "@sdk.stream"


def test_typescript_jsdoc_tag_and_sdk_wrapper_forms_are_each_marked():
    found = _marks("typescript", _TS_FORMS)
    seen = {(m["form"], m["tag"], m["name"]) for m in found}
    assert seen == {
        ("tag", "table", "churnRisk"),
        ("sdk", "table", "scored"),
        ("sdk", "command", "close"),
        ("sdk", "stream", "live"),
    }


def test_a_mark_says_what_provisa_serves_the_function_as():
    said = _node(
        "const { describe } = require(SRC + '/markers.js');"
        "const served = [{name: 'regions', kind: 'table', parameters: [{name: 'continent', type: 'text', required: true}],"
        " columns: [{name: 'code', type: 'text'}], returns: null}];"
        "console.log(JSON.stringify([describe({form: 'tag', tag: 'table', name: 'regions'}, served),"
        " describe({form: 'annotated', tag: null, name: 'regions'}, served)]));"
    )
    assert said[0] == (
        "@table (tag form): Provisa serves regions as a table; parameters continent: text; "
        "columns code: text"
    )
    assert "Annotated[T, ...] as T" in said[1]


# -- From the command palette: Build image and copy the compose snippet, through the UI's API ------


def test_build_image_starts_and_polls_the_uis_build_routes_and_ends_with_the_compose_snippet():
    out = _node(
        "const { ProvisaClient } = require(SRC + '/api.js');"
        "const { buildImage } = require(SRC + '/flows.js');"
        "const calls = []; let polls = 0;"
        "const fetchImpl = async (url, init) => {"
        "  calls.push({ url, method: init.method, body: init.body ? JSON.parse(init.body) : null });"
        "  const building = { id: 'b1', status: 'building', compose: null };"
        "  const done = { id: 'b1', status: 'done', image_ref: 'ghcr.io/acme/x:abc', compose: 'services: {}' };"
        "  const answer = init.method === 'POST' ? building : (++polls < 2 ? building : done);"
        "  return { ok: true, status: 200, text: async () => JSON.stringify(answer) };"
        "};"
        "const api = new ProvisaClient({url: 'http://h:8000', org: 'acme'}, 't', fetchImpl);"
        "buildImage(api, 'orders', {arch: 'amd64', registry_id: 'r1', download: false}, async () => {}, 0)"
        ".then((b) => console.log(JSON.stringify({ b, calls })));"
    )
    base = "http://h:8000/admin/orgs/acme/code-sources/orders/builds"
    assert out["calls"][0] == {
        "url": base,
        "method": "POST",
        "body": {"arch": "amd64", "registry_id": "r1", "download": False},
    }
    assert {c["url"] for c in out["calls"][1:]} == {f"{base}/b1"}
    assert out["b"]["compose"] == "services: {}"


def test_a_failed_build_is_shown_with_the_servers_reason():
    said = _node(
        "const { ProvisaClient } = require(SRC + '/api.js');"
        "const { buildImage } = require(SRC + '/flows.js');"
        "const fetchImpl = async () => ({ ok: true, status: 200, text: async () =>"
        "  JSON.stringify({ id: 'b1', status: 'failed', error: 'no lockfile' }) });"
        "buildImage(new ProvisaClient({url: 'http://h', org: 'o'}, 't', fetchImpl), 's', {}, async () => {}, 0)"
        ".then(() => console.log('null'), (e) => console.log(JSON.stringify(e.message)));"
    )
    assert said == "the image build failed: no lockfile"


def test_build_image_and_dev_mode_are_palette_commands_and_the_compose_snippet_goes_to_the_clipboard():
    titles = {c["command"]: c for c in PACKAGE["contributes"]["commands"]}
    assert titles["provisa.buildImage"]["category"] == "Provisa"
    assert titles["provisa.buildImage"]["title"] == "Build Image"
    assert titles["provisa.devMode"]["title"] == "Run in Dev Mode"
    wiring = (EXT / "src" / "extension.js").read_text()
    assert "vscode.env.clipboard.writeText(built.compose)" in wiring


def test_the_extension_reaches_only_the_routes_the_ui_uses_with_no_backend_of_its_own():
    api = (EXT / "src" / "api.js").read_text()
    ui_hosts = (ROOT / "provisa-ui" / "src" / "api" / "codeHosts.ts").read_text()
    ui_registries = (ROOT / "provisa-ui" / "src" / "api" / "containerRegistries.ts").read_text()
    assert "/code-sources/" in ui_hosts and "/builds" in ui_hosts
    assert "/container-registries" in ui_registries
    for route in ("/admin/graphql", "/code-sources/", "/builds", "/container-registries"):
        assert route in api
    sources = "".join(p.read_text() for p in (EXT / "src").glob("*.js"))
    assert "createServer" not in sources and "listen(" not in sources


# -- It edits no model and offers no query editor; a docs page says how --------------------------


def test_the_extension_contributes_no_query_editor_and_edits_no_model():
    contributes = PACKAGE["contributes"]
    for editor in ("languages", "grammars", "customEditors", "notebooks", "notebookRenderer"):
        assert editor not in contributes
    assert set(contributes) == {"commands", "views", "menus", "configuration"}
    api = (EXT / "src" / "api.js").read_text()
    # Its one write is the dev-mode source it points at the workspace; it changes no table,
    # relationship, role or rule.
    mutations = {
        line.split("mutation ")[1].split("(")[0] for line in api.splitlines() if "mutation " in line
    }
    assert mutations == {"CreateSource"}
    assert "updateSource" not in api and "PUT" not in api and "DELETE" not in api


def test_a_docs_page_says_how_to_author_graphql_sql_and_cypher_with_the_existing_extensions():
    page = ROOT / "docs" / "vscode.md"
    text = page.read_text()
    for needed in ("/data/sdl", "X-Provisa-Role", "PROVISA_PGWIRE_PORT", "PROVISA_BOLT_PORT"):
        assert needed in text
    for language in ("GraphQL", "SQL", "Cypher"):
        assert f"## {language}" in text or f"### {language}" in text
    nav = yaml.safe_load((ROOT / "mkdocs.yml").read_text().replace("!!python", "#"))
    assert "vscode.md" in json.dumps(nav)


@pytest.mark.parametrize("folder_has", ["package.json", "index.d.ts"])
def test_a_node_workspace_is_told_apart_as_code_check_does(tmp_path, folder_has):
    (tmp_path / folder_has).write_text("{}")
    out = _node(
        "const Module = require('module'); const orig = Module._load;"
        "Module._load = function (r, ...a) { return r === 'vscode' ? {} : orig.call(this, r, ...a); };"
        "const { languageOf } = require(SRC + '/extension.js');"
        "console.log(JSON.stringify(languageOf(process.argv[1])));",
        str(tmp_path),
    )
    assert out == "typescript"


# -- The services shim, forwarded into VS Code terminals and debug sessions (amended 2026-11-22) --

#: A ``vscode`` module standing in for the editor: it records the extension's terminal environment
#: collection, its debug configuration provider and its commands, and answers the pickers with
#: what the test queued in ``picks``.
_FAKE_VSCODE = r"""
const Module = require('module');
const orig = Module._load;
const registered = { commands: {}, debug: {} };
const picks = [];
class EventEmitter { constructor() { this.event = () => ({ dispose() {} }); } fire() {} }
const disposable = { dispose() {} };
const vscode = {
  EventEmitter,
  TreeItem: class { constructor(label) { this.label = label; } },
  TreeItemCollapsibleState: { Collapsed: 1 },
  ThemeColor: class {},
  Range: class {},
  ProgressLocation: { Notification: 15 },
  window: {
    createOutputChannel: () => ({ appendLine() {}, dispose() {} }),
    createTextEditorDecorationType: () => disposable,
    registerTreeDataProvider: () => disposable,
    showQuickPick: async (items) => items[picks.shift()],
    showInputBox: async () => picks.shift(),
    showInformationMessage() {},
    showErrorMessage(m) { throw new Error(m); },
    visibleTextEditors: [],
    onDidChangeVisibleTextEditors: () => disposable,
  },
  commands: { registerCommand: (id, fn) => { registered.commands[id] = fn; return disposable; } },
  workspace: {
    workspaceFolders: undefined,
    getConfiguration: () => ({ get: (k) => ({ cli: ['provisa'], python: '', instances: INSTANCES })[k] }),
    onDidChangeTextDocument: () => disposable,
    onDidSaveTextDocument: () => disposable,
  },
  debug: {
    registerDebugConfigurationProvider: (type, provider) => {
      registered.debug[type] = provider;
      return disposable;
    },
  },
  env: { clipboard: { writeText: async () => {} } },
};
Module._load = function (r, ...a) { return r === 'vscode' ? vscode : orig.call(this, r, ...a); };

/** The terminal environment collection VS Code gives an extension. */
function collection() {
  const vars = new Map();
  return {
    vars, persistent: true, description: undefined,
    replace(name, value) { vars.set(name, value); },
    clear() { vars.clear(); },
  };
}

function context(state, secrets) {
  return {
    subscriptions: [],
    environmentVariableCollection: collection(),
    workspaceState: { get: (k) => state[k], update: async (k, v) => { state[k] = v; } },
    secrets: { get: async (k) => secrets[k], store: async (k, v) => { secrets[k] = v; } },
  };
}

const INSTANCES = [{ name: 'local', url: 'http://localhost:8000/', org: 'acme' }];
const { activate } = require(SRC + '/extension.js');
"""

_ADDRESS = "http://localhost:8000/admin/orgs/acme/code-sources/orders_py/services/pat-123"


def test_a_new_terminal_gets_provisa_services_for_the_signed_in_dev_mode_source():
    out = _node(
        _FAKE_VSCODE
        + """
(async () => {
  const ctx = context(
    { 'provisa.instance': 'http://localhost:8000/', 'provisa.sourceId': 'orders_py' },
    { 'provisa.token.http://localhost:8000/': 'pat-123' },
  );
  await activate(ctx);
  const c = ctx.environmentVariableCollection;
  console.log(JSON.stringify({ vars: Object.fromEntries(c.vars), persistent: c.persistent,
    description: c.description }));
})();
"""
    )
    assert out["vars"] == {"PROVISA_SERVICES": _ADDRESS}
    # The address holds a personal access token: never written to the workspace's storage.
    assert out["persistent"] is False
    assert "orders_py" in out["description"]


def test_a_launched_debug_session_gets_provisa_services_and_keeps_its_own_environment():
    out = _node(
        _FAKE_VSCODE
        + """
(async () => {
  const ctx = context(
    { 'provisa.instance': 'http://localhost:8000/', 'provisa.sourceId': 'orders_py' },
    { 'provisa.token.http://localhost:8000/': 'pat-123' },
  );
  await activate(ctx);
  const provider = registered.debug['*'];
  const plain = await provider.resolveDebugConfiguration(undefined,
    { type: 'debugpy', request: 'launch', program: 'serve.py' });
  const own = await provider.resolveDebugConfiguration(undefined,
    { type: 'node', request: 'launch', env: { LOG_LEVEL: 'debug' } });
  console.log(JSON.stringify({ types: Object.keys(registered.debug), plain, own }));
})();
"""
    )
    # Every debugger type: a Python, Node, Bun or Deno launch alike.
    assert out["types"] == ["*"]
    assert out["plain"] == {
        "type": "debugpy",
        "request": "launch",
        "program": "serve.py",
        "env": {"PROVISA_SERVICES": _ADDRESS},
    }
    assert out["own"]["env"] == {"PROVISA_SERVICES": _ADDRESS, "LOG_LEVEL": "debug"}


def test_nothing_is_forwarded_until_signing_in_and_signing_in_forwards_it():
    out = _node(
        _FAKE_VSCODE
        + """
(async () => {
  const ctx = context({ 'provisa.sourceId': 'orders_py' }, {});
  await activate(ctx);
  const provider = registered.debug['*'];
  const c = ctx.environmentVariableCollection;
  const before = { vars: Object.fromEntries(c.vars),
    debug: await provider.resolveDebugConfiguration(undefined, { type: 'debugpy' }) };
  picks.push(0, 'pat-123');  // the instance, then the token
  await registered.commands['provisa.signIn']();
  const after = { vars: Object.fromEntries(c.vars),
    debug: await provider.resolveDebugConfiguration(undefined, { type: 'debugpy' }) };
  console.log(JSON.stringify({ before, after }));
})();
"""
    )
    # REQ-2106: with no address the shim's calls raise, naming the service.
    assert out["before"] == {"vars": {}, "debug": {"type": "debugpy"}}
    assert out["after"]["vars"] == {"PROVISA_SERVICES": _ADDRESS}
    assert out["after"]["debug"]["env"] == {"PROVISA_SERVICES": _ADDRESS}


def test_the_python_shim_calls_the_forwarded_address(monkeypatch):
    """What a terminal or debug session is given is what provisa_services reads: a call goes to
    the forwarded address, followed by the service and its call."""
    import http.server
    import threading

    seen: list[str] = []

    class Handler(http.server.BaseHTTPRequestHandler):
        def do_GET(self):  # noqa: N802 -- the http.server hook's name
            seen.append(self.path)
            body = b"s3cret"
            self.send_response(200)
            self.send_header("Content-Type", "text/plain; charset=utf-8")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def log_message(self, *_args):
            pass

    server = http.server.ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    threading.Thread(target=server.serve_forever, daemon=True).start()
    try:
        url = f"http://127.0.0.1:{server.server_address[1]}"
        address = _node(
            "const { servicesEnv } = require(SRC + '/services.js');"
            "console.log(JSON.stringify(servicesEnv({ url: process.argv[1], org: 'acme' },"
            " 'orders_py', 'pat-123')));",
            url,
        )["PROVISA_SERVICES"]
        sys.path.insert(0, str(ROOT / "provisa" / "python" / "services"))
        monkeypatch.setenv("PROVISA_SERVICES", address)
        import provisa_services

        assert provisa_services.secrets.get("api_key") == "s3cret"
    finally:
        server.shutdown()
        sys.path.remove(str(ROOT / "provisa" / "python" / "services"))
    assert seen == ["/admin/orgs/acme/code-sources/orders_py/services/pat-123/secrets/api_key"]
