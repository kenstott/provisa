// Copyright (c) 2026 Kenneth Stott
// Canary: 9991e1a0-5470-4806-aef0-718e3de3f909
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-2112: the Provisa extension for Node and Python code sources. This file wires VS Code to
// the modules beside it, which hold everything the extension decides: check.js (what Provisa
// would serve), markers.js (the forms marked inline), api.js and flows.js (dev mode and Build
// image on a chosen instance), services.js (the services shim forwarded into terminals and debug
// sessions). It edits no model and offers no query editor.

"use strict";

const fs = require("node:fs");
const path = require("node:path");
const vscode = require("vscode");
const { readWorkspace } = require("./check");
const { markers, describe } = require("./markers");
const { ProvisaClient } = require("./api");
const { pointAtWorkspace, buildImage } = require("./flows");
const { servicesEnv, forwardToTerminals, withServices } = require("./services");

const INSTANCE_KEY = "provisa.instance";
const SOURCE_KEY = "provisa.sourceId";
const tokenKey = (url) => `provisa.token.${url}`;

/** The workspace's language, as `provisa code check` decides it (provisa/cli_code.py _language). */
function languageOf(folder) {
  if (fs.existsSync(path.join(folder, "package.json"))) return "typescript";
  if (fs.readdirSync(folder).some((n) => n.endsWith(".d.ts"))) return "typescript";
  return "python";
}

function workspaceFolder() {
  const folders = vscode.workspace.workspaceFolders;
  if (!folders || folders.length === 0) throw new Error("open a folder holding a code source first");
  return folders[0].uri.fsPath;
}

class FunctionsTree {
  constructor() {
    this.functions = [];
    this.problem = null;
    this.changed = new vscode.EventEmitter();
    this.onDidChangeTreeData = this.changed.event;
  }

  set(functions, problem) {
    this.functions = functions;
    this.problem = problem;
    this.changed.fire();
  }

  getTreeItem(node) {
    return node;
  }

  getChildren(node) {
    if (node === undefined) {
      if (this.problem !== null) return [new vscode.TreeItem(this.problem)];
      return this.functions.map((fn) => {
        const item = new vscode.TreeItem(fn.name, vscode.TreeItemCollapsibleState.Collapsed);
        item.description = fn.kind;
        item.tooltip = fn.description || undefined;
        item.fn = fn;
        return item;
      });
    }
    if (node.fn === undefined) return [];
    const params = node.fn.parameters.map((p) => {
      const item = new vscode.TreeItem(`${p.name}: ${p.type}`);
      item.description = p.required ? "parameter" : "optional parameter";
      return item;
    });
    const cols = node.fn.columns.map((c) => {
      const item = new vscode.TreeItem(`${c.name}: ${c.type}`);
      item.description = c.nullable ? "nullable column" : "column";
      return item;
    });
    return [...params, ...cols];
  }
}

function activate(context) {
  const tree = new FunctionsTree();
  const output = vscode.window.createOutputChannel("Provisa");
  const decoration = vscode.window.createTextEditorDecorationType({
    after: { margin: "0 0 0 1em", color: new vscode.ThemeColor("editorCodeLens.foreground") },
  });
  let served = [];
  /** The variables forwarded into terminals and debug sessions; null until the developer is
   * signed in and has a dev-mode source (REQ-2106: with no address the shim's calls raise). */
  let forwarded = null;

  const config = () => vscode.workspace.getConfiguration("provisa");

  async function refresh() {
    let folder;
    try {
      folder = workspaceFolder();
    } catch (e) {
      tree.set([], e.message);
      return;
    }
    const python = config().get("python");
    try {
      const read = await readWorkspace(config().get("cli"), folder, python ? { python } : {});
      served = read.functions;
      read.warnings.forEach((w) => output.appendLine(`warning: ${w}`));
      tree.set(served, served.length ? null : "Provisa would serve no function from this folder");
    } catch (e) {
      served = [];
      output.appendLine(e.message);
      tree.set([], `Provisa refused the code: ${e.message.split("\n")[0]}`);
    }
    vscode.window.visibleTextEditors.forEach(mark);
  }

  function mark(editor) {
    const doc = editor.document;
    const found = markers(doc.languageId, doc.getText());
    editor.setDecorations(
      decoration,
      found.map((m) => ({
        range: new vscode.Range(m.line, m.start, m.line, m.end),
        hoverMessage: describe(m, served),
        renderOptions: { after: { contentText: m.form === "annotated" ? "Provisa: Annotated" : `Provisa: ${m.tag}` } },
      })),
    );
  }

  /** The chosen instance and a client signed in to it. */
  async function client() {
    const instances = config().get("instances");
    const chosen = context.workspaceState.get(INSTANCE_KEY);
    const instance = instances.find((i) => i.url === chosen);
    if (instance === undefined) throw new Error("choose a Provisa instance first: run Provisa: Choose Instance");
    const token = await context.secrets.get(tokenKey(instance.url));
    return new ProvisaClient(instance, token, fetch);
  }

  /** Forwards the services shim of the chosen instance's dev-mode source into new terminals and
   * debug sessions, or forwards nothing while there is none. */
  async function forwardServices() {
    const instances = config().get("instances");
    const instance = instances.find((i) => i.url === context.workspaceState.get(INSTANCE_KEY));
    const sourceId = context.workspaceState.get(SOURCE_KEY);
    const token = instance === undefined ? undefined : await context.secrets.get(tokenKey(instance.url));
    const collection = context.environmentVariableCollection;
    if (instance === undefined || !sourceId || !token) {
      forwarded = null;
      collection.clear();
      return;
    }
    forwarded = servicesEnv(instance, sourceId, token);
    forwardToTerminals(collection, forwarded, `Provisa's services for ${sourceId} on ${instance.url}`);
  }

  async function chooseInstance() {
    const instances = config().get("instances");
    if (instances.length === 0) throw new Error("add an instance to the provisa.instances setting first");
    const picked = await vscode.window.showQuickPick(
      instances.map((i) => ({ label: i.name || i.url, description: `${i.url} (org ${i.org})`, instance: i })),
      { placeHolder: "The Provisa instance to run this code source on" },
    );
    if (picked === undefined) return undefined;
    await context.workspaceState.update(INSTANCE_KEY, picked.instance.url);
    await forwardServices();
    return picked.instance;
  }

  async function signIn() {
    const instance = await chooseInstance();
    if (instance === undefined) return;
    const token = await vscode.window.showInputBox({
      prompt: `A personal access token for ${instance.url}`,
      password: true,
      ignoreFocusOut: true,
    });
    if (!token) return;
    await context.secrets.store(tokenKey(instance.url), token);
    await forwardServices();
    vscode.window.showInformationMessage(`Signed in to ${instance.url}`);
  }

  async function devMode() {
    const folder = workspaceFolder();
    const api = await client();
    const sourceId = await vscode.window.showInputBox({
      prompt: "The code source's name on the instance",
      value: context.workspaceState.get(SOURCE_KEY) || path.basename(folder).replace(/[^A-Za-z0-9_]/g, "_"),
    });
    if (!sourceId) return;
    const done = await pointAtWorkspace(api, sourceId, languageOf(folder), folder);
    await context.workspaceState.update(SOURCE_KEY, sourceId);
    await forwardServices();
    vscode.window.showInformationMessage(
      done.created
        ? `${sourceId} reads ${folder} in dev mode on ${api.url}; each edit is read by the next query`
        : `${sourceId} already reads ${folder} in dev mode on ${api.url}`,
    );
  }

  async function build() {
    const api = await client();
    const sourceId = context.workspaceState.get(SOURCE_KEY);
    if (!sourceId) throw new Error("run Provisa: Run in Dev Mode first; Build image builds a dev-mode source");
    const arch = await vscode.window.showQuickPick(["amd64", "arm64"], { placeHolder: "The image's architecture" });
    if (arch === undefined) return;
    const registries = await api.registries();
    const where = await vscode.window.showQuickPick(
      [
        ...registries.map((r) => ({ label: r.address, registry: r.id })),
        { label: "Download as a tarball", registry: null },
      ],
      { placeHolder: "Push the image to a container registry, or keep it for download" },
    );
    if (where === undefined) return;
    const choice = { arch, registry_id: where.registry, download: where.registry === null };
    const built = await vscode.window.withProgress(
      { location: vscode.ProgressLocation.Notification, title: `Building ${sourceId}'s image` },
      () => buildImage(api, sourceId, choice, (ms) => new Promise((r) => setTimeout(r, ms))),
    );
    await vscode.env.clipboard.writeText(built.compose);
    vscode.window.showInformationMessage(`Built ${built.image_ref}; the compose snippet is on the clipboard`);
  }

  const guarded = (fn) => async () => {
    try {
      await fn();
    } catch (e) {
      vscode.window.showErrorMessage(`Provisa: ${e.message}`);
    }
  };

  context.subscriptions.push(
    output,
    decoration,
    vscode.window.registerTreeDataProvider("provisa.functions", tree),
    vscode.commands.registerCommand("provisa.refresh", guarded(refresh)),
    vscode.commands.registerCommand("provisa.chooseInstance", guarded(chooseInstance)),
    vscode.commands.registerCommand("provisa.signIn", guarded(signIn)),
    vscode.commands.registerCommand("provisa.devMode", guarded(devMode)),
    vscode.commands.registerCommand("provisa.buildImage", guarded(build)),
    vscode.window.onDidChangeVisibleTextEditors((editors) => editors.forEach(mark)),
    vscode.workspace.onDidChangeTextDocument((e) =>
      vscode.window.visibleTextEditors.filter((ed) => ed.document === e.document).forEach(mark),
    ),
    vscode.workspace.onDidSaveTextDocument(() => refresh()),
    vscode.debug.registerDebugConfigurationProvider("*", {
      resolveDebugConfiguration: (_folder, debugConfig) =>
        forwarded === null ? debugConfig : withServices(debugConfig, forwarded),
    }),
  );
  refresh();
  return forwardServices();
}

function deactivate() {}

module.exports = { activate, deactivate, languageOf };
