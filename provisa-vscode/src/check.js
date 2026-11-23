// Copyright (c) 2026 Kenneth Stott
// Canary: 1c38aed1-e1d5-49d5-aba7-3e5575f21989
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-2112: what Provisa would serve from the open workspace, before anything is registered. The
// extension infers nothing itself: it runs `provisa code check --json` (provisa/cli_code.py), the
// server's own readers, and lists the manifest it prints.

"use strict";

const { execFile } = require("node:child_process");

/** The kinds the extension lists: a table (a @stream is a live table) or a command. */
const LISTED = new Set(["table", "stream", "command", "query"]);

/** The argv that reads `folder` the way Provisa does: `cli` is the configured Provisa command. */
function checkCommand(cli, folder, options = {}) {
  if (!Array.isArray(cli) || cli.length === 0) {
    throw new Error("provisa.cli is the Provisa command as a list, for example [\"provisa\"]");
  }
  const argv = [...cli, "code", "check", folder, "--json"];
  if (options.language) argv.push("--language", options.language);
  if (options.python) argv.push("--python", options.python);
  return argv;
}

/** The text of a shape (manifest.py _shape_json) as a column or parameter type. */
function shapeText(shape, types) {
  if (shape === null || shape === undefined) return "none";
  switch (shape.kind) {
    case "scalar":
      return shape.ir;
    case "list":
      return `list of ${shapeText(shape.element, types)}`;
    case "named":
      return shape.name;
    case "enum":
      return `${shape.name} (${shape.values.join(", ")})`;
    case "table":
      return `table of ${shapeText(shape.row, types)}`;
    case "anonymous":
      return "object";
    default:
      throw new Error(`a manifest shape this extension does not know: ${shape.kind}`);
  }
}

/** The fields of the row a table returns: a named type's, or an anonymous row's own. */
function rowFields(shape, types) {
  if (shape === null || shape === undefined) return [];
  if (shape.kind === "list") return rowFields(shape.element, types);
  if (shape.kind === "table") return rowFields(shape.row, types);
  if (shape.kind === "anonymous") return shape.fields;
  if (shape.kind === "named") {
    const named = types[shape.name];
    if (named === undefined) throw new Error(`the manifest names a type it does not hold: ${shape.name}`);
    return named.fields;
  }
  return [];
}

/** Each function Provisa would serve as a table or a command, with its columns and parameters. */
function servedFunctions(manifest) {
  const types = manifest.types;
  return manifest.exports
    .filter((e) => LISTED.has(e.kind))
    .map((e) => {
      const isTable = e.kind === "table" || e.kind === "stream";
      return {
        name: e.name,
        kind: e.kind,
        description: e.description,
        parameters: e.parameters.map((p) => ({
          name: p.name,
          type: shapeText(p.shape, types),
          required: p.required,
        })),
        columns: isTable
          ? rowFields(e.returns, types).map((f) => ({
              name: f.name,
              type: shapeText(f.shape, types),
              nullable: f.nullable,
            }))
          : [],
        returns: isTable ? null : e.returns === null ? null : shapeText(e.returns, types),
      };
    });
}

/** Runs the check: resolves to {functions, warnings}, rejects with the refusals Provisa gave. */
function readWorkspace(cli, folder, options = {}, run = execFile) {
  const argv = checkCommand(cli, folder, options);
  return new Promise((resolve, reject) => {
    run(argv[0], argv.slice(1), { cwd: folder, maxBuffer: 64 * 1024 * 1024 }, (error, stdout, stderr) => {
      const lines = String(stderr).split("\n").filter((l) => l.trim());
      if (error) {
        const said = lines.join("\n") || error.message;
        reject(new Error(said));
        return;
      }
      resolve({
        functions: servedFunctions(JSON.parse(stdout)),
        warnings: lines.filter((l) => l.startsWith("warning: ")).map((l) => l.slice(9)),
      });
    });
  });
}

module.exports = { checkCommand, servedFunctions, readWorkspace, shapeText };
