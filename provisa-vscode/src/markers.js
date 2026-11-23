// Copyright (c) 2026 Kenneth Stott
// Canary: 0dbc5109-4a14-47b2-a189-70df2ad9cab4
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-2112 (amended 2026-11-21): the forms Provisa reads, found in a file's text so the extension
// marks each inline -- the tag form (a docstring or JSDoc tag), the SDK form (a provisa_sdk
// decorator, an @provisa/sdk wrapper) and, in Python, the Annotated form (Provisa reads
// Annotated[T, ...] as T: provisa/python/reader.py).
//
// A marker is {line, start, end, form: "tag" | "sdk" | "annotated", tag, name}: zero-based line and
// columns of the text marked, the tag it stands for (null for an Annotated hint) and the function
// it belongs to (null when none follows).

"use strict";

/** Provisa's tags (provisa/codesource/manifest.py PROVISA_TAGS), and the two that qualify one. */
const TAGS = [
  "table", "command", "query", "stream", "columns", "change", "discover",
  "on-start", "on-stop", "health", "loader", "requires",
];
const TAG_LINE = new RegExp(`^(\\s*(?:\\*\\s*)?)@(${TAGS.join("|")})(?![\\w-])`);

/** provisa_sdk's decorators, by name, and the tag each writes (provisa-sdk/python). */
const PY_SDK = {
  table: "table", command: "command", query: "query", stream: "stream", columns: "columns",
  change: "change", on_start: "on-start", on_stop: "on-stop", health: "health", requires: "requires",
};
/** @provisa/sdk's wrappers, by name, and the tag each stands for (provisa-sdk/typescript). */
const TS_SDK = {
  table: "table", command: "command", query: "query", stream: "stream", columns: "columns",
  onStart: "on-start", onStop: "on-stop", health: "health",
};

function pythonSdkNames(lines) {
  const local = new Map(); // a name bound in this module -> its tag
  const modules = new Set(); // names the module itself is bound to
  for (const line of lines) {
    const from = /^\s*from\s+provisa_sdk\s+import\s+\(?([^)#]*)/.exec(line);
    if (from) {
      for (const part of from[1].split(",")) {
        const [name, alias] = part.trim().split(/\s+as\s+/);
        if (name && PY_SDK[name]) local.set(alias || name, PY_SDK[name]);
      }
    }
    const whole = /^\s*import\s+provisa_sdk(?:\s+as\s+(\w+))?\s*$/.exec(line);
    if (whole) modules.add(whole[1] || "provisa_sdk");
  }
  return { local, modules };
}

/** The Python function a line belongs to: the next `def` at or below `from`. */
function nextPythonDef(lines, from) {
  for (let i = from; i < lines.length; i++) {
    const m = /^\s*(?:async\s+)?def\s+(\w+)/.exec(lines[i]);
    if (m) return m[1];
  }
  return null;
}

/** The function whose docstring holds line `at`: the nearest `def` above it. */
function enclosingPythonDef(lines, at) {
  for (let i = at; i >= 0; i--) {
    const m = /^\s*(?:async\s+)?def\s+(\w+)/.exec(lines[i]);
    if (m) return m[1];
  }
  return null;
}

/** The end of the bracketed text opened at `open` on `line`, or the line's end. */
function closing(line, open) {
  let depth = 0;
  for (let i = open; i < line.length; i++) {
    if (line[i] === "[") depth++;
    else if (line[i] === "]" && --depth === 0) return i + 1;
  }
  return line.length;
}

function pythonMarkers(text) {
  const lines = text.split("\n");
  const sdk = pythonSdkNames(lines);
  const out = [];
  let quote = null; // the triple quote of the docstring the scan is inside
  lines.forEach((line, i) => {
    if (quote !== null) {
      const tag = TAG_LINE.exec(line);
      if (tag) {
        const start = tag[1].length;
        out.push({ line: i, start, end: start + 1 + tag[2].length, form: "tag", tag: tag[2], name: enclosingPythonDef(lines, i) });
      }
      if (line.includes(quote)) quote = null;
      return;
    }
    const opened = /("""|''')/.exec(line);
    if (opened && line.split(opened[1]).length === 2) {
      quote = opened[1];
      const rest = line.slice(opened.index + 3);
      const tag = TAG_LINE.exec(rest);
      if (tag) {
        const start = opened.index + 3 + tag[1].length;
        out.push({ line: i, start, end: start + 1 + tag[2].length, form: "tag", tag: tag[2], name: enclosingPythonDef(lines, i) });
      }
      return;
    }
    const code = line.replace(/#.*$/, "");
    const deco = /^(\s*)@([\w.]+)/.exec(code);
    if (deco) {
      const parts = deco[2].split(".");
      const tag =
        parts.length === 1
          ? sdk.local.get(parts[0])
          : parts.length === 2 && sdk.modules.has(parts[0])
            ? PY_SDK[parts[1]]
            : undefined;
      if (tag) {
        const start = deco[1].length;
        out.push({ line: i, start, end: start + 1 + deco[2].length, form: "sdk", tag, name: nextPythonDef(lines, i + 1) });
      }
    }
    const annotated = /\b(?:typing\.|t\.)?Annotated\[/g;
    let m;
    while ((m = annotated.exec(code)) !== null) {
      const open = m.index + m[0].length - 1;
      out.push({ line: i, start: m.index, end: closing(code, open), form: "annotated", tag: null, name: enclosingPythonDef(lines, i) });
    }
  });
  return out;
}

function typescriptSdkNames(text) {
  const local = new Map();
  const modules = new Set();
  const named = /import\s*(?:type\s+)?\{([^}]*)\}\s*from\s*["']@provisa\/sdk["']/g;
  let m;
  while ((m = named.exec(text)) !== null) {
    for (const part of m[1].split(",")) {
      const [name, alias] = part.trim().split(/\s+as\s+/);
      if (name && TS_SDK[name]) local.set(alias || name, TS_SDK[name]);
    }
  }
  const star = /import\s*\*\s*as\s+(\w+)\s+from\s*["']@provisa\/sdk["']/g;
  while ((m = star.exec(text)) !== null) modules.add(m[1]);
  return { local, modules };
}

/** The export a line's JSDoc belongs to: the next exported function or const. */
function nextTsExport(lines, from) {
  for (let i = from; i < lines.length; i++) {
    const m = /^\s*export\s+(?:default\s+)?(?:async\s+)?(?:function\s*\*?\s*|const\s+|let\s+)(\w+)/.exec(lines[i]);
    if (m) return m[1];
  }
  return null;
}

function typescriptMarkers(text) {
  const lines = text.split("\n");
  const sdk = typescriptSdkNames(text);
  const out = [];
  let inDoc = false;
  lines.forEach((line, i) => {
    if (!inDoc && /\/\*\*/.test(line)) inDoc = true;
    if (inDoc) {
      const body = line.replace(/^\s*\/\*\*/, (s) => " ".repeat(s.length));
      const tag = TAG_LINE.exec(body);
      if (tag) {
        const start = tag[1].length;
        out.push({ line: i, start, end: start + 1 + tag[2].length, form: "tag", tag: tag[2], name: nextTsExport(lines, i) });
      }
      if (line.includes("*/")) inDoc = false;
      return;
    }
    const code = line.replace(/\/\/.*$/, "");
    const wrapper = /\b(?:(\w+)\.)?(\w+)\s*\(/g;
    let m;
    while ((m = wrapper.exec(code)) !== null) {
      const tag = m[1] === undefined ? sdk.local.get(m[2]) : sdk.modules.has(m[1]) ? TS_SDK[m[2]] : undefined;
      if (!tag) continue;
      const bound = /^\s*export\s+(?:const|let)\s+(\w+)\s*=/.exec(code);
      out.push({
        line: i,
        start: m.index,
        end: m.index + m[0].length - 1,
        form: "sdk",
        tag,
        name: bound ? bound[1] : null,
      });
    }
  });
  return out;
}

/** The markers of a file's text, by its VS Code language id. */
function markers(languageId, text) {
  if (languageId === "python") return pythonMarkers(text);
  if (["typescript", "typescriptreact", "javascript", "javascriptreact"].includes(languageId)) {
    return typescriptMarkers(text);
  }
  return [];
}

/** What a marker says, from what Provisa infers for its function (check.js servedFunctions). */
function describe(marker, served) {
  if (marker.form === "annotated") {
    return "Annotated form: Provisa reads Annotated[T, ...] as T; the rest of the hint is not read.";
  }
  const form = marker.form === "tag" ? "tag form" : "SDK form";
  const fn = served.find((f) => f.name === marker.name);
  if (fn === undefined) return `@${marker.tag} (${form})`;
  const params = fn.parameters.map((p) => `${p.name}: ${p.type}${p.required ? "" : "?"}`).join(", ");
  const cols = fn.columns.map((c) => `${c.name}: ${c.type}`).join(", ");
  const what = fn.kind === "table" || fn.kind === "stream" ? `columns ${cols || "none"}` : `returns ${fn.returns ?? "none"}`;
  return `@${marker.tag} (${form}): Provisa serves ${fn.name} as a ${fn.kind}; parameters ${params || "none"}; ${what}`;
}

module.exports = { markers, describe, TAGS };
