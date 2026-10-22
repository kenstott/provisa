// Copyright (c) 2026 Kenneth Stott
// Canary: 6b2e9d14-3c78-4f05-a9e1-d7c4b2f8e630
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// The backport of monaco-editor 0.57.0's catch on the word highlighter's Delayer.trigger calls,
// against the 0.52.2 actually installed: both calls catch, nothing else changes, and a monaco that
// no longer reads as 0.52.2 stops the build instead of quietly bringing the "Canceled" error back.

// @vitest-environment node
import fs from "fs";
import os from "os";
import path from "path";
import { describe, expect, it } from "vitest";
import { PATCHED_MONACO, assertPatchedMonaco, backportCancelCatch } from "../monacoCancelBackport";

const ROOT = path.resolve(__dirname, "../../..");
const FILE = path.join(
  ROOT,
  "node_modules/monaco-editor/esm/vs/editor/contrib/wordHighlighter/browser/wordHighlighter.js",
);

describe("the word highlighter's dropped promises, caught (monaco 0.57.0's fix)", () => {
  it("catches both Delayer.trigger calls in the installed 0.52.2, and changes nothing else", () => {
    const code = fs.readFileSync(FILE, "utf8");
    const out = backportCancelCatch(code, FILE) as string;
    expect(out).not.toBeNull();
    expect(out.match(/\.catch\(onUnexpectedError\);/g)?.length).toBe(
      (code.match(/\.catch\(onUnexpectedError\);/g)?.length ?? 0) + 2,
    );
    expect(out).not.toContain("this.runDelayer.trigger(() => { this._run(); });");
    // Only the two calls differ.
    const removed = code.split("\n").filter((l) => !out.split("\n").includes(l));
    expect(removed.map((l) => l.trim())).toEqual([
      "this.runDelayer.trigger(() => { this._onPositionChanged(e); });",
      "this.runDelayer.trigger(() => { this._run(); });",
    ]);
  });

  it("leaves every other file alone", () => {
    expect(backportCancelCatch("x", "/node_modules/monaco-editor/esm/vs/other.js")).toBeNull();
  });

  it("refuses, loudly, a word highlighter that no longer reads as 0.52.2 does", () => {
    expect(() => backportCancelCatch("nothing to patch", FILE)).toThrow(/has changed/);
  });

  it("is for the installed monaco-editor, and refuses any other", () => {
    expect(() => assertPatchedMonaco(ROOT)).not.toThrow();
    const fake = fs.mkdtempSync(path.join(os.tmpdir(), "monaco-check-"));
    try {
      fs.mkdirSync(path.join(fake, "node_modules/monaco-editor"), { recursive: true });
      fs.writeFileSync(
        path.join(fake, "node_modules/monaco-editor/package.json"),
        JSON.stringify({ version: "0.57.0" }),
      );
      expect(() => assertPatchedMonaco(fake)).toThrow(
        new RegExp(`written for monaco-editor ${PATCHED_MONACO}`),
      );
    } finally {
      fs.rmSync(fake, { recursive: true, force: true });
    }
  });
});
