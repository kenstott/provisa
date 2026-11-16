// Copyright (c) 2026 Kenneth Stott
// Canary: 8a3d1c57-2e64-4f0b-b9a8-6d0e5c2f7a19
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// R510-M: the e2e global setup "pre-warmed" the Vite dev server by fetching the page, which
// transforms no module, so the first test on each worker loaded the app cold (run 43860857546:
// 42 s before its first click, and the 120 s test timed out). The setup now walks the module
// graph (e2e/warm-vite.ts); these tests hold the walk and that the setup uses it.

import { readFileSync } from "node:fs";
import path from "node:path";
import { describe, expect, it } from "vitest";
import { importedUrls, warmViteModuleGraph, type ModuleFetch } from "../../e2e/warm-vite";

const UI = "http://127.0.0.1:3901";

function served(modules: Record<string, string>, asked: string[]): ModuleFetch {
  return async (url) => {
    asked.push(url);
    const pathAndQuery = url.slice(UI.length);
    const body = modules[pathAndQuery];
    const image = pathAndQuery.endsWith(".png");
    return {
      ok: body !== undefined,
      status: body === undefined ? 404 : 200,
      contentType: image ? "image/png" : "text/javascript",
      text: async () => body,
    };
  };
}

describe("importedUrls", () => {
  it("reads every import form the dev server writes", () => {
    const code = [
      'import __vite__cjsImport0_react from "/node_modules/.vite/deps/react.js?v=a27ee467";',
      'import { a,\n  b } from "/src/lib/a.ts";',
      'import * as ns from "/src/lib/ns.ts"',
      'import "/src/index.css";',
      'export * from "/src/lib/star.ts";',
      'export { x } from "./sibling.ts";',
      'const Page = lazy(() => (cov().f[3]++, import("/src/pages/SqlPage.tsx").then((m) => m)));',
      'import{c}from"/src/lib/c.ts";import{d}from"/src/lib/d.ts";',
    ].join("\n");
    expect(importedUrls(code, `${UI}/src/App.tsx`).sort()).toEqual(
      [
        "/src/index.css",
        "/src/lib/a.ts",
        "/src/lib/c.ts",
        "/src/lib/d.ts",
        "/src/lib/ns.ts",
        "/src/lib/star.ts",
        "/src/pages/SqlPage.tsx",
        "/src/sibling.ts",
      ].map((p) => UI + p),
    );
  });

  it("takes no specifier from text that is not an import", () => {
    const code = [
      'export const hint = "/src/not-a-module.ts";',
      'const text = "copied from \\"/etc/hosts\\"";',
      'import.meta.hot = __vite__createHotContext("/src/App.tsx");',
      'import x from "https://elsewhere.example/x.js";',
      "const dyn = import(somewhere);",
    ].join("\n");
    expect(importedUrls(code, `${UI}/src/App.tsx`)).toEqual([]);
  });
});

describe("warmViteModuleGraph", () => {
  it("fetches each reachable module once, through static and dynamic imports", async () => {
    const asked: string[] = [];
    const modules = {
      "/src/main.tsx": 'import App from "/src/App.tsx";\nimport "/src/index.css";',
      "/src/App.tsx":
        'import { u } from "/src/util.ts";\nimport logo from "/src/logo.png";\n' +
        'const P = () => import("/src/pages/A.tsx");\nconst Q = () => import("/src/pages/B.tsx");',
      "/src/index.css": "export default 'css'",
      "/src/util.ts": 'import React from "/node_modules/.vite/deps/react.js?v=1";',
      // not parsed: its body is not JavaScript
      "/src/logo.png": 'import "/src/never.ts";',
      "/src/pages/A.tsx": 'import { u } from "/src/util.ts";\nimport App from "/src/App.tsx";',
      "/src/pages/B.tsx": 'import { u } from "/src/util.ts";',
    };
    const fetched = await warmViteModuleGraph(UI, ["/src/main.tsx"], served(modules, asked), 2);
    expect(fetched).toBe(7);
    expect([...asked].sort()).toEqual(
      Object.keys(modules)
        .map((p) => UI + p)
        .sort(),
    );
  });

  it("fails on a module the server does not serve, naming it and its importer", async () => {
    const modules = { "/src/main.tsx": 'import gone from "/src/gone.ts";' };
    await expect(warmViteModuleGraph(UI, ["/src/main.tsx"], served(modules, []))).rejects.toThrow(
      `Vite dev server answered 404 for ${UI}/src/gone.ts, imported by ${UI}/src/main.tsx`,
    );
  });
});

describe("the e2e global setup", () => {
  const setup = readFileSync(path.resolve(__dirname, "../../e2e/global-setup.ts"), "utf8");

  it("warms the dev server by walking the app's module graph from its entry", () => {
    expect(setup).toMatch(
      /await warmViteModuleGraph\(UI_URL, \[[^\]]*"\/src\/main\.tsx"[^\]]*\]\)/,
    );
  });

  it("does not take a fetch of the page for a warm-up", () => {
    expect(setup).not.toMatch(/fetch\(UI_URL\b/);
  });

  it("names the entry index.html loads", () => {
    const html = readFileSync(path.resolve(__dirname, "../../index.html"), "utf8");
    expect(html).toContain('<script type="module" src="/src/main.tsx"></script>');
  });
});
