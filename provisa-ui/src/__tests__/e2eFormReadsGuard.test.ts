// Copyright (c) 2026 Kenneth Stott
// Canary: 5d2e8a41-9c7f-4b3e-a610-2f84c9d7b1e5
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// R510-B: e2e/source-to-query-helpers.ts's expectDiscoveredColumns and discoveredColumnCount read
// the columns of the table picked on the Register Table form from the server, for the source and
// schema chosen there. Those helpers serve specs that point the page at another backend with
// page.route(): the Trino lane's routeToTrinoBackend (source-to-query-olap-lake-trino.spec.ts) and
// engine-swap's reboot backend. page.route() rewrites only requests the page itself issues;
// `page.request` is a separate APIRequestContext it never intercepts. A form read made through
// `page.request` therefore asks the default backend, which has never heard of the source the page
// created on the routed one, and the Trino lane's pinot, hive_s3, hive and druid cases failed
// "no columns discovered" (release 510, run 43725839466). This guard reads the helper file and
// fails when a form read is made other than by the page's own fetch.

import { describe, expect, it } from "vitest";
import { readFileSync } from "node:fs";
import { resolve } from "node:path";

const HELPERS = resolve(__dirname, "../../e2e/source-to-query-helpers.ts");

/** The reads made for what the Register Table form shows. */
const FORM_READS = ["columnsOf", "availableSchemas"] as const;

/** The body of top-level function `name` in `text`: from its declaration to its closing brace. */
export function functionBody(text: string, name: string): string {
  const start = text.search(new RegExp(`\\n(?:export )?async function ${name}\\(`));
  if (start < 0) throw new Error(`no function ${name}`);
  const end = text.indexOf("\n}\n", start);
  if (end < 0) throw new Error(`function ${name} has no closing brace`);
  return text.slice(start, end + 3);
}

describe("e2e Register Table form reads go through the page", () => {
  const text = readFileSync(HELPERS, "utf8");

  it.each(FORM_READS)("%s never asks through page.request", (name) => {
    expect(functionBody(text, name)).not.toMatch(/\bpage\.request\b/);
  });

  it.each(FORM_READS)("%s reads through pageGraphql", (name) => {
    expect(functionBody(text, name)).toMatch(/\bpageGraphql\(/);
  });

  it("pageGraphql issues the request from the page itself", () => {
    const body = functionBody(text, "pageGraphql");
    expect(body).toMatch(/\bpage\.evaluate\(/);
    expect(body).toMatch(/\bfetch\("\/admin\/graphql"/);
    expect(body).not.toMatch(/\bpage\.request\b/);
  });

  it("functionBody finds a function and stops at its closing brace", () => {
    const sample = "\nasync function a(x) {\n  return x;\n}\n\nasync function b() {\n}\n";
    expect(functionBody(sample, "a")).toBe("\nasync function a(x) {\n  return x;\n}\n");
  });
});
