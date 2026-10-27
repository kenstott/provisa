// Copyright (c) 2026 Kenneth Stott
// Canary: 6e2b9d47-1a83-4c5f-b0e6-9d4a7c3f2e18
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-2167: Provisa's GraphQL over HTTP against the graphql-http server audit (the GraphQL over
// HTTP spec's own audit suite, npm graphql-http, MIT), installed at a pinned version, never
// vendored. Every audit's result is written: a MUST that fails is a failure; a SHOULD or MAY that
// fails is listed as a recommendation not met. A failure is data: the exit status is non-zero
// only when no audit ran.
//
// Usage: node graphql_http_audit.mjs <graphql-http dir> <version> <url> <role> <out.json> <out.md>

import { writeFileSync } from "node:fs";
import { join } from "node:path";
import { pathToFileURL } from "node:url";

const [dir, version, url, role, outJson, outMd] = process.argv.slice(2);
const { auditServer } = await import(
  pathToFileURL(join(dir, "node_modules", "graphql-http", "lib", "index.mjs")).href
);

// The acting role travels as a header (no auth provider): every audit request carries it.
const fetchFn = (input, init = {}) => {
  const headers = new Headers(init.headers ?? {});
  headers.set("x-provisa-role", role);
  return fetch(input, { ...init, headers });
};

const results = await auditServer({ url, fetchFn });
const level = (name) => (name.startsWith("MUST") ? "MUST" : name.startsWith("SHOULD") ? "SHOULD" : "MAY");
const must = results.filter((r) => level(r.name) === "MUST");
const passed = must.filter((r) => r.status === "ok").length;
const doc = {
  surface: "graphql",
  suite: "graphql-http server audit",
  version,
  total: results.length,
  ran: must.length,
  passed,
  failed: must.length - passed,
  pass_rate: must.length ? Math.round((passed / must.length) * 10000) / 10000 : null,
  recommendations_met: results.filter((r) => level(r.name) !== "MUST" && r.status === "ok").length,
  recommendations: results.filter((r) => level(r.name) !== "MUST").length,
  failures: results
    .filter((r) => r.status !== "ok")
    .map((r) => ({
      feature: level(r.name) === "MUST" ? "Requirements (MUST)" : `Recommendations (${level(r.name)})`,
      scenario: `${r.id} ${r.name}`,
      reason: String(r.reason ?? r.status),
    })),
};
writeFileSync(outJson, JSON.stringify(doc, null, 2) + "\n");
const rate = doc.pass_rate === null ? "n/a" : `${(doc.pass_rate * 100).toFixed(1)}%`;
const md = [
  `## GraphQL over HTTP conformance: ${doc.suite} ${version}`,
  "",
  `MUST audits passed ${passed} of ${must.length} (${rate}); recommendations met ${doc.recommendations_met} of ${doc.recommendations}.`,
  "",
  "| Level | Audit | Reason |",
  "| --- | --- | --- |",
  ...doc.failures.map((f) => `| ${f.feature} | ${f.scenario} | ${f.reason.replace(/\|/g, "/")} |`),
].join("\n");
writeFileSync(outMd, md + "\n");
process.exit(results.length ? 0 : 1);
