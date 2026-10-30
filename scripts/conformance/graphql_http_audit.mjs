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
// vendored. Every audit is a case, passed, skipped by design or failed (REQ-2167, amended
// 2026-10-28), whatever its level (MUST, SHOULD or MAY): an audit whose status is not "ok" is
// failed, since no by-design group is declared for GraphQL over HTTP (scripts/conformance/by_design
// has none), so none is skipped by design. Each failure names its level. A failure is data: the
// exit status is non-zero only when no audit ran.
//
// Usage: node graphql_http_audit.mjs <graphql-http dir> <version> <url> <role> <out.json> <out.md>

import { writeFileSync } from "node:fs";
import { join } from "node:path";
import { pathToFileURL } from "node:url";

const [dir, version, url, role, authorization, outJson, outMd] = process.argv.slice(2);
const { auditServer } = await import(
  pathToFileURL(join(dir, "node_modules", "graphql-http", "lib", "index.mjs")).href
);

// Every audit request presents the job user's credential and the role it acts as.
const fetchFn = (input, init = {}) => {
  const headers = new Headers(init.headers ?? {});
  headers.set("authorization", authorization);
  headers.set("x-provisa-role", role);
  return fetch(input, { ...init, headers });
};

const results = await auditServer({ url, fetchFn });
const level = (name) => (name.startsWith("MUST") ? "MUST" : name.startsWith("SHOULD") ? "SHOULD" : "MAY");
const must = results.filter((r) => level(r.name) === "MUST");
const passed = results.filter((r) => r.status === "ok").length;
const failed = results.length - passed;
const doc = {
  surface: "graphql",
  suite: "graphql-http server audit",
  version,
  total: results.length,
  passed,
  by_design: 0,
  failed,
  ran: passed + failed,
  pass_rate: passed + failed ? Math.round((passed / (passed + failed)) * 10000) / 10000 : null,
  by_design_groups: [],
  must_passed: must.filter((r) => r.status === "ok").length,
  must: must.length,
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
  `${doc.total} audits: ${passed} passed, ${doc.by_design} skipped by design, ${failed} failed. Pass rate ${rate} (passed over passed and failed). MUST audits passed: ${doc.must_passed} of ${doc.must}.`,
  "",
  "### Failed",
  "",
  "| Level | Audit | Reason |",
  "| --- | --- | --- |",
  ...doc.failures.map((f) => `| ${f.feature} | ${f.scenario} | ${f.reason.replace(/\|/g, "/")} |`),
].join("\n");
writeFileSync(outMd, md + "\n");
process.exit(results.length ? 0 : 1);
