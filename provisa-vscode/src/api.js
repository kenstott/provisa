// Copyright (c) 2026 Kenneth Stott
// Canary: 48a73df4-d112-4679-878c-59395dd902b7
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-2112: the extension's calls to a Provisa instance, each one the Provisa UI makes, with no
// backend of its own: the admin GraphQL source mutations the Sources page sends, and the code-host
// routes its Build image panel calls (REQ-2098, provisa/api/admin/code_hosts_router.py,
// provisa-ui/src/api/codeHosts.ts). Signed in with a personal access token, as any API client.

"use strict";

/** The header the UI names the organization by (provisa-ui/src/lib/authFetch.ts ORG_HEADER). */
const ORG_HEADER = "X-Org-Provisa";

/** A refusal's text: the server's message, as the UI reads it (serverMessage.ts). */
function refusal(body, fallback) {
  if (body && typeof body === "object") {
    if (typeof body.message === "string" && body.message) return body.message;
    if (typeof body.detail === "string" && body.detail) return body.detail;
    if (body.error && typeof body.error.message === "string") return body.error.message;
  }
  return fallback;
}

class ProvisaClient {
  /** `instance` is {url, org}; `token` the personal access token it is signed in with. */
  constructor(instance, token, fetchImpl) {
    if (!instance.url) throw new Error("the Provisa instance has no url");
    if (!instance.org) throw new Error(`the Provisa instance ${instance.url} names no org`);
    if (!token) throw new Error(`not signed in to ${instance.url}: run Provisa: Sign In`);
    this.url = instance.url.replace(/\/+$/, "");
    this.org = instance.org;
    this.token = token;
    this.fetch = fetchImpl;
  }

  async request(method, path, body) {
    const headers = { Authorization: `Bearer ${this.token}`, [ORG_HEADER]: this.org };
    if (body !== undefined) headers["Content-Type"] = "application/json";
    const res = await this.fetch(`${this.url}${path}`, {
      method,
      headers,
      body: body === undefined ? undefined : JSON.stringify(body),
    });
    const text = await res.text();
    const parsed = text ? JSON.parse(text) : null;
    if (!res.ok) throw new Error(refusal(parsed, `${method} ${path} failed (${res.status})`));
    return parsed;
  }

  async graphql(query, variables) {
    const answer = await this.request("POST", "/admin/graphql", { query, variables });
    if (answer.errors && answer.errors.length) throw new Error(answer.errors.map((e) => e.message).join("; "));
    return answer.data;
  }

  codeSourcePath(sourceId) {
    return `/admin/orgs/${encodeURIComponent(this.org)}/code-sources/${encodeURIComponent(sourceId)}`;
  }

  async sources() {
    const data = await this.graphql("query { sources { id type mappingJson } }", {});
    return data.sources;
  }

  async createSource(input) {
    const data = await this.graphql(
      "mutation CreateSource($input: SourceInput!) { createSource(input: $input) { success message } }",
      { input },
    );
    if (!data.createSource.success) throw new Error(data.createSource.message);
  }

  async registries() {
    const answer = await this.request("GET", `/admin/orgs/${encodeURIComponent(this.org)}/container-registries`);
    return answer.registries;
  }

  async startBuild(sourceId, choice) {
    return this.request("POST", `${this.codeSourcePath(sourceId)}/builds`, choice);
  }

  async build(sourceId, buildId) {
    return this.request("GET", `${this.codeSourcePath(sourceId)}/builds/${encodeURIComponent(buildId)}`);
  }
}

module.exports = { ProvisaClient, ORG_HEADER };
