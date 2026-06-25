// Copyright (c) 2026 Kenneth Stott
// Canary: a3f7c2e1-d84b-4a19-b65c-2e0f9d1a7b38
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

import { execSync, spawnSync } from "child_process";
import { test, expect } from "./coverage";

const NEO4J_HTTP_PORT = 17474;
const NEO4J_BOLT_PORT = 17687;
const NEO4J_URL = `http://localhost:${NEO4J_HTTP_PORT}`;
const CONTAINER_NAME = "e2e-neo4j-community-export";
const EXPORT_BATCH = 200;

type ApiNode = { id: number; tableLabel: string; properties: Record<string, unknown> };
type ApiEdge = {
  identity: string;
  start: number;
  end: number;
  type: string;
  startNode: ApiNode;
  endNode: ApiNode;
};
type ExportNode = { id: number; tableLabel: string; properties: Record<string, unknown> };
type ExportEdge = { start: number; end: number; type: string; startNodeLabel: string; endNodeLabel: string };

async function waitForNeo4j(timeoutMs = 90_000): Promise<void> {
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    try {
      const res = await fetch(`${NEO4J_URL}/`);
      if (res.ok) return;
    } catch {
      // not ready yet
    }
    await new Promise((r) => setTimeout(r, 1500));
  }
  throw new Error(`Neo4j container did not become ready within ${timeoutMs}ms`);
}

/** Returns rows, or null if the query requires parameters or otherwise fails. */
async function tryCypherQuery(
  baseUrl: string,
  query: string,
): Promise<Array<Record<string, unknown>> | null> {
  try {
    const resp = await fetch(`${baseUrl}/data/cypher`, {
      method: "POST",
      headers: { "Content-Type": "application/json", "X-Role": "DEV" },
      body: JSON.stringify({ query }),
    });
    if (!resp.ok) return null;
    const body = await resp.json();
    if (body.error) return null;
    return body.rows ?? [];
  } catch {
    return null;
  }
}

function extractNodesAndEdges(
  rows: Array<Record<string, unknown>>,
  nodesById: Map<number, ExportNode>,
  edges: ExportEdge[],
): void {
  for (const row of rows) {
    for (const val of Object.values(row)) {
      if (!val || typeof val !== "object") continue;
      const v = val as Record<string, unknown>;
      if ("identity" in v && "startNode" in v && "endNode" in v) {
        const e = v as unknown as ApiEdge;
        edges.push({
          start: e.start,
          end: e.end,
          type: e.type,
          startNodeLabel: e.startNode.tableLabel,
          endNodeLabel: e.endNode.tableLabel,
        });
        for (const n of [e.startNode, e.endNode]) {
          if (!nodesById.has(n.id))
            nodesById.set(n.id, { id: n.id, tableLabel: n.tableLabel, properties: n.properties ?? {} });
        }
      } else if ("id" in v && "tableLabel" in v && typeof (v as ApiNode).id === "number") {
        const n = v as unknown as ApiNode;
        if (!nodesById.has(n.id))
          nodesById.set(n.id, { id: n.id, tableLabel: n.tableLabel, properties: n.properties ?? {} });
      }
    }
  }
}

test.beforeAll(() => {
  spawnSync("docker", ["rm", "-f", CONTAINER_NAME], { stdio: "pipe" });
  execSync(
    [
      "docker", "run", "-d",
      "--name", CONTAINER_NAME,
      "-p", `${NEO4J_HTTP_PORT}:7474`,
      "-p", `${NEO4J_BOLT_PORT}:7687`,
      "-e", "NEO4J_AUTH=none",
      "neo4j:5.19-community",
    ].join(" "),
    { stdio: "pipe" },
  );
});

test.afterAll(() => {
  spawnSync("docker", ["rm", "-f", CONTAINER_NAME], { stdio: "pipe" });
});

test("neo4j export: exports all queryable graph nodes and relationships to a community docker instance", async ({
  request,
}) => {
  test.setTimeout(300_000);

  await waitForNeo4j();

  // ── 1. Discover all node labels from the graph schema ─────────────────────
  const schemaResp = await request.get("http://localhost:8000/data/graph-schema");
  expect(schemaResp.status()).toBe(200);
  const schema = await schemaResp.json();
  const allLabels: string[] = (schema.node_labels ?? []).map(
    (n: { label: string }) => n.label.split(":")[1],
  );
  expect(allLabels.length, "schema must have node labels").toBeGreaterThan(0);

  // ── 2. Query every label; skip those that require WHERE clause parameters ──
  const nodesById = new Map<number, ExportNode>();
  const edges: ExportEdge[] = [];
  const BASE = "http://localhost:8000";

  for (const label of allLabels) {
    const rows = await tryCypherQuery(BASE, `MATCH (n:${label}) RETURN n`);
    if (rows !== null) extractNodesAndEdges(rows, nodesById, edges);
  }

  // ── 3. Query all relationships from schema ────────────────────────────────
  const relTypes: Array<{ type: string; source: string; target: string }> =
    schema.relationship_types ?? [];
  for (const rel of relTypes) {
    const relRows = await tryCypherQuery(
      BASE,
      `MATCH (a:${rel.source})-[r:${rel.type}]->(b:${rel.target}) RETURN a, r, b`,
    );
    if (relRows) extractNodesAndEdges(relRows, nodesById, edges);
  }

  const nodes = Array.from(nodesById.values());
  expect(nodes.length, "must have nodes to export").toBeGreaterThan(0);

  // ── 4. Export in batches to avoid Neo4j transaction timeout ───────────────
  const allErrors: string[] = [];

  for (let i = 0; i < nodes.length; i += EXPORT_BATCH) {
    const resp = await request.post("http://localhost:8000/data/neo4j-export", {
      data: {
        url: NEO4J_URL, username: "neo4j", password: "neo4j", database: "neo4j",
        nodes: nodes.slice(i, i + EXPORT_BATCH), edges: [],
      },
      headers: { "Content-Type": "application/json", "X-Role": "DEV" },
    });
    expect(resp.status(), "neo4j-export status").toBe(200);
    const body = await resp.json();
    allErrors.push(...(body.errors ?? []));
  }

  if (edges.length > 0) {
    const resp = await request.post("http://localhost:8000/data/neo4j-export", {
      data: {
        url: NEO4J_URL, username: "neo4j", password: "neo4j", database: "neo4j",
        nodes: [], edges,
      },
      headers: { "Content-Type": "application/json", "X-Role": "DEV" },
    });
    expect(resp.status(), "neo4j-export edge status").toBe(200);
    const body = await resp.json();
    allErrors.push(...(body.errors ?? []));
  }

  expect(allErrors, `neo4j-export errors: ${JSON.stringify(allErrors)}`).toHaveLength(0);

  // ── 5. Verify counts in Neo4j ─────────────────────────────────────────────
  const countResp = await fetch(`${NEO4J_URL}/db/neo4j/tx/commit`, {
    method: "POST",
    headers: { "Content-Type": "application/json", "Accept": "application/json" },
    body: JSON.stringify({
      statements: [
        { statement: "MATCH (n) RETURN count(n) AS nodeCount" },
        { statement: "MATCH ()-[r]->() RETURN count(r) AS relCount" },
      ],
    }),
  });
  expect(countResp.status, "neo4j count query must succeed").toBe(200);
  const countBody = (await countResp.json()) as {
    results: Array<{ data: Array<{ row: [number] }> }>;
    errors: Array<{ message: string }>;
  };
  expect(countBody.errors, `neo4j count errors: ${JSON.stringify(countBody.errors)}`).toHaveLength(0);

  const nodeCount = countBody.results[0]?.data[0]?.row[0] ?? 0;
  const relCount = countBody.results[1]?.data[0]?.row[0] ?? 0;

  // MERGE deduplicates on _provisa_id within a label, so nodeCount <= nodes.length
  expect(nodeCount, "neo4j must contain exported nodes").toBeGreaterThan(0);
  expect(nodeCount, "neo4j node count must not exceed exported count").toBeLessThanOrEqual(nodes.length);
  expect(relCount, "neo4j relationship count").toBe(edges.length);
});
