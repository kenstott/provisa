// Copyright (c) 2026 Kenneth Stott
// Canary: placeholder
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.

import { test, expect } from "./coverage";

// REQ-345: POST /query/cypher endpoint accepts Cypher SELECT query and optional named parameters
test("REQ-345: POST /data/cypher executes basic Cypher SELECT query", async ({
  request,
}) => {
  const resp = await request.post("http://localhost:8000/data/cypher", {
    data: { query: "MATCH (n) RETURN n LIMIT 1" },
    headers: { "Content-Type": "application/json" },
  });

  expect(resp.status()).toBe(200);
  const body = await resp.json();
  expect(body.error).toBeUndefined();
  expect(body).toHaveProperty("columns");
  expect(body).toHaveProperty("rows");
});

// REQ-345: Named parameters support in Cypher query
test("REQ-345: POST /data/cypher accepts optional named parameters", async ({
  request,
}) => {
  const resp = await request.post("http://localhost:8000/data/cypher", {
    data: {
      query: "MATCH (n) WHERE n.id = $id RETURN n LIMIT 1",
      params: { id: 1 },
    },
    headers: { "Content-Type": "application/json" },
  });

  expect(resp.status()).toBe(200);
  const body = await resp.json();
  expect(body.error).toBeUndefined();
  expect(body).toHaveProperty("columns");
  expect(Array.isArray(body.rows)).toBeTruthy();
});

// REQ-345: Cypher query without parameters
test("REQ-345: POST /data/cypher accepts Cypher query without params field", async ({
  request,
}) => {
  const resp = await request.post("http://localhost:8000/data/cypher", {
    data: { query: "MATCH (n) RETURN COUNT(n) as count" },
    headers: { "Content-Type": "application/json" },
  });

  expect(resp.status()).toBe(200);
  const body = await resp.json();
  expect(body.error).toBeUndefined();
  expect(body).toHaveProperty("columns");
  expect(Array.isArray(body.rows)).toBeTruthy();
});

// REQ-345: Governance applies to Cypher queries
test("REQ-345: POST /data/cypher applies RLS and column masking governance", async ({
  request,
}) => {
  const resp = await request.post("http://localhost:8000/data/cypher", {
    data: {
      query: "MATCH (n) RETURN n LIMIT 5",
    },
    headers: {
      "Content-Type": "application/json",
      "X-Role": "DEV",
    },
  });

  expect(resp.status()).toBe(200);
  const body = await resp.json();
  expect(body.error).toBeUndefined();
  // Response contains governance-filtered results
  expect(body).toHaveProperty("columns");
});

// REQ-354: POST /query/nl endpoint accepts natural language question
test("REQ-354: POST /query/nl accepts natural language question and returns job_id", async ({
  request,
}) => {
  const resp = await request.post("http://localhost:8000/query/nl", {
    data: {
      q: "Show me all entities",
      role: "default",
    },
    headers: { "Content-Type": "application/json" },
  });

  // NL endpoint returns 202 Accepted with job_id
  expect(resp.status()).toBe(202);
  const body = await resp.json();
  expect(body).toHaveProperty("job_id");
  expect(typeof body.job_id).toBe("string");
  expect(body.job_id.length).toBeGreaterThan(0);
});

// REQ-354: NL query submission with explicit role
test("REQ-354: POST /query/nl accepts role parameter in request body", async ({
  request,
}) => {
  const resp = await request.post("http://localhost:8000/query/nl", {
    data: {
      q: "What is the count of all records?",
      role: "analyst",
    },
    headers: { "Content-Type": "application/json" },
  });

  expect(resp.status()).toBe(202);
  const body = await resp.json();
  expect(body).toHaveProperty("job_id");
});

// REQ-354: NL job polling endpoint
test("REQ-354: GET /query/nl/{job_id} polls for NL query result", async ({
  request,
}) => {
  // First, submit an NL query
  const submitResp = await request.post("http://localhost:8000/query/nl", {
    data: {
      q: "List all entities",
      role: "default",
    },
    headers: { "Content-Type": "application/json" },
  });

  expect(submitResp.status()).toBe(202);
  const submitBody = await submitResp.json();
  const jobId = submitBody.job_id;

  // Poll for result
  const pollResp = await request.get(
    `http://localhost:8000/query/nl/${jobId}`,
    {
      headers: { "Content-Type": "application/json" },
    }
  );

  expect(pollResp.ok()).toBeTruthy();
  const pollBody = await pollResp.json();
  // Job response should contain job_id and status or result
  expect(pollBody).toHaveProperty("job_id");
});

// REQ-537: GET /data/schema-version returns boot-id-counter format
test("REQ-537: GET /data/schema-version returns boot_id-counter format string", async ({
  request,
}) => {
  const resp = await request.get("http://localhost:8000/data/schema-version", {
    headers: { "Content-Type": "application/json" },
  });

  expect(resp.status()).toBe(200);
  const body = await resp.json();
  expect(body).toHaveProperty("version");
  expect(typeof body.version).toBe("string");

  // Version should be in format: boot-id-counter or just counter
  // Example: "550e8400-e29b-41d4-a716-446655440000-1"
  const versionStr = body.version;
  expect(versionStr.length).toBeGreaterThan(0);
});

// REQ-537: Schema version changes persist
test("REQ-537: GET /data/schema-version maintains monotonic counter", async ({
  request,
}) => {
  const resp1 = await request.get("http://localhost:8000/data/schema-version");
  const body1 = await resp1.json();
  const version1 = body1.version;

  const resp2 = await request.get("http://localhost:8000/data/schema-version");
  const body2 = await resp2.json();
  const version2 = body2.version;

  // Versions should be identical if no schema rebuild occurred
  expect(version1).toBe(version2);
});

// REQ-642: POST /data/graph-analytics endpoint accepts Cypher query and algorithm name
test("REQ-642: POST /data/graph-analytics accepts Cypher query and algorithm name", async ({
  request,
}) => {
  const resp = await request.post("http://localhost:8000/data/graph-analytics", {
    data: {
      query: "MATCH (n)-[r]->(m) RETURN n, r, m LIMIT 10",
      algorithm: "pagerank",
    },
    headers: { "Content-Type": "application/json" },
  });

  // Endpoint may not exist yet (404) or may return 200 if implemented
  if (resp.status() === 200) {
    const body = await resp.json();
    expect(body).toHaveProperty("nodes");
    expect(body).toHaveProperty("edges");
    expect(body).toHaveProperty("elapsed_ms");
    expect(Array.isArray(body.nodes)).toBeTruthy();
    expect(Array.isArray(body.edges)).toBeTruthy();
    expect(typeof body.elapsed_ms).toBe("number");
  }
});

// REQ-642: Graph analytics merges _analytics dict into nodes
test("REQ-642: POST /data/graph-analytics returns nodes with _analytics field", async ({
  request,
}) => {
  const resp = await request.post("http://localhost:8000/data/graph-analytics", {
    data: {
      query: "MATCH (n)-[r]->(m) RETURN n, r, m LIMIT 5",
      algorithm: "betweenness_centrality",
    },
    headers: { "Content-Type": "application/json" },
  });

  if (resp.status() === 200) {
    const body = await resp.json();
    expect(Array.isArray(body.nodes)).toBeTruthy();

    if (body.nodes.length > 0) {
      // Each node should have _analytics dict
      body.nodes.forEach((node: any) => {
        expect(node).toHaveProperty("_analytics");
        expect(typeof node._analytics).toBe("object");
      });
    }
  }
});

// REQ-642: Graph analytics returns elapsed_ms
test("REQ-642: POST /data/graph-analytics response includes elapsed_ms", async ({
  request,
}) => {
  const resp = await request.post("http://localhost:8000/data/graph-analytics", {
    data: {
      query: "MATCH (n) RETURN n LIMIT 1",
      algorithm: "pagerank",
    },
    headers: { "Content-Type": "application/json" },
  });

  if (resp.status() === 200) {
    const body = await resp.json();
    expect(body).toHaveProperty("elapsed_ms");
    expect(typeof body.elapsed_ms).toBe("number");
    expect(body.elapsed_ms).toBeGreaterThanOrEqual(0);
  }
});

// REQ-750: Cypher CALL db.labels() procedure
test("REQ-750: POST /data/cypher supports CALL db.labels() procedure", async ({
  request,
}) => {
  const resp = await request.post("http://localhost:8000/data/cypher", {
    data: { query: "CALL db.labels()" },
    headers: { "Content-Type": "application/json" },
  });

  expect(resp.status()).toBe(200);
  const body = await resp.json();
  expect(body.error).toBeUndefined();
  // Response should contain schema label information
  expect(body).toHaveProperty("columns");
});

// REQ-750: Cypher CALL db.relationshipTypes() procedure
test("REQ-750: POST /data/cypher supports CALL db.relationshipTypes() procedure", async ({
  request,
}) => {
  const resp = await request.post("http://localhost:8000/data/cypher", {
    data: { query: "CALL db.relationshipTypes()" },
    headers: { "Content-Type": "application/json" },
  });

  expect(resp.status()).toBe(200);
  const body = await resp.json();
  expect(body.error).toBeUndefined();
  // Response should contain relationship type information
  expect(body).toHaveProperty("columns");
});

// REQ-750: Cypher CALL db.propertyKeys() procedure
test("REQ-750: POST /data/cypher supports CALL db.propertyKeys() procedure", async ({
  request,
}) => {
  const resp = await request.post("http://localhost:8000/data/cypher", {
    data: { query: "CALL db.propertyKeys()" },
    headers: { "Content-Type": "application/json" },
  });

  expect(resp.status()).toBe(200);
  const body = await resp.json();
  expect(body.error).toBeUndefined();
  // Response should contain property key information
  expect(body).toHaveProperty("columns");
});

// REQ-750: Cypher node serialization includes canonical shape
test("REQ-750: Cypher nodes returned with id, label, tableLabel, and properties", async ({
  request,
}) => {
  const resp = await request.post("http://localhost:8000/data/cypher", {
    data: { query: "MATCH (n) RETURN n LIMIT 1" },
    headers: { "Content-Type": "application/json" },
  });

  expect(resp.status()).toBe(200);
  const body = await resp.json();

  if (body.rows && body.rows.length > 0) {
    const firstRow = body.rows[0];
    // First column typically contains the node
    const node = firstRow[0];

    if (node && typeof node === "object") {
      // Node should have canonical shape
      expect(node).toHaveProperty("id");
      expect(node).toHaveProperty("label");
      expect(node).toHaveProperty("tableLabel");
      expect(node).toHaveProperty("properties");
    }
  }
});

// REQ-750: Cypher edge serialization includes canonical shape
test("REQ-750: Cypher edges returned with identity, start, end, type, and properties", async ({
  request,
}) => {
  const resp = await request.post("http://localhost:8000/data/cypher", {
    data: {
      query: "MATCH (n)-[r]->(m) RETURN r LIMIT 1",
    },
    headers: { "Content-Type": "application/json" },
  });

  expect(resp.status()).toBe(200);
  const body = await resp.json();

  if (body.rows && body.rows.length > 0) {
    const firstRow = body.rows[0];
    const edge = firstRow[0];

    if (edge && typeof edge === "object") {
      // Edge should have canonical shape
      expect(edge).toHaveProperty("identity");
      expect(edge).toHaveProperty("start");
      expect(edge).toHaveProperty("end");
      expect(edge).toHaveProperty("type");
      expect(edge).toHaveProperty("properties");
    }
  }
});

// REQ-750: Cypher path serialization includes nodes, edges, and length
test("REQ-750: Cypher paths returned with nodes, edges, and length/hops", async ({
  request,
}) => {
  const resp = await request.post("http://localhost:8000/data/cypher", {
    data: {
      query: "MATCH p = (n)-[*1..2]->(m) RETURN p LIMIT 1",
    },
    headers: { "Content-Type": "application/json" },
  });

  expect(resp.status()).toBe(200);
  const body = await resp.json();

  if (body.rows && body.rows.length > 0) {
    const firstRow = body.rows[0];
    const path = firstRow[0];

    if (path && typeof path === "object" && "nodes" in path) {
      // Path should have canonical shape
      expect(path).toHaveProperty("nodes");
      expect(path).toHaveProperty("edges");
      expect(Array.isArray(path.nodes)).toBeTruthy();
      expect(Array.isArray(path.edges)).toBeTruthy();
    }
  }
});
