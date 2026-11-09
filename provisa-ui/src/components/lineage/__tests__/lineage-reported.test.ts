// Copyright (c) 2026 Kenneth Stott
// Canary: 4f8d2b17-93e6-4c05-a1d8-6e0b7c3f9a24
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-2229: what an OpenLineage event reported joins the graph as a job's box between the tables,
// a dataset that is not registered is named as outside the model, and a reported edge says so.

import { describe, expect, it } from "vitest";
import { buildLineageModel, edgeText } from "../lineage-layout";
import type { LineageGraphData } from "../../../api/lineage";

const JOB = "openlineage-job:airflow/load_mart";
const OUTSIDE = "outside:s3://archive/out.csv";

const GRAPH: LineageGraphData = {
  nodes: [
    {
      id: "sales.orders.amount",
      column: "amount",
      relation: "sales.orders",
      kind: "source",
      materialized: false,
    },
    {
      id: `${JOB}.*`,
      column: "*",
      relation: JOB,
      kind: "job",
      materialized: false,
      label: "load_mart",
    },
    {
      id: `${OUTSIDE}.*`,
      column: "*",
      relation: OUTSIDE,
      kind: "external",
      materialized: false,
      label: "s3://archive/out.csv",
    },
  ],
  edges: [
    {
      source: "sales.orders.amount",
      target: `${JOB}.*`,
      transform: "",
      ops: [],
      origin: "openlineage",
    },
    {
      source: `${JOB}.*`,
      target: `${OUTSIDE}.*`,
      transform: "",
      ops: [],
      origin: "openlineage",
    },
    {
      source: "sales.a.x",
      target: "sales.orders.amount",
      transform: "x",
      ops: [],
      origin: "definition",
    },
  ],
  outputs: [],
};

const WORDS = { job: "OpenLineage job", outside: "outside the model", reported: "reported" };
const model = () => buildLineageModel(GRAPH, undefined, { reportedWords: WORDS });

describe("what OpenLineage reported", () => {
  it("names a job's box by the job, and says what it is", () => {
    const box = model().boxes.find((b) => b.relation === JOB);
    expect(box?.label).toBe("load_mart · OpenLineage job");
    expect(box?.rows[0].role).toBe("job");
  });

  it("names a dataset that is not registered as outside the model", () => {
    const box = model().boxes.find((b) => b.relation === OUTSIDE);
    expect(box?.label).toBe("s3://archive/out.csv · outside the model");
    expect(box?.rows[0].role).toBe("external");
  });

  it("marks a reported edge, and only a reported edge", () => {
    const edges = model().edges;
    expect(edges.filter((e) => e.reported).map((e) => e.target)).toEqual([
      `${JOB}.*`,
      `${OUTSIDE}.*`,
    ]);
  });

  it("says on the edge that it was reported", () => {
    const edge = model().edges.find((e) => e.reported)!;
    expect(edgeText(edge, WORDS.reported)).toBe("reported");
    expect(edgeText({ ...edge, label: "x", reported: false }, WORDS.reported)).toBe("x");
  });

  it("keeps a reported mark when several columns roll up into one drawn edge", () => {
    const rolled = buildLineageModel(
      {
        ...GRAPH,
        edges: [
          {
            source: "sales.orders.amount",
            target: `${JOB}.*`,
            transform: "",
            ops: [],
            origin: "openlineage",
          },
          {
            source: "sales.orders.id",
            target: `${JOB}.*`,
            transform: "",
            ops: [],
            origin: "openlineage",
          },
        ],
        nodes: [
          ...GRAPH.nodes,
          {
            id: "sales.orders.id",
            column: "id",
            relation: "sales.orders",
            kind: "source",
            materialized: false,
          },
        ],
      },
      new Set(["sales.orders"]),
      { reportedWords: WORDS },
    );
    const edge = rolled.edges[0];
    expect(edge.count).toBe(2);
    expect(edge.reported).toBe(true);
    expect(edgeText(edge, WORDS.reported)).toBe("reported");
  });
});
