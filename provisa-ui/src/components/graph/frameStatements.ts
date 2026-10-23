// Copyright (c) 2026 Kenneth Stott
// Canary: 13a79993-d251-42c5-8b16-4a7a291df07c
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// A Cypher query holding several statements, answered one result per statement (/data/cypher with
// "results": "each"): each one's drawing, rows, or why it failed, the frame showing one at a time.

import { toResultRedirect } from "../../api/admin";
import { requestFailed, serverMessage } from "../../i18n/serverMessage";
import { drawnElements } from "./graph-drawing";
import type { FrameData, FrameStatement } from "./graph-model";

/** One statement's entry: the status and body /data/cypher answers it with alone. */
export interface CypherAnswer {
  statement: string;
  status: number;
  body: {
    rows?: Record<string, unknown>[];
    columns?: string[];
    graph_columns?: Record<string, string>;
    provisa_stats?: unknown;
    redirect?: Parameters<typeof toResultRedirect>[0];
    error?: string;
    code?: string;
    message?: string;
    detail?: string;
  };
}

type Clusters = Record<string, { scl1: number | null; scl2: number | null; scl3: number | null }>;

export function frameStatement(
  a: CypherAnswer,
  maxNodes: number,
  clusters: Clusters,
): FrameStatement {
  const empty = { nodes: new Map(), edges: new Map(), rows: [], columns: [] };
  if (a.status !== 200)
    return {
      ...empty,
      query: a.statement,
      status: "error",
      // Cypher says its refusals as `error`; the app's own handlers by code and message.
      error: a.body.error ?? serverMessage(a.body, requestFailed("cypher", a.status)),
    };
  // A result that returns nodes, relationships or paths is drawn, up to max nodes; one that
  // returns rows only is a table (the server says which, from the translation).
  const { nodes, edges, truncated } = drawnElements(a.body, maxNodes);
  nodes.forEach((node) => {
    const c = clusters[node.label];
    if (c) Object.assign(node.properties, { scl1: c.scl1, scl2: c.scl2, scl3: c.scl3 });
  });
  return {
    query: a.statement,
    status: "done",
    nodes,
    edges,
    rows: a.body.rows ?? [],
    columns: a.body.columns ?? [],
    // REQ-1224: a result over the large-result threshold comes back as a handle to the file it
    // was delivered to, with no rows; the frame says so rather than drawing nothing.
    redirect: a.body.redirect ? toResultRedirect(a.body.redirect) : undefined,
    truncated,
    queryStats: a.body.provisa_stats,
  };
}

/** What the frame shows of one statement: its result, under the frame's own query. */
export function showing(st: FrameStatement): Partial<FrameData> {
  // Every field, so one statement's error or redirect does not stay on another's.
  return {
    status: st.status,
    nodes: st.nodes,
    edges: st.edges,
    rows: st.rows,
    columns: st.columns,
    error: st.error,
    redirect: st.redirect,
    truncated: st.truncated,
    queryStats: st.queryStats,
  };
}

/** The label a statement matches first (`Pets` of `MATCH (p:Pets)`), to tell its result apart;
 * null when it names none. */
export function statementName(statement: string): string | null {
  const m = statement.match(/\(\s*\w*\s*:\s*`?([\w]+)`?/);
  return m ? m[1] : null;
}
