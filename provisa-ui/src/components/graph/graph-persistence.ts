// Copyright (c) 2026 Kenneth Stott
// Canary: 718a3333-de40-4539-97bd-f7bbc58a6280
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

import { useState, useCallback } from "react";
import type { ResultRedirect } from "../../types/resultRedirect";
import type { FrameData, FrameStatement, GNode, GEdge } from "./graph-model";
import { getStored, setStored } from "../../lib/browserStorage";

export type Favorite = { id: string; query: string; label: string; ts: number };

// ── localStorage-backed state ─────────────────────────────────────────────────
export function useLocalStorage<T>(
  key: string,
  initial: T,
): [T, (v: T | ((prev: T) => T)) => void] {
  const [value, setValue] = useState<T>(() => {
    try {
      const raw = getStored(key);
      return raw !== null ? (JSON.parse(raw) as T) : initial;
    } catch {
      return initial;
    }
  });

  const set = useCallback(
    (v: T | ((prev: T) => T)) => {
      setValue((prev) => {
        const next = typeof v === "function" ? (v as (p: T) => T)(prev) : v;
        try {
          setStored(key, JSON.stringify(next));
        } catch {
          /* quota */
        }
        return next;
      });
    },
    [key],
  );

  return [value, set];
}

// ── Module-level state (survives SPA navigation, persisted to localStorage) ───
/** The graph visualizer's query languages (REQ-2135): Cypher, and SPARQL with its own vocabulary. */
export type GraphLanguage = "cypher" | "sparql";

/** Each language's own frames, history and query, kept apart. */
const LS_KEY: Record<GraphLanguage, string> = {
  cypher: "provisa.graph.state",
  sparql: "provisa.sparql.state",
};
export const DEFAULT_QUERY: Record<GraphLanguage, string> = {
  cypher: "MATCH (n) RETURN n LIMIT 25",
  sparql: "SELECT * WHERE {\n  ?s ?p ?o\n}\nLIMIT 25",
};

/** A setting's key in the browser, kept apart per language: Cypher's as it always was. */
export const settingKey = (language: GraphLanguage, name: string): string =>
  language === "cypher" ? `provisa.graph.${name}` : `provisa.sparql.${name}`;

export interface GraphState {
  language: GraphLanguage;
  frames: FrameData[];
  history: string[];
  currentQuery: string;
}

interface SerializedFrame {
  id: string;
  query: string;
  status: "done" | "error";
  nodes: [string, GNode][];
  edges: [string, GEdge][];
  rows: Record<string, unknown>[];
  columns: string[];
  error?: string;
  redirect?: ResultRedirect;
  truncated?: boolean;
  elapsed?: number;
  pinned?: boolean;
  statements?: SerializedStatement[];
  statementIndex?: number;
}

interface SerializedStatement {
  query: string;
  status: "done" | "error";
  nodes: [string, GNode][];
  edges: [string, GEdge][];
  rows: Record<string, unknown>[];
  columns: string[];
  error?: string;
  redirect?: ResultRedirect;
  truncated?: boolean;
}

function serializeFrame(f: FrameData): SerializedFrame {
  return {
    id: f.id,
    query: f.query,
    status: f.status === "loading" ? "error" : f.status,
    error: f.status === "loading" ? "Session interrupted" : f.error,
    nodes: [...f.nodes.entries()],
    edges: [...f.edges.entries()],
    rows: f.rows,
    columns: f.columns,
    redirect: f.redirect,
    truncated: f.truncated,
    elapsed: f.elapsed,
    pinned: f.pinned,
    statements: f.statements?.map((p) => ({
      query: p.query,
      status: p.status,
      error: p.error,
      nodes: [...p.nodes.entries()],
      edges: [...p.edges.entries()],
      rows: p.rows,
      columns: p.columns,
      redirect: p.redirect,
      truncated: p.truncated,
    })),
    statementIndex: f.statementIndex,
  };
}

function deserializeFrame(s: SerializedFrame): FrameData {
  return {
    ...s,
    nodes: new Map(s.nodes),
    edges: new Map(s.edges),
    statements: s.statements?.map(
      (p): FrameStatement => ({ ...p, nodes: new Map(p.nodes), edges: new Map(p.edges) }),
    ),
  };
}

export function saveGraphState(state: GraphState): void {
  try {
    setStored(
      LS_KEY[state.language],
      JSON.stringify({
        frames: state.frames.map(serializeFrame),
        history: state.history,
        currentQuery: state.currentQuery,
      }),
    );
  } catch {
    /* quota */
  }
}

function loadGraphState(language: GraphLanguage): GraphState {
  const empty = { language, frames: [], history: [], currentQuery: DEFAULT_QUERY[language] };
  try {
    const raw = getStored(LS_KEY[language]);
    if (!raw) return empty;
    const s = JSON.parse(raw) as {
      frames?: SerializedFrame[];
      history?: string[];
      currentQuery?: string;
    };
    return {
      language,
      frames: (s.frames ?? []).map(deserializeFrame),
      history: s.history ?? [],
      currentQuery: s.currentQuery ?? DEFAULT_QUERY[language],
    };
  } catch {
    return empty;
  }
}

// Module-level, so each survives SPA navigation; read from the browser the first time asked for.
const states: Partial<Record<GraphLanguage, GraphState>> = {};

/** The language's state, loaded once. */
export function graphStateOf(language: GraphLanguage): GraphState {
  return (states[language] ??= loadGraphState(language));
}
