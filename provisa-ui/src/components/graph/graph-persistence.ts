// Copyright (c) 2026 Kenneth Stott
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

import { useState, useCallback } from "react";
import type { FrameData, GNode, GEdge } from "./graph-model";

// ── localStorage-backed state ─────────────────────────────────────────────────
export function useLocalStorage<T>(
  key: string,
  initial: T,
): [T, (v: T | ((prev: T) => T)) => void] {
  const [value, setValue] = useState<T>(() => {
    try {
      const raw = localStorage.getItem(key);
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
          localStorage.setItem(key, JSON.stringify(next));
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
const LS_KEY = "provisa.graph.state";
export const DEFAULT_QUERY = "MATCH (n) RETURN n LIMIT 25";

export interface GraphState {
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
  elapsed?: number;
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
    elapsed: f.elapsed,
  };
}

function deserializeFrame(s: SerializedFrame): FrameData {
  return { ...s, nodes: new Map(s.nodes), edges: new Map(s.edges) };
}

export function saveGraphState(state: GraphState): void {
  try {
    localStorage.setItem(
      LS_KEY,
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

function loadGraphState(): GraphState {
  try {
    const raw = localStorage.getItem(LS_KEY);
    if (!raw) return { frames: [], history: [], currentQuery: DEFAULT_QUERY };
    const s = JSON.parse(raw) as {
      frames?: SerializedFrame[];
      history?: string[];
      currentQuery?: string;
    };
    return {
      frames: (s.frames ?? []).map(deserializeFrame),
      history: s.history ?? [],
      currentQuery: s.currentQuery ?? DEFAULT_QUERY,
    };
  } catch {
    return { frames: [], history: [], currentQuery: DEFAULT_QUERY };
  }
}

export const graphState: GraphState = loadGraphState();
