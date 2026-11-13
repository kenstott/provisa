// Copyright (c) 2026 Kenneth Stott
// Canary: 718a3333-de40-4539-97bd-f7bbc58a6280
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

import { useEffect, useState, useCallback, type Dispatch, type SetStateAction } from "react";
import type { ResultRedirect } from "../../types/resultRedirect";
import type { FrameData, FrameStatement, GNode, GEdge, SparqlForm } from "./graph-model";
import type { SelectPairing, Triple } from "./sparql";
import {
  getStoredJson,
  reportStorageFailure,
  setStored,
  storageKey,
} from "../../lib/browserStorage";
import { readBulk, writeBulk } from "../../lib/bulkStorage";
import { stableNodeIdsLoaded } from "./stableNodeId";

export type Favorite = { id: string; query: string; label: string; ts: number };

// ── localStorage-backed state ─────────────────────────────────────────────────
export function useLocalStorage<T>(
  key: string,
  initial: T,
): [T, (v: T | ((prev: T) => T)) => void] {
  const [value, setValue] = useState<T>(() => getStoredJson<T>(key) ?? initial);

  const set = useCallback(
    (v: T | ((prev: T) => T)) => {
      setValue((prev) => {
        const next = typeof v === "function" ? (v as (p: T) => T)(prev) : v;
        setStored(key, JSON.stringify(next));
        return next;
      });
    },
    [key],
  );

  return [value, set];
}

// ── Module-level state (survives SPA navigation, persisted to IndexedDB) ──────
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
  status: "done" | "error" | "stopped";
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
  sparqlForm?: SparqlForm;
  sparqlTriples?: Triple[];
  sparqlPairing?: SelectPairing;
  sparqlExpansions?: Record<string, Triple[]>;
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
    sparqlForm: f.sparqlForm,
    sparqlTriples: f.sparqlTriples,
    sparqlPairing: f.sparqlPairing,
    sparqlExpansions: f.sparqlExpansions,
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

type SavedGraphState = {
  frames?: SerializedFrame[];
  history?: string[];
  currentQuery?: string;
};

/** A state whose saved copy has not been read back yet: saving it now would overwrite that copy. */
const unread = new WeakSet<GraphState>();

export function saveGraphState(state: GraphState): void {
  if (unread.has(state)) return;
  const saved: SavedGraphState = {
    frames: state.frames.map(serializeFrame),
    history: state.history,
    currentQuery: state.currentQuery,
  };
  void writeBulk(LS_KEY[state.language], saved);
}

export async function loadGraphState(language: GraphLanguage): Promise<GraphState> {
  const s = await readBulk<SavedGraphState>(LS_KEY[language]);
  const empty = { language, frames: [], history: [], currentQuery: DEFAULT_QUERY[language] };
  try {
    return {
      ...empty,
      frames: (s?.frames ?? []).map(deserializeFrame),
      history: s?.history ?? [],
      currentQuery: s?.currentQuery ?? empty.currentQuery,
    };
  } catch (e) {
    reportStorageFailure("parse", LS_KEY[language], e);
    return empty;
  }
}

// Module-level, so each survives SPA navigation. Per scope, as everything saved is (REQ-2008).
const states = new Map<string, GraphState>();
const reads = new WeakMap<GraphState, Promise<void>>();
/** A state's saved copy as read, until the page on screen has joined it to what it shows. */
const readCopies = new WeakMap<GraphState, GraphState>();

/** The language's state: at once and empty the first time, for the page's first render, and
 * filled in by `readGraphState`. */
export function graphStateOf(language: GraphLanguage): GraphState {
  const at = storageKey(LS_KEY[language]);
  let state = states.get(at);
  if (!state) {
    state = { language, frames: [], history: [], currentQuery: DEFAULT_QUERY[language] };
    states.set(at, state);
    unread.add(state);
  }
  return state;
}

/** Read the state's saved copy into it, once. What the page did meanwhile stands: a frame run
 * comes ahead of the saved ones, and a query typed stays in the editor. */
export function readGraphState(state: GraphState): Promise<void> {
  let read = reads.get(state);
  if (!read) {
    read = Promise.all([loadGraphState(state.language), stableNodeIdsLoaded()]).then(([saved]) => {
      state.frames = [...state.frames, ...saved.frames];
      state.history = [...new Set([...state.history, ...saved.history])];
      if (state.currentQuery === DEFAULT_QUERY[state.language]) {
        state.currentQuery = saved.currentQuery;
      }
      readCopies.set(state, saved);
      unread.delete(state);
      saveGraphState(state);
    });
    reads.set(state, read);
  }
  return read;
}

/** Show the state's saved copy once it has been read: the frames, history and the query.
 *
 * The saved frames and history are joined to what the page holds when React applies the change,
 * not to a copy taken when the read lands: an answer the page has been handed but not yet shown
 * (its update still queued) would be lost under that copy, and its frame would stay "Running…". */
export function useSavedGraphState(
  state: GraphState,
  setFrames: Dispatch<SetStateAction<FrameData[]>>,
  setHistory: Dispatch<SetStateAction<string[]>>,
  setQuery: (query: string) => void,
): void {
  useEffect(() => {
    let live = true;
    const shown = state.currentQuery;
    void readGraphState(state).then(() => {
      if (!live) return;
      const saved = readCopies.get(state);
      readCopies.delete(state);
      if (saved) {
        setFrames((current) => {
          const held = new Set(current.map((f) => f.id));
          const next = [...current, ...saved.frames.filter((f) => !held.has(f.id))];
          state.frames = next;
          saveGraphState(state);
          return next;
        });
        setHistory((current) => {
          const next = [...new Set([...current, ...saved.history])];
          state.history = next;
          saveGraphState(state);
          return next;
        });
      }
      if (state.currentQuery !== shown) setQuery(state.currentQuery);
    });
    return () => {
      live = false;
    };
  }, [state, setFrames, setHistory, setQuery]);
}
