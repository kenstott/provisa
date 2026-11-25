// Copyright (c) 2026 Kenneth Stott
// Canary: b87093c2-d66c-4a50-83ea-3295269f4447
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// The Glossary page's per-viewer preferences, and its main panel: it reopens as it was left -- the
// graph, a term read or in its form, or with no term the stewardship summary. A link's query
// decides that visit instead and is not saved over; only the reader's own changes are.

import { useCallback, useEffect, useLayoutEffect, useRef, useState } from "react";
import { useSearchParams } from "react-router-dom";
import { rememberedDetail, useRememberedDetail } from "../list/useDetailParam";
import { getStored, setStored } from "../../lib/browserStorage";
import { listGlossaryTerms, type GlossaryTermSummary } from "../../api/glossary";
import { useNavPayload } from "../../hooks/useNavPayload";
import { useScreenInputsReader } from "../../mcp/screenState";
import {
  ONTOLOGY_SETTINGS,
  savedGraphSettings,
  writeGraphSettings,
} from "../../pages/tables/profile3d/graphSettings";

// Per-viewer list-filter preferences; never reachable from other viewers or the server.
export function readStoredBool(key: string, fallback: boolean): boolean {
  try {
    const raw = getStored(key);
    return raw === null ? fallback : raw === "true";
  } catch {
    return fallback;
  }
}

export function writeStoredBool(key: string, value: boolean): void {
  writeStored(key, String(value));
}

export function readStoredString(key: string): string | null {
  try {
    return getStored(key);
  } catch {
    return null;
  }
}

export function writeStored(key: string, value: string): void {
  try {
    setStored(key, value);
  } catch {
    // Private mode / storage disabled: the preference just doesn't persist this session.
  }
}

/**
 * REQ-2260: the ids of every term the viewer can read, unnarrowed by the search: what the term left
 * open is checked against before it is reopened; null until read. `read` reads the list (every
 * status) for a search: an unnarrowed read is the ids; under a search the unnarrowed list is read
 * too, once per domains shown.
 */
export function useReadableTermIds() {
  const [ids, setIds] = useState<string[] | null>(null);
  const scope = useRef<string | null>(null);
  const read = useCallback(
    async (query: string, domains: string[] | null): Promise<GlossaryTermSummary[]> => {
      const listed = await listGlossaryTerms(query, true, domains);
      const at = JSON.stringify(domains);
      if (query === "" || scope.current !== at) {
        const readable = query === "" ? listed : await listGlossaryTerms("", true, domains);
        scope.current = at;
        setIds(readable.map((term) => String(term.id)));
      }
      return listed;
    },
    [],
  );
  return [ids, read] as const;
}

/** REQ-2277: the page's tabs -- the term list, the Ontology View (REQ-2130) and, for a curator,
 * the ontology imports (REQ-2237) -- named in the URL's ?panel= (the terms: none). */
export const GLOSSARY_VIEWS = ["terms", "graph", "imports"] as const;
export type GlossaryView = (typeof GLOSSARY_VIEWS)[number];

function viewNamed(param: string | null): GlossaryView | null {
  return (GLOSSARY_VIEWS as readonly string[]).includes(param ?? "")
    ? (param as GlossaryView)
    : null;
}

/**
 * `listed`: the ids of every term the viewer can read, unnarrowed by the search; `null` while
 * they load.
 */
export function useGlossaryPanel(canEdit: boolean, listed: readonly string[] | null) {
  // REQ-1387, REQ-2241: the selected term is the URL's ?term= (the shared detail parameter), so a
  // relationship link, a shared URL, or Back and Forward all show the term it names.
  const [searchParams] = useSearchParams();
  const namedView = viewNamed(searchParams.get("panel"));
  // A visit naming nothing reopens the view it was left on: the graph is written into the URL as
  // the page arrives. A ?term= link opens the term, so it opens the list.
  const [restoringGraph, setRestoringGraph] = useState(
    () =>
      searchParams.toString() === "" && readStoredString("glossary.view") === "ontology",
  );
  const view: GlossaryView = namedView ?? (restoringGraph ? "graph" : "terms");
  const ontology = view !== "terms";
  // REQ-2260: a visit naming no term reopens the term left open (the shared memory of list pages),
  // once the terms are read; under the graph it waits, as the graph holds the page's place.
  const detail = useRememberedDetail("ontology", ontology ? null : listed);
  const { setParam } = detail;
  useLayoutEffect(() => {
    if (restoringGraph) setParam("panel", "graph", { replace: true });
  }, [restoringGraph, setParam]);
  // Once the URL names a view it decides from then on.
  if (restoringGraph && namedView !== null) setRestoringGraph(false);
  const { open: openTerm, close: closeTerm } = detail;
  // The term the page reopens: it is the page's term once the terms read hold it, while the URL is
  // given it.
  const [reopened] = useState(() =>
    searchParams.toString() === "" && !ontology ? rememberedDetail("ontology") : null,
  );
  // The term last open: under the graph a term is not in the URL (reloading opens what was left,
  // the graph), and back to the list it is open again.
  const [held, setHeld] = useState<string | null>(
    () => detail.id ?? (searchParams.toString() === "" ? rememberedDetail("ontology") : null),
  );
  if (detail.id !== null && detail.id !== held) setHeld(detail.id);
  // A reopened term the terms read do not hold is not reopened (useRememberedDetail forgets it).
  const reopenHeld =
    held === reopened && reopened !== null && listed !== null && listed.includes(reopened);
  const named = ontology ? held : (detail.id ?? (reopenHeld ? reopened : null));
  const parsed = named === null ? NaN : Number(named);
  const selectedId = Number.isFinite(parsed) ? parsed : null;
  const setSelectedId = useCallback(
    (id: number | null) => {
      setHeld(id === null ? null : String(id));
      if (id === null) closeTerm();
      else openTerm(String(id));
    },
    [openTerm, closeTerm],
  );
  // A tab chosen is one history entry: the panel named, and the term set aside under the graph or
  // the imports, open again back on the list.
  const setView = useCallback(
    (next: GlossaryView) => {
      if (next === view) return;
      if (next !== "imports") writeStored("glossary.view", next === "graph" ? "ontology" : "list");
      setParam("panel", next === "terms" ? null : next);
      if (next !== "terms") closeTerm({ replace: true, keep: true });
      else if (held !== null) openTerm(held, { replace: true });
    },
    [view, setParam, closeTerm, openTerm, held],
  );
  // The term open as one form (TermEditForm), or read. A URL's ?term= naming the remembered term
  // is that term reloaded, so its form reopens too.
  const [editing, setEditingState] = useState(() => {
    const opening = detail.id ?? reopened;
    return (
      canEdit &&
      opening !== null &&
      rememberedDetail("ontology") === opening &&
      readStoredBool("glossary.editing", false)
    );
  });
  const setEditing = useCallback((on: boolean) => {
    setEditingState(on);
    writeStoredBool("glossary.editing", on);
  }, []);

  // Polly's navigate: which panel, which term, and the graph's own controls.
  useNavPayload<{ panel?: "graph" | "terms"; term?: number; graph?: Record<string, unknown> }>(
    (payload) => {
      if (payload.graph) writeGraphSettings(ONTOLOGY_SETTINGS, payload.graph);
      if (payload.term !== undefined) {
        // The term opens on the list, in one write with its panel.
        setParam("panel", null);
        setSelectedId(payload.term);
      } else if (payload.panel) setView(payload.panel);
    },
  );

  // REQ-2115: Polly's read_screen reads the page's main panel, and the graph's controls as they
  // are remembered, from here.
  const readInputs = useCallback(() => {
    const { flat, layout, preset, layers, domains, edgeTypes } =
      savedGraphSettings(ONTOLOGY_SETTINGS);
    return {
      panel: view,
      term: selectedId,
      editing,
      graph: { flat, layout, preset, layers, domains, edgeTypes },
    };
  }, [view, selectedId, editing]);
  useScreenInputsReader("/context/ontology", readInputs);

  // Each term opens read, not in the form the previous term was left in; the term the page opened
  // on keeps its remembered form. Deferred for the same set-state-in-effect rule as the list
  // refresh in GlossaryTab.
  const termOpened = useRef(selectedId ?? (reopened === null ? null : Number(reopened)));
  useEffect(() => {
    // Waiting for the terms read to reopen the remembered term is no change of term; closing is.
    if (selectedId === null) {
      if (held !== reopened) termOpened.current = null;
      return;
    }
    if (termOpened.current === selectedId) return;
    termOpened.current = selectedId;
    const timer = window.setTimeout(() => setEditing(false), 0);
    return () => window.clearTimeout(timer);
  }, [selectedId, setEditing, held, reopened]);

  return { view, setView, selectedId, setSelectedId, editing, setEditing };
}
