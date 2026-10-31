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

import { useCallback, useEffect, useRef, useState } from "react";
import { useSearchParams } from "react-router-dom";
import { getStored, setStored } from "../../lib/browserStorage";

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

export function useGlossaryPanel(canEdit: boolean) {
  // REQ-1387: the selected term is mirrored to the ?term= query param, so a relationship link,
  // a shared URL, or the back/forward buttons all resolve to the same term this state does.
  const [searchParams, setSearchParams] = useSearchParams();
  // REQ-2130: the Ontology View, in place of the term list while it is open. A ?term= link opens
  // the term, so it opens the list.
  const [ontology, setOntologyState] = useState(
    () => !searchParams.has("term") && readStoredString("glossary.view") === "ontology",
  );
  const setOntology = (open: boolean) => {
    setOntologyState(open);
    writeStored("glossary.view", open ? "ontology" : "list");
  };
  const [selectedId, setSelectedIdState] = useState<number | null>(() => {
    const raw =
      searchParams.get("term") ??
      (searchParams.toString() === "" ? readStoredString("glossary.term") : null);
    const parsed = raw ? Number(raw) : NaN;
    return Number.isFinite(parsed) ? parsed : null;
  });
  const setSelectedId = useCallback((id: number | null) => {
    setSelectedIdState(id);
    writeStored("glossary.term", id === null ? "" : String(id));
  }, []);
  // The term open as one form (TermEditForm), or read. A URL's ?term= naming the remembered term
  // is that term reloaded, so its form reopens too.
  const [editing, setEditingState] = useState(
    () =>
      canEdit &&
      selectedId !== null &&
      readStoredString("glossary.term") === String(selectedId) &&
      readStoredBool("glossary.editing", false),
  );
  const setEditing = useCallback((on: boolean) => {
    setEditingState(on);
    writeStoredBool("glossary.editing", on);
  }, []);

  // Each term opens read, not in the form the previous term was left in; the term the page opened
  // on keeps its remembered form. Deferred for the same set-state-in-effect rule as the list
  // refresh in GlossaryTab.
  const termOpened = useRef(selectedId);
  useEffect(() => {
    if (termOpened.current === selectedId) return;
    termOpened.current = selectedId;
    const timer = window.setTimeout(() => setEditing(false), 0);
    return () => window.clearTimeout(timer);
  }, [selectedId, setEditing]);

  useEffect(() => {
    setSearchParams(
      (prev) => {
        const next = new URLSearchParams(prev);
        // Under the graph a term is not in the URL: reloading opens what was left, the graph.
        if (selectedId === null || ontology) next.delete("term");
        else next.set("term", String(selectedId));
        return next;
      },
      { replace: true },
    );
    // eslint-disable-next-line react-hooks/exhaustive-deps -- setSearchParams identity is stable per render, not per selection.
  }, [selectedId, ontology]);

  return { ontology, setOntology, selectedId, setSelectedId, editing, setEditing };
}
