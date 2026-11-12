// Copyright (c) 2026 Kenneth Stott
// Canary: placeholder
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-2240: rows of a list selected and deleted together. The selection is the list's own
// (`ListTable` `selection`): a checkbox column, shift-click selecting a range, and a header
// checkbox selecting every row the current filter shows. Once anything is selected, the bar
// (`ListBulkBar`) shows the count with Delete and Clear; Delete asks once, naming the rows, and
// the page's one bulk call deletes what can be deleted and names each row refused.

import { createContext, useCallback, useMemo, useRef, useState } from "react";
import type { Dependent } from "../../lib/dependents";

export interface ListSelection {
  /** The keys selected, among the rows the current filter shows. */
  selected: ReadonlySet<string>;
  /** The keys of the rows the current filter shows, in the list's order. */
  shown: readonly string[];
  /** The keys being deleted now: their rows are marked so (`ListRow`). */
  deleting: ReadonlySet<string>;
  /** One row's box pressed; `range` (shift held) sets every row from the last one pressed. */
  toggle: (key: string, range: boolean) => void;
  /** The header's box: every shown row selected, or none where all already are. */
  toggleAll: () => void;
  clear: () => void;
  markDeleting: (keys: ReadonlySet<string>) => void;
}

/** The selection a `ListTable` reads; null where the list offers none. */
export const ListSelectionContext = createContext<ListSelection | null>(null);

/**
 * The selection of a list whose current filter shows the rows keyed `shown`, in order. A row the
 * filter hides is not selected: what is deleted is only ever what the reader can see chosen.
 */
export function useListSelection(shown: ReadonlyArray<string | number>): ListSelection {
  const shownKeys = useMemo(() => shown.map(String), [shown]);
  const [chosen, setChosen] = useState<ReadonlySet<string>>(new Set());
  const [deleting, setDeleting] = useState<ReadonlySet<string>>(new Set());
  const anchor = useRef<string | null>(null);
  const selected = useMemo(
    () => new Set(shownKeys.filter((k) => chosen.has(k))) as ReadonlySet<string>,
    [shownKeys, chosen],
  );
  const toggle = useCallback(
    (key: string, range: boolean) => {
      const on = !selected.has(key);
      const from = anchor.current === null ? -1 : shownKeys.indexOf(anchor.current);
      const to = shownKeys.indexOf(key);
      const keys =
        range && from !== -1 && to !== -1
          ? shownKeys.slice(Math.min(from, to), Math.max(from, to) + 1)
          : [key];
      setChosen((prev) => {
        const next = new Set(prev);
        for (const k of keys) {
          if (on) next.add(k);
          else next.delete(k);
        }
        return next;
      });
      anchor.current = key;
    },
    [selected, shownKeys],
  );
  const toggleAll = useCallback(() => {
    const all = shownKeys.length > 0 && selected.size === shownKeys.length;
    setChosen(all ? new Set() : new Set(shownKeys));
    anchor.current = null;
  }, [selected, shownKeys]);
  const clear = useCallback(() => {
    setChosen(new Set());
    anchor.current = null;
  }, []);
  return useMemo(
    () => ({
      selected,
      shown: shownKeys,
      deleting,
      toggle,
      toggleAll,
      clear,
      markDeleting: setDeleting,
    }),
    [selected, shownKeys, deleting, toggle, toggleAll, clear],
  );
}

/** One row a bulk delete refused: its name, why, and what still refers to it. */
export interface BulkDeleteRefusal {
  name: string;
  reason: string;
  dependents: Dependent[];
}

/** A bulk delete's answer: the names of the rows deleted, and each row refused. */
export interface BulkDeleteResult {
  deleted: string[];
  refused: BulkDeleteRefusal[];
}

