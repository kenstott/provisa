// Copyright (c) 2026 Kenneth Stott
// Canary: f86c05c1-fbc8-40c2-aee0-899824091cd1
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// Where a detail panel's content puts its actions: the pinned head of the panel it is in.
// `undefined` means "not in a panel" (render the actions in place); `null` means the panel's head
// is not mounted yet.

import { createContext, useLayoutEffect, useMemo, useRef, useState } from "react";

export const PanelActionsContext = createContext<HTMLElement | null | undefined>(undefined);

// Where a form in a detail panel says what is wrong with it: one line under the panel's pinned
// head, in view however far the form is scrolled. Same three values as the actions' slot.
export const PanelNoticeContext = createContext<HTMLElement | null | undefined>(undefined);

// REQ-2198: a detail panel's delete in progress. `lock` disables the panel's fields and actions
// until the release it returns is called; `markDeleting` marks the item's row in the list beside
// the panel as being deleted (not offered for use) until its release. `close` closes the panel,
// without asking about changes: what it showed is gone. `undefined`: not in a panel.
export interface PanelDeleting {
  lock: () => () => void;
  markDeleting: (key: string) => () => void;
  close: () => void;
}
export const PanelDeletingContext = createContext<PanelDeleting | undefined>(undefined);

// REQ-2198: the keys of the list's items being deleted, read by each row (`ListRow`).
export const DeletingKeysContext = createContext<ReadonlySet<string>>(new Set());

/**
 * REQ-2198: the state behind `PanelDeletingContext`, for whatever holds a detail panel (SplitPane,
 * or a list whose rows open their detail beneath them): whether the panel is locked, the keys
 * being deleted (for `DeletingKeysContext`), and the context's value. `open`: whether the panel
 * is shown; a closed panel is not locked. `onClose` closes it.
 */
export function usePanelDeleting(open: boolean, onClose: () => void) {
  const [locks, setLocks] = useState(0);
  const [deleting, setDeleting] = useState<ReadonlySet<string>>(new Set());
  const closeRef = useRef(onClose);
  useLayoutEffect(() => {
    closeRef.current = onClose;
  });
  const api = useMemo<PanelDeleting>(
    () => ({
      lock: () => {
        setLocks((n) => n + 1);
        let held = true;
        return () => {
          if (!held) return;
          held = false;
          setLocks((n) => n - 1);
        };
      },
      markDeleting: (key) => {
        setDeleting((prev) => new Set(prev).add(key));
        return () =>
          setDeleting((prev) => {
            const next = new Set(prev);
            next.delete(key);
            return next;
          });
      },
      close: () => closeRef.current(),
    }),
    [],
  );
  return { locked: open && locks > 0, deleting, api };
}
