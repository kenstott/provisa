// Copyright (c) 2026 Kenneth Stott
// Canary: 2a9e6d41-7c13-4f58-b0e2-8d3c5f1a6b97
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-2187: the ERD's state is the page's URL, read the same on Tables and on Relationships: the
// view, the focus, and -- while the view is on -- the item whose panel is open.

import { useCallback, useMemo } from "react";
import { useNavigate, useSearchParams } from "react-router-dom";
import { erdSearch, readErdFocus, writeErdFocus, type ErdFocus } from "./erd-focus";

export function useErdView() {
  const [params] = useSearchParams();
  const navigate = useNavigate();
  const domains = params.get("domains");
  const objects = params.get("objects");
  const focus = useMemo(
    () => readErdFocus(new URLSearchParams({ domains: domains ?? "", objects: objects ?? "" })),
    [domains, objects],
  );
  const write = useCallback(
    (change: (next: URLSearchParams) => void, replace = false) => {
      const next = new URLSearchParams(params);
      change(next);
      navigate({ search: erdSearch(next) }, { replace });
    },
    [params, navigate],
  );
  return {
    on: params.get("view") === "erd",
    focus,
    /** The domains a link asks to collapse, "all" for every one; null when it asks nothing. */
    collapsed: params.get("collapsed"),
    /** The icon: the view on, or off and back to the list without its focus. */
    setOn: (on: boolean) =>
      write((next) => {
        if (on) {
          next.set("view", "erd");
          return;
        }
        next.delete("view");
        next.delete("collapsed");
        next.delete("layout");
        writeErdFocus(next, { domains: [], objects: [] });
      }),
    setFocus: (changed: ErdFocus) => write((next) => writeErdFocus(next, changed)),
    /** REQ-2189: the saved layout shown, by name; null for Automatic. */
    layout: params.get("layout"),
    setLayout: (name: string | null) =>
      write((next) => {
        if (name === null) next.delete("layout");
        else next.set("layout", name);
        next.delete("collapsed");
      }),
    /** REQ-2189: the focus a saved layout holds, taking the link's place as the layout loads. */
    takeFocus: (held: ErdFocus) => write((next) => writeErdFocus(next, held), true),
    /** The item whose panel is open, in the link so that a copy of it opens the same. */
    setPanel: (key: "table" | "relationship", id: string | null) =>
      write((next) => {
        if (id === null) next.delete(key);
        else next.set(key, id);
      }, true),
  };
}
