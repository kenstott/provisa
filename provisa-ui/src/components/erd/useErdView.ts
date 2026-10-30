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
    /** The icon: the view on, or off and back to the list without its focus. */
    setOn: (on: boolean) =>
      write((next) => {
        if (on) {
          next.set("view", "erd");
          return;
        }
        next.delete("view");
        writeErdFocus(next, { domains: [], objects: [] });
      }),
    setFocus: (changed: ErdFocus) => write((next) => writeErdFocus(next, changed)),
    /** The item whose panel is open, in the link so that a copy of it opens the same. */
    setPanel: (key: "table" | "relationship", id: string | null) =>
      write((next) => {
        if (id === null) next.delete(key);
        else next.set(key, id);
      }, true),
  };
}
