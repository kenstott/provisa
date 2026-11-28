// Copyright (c) 2026 Kenneth Stott
// Canary: dfcadabd-c945-45bd-8827-366b67ee1686
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-2277: a list page's filter and its tab live in the URL, so a link, a reload and a new tab
// show the list as it was shared. The filter replaces its history entry as it is typed; a tab is a
// new entry, as a detail panel's tab is (useDetailParam).

import { useCallback, useState } from "react";
import { useSearchParams } from "react-router-dom";

/** The list filter named by `param` in the URL, and its setter. */
export function useUrlFilter(param = "search"): [string, (value: string) => void] {
  const [searchParams, setSearchParams] = useSearchParams();
  const linked = searchParams.get(param) ?? "";
  const [value, setValue] = useState(linked);
  // A link naming the filter on a page already open decides it.
  const [seeded, setSeeded] = useState(linked);
  if (linked !== seeded) {
    setSeeded(linked);
    setValue(linked);
  }
  const update = useCallback(
    (next: string) => {
      setValue(next);
      setSearchParams(
        (p) => {
          const n = new URLSearchParams(p);
          if (next) n.set(param, next);
          else n.delete(param);
          return n;
        },
        { replace: true },
      );
    },
    [param, setSearchParams],
  );
  return [value, update];
}

/**
 * The page tab named by `param` in the URL among `tabs`, and its setter; `fallback` (the first tab
 * unless given) while the URL names none, or one the page does not offer. A tab chosen is always
 * named, so the link shows that tab whatever the fallback.
 */
export function useUrlTab<T extends string>(
  param: string,
  tabs: readonly T[],
  fallback: T = tabs[0],
): [T, (tab: T) => void] {
  const [searchParams, setSearchParams] = useSearchParams();
  const named = searchParams.get(param);
  const tab =
    named !== null && (tabs as readonly string[]).includes(named) ? (named as T) : fallback;
  const choose = useCallback(
    (next: T) =>
      setSearchParams((p) => {
        const n = new URLSearchParams(p);
        n.set(param, next);
        return n;
      }),
    [param, setSearchParams],
  );
  return [tab, choose];
}
