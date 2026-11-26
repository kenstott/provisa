// Copyright (c) 2026 Kenneth Stott
// Canary: 040009cc-ae8f-4995-96d0-4f8860e2de7d
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-2277: the Discover relationships dialog holds a multi-step task, so it has a deep link.
// `?discover=new` is the open dialog setting up a run, `?discover=<runId>` the open dialog showing
// that run. Opening and closing are history entries; the dialog moving from the form to a run and
// back replaces the entry.

import { useCallback } from "react";
import { useSearchParams } from "react-router-dom";

export const DISCOVER_PARAM = "discover";
/** The dialog open on the form that sets up a run. */
export const NEW_RUN = "new";

export interface DiscoverParam {
  /** What the URL names: `new`, a run id, or null while the dialog is closed. */
  discover: string | null;
  /** The run the URL names; null while it names none. */
  runId: number | null;
  open: () => void;
  /** The dialog shows ``value`` (`new` or a run id) in place of what it showed. */
  replace: (value: string) => void;
  close: () => void;
}

export function useDiscoverParam(): DiscoverParam {
  const [params, setParams] = useSearchParams();
  const change = useCallback(
    (value: string | null, replace: boolean) =>
      setParams(
        (current) => {
          const next = new URLSearchParams(current);
          if (value === null) next.delete(DISCOVER_PARAM);
          else next.set(DISCOVER_PARAM, value);
          return next;
        },
        { replace },
      ),
    [setParams],
  );
  const discover = params.get(DISCOVER_PARAM);
  return {
    discover,
    runId: discover === null || discover === NEW_RUN ? null : Number(discover),
    open: useCallback(() => change(NEW_RUN, false), [change]),
    replace: useCallback((value: string) => change(value, true), [change]),
    close: useCallback(() => change(null, false), [change]),
  };
}
