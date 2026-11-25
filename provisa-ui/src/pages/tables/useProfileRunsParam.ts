// Copyright (c) 2026 Kenneth Stott
// Canary: a46651c2-ec1f-418a-89e8-33542f1e2de4
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-2277: the profile runs dialog has a deep link. `?profile=<runId|latest>` is the open dialog
// and its run, `?profileView=<view>` the result shown in it (the one shown without it is Columns).
// Opening and closing are history entries; choosing a run or a view replaces the entry. The
// parameters belong to the open table (DETAIL_VIEWS' alsoClears), so they go when it closes.
// `view` is the ERD's parameter on this page, hence `profileView`.

import { useCallback } from "react";
import { useSearchParams } from "react-router-dom";

export const PROFILE_PARAM = "profile";
export const PROFILE_VIEW_PARAM = "profileView";
/** The run a link names when it names none: the latest that succeeded. */
export const LATEST_RUN = "latest";
/** The result shown while the URL names none. */
export const FIRST_VIEW = "columns";

export interface ProfileRunsParam {
  /** The open dialog's run as the URL names it (a run id, or `latest`); null while it is closed. */
  profile: string | null;
  /** The result the URL names; the first when it names none. */
  view: string;
  open: () => void;
  selectRun: (runId: string) => void;
  selectView: (view: string) => void;
  close: () => void;
}

export function useProfileRunsParam(): ProfileRunsParam {
  const [params, setParams] = useSearchParams();
  const change = useCallback(
    (edit: (next: URLSearchParams) => void, replace: boolean) =>
      setParams(
        (current) => {
          const next = new URLSearchParams(current);
          edit(next);
          return next;
        },
        { replace },
      ),
    [setParams],
  );
  return {
    profile: params.get(PROFILE_PARAM),
    view: params.get(PROFILE_VIEW_PARAM) ?? FIRST_VIEW,
    open: () => change((p) => p.set(PROFILE_PARAM, LATEST_RUN), false),
    selectRun: (runId) => change((p) => p.set(PROFILE_PARAM, runId), true),
    selectView: (view) =>
      change((p) => {
        if (view === FIRST_VIEW) p.delete(PROFILE_VIEW_PARAM);
        else p.set(PROFILE_VIEW_PARAM, view);
      }, true),
    close: () =>
      change((p) => {
        p.delete(PROFILE_PARAM);
        p.delete(PROFILE_VIEW_PARAM);
      }, false),
  };
}
