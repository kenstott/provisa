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
//
// On a page that shows several tables' profiles at once (a data product's Profile section), the
// same `?table=<id>` the tables page opens a table by names the table whose dialog is open: there
// the dialog writes it on opening and removes it on closing (`namesTable`).

import { useCallback } from "react";
import { useSearchParams } from "react-router-dom";

export const PROFILE_PARAM = "profile";
export const PROFILE_VIEW_PARAM = "profileView";
/** The table whose dialog is open, where the page does not name one otherwise. */
export const PROFILE_TABLE_PARAM = "table";
/** The run a link names when it names none: the latest that succeeded. */
export const LATEST_RUN = "latest";
/** The result shown while the URL names none. */
export const FIRST_VIEW = "columns";

/**
 * What a page publishes of the dialog for Polly's read_screen (REQ-2115): its run and result as the
 * URL names them, or none while `params` is null (no table open) or names no dialog.
 */
export function profileDialogInputs(params: URLSearchParams | null): {
  profile: string | null;
  profileView: string | null;
} {
  const profile = params?.get(PROFILE_PARAM) ?? null;
  return {
    profile,
    profileView: profile === null ? null : (params?.get(PROFILE_VIEW_PARAM) ?? FIRST_VIEW),
  };
}

export interface ProfileRunsParam {
  /** The open dialog's run as the URL names it (a run id, or `latest`); null while it is closed. */
  profile: string | null;
  /** The result the URL names; the first when it names none. */
  view: string;
  /** The table the URL names (`?table=`), as an id; null when it names none. */
  tableId: number | null;
  /** Opens the dialog; with `namesTable`, on the table given. */
  open: (tableId?: number) => void;
  selectRun: (runId: string) => void;
  selectView: (view: string) => void;
  close: () => void;
}

/**
 * `namesTable`: the page names the dialog's table by `?table=` only while the dialog is open, so
 * opening writes it and closing removes it. Without it the table is the page's own (the tables
 * page's open table) and the dialog leaves it.
 */
export function useProfileRunsParam(namesTable = false): ProfileRunsParam {
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
    tableId: params.has(PROFILE_TABLE_PARAM) ? Number(params.get(PROFILE_TABLE_PARAM)) : null,
    open: (tableId) =>
      change((p) => {
        if (namesTable) {
          if (tableId === undefined)
            throw new Error("this page's profile runs dialog names its table");
          p.set(PROFILE_TABLE_PARAM, String(tableId));
        }
        p.set(PROFILE_PARAM, LATEST_RUN);
      }, false),
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
        if (namesTable) p.delete(PROFILE_TABLE_PARAM);
      }, false),
  };
}
