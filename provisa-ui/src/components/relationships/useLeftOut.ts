// Copyright (c) 2026 Kenneth Stott
// Canary: 1f8b3d76-9a2c-4e50-b4d1-0c7e5a9f3b68
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-413: what an ended discovery run left out, read when the run has ended and again after one
// of its proposals is accepted.

import { useCallback, useEffect, useState } from "react";
import { fetchLeftOut, type DiscoveryRun, type LeftOut } from "../../api/admin";

export interface LeftOutState {
  /** The shown run's left-out proposals; null until an ended run's have been read. */
  leftOut: LeftOut | null;
  error: string;
  /** Accept one with ``accept``, then read the list again; a refusal is shown as the error. */
  accept: (id: number, name: string) => Promise<void>;
}

const errorText = (e: unknown) => (e instanceof Error ? e.message : String(e));

export function useLeftOut(
  run: DiscoveryRun | null,
  accept: (id: number, name: string) => Promise<void>,
): LeftOutState {
  const [read, setRead] = useState<{ runId: number; leftOut: LeftOut } | null>(null);
  const [error, setError] = useState("");
  const runId = run?.id;
  const ended = run !== null && run.status !== "running";

  useEffect(() => {
    if (runId === undefined || !ended) return;
    let live = true;
    fetchLeftOut(runId)
      .then((leftOut) => {
        if (!live) return;
        setError("");
        setRead({ runId, leftOut });
      })
      .catch((e) => live && setError(errorText(e)));
    return () => {
      live = false;
    };
  }, [runId, ended]);

  const acceptOne = useCallback(
    async (id: number, name: string) => {
      if (runId === undefined) return;
      try {
        await accept(id, name);
        setError("");
        setRead({ runId, leftOut: await fetchLeftOut(runId) });
      } catch (e) {
        setError(errorText(e));
      }
    },
    [accept, runId],
  );

  return {
    leftOut: read !== null && read.runId === runId && ended ? read.leftOut : null,
    error,
    accept: acceptOne,
  };
}
