// Copyright (c) 2026 Kenneth Stott
// Canary: 3e6a41a4-fea2-41fc-8934-6032778de3a8
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

import { useEffect, useRef, useState } from "react";

// REQ-1824: a files/sharepoint/splunk source's bundled Calcite server can take longer to boot
// than the backend is willing to block a discovery request for (a large `files` directory's eager
// schema scan especially) — the resolver fails fast with this fixed, machine-parseable prefix
// instead of hanging the whole request, so a discovery hook can poll instead of showing a hard
// error or spinning forever.
export const STILL_STARTING_PREFIX = "STARTING:";
export const STILL_STARTING_POLL_MS = 3000;
// A source whose bundled server never comes up (genuinely broken, not just slow) would otherwise
// poll forever — 10 minutes covers even an 8k-file dataset's Calcite boot with room to spare, per
// the size case this fix was built for, without polling indefinitely on a truly dead source.
export const STILL_STARTING_MAX_MS = 10 * 60 * 1000;

export function isStillStarting(error: { message?: string } | undefined): boolean {
  return (error?.message ?? "").includes(STILL_STARTING_PREFIX);
}

/** Polls `refetch` on an interval while `error` reports STILL_STARTING, giving up (and stopping)
 * after STILL_STARTING_MAX_MS so a genuinely broken source doesn't poll forever. */
export function useStartingPoll(
  error: { message?: string } | undefined,
  refetch: () => unknown,
): { starting: boolean; timedOut: boolean } {
  const since = useRef<number | null>(null);
  const [gaveUp, setGaveUp] = useState(false);
  const starting = isStillStarting(error);
  useEffect(() => {
    if (!starting) {
      since.current = null;
      return;
    }
    if (since.current === null) since.current = Date.now();
    if (Date.now() - since.current >= STILL_STARTING_MAX_MS) {
      setGaveUp(true);
      return;
    }
    const timer = window.setInterval(() => void refetch(), STILL_STARTING_POLL_MS);
    return () => window.clearInterval(timer);
  }, [starting, refetch]);
  return { starting: starting && !gaveUp, timedOut: gaveUp };
}
