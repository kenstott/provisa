// Copyright (c) 2026 Kenneth Stott
// Canary: 6955c498-ff78-4597-bc9c-0bb3f5501712
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-1739: an RSS or Atom feed's settings that host, port and path cannot carry travel in its
// federation hints.

import { hintKey } from "./descriptor";

export const RSS = "rss";
// REQ-2114: the hint keys are the descriptor's (provisa/api/admin/source_types.py).
const FEED_URL = hintKey(RSS, "feed_url");
const POLL_INTERVAL = hintKey(RSS, "poll_interval");
const USE_SSL = hintKey(RSS, "use_ssl");

/** The form's RSS settings. */
export interface RssSeed {
  feedUrl: string;
  pollInterval: string;
  useSsl: boolean;
}

/** The settings of a new RSS source: read over TLS. */
export const EMPTY_RSS: RssSeed = { feedUrl: "", pollInterval: "", useSsl: true };

/** The hints a form saves: each setting given, and always whether it reads over TLS. */
export function rssHints(rss: RssSeed): Record<string, string> {
  return Object.fromEntries(
    (
      [
        [FEED_URL, rss.feedUrl.trim()],
        [POLL_INTERVAL, rss.pollInterval.trim()],
        [USE_SSL, rss.useSsl ? "true" : "false"],
      ] as const
    ).filter(([, v]) => v),
  );
}

/** A saved source's hints into the form; a setting it does not hold is a new source's. */
export function rssFromHints(hints: Record<string, string> | null): RssSeed {
  const useSsl = hints?.[USE_SSL];
  return {
    feedUrl: hints?.[FEED_URL] ?? EMPTY_RSS.feedUrl,
    pollInterval: hints?.[POLL_INTERVAL] ?? EMPTY_RSS.pollInterval,
    useSsl: useSsl === undefined ? EMPTY_RSS.useSsl : useSsl.toLowerCase() === "true",
  };
}
