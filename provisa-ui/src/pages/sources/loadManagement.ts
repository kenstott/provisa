// Copyright (c) 2026 Kenneth Stott
// Canary: 1f7a4c58-2e9b-4d06-a3c1-8b5e0d6f9a24
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// Client-side mirrors of the server's source load/timeliness validation (the server re-checks
// every rule; these only stop an obviously bad value before the save round trip).

import {
  replicateContradictsLoadProtection,
  saysReplicated,
} from "../../components/admin/replicate";

/** Empty (no cap) or a whole number >= 1 (schema.max_live_concurrency_invalid). */
export function maxLiveConcurrencyValid(value: string): boolean {
  if (value.trim() === "") return true;
  const n = Number(value);
  return Number.isInteger(n) && n >= 1;
}

const SENTINEL_SCHEMES = ["file:", "ftp:", "sftp:", "http:", "https:"];

/** Empty (no sentinel) or a URL whose scheme the prober reads (schema.invalid_sentinel_path). */
export function sentinelPathValid(value: string): boolean {
  const v = value.trim();
  if (v === "") return true;
  return SENTINEL_SCHEMES.some((scheme) => v.toLowerCase().startsWith(scheme));
}

/** True when the panel's fields can be sent to the server. */
export function sourceLoadFieldsValid(form: {
  maxLiveConcurrency: string;
  sentinelPath: string;
  changeSignal: string;
  cacheTtl: string;
  replicate: number | null;
  loadProtected: boolean;
}): boolean {
  return (
    maxLiveConcurrencyValid(form.maxLiveConcurrency) &&
    sentinelPathValid(form.sentinelPath) &&
    !replicateContradictsLoadProtection(form.replicate, form.loadProtected) &&
    !ttlSignalMissingCacheTtl(
      form.changeSignal,
      sourceFormCacheTtl(form.cacheTtl),
      sourceFormLanded(form),
    )
  );
}

/** The change signals whose staleness clock IS the Cache TTL (REQ-930). */
const TTL_CLOCK_SIGNALS = ["ttl", "ttl_probe"];

/**
 * A ttl/ttl_probe change signal with no resolved landing Cache TTL (the table's own, else its
 * source's — the global response-cache default is not a landing clock) is refused only when the
 * data is replicated: `landed` is true for materialize / row_materialize / Replicate set to
 * Always or a Hot threshold / Load Protected. A live-read ttl table saves normally; the server raises on read if it ever lands
 * without a TTL. Returns true when the save must be refused.
 */
export function ttlSignalMissingCacheTtl(
  changeSignal: string | null,
  resolvedCacheTtl: number | null,
  landed: boolean,
): boolean {
  return (
    landed &&
    changeSignal !== null &&
    TTL_CLOCK_SIGNALS.includes(changeSignal) &&
    resolvedCacheTtl === null
  );
}

/** A source form's Cache TTL string as the landing TTL, or null when empty. */
export function sourceFormCacheTtl(cacheTtl: string): number | null {
  return cacheTtl.trim() === "" ? null : Number(cacheTtl);
}

/** A source's settings say its tables are replicated: Always, a Hot threshold, or load protection. */
export function sourceFormLanded(form: { replicate: number | null; loadProtected: boolean }) {
  return saysReplicated(form.replicate, form.loadProtected);
}
