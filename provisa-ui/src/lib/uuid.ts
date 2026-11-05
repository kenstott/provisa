// Copyright (c) 2026 Kenneth Stott
// Canary: ec0a396b-3f5a-4267-8ec6-7812d9e5fa08
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// crypto.randomUUID exists only in a secure context (https or localhost), so a page served over
// plain http from any other host has none. crypto.getRandomValues exists in every context, so the
// ids are built from it: RFC 4122 version 4.

/** A random RFC 4122 version 4 UUID. */
export function uuid(): string {
  const b = crypto.getRandomValues(new Uint8Array(16));
  b[6] = (b[6] & 0x0f) | 0x40;
  b[8] = (b[8] & 0x3f) | 0x80;
  const h = Array.from(b, (x) => x.toString(16).padStart(2, "0")).join("");
  return `${h.slice(0, 8)}-${h.slice(8, 12)}-${h.slice(12, 16)}-${h.slice(16, 20)}-${h.slice(20)}`;
}
