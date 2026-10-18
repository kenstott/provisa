// Copyright (c) 2026 Kenneth Stott
// Canary: 35d3ac1e-e2bd-40fe-bcd8-695019eef0b9
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

/** The instant a role grant given "until" ``day`` (YYYY-MM-DD) ends: the close of that day, local
 * time (REQ-2077). */
export function untilEndOf(day: string): string {
  return new Date(`${day}T23:59:59`).toISOString();
}

/** The first day a grant can be given until: tomorrow, local time. */
export function tomorrow(): Date {
  const d = new Date();
  d.setDate(d.getDate() + 1);
  d.setHours(0, 0, 0, 0);
  return d;
}
