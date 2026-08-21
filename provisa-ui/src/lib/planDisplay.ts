// Copyright (c) 2026 Kenneth Stott
// Canary: 8b3d5f19-6c40-4a7e-9d21-3f8a0c1e7b45
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// How a plan reads on screen. Shared by the Billing page and the signup picker (REQ-1514) so the
// same plan carries the same name and the same money formatting on both.

/** Cents as the store states them — the amounts are USD, the only currency the store sells in. */
export function formatMoney(cents: number): string {
  return `$${(cents / 100).toFixed(2)}`;
}

const PLAN_NAMES: Record<string, string> = {
  trial: "Trial",
  starter: "Starter",
  pro_s: "Pro S",
  pro_m: "Pro M",
  pro_l: "Pro L",
};

export function planName(plan: string): string {
  return PLAN_NAMES[plan] ?? plan;
}
