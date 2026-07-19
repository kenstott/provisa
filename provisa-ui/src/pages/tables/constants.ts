// Copyright (c) 2026 Kenneth Stott
// Canary: 1d4b7e90-5c3a-4f28-b1e7-8a2d6c9f0e15
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

export const NAMING_CONVENTIONS = [
  { value: "", label: "Inherit (source)" },
  { value: "none", label: "none" },
  { value: "snake_case", label: "snake_case" },
  { value: "camelCase", label: "camelCase" },
  { value: "PascalCase", label: "PascalCase" },
];

export const CDC_TYPES = new Set(["postgresql", "mongodb", "kafka", "debezium"]);

// REQ-1141: IANA time-zone identifiers for the off-peak-window zone picklist. Sourced from the
// runtime's own zone database (Intl.supportedValuesOf) — the same identifiers Python's ZoneInfo
// accepts server-side. UTC is pinned first as the default; the rest follow in database order.
const _tzValues: string[] =
  typeof Intl.supportedValuesOf === "function" ? Intl.supportedValuesOf("timeZone") : ["UTC"];
export const IANA_TIME_ZONES: string[] = [
  "UTC",
  ..._tzValues.filter((z) => z !== "UTC"),
];
