// Copyright (c) 2026 Kenneth Stott
// Canary: 6a1d9e38-4f2b-4c70-b5e8-3c7f0a9d2e16
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-930/REQ-1907: a table is refused only for a ttl/ttl_probe signal (its own, else its
// source's) with no landing Cache TTL (table, else source) while its data lands.

import { describe, it, expect } from "vitest";
import { tableTtlSignalError } from "../roleTtl";

const LIVE = { materialize: false, preferMaterialized: null, loadProtected: null };
const SRC = { changeSignal: "ttl", preferMaterialized: false, loadProtected: false };

describe("tableTtlSignalError", () => {
  it("refuses a landed ttl/ttl_probe table with no Cache TTL", () => {
    for (const changeSignal of ["ttl", "ttl_probe"]) {
      expect(tableTtlSignalError({ ...LIVE, changeSignal, materialize: true }, SRC, null)).toBe(
        true,
      );
      expect(
        tableTtlSignalError({ ...LIVE, changeSignal, preferMaterialized: true }, SRC, null),
      ).toBe(true);
      expect(tableTtlSignalError({ ...LIVE, changeSignal, loadProtected: true }, SRC, null)).toBe(
        true,
      );
    }
  });

  it("inherits the signal and the landing mode from the source", () => {
    const table = { ...LIVE, changeSignal: null };
    expect(tableTtlSignalError(table, { ...SRC, preferMaterialized: true }, null)).toBe(true);
    expect(tableTtlSignalError(table, { ...SRC, loadProtected: true }, null)).toBe(true);
    expect(tableTtlSignalError(table, SRC, null)).toBe(false);
  });

  it("a table's own off setting overrides a landing source", () => {
    expect(
      tableTtlSignalError(
        { ...LIVE, changeSignal: "ttl", preferMaterialized: false, loadProtected: false },
        { ...SRC, preferMaterialized: true, loadProtected: true },
        null,
      ),
    ).toBe(false);
  });

  it("saves a live-read ttl table, any Cache TTL, and every self-clocked signal", () => {
    expect(tableTtlSignalError({ ...LIVE, changeSignal: "ttl" }, SRC, null)).toBe(false);
    expect(tableTtlSignalError({ ...LIVE, changeSignal: "ttl", materialize: true }, SRC, 30)).toBe(
      false,
    );
    for (const changeSignal of ["probe", "native", "debezium", "kafka"]) {
      expect(tableTtlSignalError({ ...LIVE, changeSignal, materialize: true }, SRC, null)).toBe(
        false,
      );
    }
  });
});
