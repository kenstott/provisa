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

const LIVE = { materialize: false, replicate: null, loadProtected: null };
const SRC = { changeSignal: "ttl", replicate: null, loadProtected: false };

describe("tableTtlSignalError", () => {
  it("refuses a landed ttl/ttl_probe table with no Cache TTL", () => {
    for (const changeSignal of ["ttl", "ttl_probe"]) {
      expect(tableTtlSignalError({ ...LIVE, changeSignal, materialize: true }, SRC, null)).toBe(
        true,
      );
      expect(tableTtlSignalError({ ...LIVE, changeSignal, replicate: 0 }, SRC, null)).toBe(true);
      expect(tableTtlSignalError({ ...LIVE, changeSignal, loadProtected: true }, SRC, null)).toBe(
        true,
      );
    }
  });

  it("inherits the signal and the landing mode from the source", () => {
    const table = { ...LIVE, changeSignal: null };
    expect(tableTtlSignalError(table, { ...SRC, replicate: 0 }, null)).toBe(true);
    expect(tableTtlSignalError(table, { ...SRC, loadProtected: true }, null)).toBe(true);
    expect(tableTtlSignalError(table, SRC, null)).toBe(false);
  });

  it("a table set to Never under a source that replicates only when busy is not refused", () => {
    expect(
      tableTtlSignalError(
        { ...LIVE, changeSignal: "ttl", replicate: -1 },
        { ...SRC, replicate: 500 },
        null,
      ),
    ).toBe(false);
  });

  it("a Hot threshold says the table is replicated once busy, so it needs its clock", () => {
    expect(tableTtlSignalError({ ...LIVE, changeSignal: "ttl", replicate: 500 }, SRC, null)).toBe(
      true,
    );
    expect(
      tableTtlSignalError({ ...LIVE, changeSignal: "ttl" }, { ...SRC, replicate: 500 }, null),
    ).toBe(true);
  });

  it("a table cannot opt out of a source floored as a whole", () => {
    // Always or Load Protected on the source: it has no live attach, so every table of it is
    // served from its replica whatever the table says for itself.
    for (const floor of [{ replicate: 0 }, { loadProtected: true }]) {
      expect(
        tableTtlSignalError(
          { ...LIVE, changeSignal: "ttl", loadProtected: false },
          { ...SRC, ...floor },
          null,
        ),
      ).toBe(true);
    }
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
