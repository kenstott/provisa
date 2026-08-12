// Copyright (c) 2026 Kenneth Stott
// Canary: 21abd302-8c7a-4bf8-a66c-17d526bc7a0c
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-1437: a grid cell shows an instant the way a reader reads a clock.

import { describe, it, expect } from "vitest";
import { formatCell, formatTimestamp } from "../formatCell";

describe("formatTimestamp", () => {
  it("drops the T, the fractional seconds and the zone suffix", () => {
    const zoneless = formatTimestamp("2026-08-12T12:01:59.174658");
    expect(zoneless).toBe("2026-08-12 12:01:59");
  });

  it("leaves a zoneless value on its own wall clock", () => {
    // No zone means the value is already the reader's local time; shifting it would invent an offset.
    expect(formatTimestamp("2026-08-12 00:30:00")).toBe("2026-08-12 00:30:00");
  });

  it("fills in seconds a minute-precision value omits", () => {
    expect(formatTimestamp("2026-08-12T07:05")).toBe("2026-08-12 07:05:00");
  });

  it("shows a zoned value on the viewer's clock", () => {
    const at = new Date("2026-08-12T12:01:59Z");
    const pad = (n: number) => String(n).padStart(2, "0");
    const expected =
      `${at.getFullYear()}-${pad(at.getMonth() + 1)}-${pad(at.getDate())} ` +
      `${pad(at.getHours())}:${pad(at.getMinutes())}:${pad(at.getSeconds())}`;
    expect(formatTimestamp("2026-08-12T12:01:59Z")).toBe(expected);
    expect(formatTimestamp("2026-08-12T12:01:59+00:00")).toBe(expected);
  });

  it("declines anything that is not an instant", () => {
    expect(formatTimestamp("hello")).toBeNull();
    expect(formatTimestamp("2026-08-12")).toBeNull();
    expect(formatTimestamp("1755000119174658000")).toBeNull();
  });
});

describe("formatCell", () => {
  it("passes non-instant values through untouched", () => {
    expect(formatCell(42)).toBe("42");
    expect(formatCell("orders")).toBe("orders");
    expect(formatCell(false)).toBe("false");
  });

  it("returns null for a null or missing value", () => {
    expect(formatCell(null)).toBeNull();
    expect(formatCell(undefined)).toBeNull();
  });

  it("formats an instant", () => {
    expect(formatCell("2026-08-12T12:01:59.174658")).toBe("2026-08-12 12:01:59");
  });
});
