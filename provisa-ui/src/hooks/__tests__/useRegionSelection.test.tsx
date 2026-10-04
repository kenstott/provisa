// Copyright (c) 2026 Kenneth Stott
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-1922: the region selection is remembered per viewer in browser storage, wrapped so a storage
// failure (private window, blocked site data) degrades to not-remembered rather than breaking.

import { afterEach, describe, expect, it, vi } from "vitest";
import { act, renderHook } from "@testing-library/react";
import { useRegionSelection } from "../useRegionSelection";
import { REGION_ALL } from "../regionFilter";

const REGIONS = ["us", "eu"];

afterEach(() => {
  try {
    localStorage.clear();
  } catch {
    /* ignore */
  }
  vi.restoreAllMocks();
});

describe("useRegionSelection (REQ-1922)", () => {
  it("defaults to the connected region and remembers an explicit choice", () => {
    const { result } = renderHook(() => useRegionSelection(REGIONS, "us"));
    expect(result.current[0]).toBe("us");
    act(() => result.current[1]("eu"));
    expect(result.current[0]).toBe("eu");
    expect(localStorage.getItem("provisa.admin.regionFilter")).toBe("eu");
  });

  it("restores a stored selection on the next mount", () => {
    localStorage.setItem("provisa.admin.regionFilter", "eu");
    const { result } = renderHook(() => useRegionSelection(REGIONS, "us"));
    expect(result.current[0]).toBe("eu");
  });

  it("re-defaults to the connected region once choices arrive, when nothing is stored", () => {
    // regions/connected arrive after the first render (empty, then populated).
    const { result, rerender } = renderHook(
      ({ regions, connected }: { regions: string[]; connected: string | null }) =>
        useRegionSelection(regions, connected),
      { initialProps: { regions: [] as string[], connected: null as string | null } },
    );
    expect(result.current[0]).toBe(REGION_ALL); // nothing known yet
    rerender({ regions: REGIONS, connected: "us" });
    expect(result.current[0]).toBe("us"); // now defaults to connected
  });

  it("clamps a stored region that no longer exists to the default", () => {
    localStorage.setItem("provisa.admin.regionFilter", "gone");
    const { result } = renderHook(() => useRegionSelection(REGIONS, "us"));
    expect(result.current[0]).toBe("us");
  });

  it("works when storage throws (reads and writes are wrapped)", () => {
    vi.spyOn(Storage.prototype, "getItem").mockImplementation(() => {
      throw new Error("blocked");
    });
    vi.spyOn(Storage.prototype, "setItem").mockImplementation(() => {
      throw new Error("blocked");
    });
    const { result } = renderHook(() => useRegionSelection(REGIONS, "us"));
    expect(result.current[0]).toBe("us"); // default despite unreadable storage
    expect(() => act(() => result.current[1]("eu"))).not.toThrow(); // write failure swallowed
    expect(result.current[0]).toBe("eu"); // in-memory selection still updates
  });
});
