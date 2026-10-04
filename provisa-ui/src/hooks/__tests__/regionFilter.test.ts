// Copyright (c) 2026 Kenneth Stott
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-1922: the admin lists are filtered by region. The filter semantics, the hidden count, the
// selectable choices and the clamping of a persisted choice.

import { describe, expect, it } from "vitest";
import {
  coerceSelection,
  defaultSelection,
  filterByRegion,
  regionMatches,
  REGION_ALL,
  REGION_NONE,
  selectionValues,
} from "../regionFilter";

interface Obj {
  name: string;
  region: string | null;
}

const OBJECTS: Obj[] = [
  { name: "a", region: "us" },
  { name: "b", region: "us" },
  { name: "c", region: "eu" },
  { name: "d", region: null },
  { name: "e", region: "ap" },
];

const REGIONS = ["us", "eu", "ap"];
const CONNECTED = "us";

const names = (items: Obj[]) => items.map((o) => o.name).sort();

describe("regionMatches (REQ-1922)", () => {
  it("all regions matches everything, including no-region", () => {
    for (const o of OBJECTS) expect(regionMatches(REGION_ALL, CONNECTED, o.region)).toBe(true);
  });

  it("no region matches only objects that name no region", () => {
    expect(regionMatches(REGION_NONE, CONNECTED, null)).toBe(true);
    expect(regionMatches(REGION_NONE, CONNECTED, "us")).toBe(false);
  });

  it("the connected region matches objects homed there AND objects with no region", () => {
    expect(regionMatches("us", CONNECTED, "us")).toBe(true);
    expect(regionMatches("us", CONNECTED, null)).toBe(true);
    expect(regionMatches("us", CONNECTED, "eu")).toBe(false);
  });

  it("another region by name matches only that region, not no-region", () => {
    expect(regionMatches("eu", CONNECTED, "eu")).toBe(true);
    expect(regionMatches("eu", CONNECTED, null)).toBe(false);
    expect(regionMatches("eu", CONNECTED, "us")).toBe(false);
  });
});

describe("filterByRegion hidden count (REQ-1922)", () => {
  const get = (o: Obj) => o.region;

  it("all regions hides nothing", () => {
    const r = filterByRegion(OBJECTS, REGION_ALL, CONNECTED, get);
    expect(names(r.visible)).toEqual(["a", "b", "c", "d", "e"]);
    expect(r.hidden).toBe(0);
  });

  it("connected region keeps homed + no-region, and counts the rest hidden", () => {
    const r = filterByRegion(OBJECTS, "us", CONNECTED, get);
    expect(names(r.visible)).toEqual(["a", "b", "d"]); // us, us, null
    expect(r.hidden).toBe(2); // eu + ap hidden
  });

  it("another region keeps only its own, counting the rest hidden", () => {
    const r = filterByRegion(OBJECTS, "eu", CONNECTED, get);
    expect(names(r.visible)).toEqual(["c"]);
    expect(r.hidden).toBe(4);
  });

  it("no region keeps only the region-less rows", () => {
    const r = filterByRegion(OBJECTS, REGION_NONE, CONNECTED, get);
    expect(names(r.visible)).toEqual(["d"]);
    expect(r.hidden).toBe(4);
  });
});

describe("selectionValues order (REQ-1922)", () => {
  it("is connected first, then all, none, then each other region", () => {
    expect(selectionValues(REGIONS, CONNECTED)).toEqual([
      "us",
      REGION_ALL,
      REGION_NONE,
      "eu",
      "ap",
    ]);
  });

  it("omits a connected region that is not among the declared regions", () => {
    expect(selectionValues(REGIONS, null)).toEqual([REGION_ALL, REGION_NONE, "us", "eu", "ap"]);
  });
});

describe("coerceSelection / defaultSelection (REQ-1922)", () => {
  it("defaults to the connected region, or all regions when none is connected", () => {
    expect(defaultSelection("us")).toBe("us");
    expect(defaultSelection(null)).toBe(REGION_ALL);
  });

  it("keeps a stored value that is still valid", () => {
    expect(coerceSelection("eu", REGIONS, CONNECTED)).toBe("eu");
    expect(coerceSelection(REGION_NONE, REGIONS, CONNECTED)).toBe(REGION_NONE);
  });

  it("falls back to the default for a stored region that no longer exists or nothing stored", () => {
    expect(coerceSelection("gone", REGIONS, CONNECTED)).toBe("us");
    expect(coerceSelection(null, REGIONS, CONNECTED)).toBe("us");
    expect(coerceSelection(undefined, REGIONS, null)).toBe(REGION_ALL);
  });
});
