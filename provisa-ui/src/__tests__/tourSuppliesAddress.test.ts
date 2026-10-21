// Copyright (c) 2026 Kenneth Stott
// Canary: 961eebe3-eabf-415a-b224-7626edc80b3d
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-2115, REQ-2101 (amended 2026-10-21): the core tour's live-built source reads the org's own
// copy of supplies.parquet where its demo ships one (no network: a sandbox may reach no address
// its demo does not declare), and the copy Provisa publishes elsewhere.

import { describe, expect, it } from "vitest";
import { SUPPLIES_PARQUET_URL, TOUR_STEPS, suppliesAddress } from "../tour/tourSteps";

const step = TOUR_STEPS.find((s) => s.key === "stepBuildSource")!;
const address = (orgFiles: string[]) => {
  const actions = typeof step.actions === "function" ? step.actions({ orgFiles }) : step.actions!;
  const fill = actions.find(
    (a) => a.do === "fill" && "testId" in a.target && a.target.testId === "source-file-location",
  );
  return fill && "value" in fill ? fill.value : undefined;
};

describe("the live-built source's address", () => {
  it("is the org's own copy where the demo ships it", () => {
    expect(suppliesAddress(["supplies.parquet"])).toBe("{files}/supplies.parquet");
    expect(address(["supplies.parquet"])).toBe("{files}/supplies.parquet");
  });

  it("is the published copy where it does not", () => {
    expect(address([])).toBe(SUPPLIES_PARQUET_URL);
    expect(address(["other.csv"])).toBe(SUPPLIES_PARQUET_URL);
  });
});
