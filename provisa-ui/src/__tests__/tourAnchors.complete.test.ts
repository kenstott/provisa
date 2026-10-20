// Copyright (c) 2026 Kenneth Stott
// Canary: 88e7c500-4a2f-43b3-8e1f-f77a5bc3d8e8
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// The anchor census (tourAnchors.*.test.tsx) names every tour step: a step no census file runs
// against its page is one whose anchor nothing checks, and a page change that drops it leaves the
// visitor's tour waiting out its whole window.

import { describe, it, expect } from "vitest";
import { readdirSync, readFileSync } from "node:fs";
import { resolve } from "node:path";
import { TOUR_STEPS } from "../tour/tourSteps";

const census = readdirSync(__dirname)
  .filter((f) => /^tourAnchors\.\w+\.test\.tsx$/.test(f))
  .map((f) => readFileSync(resolve(__dirname, f), "utf8"))
  .join("\n");

describe("the tour anchor census", () => {
  it("names every tour step", () => {
    const unnamed = TOUR_STEPS.map((s) => s.key).filter((key) => !census.includes(`"${key}"`));
    expect(unnamed).toEqual([]);
  });
});
