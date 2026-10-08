// Copyright (c) 2026 Kenneth Stott
// Canary: 719b5f90-d265-4e08-91dd-9741f1abdd75
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// @vitest-environment node

// The marketing site renders the product's source picker from constants.ts. A source added or
// renamed there must be regenerated into the site (node scripts/gen-site-source-picker.mjs).

import { describe, it, expect } from "vitest";
// @ts-expect-error the generator is plain ESM outside the UI's tsconfig
import { renderPicker, stalePages, REQUIREMENT_ONLY } from "../../../scripts/gen-site-source-picker.mjs";
import { SOURCE_TYPES } from "../pages/sources/constants";

describe("site source picker", () => {
  it("lists every type the picker lists, plus the requirement-only additions", async () => {
    const { total } = await renderPicker();
    expect(total).toBe(SOURCE_TYPES.length + REQUIREMENT_ONLY.length);
  });

  it("is in step with constants.ts in site/index.html and site/why/sources.html", async () => {
    expect(await stalePages()).toEqual([]);
  });
});
