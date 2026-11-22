// Copyright (c) 2026 Kenneth Stott
// Canary: 1f6c3a85-9d2e-4b70-8c13-5e7a0b9d4f26
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-2243: a term's calculation method is edited with its definition and saved as one field
// change of the term.

import { describe, it, expect, vi } from "vitest";

vi.mock("../../../api/glossary", async (importOriginal) => ({
  ...(await importOriginal<typeof import("../../../api/glossary")>()),
  updateGlossaryTerm: vi.fn(async () => undefined),
}));

import { updateGlossaryTerm, type GlossaryTermDetail } from "../../../api/glossary";
import { changes, draftOf } from "../termDraft";

const TERM = {
  id: 7,
  name: "net revenue",
  definition: "Revenue kept after refunds.",
  calculation_method: null,
  export_excluded: false,
  is_abstract: true,
  domains: ["sales"],
  tags: [],
  edges_out: [],
  edges_in: [],
  experts: [],
  refs: [],
} as unknown as GlossaryTermDetail;

const tags = { mark: vi.fn(), unmark: vi.fn() };

describe("a term's calculation method", () => {
  it("starts from the term and is saved when changed", async () => {
    const draft = draftOf(TERM, []);
    expect(draft.calculationMethod).toBe("");
    expect(changes(TERM, [], draft, tags)).toEqual([]);
    const edited = { ...draft, calculationMethod: "Gross less refunds." };
    const [change] = changes(TERM, [], edited, tags);
    await change.run();
    expect(updateGlossaryTerm).toHaveBeenCalledWith(7, { calculation_method: "Gross less refunds." });
  });

  it("is cleared to null when emptied", async () => {
    const held = { ...TERM, calculation_method: "Gross less refunds." };
    const [change] = changes(held, [], { ...draftOf(held, []), calculationMethod: "  " }, tags);
    await change.run();
    expect(updateGlossaryTerm).toHaveBeenLastCalledWith(7, { calculation_method: null });
  });
});
