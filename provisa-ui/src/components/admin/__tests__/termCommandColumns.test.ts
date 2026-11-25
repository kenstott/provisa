// Copyright (c) 2026 Kenneth Stott
// Canary: 6e2b9d47-1a83-4c5f-9b06-d3f7a8c1e254
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-1159 (amended 2026-11-25): on the Glossary page a term is made the meaning of a command's
// output column, and taken off one, as one column change of the term each.

import { describe, it, expect, vi } from "vitest";

vi.mock("../../../api/glossary", async (importOriginal) => ({
  ...(await importOriginal<typeof import("../../../api/glossary")>()),
  groundCommandColumn: vi.fn(async () => undefined),
  ungroundCommandColumn: vi.fn(async () => undefined),
}));

import {
  groundCommandColumn,
  ungroundCommandColumn,
  type GlossaryTermDetail,
} from "../../../api/glossary";
import { changes, commandColumnKey, draftOf } from "../termDraft";

const TERM = {
  id: 7,
  name: "tax identifier",
  definition: "A person's tax id.",
  calculation_method: null,
  export_excluded: false,
  is_abstract: false,
  domains: ["sales"],
  tags: [],
  edges_out: [],
  edges_in: [],
  experts: [],
  refs: [],
  command_refs: [{ command_name: "lookup", column_name: "ssn" }],
} as unknown as GlossaryTermDetail;

const tags = { mark: vi.fn(), unmark: vi.fn() };

describe("a term's command output columns", () => {
  it("starts from the columns the term holds and changes nothing unedited", () => {
    const draft = draftOf(TERM, []);
    expect(draft.commandColumns).toEqual([
      commandColumnKey({ command_name: "lookup", column_name: "ssn" }),
    ]);
    expect(changes(TERM, [], draft, tags)).toEqual([]);
  });

  it("grounds an added column and ungrounds a removed one", async () => {
    const added = commandColumnKey({ command_name: "enrich", column_name: "tax:id" });
    const out = changes(TERM, [], { ...draftOf(TERM, []), commandColumns: [added] }, tags);
    expect(out.map((c) => c.field)).toEqual(["columns", "columns"]);
    for (const c of out) await c.run();
    expect(groundCommandColumn).toHaveBeenCalledWith(7, {
      command_name: "enrich",
      column_name: "tax:id",
    });
    expect(ungroundCommandColumn).toHaveBeenCalledWith({
      command_name: "lookup",
      column_name: "ssn",
    });
  });
});
