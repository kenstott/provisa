// Copyright (c) 2026 Kenneth Stott
// Canary: 8b3f2e71-5c94-4d0a-a6e2-1f7d9c4b0e58
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// A table's "Rows are instances of": a draft with no term reads none, and a term that cannot be
// read says what could not be read and why, in the reader's words: no model-store table is named.

import { beforeEach, describe, expect, it, vi } from "vitest";
import { render, screen } from "../../../test-utils/render";

const answer = vi.hoisted(() => ({
  reader: true,
  rows: [] as { id: number; classTerm?: { id: number; name: string } | null }[],
}));

vi.mock("../../../context/AuthContext", () => ({
  useAuth: () => ({ actingRoles: "org_admin" }),
}));
vi.mock("../../../api/metaGraph", async (importOriginal) => ({
  ...(await importOriginal<typeof import("../../../api/metaGraph")>()),
  metaReader: async () => ({ schema: null, roots: new Map() }),
  rootQuery: () => (answer.reader ? "query" : null),
  dataGraphql: async () => ({ registered_tables: answer.rows }),
}));

import { TableClassTerm } from "../TableClassTerm";

beforeEach(() => {
  answer.reader = true;
  answer.rows = [];
});

describe("a table's term", () => {
  it("reads none for a draft no term grounds", async () => {
    answer.rows = [{ id: 303, classTerm: null }];
    render(<TableClassTerm tableId={303} />);
    expect(await screen.findByTestId("table-class-term-none")).toHaveTextContent("No term");
  });

  it.each([
    [
      "its own entry is not read",
      () => {
        answer.rows = [];
      },
      "Its term could not be read: your roles cannot read this table's own entry in the model.",
    ],
    [
      "no table of the model is read",
      () => {
        answer.reader = false;
      },
      "Its term could not be read: your roles cannot read the model's tables.",
    ],
    [
      "which term it is is not read",
      () => {
        answer.rows = [{ id: 303 }];
      },
      "Its term could not be read: your roles cannot read which term a table's rows are instances of.",
    ],
  ])("says why when %s, naming no model table", async (_case, arrange, said) => {
    arrange();
    render(<TableClassTerm tableId={303} />);
    const error = await screen.findByTestId("table-class-term-error");
    expect(error).toHaveTextContent(said);
    expect(error.textContent).not.toMatch(/registered_tables|classTerm|303/);
  });
});
