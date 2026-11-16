// Copyright (c) 2026 Kenneth Stott
// Canary: 2e7c5a91-8b34-4d6f-a0c2-91f6e3b7d458
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// A suggestion with a table that requires a parameter was found with no value of that table read.
// Its row says so with a "Reduced information" badge; a suggestion between tables read whole has
// none.

import { describe, it, expect, vi } from "vitest";
import { render, screen } from "../../../test-utils/render";
import { CandidatesTable } from "../CandidatesTable";
import type { Candidate } from "../relationship-types";

const base: Candidate = {
  id: 1,
  source_table_id: 10,
  target_table_id: 20,
  source_column: "_nf_orderId",
  target_column: "id",
  cardinality: "many-to-one",
  confidence: 0.9,
  reasoning: "the parameter names an order",
  suggested_name: "order-lookup",
};

function list(candidates: Candidate[]) {
  render(
    <CandidatesTable
      candidates={candidates}
      tableDomainById={{ "10": "pet-store", "20": "pet-store", "30": "pet-store" }}
      tableNameById={{ "10": "get_order_by_id", "20": "orders", "30": "pets" }}
      onAccept={vi.fn()}
      onReject={vi.fn()}
      onAcceptAll={vi.fn()}
      onRejectAll={vi.fn()}
    />,
  );
}

describe("a suggestion found on reduced information", () => {
  it("is marked in its row, and only that one", () => {
    list([
      { ...base, reduced_information: true },
      {
        ...base,
        id: 2,
        source_table_id: 20,
        source_column: "pet_id",
        target_table_id: 30,
        suggested_name: "order-pet",
        reduced_information: false,
      },
    ]);
    expect(screen.getByTestId("candidate-reduced-1").textContent).toBe(
      "Reduced information: no values were compared",
    );
    expect(screen.queryByTestId("candidate-reduced-2")).toBeNull();
  });

  it("is not marked when the server says nothing of it", () => {
    list([base]);
    expect(screen.queryByTestId("candidate-reduced-1")).toBeNull();
  });
});
