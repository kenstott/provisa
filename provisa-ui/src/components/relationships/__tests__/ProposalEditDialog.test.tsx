// Copyright (c) 2026 Kenneth Stott
// Canary: 8d2c6e41-0b7a-4f39-95e8-c1a4d7f30b62
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-2262: the relationship review's per-row edit dialog sets a candidate's name and cardinality
// before it is accepted.

import { describe, it, expect, vi } from "vitest";
import { fireEvent, screen } from "@testing-library/react";
import { renderWithProviders } from "../../../test-utils/render";
import { ProposalEditDialog } from "../ProposalEditDialog";
import { ProposalsTable } from "../ProposalsTable";
import type { RunProposal } from "../../../api/admin";
import type { RunProposalsState } from "../useRunProposals";

const proposal: RunProposal = {
  id: 7,
  run_id: 1,
  kind: "semantics",
  source_table_id: 10,
  source_column: "customer_id",
  target_table_id: 20,
  target_column: "id",
  cardinality: "many-to-one",
  score: 0.9,
  measured: false,
  measure: null,
  confidence: 0.8,
  scored: null,
  reasoning: "ids match",
  suggested_name: "orders-customers",
  reason: null,
  answer: null,
  decidable: true,
  acceptable: true,
  reduced_information: false,
  admitted_on_key_values: false,
  dimension: null,
  via: null,
};

describe("ProposalEditDialog", () => {
  it("saves only what was changed", () => {
    const onSave = vi.fn();
    renderWithProviders(
      <ProposalEditDialog
        name="orders-customers"
        cardinality="many-to-one"
        edit={undefined}
        onSave={onSave}
        onClose={() => {}}
      />,
    );
    fireEvent.change(screen.getByTestId("proposal-edit-name"), { target: { value: "my-rel" } });
    fireEvent.click(screen.getByTestId("proposal-edit-save"));
    expect(onSave).toHaveBeenCalledWith({ name: "my-rel" });
  });

  it("saves nothing, dropping an edit, when both fields are as the run had them", () => {
    const onSave = vi.fn();
    renderWithProviders(
      <ProposalEditDialog
        name="orders-customers"
        cardinality="many-to-one"
        edit={{ name: "x" }}
        onSave={onSave}
        onClose={() => {}}
      />,
    );
    fireEvent.change(screen.getByTestId("proposal-edit-name"), {
      target: { value: "orders-customers" },
    });
    fireEvent.click(screen.getByTestId("proposal-edit-save"));
    expect(onSave).toHaveBeenCalledWith(null);
  });

  it("offers no cardinality where accepting fixes it", () => {
    renderWithProviders(
      <ProposalEditDialog
        name="d"
        cardinality={null}
        edit={undefined}
        onSave={() => {}}
        onClose={() => {}}
      />,
    );
    expect(screen.queryByTestId("proposal-edit-cardinality")).toBeNull();
  });

  it("cannot save an empty name", () => {
    renderWithProviders(
      <ProposalEditDialog
        name="a"
        cardinality="many-to-one"
        edit={undefined}
        onSave={() => {}}
        onClose={() => {}}
      />,
    );
    fireEvent.change(screen.getByTestId("proposal-edit-name"), { target: { value: " " } });
    expect(screen.getByTestId("proposal-edit-save")).toBeDisabled();
  });
});

describe("a row's Edit action", () => {
  const state: RunProposalsState = {
    proposals: [proposal],
    shown: [proposal],
    scoreFilter: null,
    setScoreFilter: () => {},
    threshold: 0.7,
    keptAt: 0.7,
    setThreshold: () => {},
    accept: new Set(),
    edits: new Map([[7, { cardinality: "one-to-many" }]]),
    setEdit: vi.fn(),
    notAgain: new Set(),
    toggleAccept: () => {},
    toggleNotAgain: () => {},
    toggleAcceptAll: () => {},
    apply: async () => {},
    applying: false,
    failures: [],
    applied: null,
    error: "",
  };

  it("opens the dialog and the row shows the edited cardinality", () => {
    renderWithProviders(
      <ProposalsTable
        state={state}
        tableDomainById={{ 10: "sales", 20: "sales" }}
        tableNameById={{ 10: "orders", 20: "customers" }}
        newRun={null}
      />,
    );
    expect(screen.getByTestId("proposal-cardinality-7")).toHaveTextContent("one-to-many");
    fireEvent.click(screen.getByTestId("proposal-edit-7"));
    expect(screen.getByTestId("proposal-edit-name")).toBeInTheDocument();
  });
});
