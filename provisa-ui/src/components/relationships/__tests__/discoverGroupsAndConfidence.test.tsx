// Copyright (c) 2026 Kenneth Stott
// Canary: 5b8e1f37-9c2a-4d06-a7e4-3f91c6b2d8a0
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-413: a date or timestamp is never a key candidate, so the dialog names only the four
// compared type groups the server sends (provisa/discovery/columns.py GROUPS), in every locale;
// and a run's result heads the model's own rating "Confidence", beside the final Score.

import { describe, it, expect } from "vitest";
import { fireEvent, screen, within } from "@testing-library/react";
import { renderWithProviders } from "../../../test-utils/render";
import { GROUP_ORDER } from "../discoveryResult";
import { ProposalsTable } from "../ProposalsTable";
import type { RunProposal } from "../../../api/admin";
import type { RunProposalsState } from "../useRunProposals";

const GROUPS = ["whole_number", "decimal", "text", "uuid"];

const catalogs = import.meta.glob<{ discoverDialog: { group: Record<string, string> } }>(
  "../../../i18n/locales/*/discoverDialog.json",
  { eager: true, import: "default" },
);

describe("discover type groups", () => {
  it("are the four compared groups, in the server's order", () => {
    expect(GROUP_ORDER).toEqual(GROUPS);
  });

  it("are named for exactly those groups in every locale", () => {
    const locales = Object.keys(catalogs);
    expect(locales).toHaveLength(13);
    for (const path of locales) {
      expect(Object.keys(catalogs[path].discoverDialog.group).sort(), path).toEqual(
        [...GROUPS].sort(),
      );
    }
  });
});

const proposal: RunProposal = {
  id: 1,
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
  suggested_name: null,
  reason: null,
  answer: null,
  decidable: true,
  acceptable: true,
  reduced_information: false,
  admitted_on_key_values: false,
  dimension: null,
  via: null,
};

const state: RunProposalsState = {
  proposals: [proposal],
  shown: [proposal],
  scoreFilter: null,
  setScoreFilter: () => {},
  threshold: 0.7,
  keptAt: 0.7,
  setThreshold: () => {},
  accept: new Set(),
  notAgain: new Set(),
  toggleAccept: () => {},
  edits: new Map(),
  setEdit: () => {},
  toggleNotAgain: () => {},
  toggleAcceptAll: () => {},
  apply: async () => {},
  applying: false,
  failures: [],
  applied: null,
  error: "",
};

describe("proposals table", () => {
  it("heads the model's rating Confidence, second after Score", () => {
    renderWithProviders(
      <ProposalsTable
        state={state}
        tableDomainById={{ 10: "sales", 20: "sales" }}
        tableNameById={{ 10: "orders", 20: "customers" }}
        newRun={null}
      />,
    );
    const headers = within(screen.getByTestId("discover-proposals"))
      .getAllByRole("columnheader")
      .map((h) => h.textContent?.trim() ?? "");
    const score = headers.findIndex((h) => h.startsWith("Score"));
    expect(headers[score + 1]).toMatch(/^Confidence/);
    expect(headers.some((h) => h.startsWith("Model"))).toBe(false);
  });
});

describe("a junction suggestion", () => {
  // REQ-1586: a discovery run that finds both legs of a junction proposes the combined
  // relationship as one suggestion; its row names the junction its two ends are joined through.
  const render = (p: RunProposal) =>
    renderWithProviders(
      <ProposalsTable
        state={{ ...state, proposals: [p], shown: [p] }}
        tableDomainById={{ 10: "sales", 20: "sales", 30: "sales" }}
        tableNameById={{ 10: "orders", 20: "customers", 30: "order_customers" }}
        newRun={null}
      />,
    );

  it("names the junction it joins through", () => {
    render({
      ...proposal,
      via: { table_id: 30, source_column: "order_id", target_column: "customer_id" },
    });
    expect(screen.getByText(/Through order_customers\./)).toBeInTheDocument();
  });

  it("names none on an ordinary suggestion", () => {
    render(proposal);
    expect(screen.queryByText(/Through /)).toBeNull();
  });
});

describe("the Rel type column", () => {
  // REQ-1586 (amended 2026-11-22): each row names the relationship accepting it makes -- fk (a
  // direct foreign key), jct (through a junction) or dim (through a shared dimension) -- in a
  // column the list sorts and filters as any other.
  const junction: RunProposal = {
    ...proposal,
    id: 2,
    via: { table_id: 30, source_column: "order_id", target_column: "customer_id" },
  };
  const dimension: RunProposal = {
    ...proposal,
    id: 3,
    source_column: "hood",
    target_column: "hood",
    cardinality: "many-to-many",
    dimension: { name: "neighborhood", reused: false },
  };
  const rows = [proposal, junction, dimension];
  const show = () =>
    renderWithProviders(
      <ProposalsTable
        state={{ ...state, proposals: rows, shown: rows }}
        tableDomainById={{ 10: "sales", 20: "sales", 30: "sales" }}
        tableNameById={{ 10: "orders", 20: "customers", 30: "order_customers" }}
        newRun={null}
      />,
    );

  it("heads a column Rel type the list sorts and filters", () => {
    show();
    expect(screen.getByTestId("discover-proposals-sort-rel_type")).toHaveTextContent("Rel type");
    const head = screen.getByTestId("discover-proposals-sort-rel_type").closest("th");
    expect(within(head as HTMLElement).getByRole("button", { name: /filter/i })).toBeTruthy();
  });

  it("names each row fk, jct or dim, the tip spelling it out", async () => {
    show();
    expect(screen.getByTestId("proposal-rel-type-1")).toHaveTextContent(/^fk$/);
    expect(screen.getByTestId("proposal-rel-type-2")).toHaveTextContent(/^jct$/);
    expect(screen.getByTestId("proposal-rel-type-3")).toHaveTextContent(/^dim$/);
    fireEvent.mouseEnter(within(screen.getByTestId("proposal-rel-type-2")).getByText("jct"));
    expect(await screen.findByRole("tooltip")).toHaveTextContent(
      "Through junction order_customers",
    );
  });

  it("tips a dimension row with the dimension's name", async () => {
    show();
    fireEvent.mouseEnter(within(screen.getByTestId("proposal-rel-type-3")).getByText("dim"));
    expect(await screen.findByRole("tooltip")).toHaveTextContent(
      "Through shared dimension neighborhood",
    );
  });
});
