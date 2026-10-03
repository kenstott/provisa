// Copyright (c) 2026 Kenneth Stott
// Canary: 4712c690-53ea-43a8-9cd7-1e6b25e65e12
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-464: the register form searches a source's schema by description and offers the ranked
// candidates; choosing one only fills in the table to register.

import { describe, it, expect, vi, beforeEach } from "vitest";
import { render, screen, fireEvent, waitFor } from "../../../test-utils/render";

const search = vi.fn();
vi.mock("../../../api/admin", () => ({
  searchSourceTables: (...args: unknown[]) => search(...args),
}));

import { NlTableSearch } from "../NlTableSearch";

const CANDIDATES = [
  {
    schema_name: "public",
    table_name: "invoices",
    comment: null,
    confidence: 0.91,
    reasoning: "invoice lines with amounts",
    cache_warm: true,
  },
  {
    schema_name: "public",
    table_name: "payments",
    comment: "card payments",
    confidence: 0.64,
    reasoning: "",
    cache_warm: true,
  },
];

beforeEach(() => search.mockReset());

function setup(registered: string[] = []) {
  const onPick = vi.fn();
  render(
    <NlTableSearch
      sourceId="erp"
      schemaName="public"
      isRegistered={(t) => registered.includes(t.name)}
      onPick={onPick}
    />,
  );
  return { onPick };
}

describe("NlTableSearch", () => {
  it("searches the source's schema with the description and lists the ranked candidates", async () => {
    search.mockResolvedValue(CANDIDATES);
    setup();
    fireEvent.change(screen.getByTestId("register-table-nl-query"), {
      target: { value: "customer invoicing and payment tables" },
    });
    fireEvent.click(screen.getByTestId("register-table-nl-run"));
    await screen.findByText("invoices");
    expect(search).toHaveBeenCalledWith("erp", "customer invoicing and payment tables", "public");
    expect(screen.getByText("91%")).toBeInTheDocument();
    expect(screen.getByText("invoice lines with amounts")).toBeInTheDocument();
    expect(screen.getByText("card payments")).toBeInTheDocument();
  });

  it("fills in the chosen table and offers nothing for one already registered", async () => {
    search.mockResolvedValue(CANDIDATES);
    const { onPick } = setup(["payments"]);
    fireEvent.change(screen.getByTestId("register-table-nl-query"), { target: { value: "x" } });
    fireEvent.click(screen.getByTestId("register-table-nl-run"));
    await screen.findByText("invoices");
    const use = screen.getAllByRole("button", { name: "Use" });
    expect(use).toHaveLength(1);
    fireEvent.click(use[0]);
    expect(onPick).toHaveBeenCalledWith("invoices");
    expect(screen.getByText("Registered")).toBeInTheDocument();
  });

  it("says when nothing matches, and shows the server's refusal", async () => {
    search.mockResolvedValueOnce([]);
    setup();
    fireEvent.change(screen.getByTestId("register-table-nl-query"), { target: { value: "x" } });
    fireEvent.click(screen.getByTestId("register-table-nl-run"));
    expect(
      await screen.findByText("No table in this schema matches that description."),
    ).toBeInTheDocument();
    search.mockRejectedValueOnce(new Error("Missing capability: source_registration"));
    fireEvent.click(screen.getByTestId("register-table-nl-run"));
    await waitFor(() =>
      expect(screen.getByText("Missing capability: source_registration")).toBeInTheDocument(),
    );
  });
});
