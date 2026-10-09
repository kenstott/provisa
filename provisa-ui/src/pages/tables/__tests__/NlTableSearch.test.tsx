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

import { COLUMNS_REASK_MS, NlTableSearch } from "../NlTableSearch";

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

const answer = (
  candidates: typeof CANDIDATES,
  column_names: "complete" | "loading" | "unavailable" = "complete",
) => ({ table_names: "complete" as const, column_names, candidates });

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
    search.mockResolvedValue(answer(CANDIDATES));
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
    search.mockResolvedValue(answer(CANDIDATES));
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
    search.mockResolvedValueOnce(answer([]));
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

  // REQ-464: a schema's column names are loaded the first time it is searched. An answer given
  // meanwhile says so, and the same search is asked again until they are in.
  it("says column names are still loading and asks again until they are loaded", async () => {
    vi.useFakeTimers({ shouldAdvanceTime: true });
    try {
      search
        .mockResolvedValueOnce(answer([], "loading"))
        .mockResolvedValueOnce(answer([CANDIDATES[0]], "complete"));
      setup();
      fireEvent.change(screen.getByTestId("register-table-nl-query"), {
        target: { value: "amount due" },
      });
      fireEvent.click(screen.getByTestId("register-table-nl-run"));
      expect(await screen.findByTestId("register-table-nl-columns-loading")).toHaveTextContent(
        "Column names in this schema are still loading",
      );
      expect(search).toHaveBeenCalledTimes(1);

      await vi.advanceTimersByTimeAsync(COLUMNS_REASK_MS + 50);
      await screen.findByText("invoices");
      expect(search).toHaveBeenCalledTimes(2);
      expect(search).toHaveBeenLastCalledWith("erp", "amount due", "public");
      expect(screen.queryByTestId("register-table-nl-columns-loading")).toBeNull();

      await vi.advanceTimersByTimeAsync(COLUMNS_REASK_MS * 3);
      expect(search).toHaveBeenCalledTimes(2); // loaded: asked no more
    } finally {
      vi.useRealTimers();
    }
  });

  it("says nothing of columns when the answer searched them all", async () => {
    search.mockResolvedValue(answer(CANDIDATES, "complete"));
    setup();
    fireEvent.change(screen.getByTestId("register-table-nl-query"), { target: { value: "x" } });
    fireEvent.click(screen.getByTestId("register-table-nl-run"));
    await screen.findByText("invoices");
    expect(screen.queryByTestId("register-table-nl-columns-loading")).toBeNull();
    expect(screen.queryByTestId("register-table-nl-columns-unavailable")).toBeNull();
    expect(search).toHaveBeenCalledTimes(1);
  });

  it("says when column names could not be loaded, and does not ask again", async () => {
    vi.useFakeTimers({ shouldAdvanceTime: true });
    try {
      search.mockResolvedValue(answer(CANDIDATES, "unavailable"));
      setup();
      fireEvent.change(screen.getByTestId("register-table-nl-query"), { target: { value: "x" } });
      fireEvent.click(screen.getByTestId("register-table-nl-run"));
      expect(await screen.findByTestId("register-table-nl-columns-unavailable")).toHaveTextContent(
        "could not be loaded",
      );
      await vi.advanceTimersByTimeAsync(COLUMNS_REASK_MS * 3);
      expect(search).toHaveBeenCalledTimes(1);
    } finally {
      vi.useRealTimers();
    }
  });
});
