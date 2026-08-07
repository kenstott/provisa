// Copyright (c) 2026 Kenneth Stott
// Canary: 8d3f0a27-6b9c-4e15-a2d4-1f7e5c8b9a06
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-1387: glossary hover popup on Tables-surface column names.

import { describe, it, expect, vi, beforeEach } from "vitest";
import { render, screen, fireEvent, waitFor } from "../test-utils/render";
import i18n from "../i18n";
import { ColumnGlossaryHover } from "../pages/tables/ColumnGlossaryHover";
import { clearColumnGlossaryCache } from "../pages/tables/columnGlossaryCache";
import type { GlossaryTermDetail } from "../api/glossary";

const t = i18n.getFixedT("en");

vi.mock("../api/glossary", async (importOriginal) => ({
  ...(await importOriginal<typeof import("../api/glossary")>()),
  fetchGlossaryTermByRef: vi.fn(),
}));

import { fetchGlossaryTermByRef } from "../api/glossary";

const mockFetchByRef = vi.mocked(fetchGlossaryTermByRef);

const REVENUE_DETAIL: GlossaryTermDetail = {
  id: 1,
  name: "Revenue",
  definition: "Money in.",
  is_abstract: false,
  deprecated: false,
  export_excluded: false,
  ref_count: 1,
  refs: [
    {
      table_id: 10,
      column_name: "amount",
      source_id: "pet-store-pg",
      schema_name: "public",
      table_name: "orders",
      alias: null,
      domain_id: "pet-store",
    },
  ],
  edges_out: [{ term_id: 2, rel_type: "KIND_OF", name: "Income" }],
  edges_in: [],
  experts: [{ user_id: "alice", kind: "expert" }],
};

function renderHover(tableId: number, columnName: string) {
  return render(
    <ColumnGlossaryHover tableId={tableId} columnName={columnName}>
      <code>{columnName}</code>
    </ColumnGlossaryHover>,
  );
}

describe("ColumnGlossaryHover (REQ-1387)", () => {
  beforeEach(() => {
    vi.clearAllMocks();
    clearColumnGlossaryCache();
  });

  it("hover shows term name, definition, refs, experts, and related terms", async () => {
    mockFetchByRef.mockResolvedValue(REVENUE_DETAIL);
    renderHover(10, "amount");

    fireEvent.mouseEnter(screen.getByText("amount"));

    expect(await screen.findByText("Revenue")).toBeInTheDocument();
    expect(screen.getByText("Money in.")).toBeInTheDocument();
    expect(screen.getByText(/orders\.amount/)).toBeInTheDocument();
    expect(screen.getByText(/alice/)).toBeInTheDocument();
    expect(
      screen.getByText(`${t("glossaryTab.rel_KIND_OF")}: Income`),
    ).toBeInTheDocument();
    expect(mockFetchByRef).toHaveBeenCalledWith(10, "amount");
  });

  it("shows the muted no-definition line and deprecated badge", async () => {
    mockFetchByRef.mockResolvedValue({
      ...REVENUE_DETAIL,
      definition: null,
      deprecated: true,
      experts: [],
      edges_out: [],
    });
    renderHover(11, "legacy_col");

    fireEvent.mouseEnter(screen.getByText("legacy_col"));

    expect(await screen.findByText(t("glossaryTab.hoverNoDefinition"))).toBeInTheDocument();
    expect(screen.getByText(t("glossaryTab.deprecated"))).toBeInTheDocument();
  });

  it("renders no popup when the column has no term (404 → null)", async () => {
    mockFetchByRef.mockResolvedValue(null);
    renderHover(12, "untermed");

    fireEvent.mouseEnter(screen.getByText("untermed"));

    await waitFor(() => expect(mockFetchByRef).toHaveBeenCalledTimes(1));
    expect(screen.queryByRole("dialog")).not.toBeInTheDocument();
    expect(screen.queryByText(t("glossaryTab.hoverNoDefinition"))).not.toBeInTheDocument();
  });

  it("second hover serves from cache without refetching", async () => {
    mockFetchByRef.mockResolvedValue(REVENUE_DETAIL);
    renderHover(13, "amount");
    const target = screen.getByText("amount");

    fireEvent.mouseEnter(target);
    expect(await screen.findByText("Revenue")).toBeInTheDocument();

    fireEvent.mouseLeave(target);
    await waitFor(() => expect(screen.queryByText("Revenue")).not.toBeInTheDocument());

    fireEvent.mouseEnter(target);
    expect(await screen.findByText("Revenue")).toBeInTheDocument();
    expect(mockFetchByRef).toHaveBeenCalledTimes(1);
  });

  it("caches the 404 miss — second hover does not refetch", async () => {
    mockFetchByRef.mockResolvedValue(null);
    renderHover(14, "untermed");
    const target = screen.getByText("untermed");

    fireEvent.mouseEnter(target);
    await waitFor(() => expect(mockFetchByRef).toHaveBeenCalledTimes(1));

    fireEvent.mouseLeave(target);
    fireEvent.mouseEnter(target);
    expect(mockFetchByRef).toHaveBeenCalledTimes(1);
    expect(screen.queryByText(t("glossaryTab.hoverNoDefinition"))).not.toBeInTheDocument();
  });
});
