// Copyright (c) 2026 Kenneth Stott
// Canary: 9b7c1e34-6a2d-4d81-8f05-c3e1a7d24b96
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-1386 (DRIFT FROM THE MODEL'S HISTORY): the Ontology quality report shows drift, computed
// from the model's history, and says so when there is no history rather than showing zero.

import { beforeEach, describe, expect, it, vi } from "vitest";
import { render, screen, within } from "../test-utils/render";
import type { OntologyDrift } from "../api/glossary";

const fetchOntologyDrift = vi.fn<() => Promise<OntologyDrift>>();
vi.mock("../api/glossary", async (importOriginal) => ({
  ...(await importOriginal<typeof import("../api/glossary")>()),
  fetchOntologyDrift: () => fetchOntologyDrift(),
}));

import { OntologyDriftPanel } from "../components/admin/OntologyDriftPanel";

const CUTOFF = "2026-09-01T00:00:00+00:00";

const WITH_HISTORY: OntologyDrift = {
  has_history: true,
  cutoff: CUTOFF,
  base: { sha: "abc", committed_at: "2026-08-20T00:00:00+00:00" },
  terms: [
    { name: "amount", before: "grounded", after: "not_grounded", change: "lost", term_id: 7 },
    { name: "old_name", before: "grounded", after: null, change: "lost", term_id: null },
    { name: "revenue", before: "grounded_pending", after: "grounded", change: "gained", term_id: 8 },
    { name: "total", before: null, after: "grounded", change: "new", term_id: 9 },
  ],
  counts: { lost: 2, gained: 1, new: 1 },
};

describe("OntologyDriftPanel", () => {
  beforeEach(() => {
    fetchOntologyDrift.mockReset();
  });

  it("lists each moved term with its grades, linking the ones that still exist", async () => {
    fetchOntologyDrift.mockResolvedValue(WITH_HISTORY);
    render(<OntologyDriftPanel />);
    const row = within(await screen.findByTestId("drift-row-amount"));
    expect(row.getByRole("link", { name: "amount" })).toHaveAttribute(
      "href",
      "/admin/ontology?term=7",
    );
    expect(row.getByText("Grounded")).toBeInTheDocument();
    expect(row.getByText("Not grounded")).toBeInTheDocument();
    expect(row.getByText("Lost")).toBeInTheDocument();
  });

  it("shows a term with no term to open as text, and its absence as such", async () => {
    fetchOntologyDrift.mockResolvedValue(WITH_HISTORY);
    render(<OntologyDriftPanel />);
    const gone = within(await screen.findByTestId("drift-row-old_name"));
    expect(gone.queryByRole("link")).toBeNull();
    expect(gone.getByText("Not in the model")).toBeInTheDocument();
    const fresh = within(screen.getByTestId("drift-row-total"));
    expect(fresh.getByText("Not in the model")).toBeInTheDocument();
    expect(fresh.getByText("New")).toBeInTheDocument();
  });

  it("counts lost, gained and new and states the rename rule and the baseline", async () => {
    fetchOntologyDrift.mockResolvedValue(WITH_HISTORY);
    render(<OntologyDriftPanel />);
    expect(await screen.findByTestId("drift-counts")).toHaveTextContent(
      "Lost: 2 · Gained: 1 · New: 1",
    );
    expect(screen.getByTestId("drift-rename-rule")).toHaveTextContent(
      "a rename shows as one lost and one new",
    );
    expect(screen.getByTestId("drift-baseline")).toHaveTextContent("2026-08-20");
    expect(screen.getByTestId("drift-baseline")).toHaveTextContent("2026-09-01");
  });

  it("says no history, with no counts, when there is nothing to compare with", async () => {
    fetchOntologyDrift.mockResolvedValue({
      has_history: false,
      cutoff: CUTOFF,
      base: null,
      terms: null,
      counts: null,
    });
    render(<OntologyDriftPanel />);
    expect(await screen.findByTestId("drift-no-history")).toHaveTextContent("2026-09-01");
    expect(screen.queryByTestId("drift-counts")).toBeNull();
    expect(screen.queryByTestId("drift-none")).toBeNull();
  });

  it("says when history holds no drift, apart from no history", async () => {
    fetchOntologyDrift.mockResolvedValue({
      ...WITH_HISTORY,
      terms: [],
      counts: { lost: 0, gained: 0, new: 0 },
    });
    render(<OntologyDriftPanel />);
    expect(await screen.findByTestId("drift-none")).toBeInTheDocument();
    expect(screen.queryByTestId("drift-no-history")).toBeNull();
  });

  it("shows why drift could not be read", async () => {
    fetchOntologyDrift.mockRejectedValue(new Error("the repository is unreachable"));
    render(<OntologyDriftPanel />);
    expect(await screen.findByTestId("drift-error")).toHaveTextContent(
      "the repository is unreachable",
    );
  });
});
