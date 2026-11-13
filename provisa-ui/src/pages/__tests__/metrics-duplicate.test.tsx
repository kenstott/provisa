// Copyright (c) 2026 Kenneth Stott
// Canary: 401a8742-fb57-42dd-95bf-dc3410510968
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// Creating a metric: "+ New metric" opens an empty form whatever metric was selected or edited
// before it, and Duplicate (the panel head, beside Edit) opens the same create form filled from
// the selected metric under a new name: its definition (rule:
// components/list/__tests__/panelDuplicateGuard.test.ts).

import { describe, it, expect, vi, beforeEach } from "vitest";
import { render, screen, within } from "../../test-utils/render";
import userEvent from "@testing-library/user-event";
import { expectHoldsNoneOf } from "../../test-utils/createForm";
import type { Metric } from "../../types/admin";

const upsertMetric = vi.fn();

const METRICS: Metric[] = [
  {
    name: "revenue",
    expression: "SUM(orders.amount)",
    datatype: "numeric",
    description: "Total revenue",
    aiContext: "Money taken, net of refunds",
    visibleTo: ["*"],
    fromFact: "orders",
    term: null,
  },
];

vi.mock("../../context/AuthContext", () => ({ useAuth: () => ({ role: { id: "analyst" } }) }));
vi.mock("../../hooks/useAdminQueries", async (importOriginal) => ({
  ...(await importOriginal<typeof import("../../hooks/useAdminQueries")>()),
  useMetrics: () => ({ metrics: METRICS, loading: false, error: undefined, refetch: vi.fn() }),
  useTables: () => ({ tables: [], loading: false, error: undefined, refetch: vi.fn() }),
  useUpsertMetric: () => ({ upsertMetric, loading: false }),
  useDeleteMetric: () => ({ deleteMetric: vi.fn(), loading: false }),
}));
vi.mock("../../api/glossary", async (importOriginal) => ({
  ...(await importOriginal<typeof import("../../api/glossary")>()),
  listGlossaryTerms: vi.fn(async () => []),
}));

import { MetricsPage } from "../MetricsPage";

const VALUES = ["revenue", "SUM(orders.amount)", "numeric", "Total revenue"];
const panel = () => screen.getByTestId("metrics-split-panel");

beforeEach(() => vi.clearAllMocks());

describe("+ New metric opens an empty form", () => {
  it("holds none of the values of the metric selected before it", async () => {
    render(<MetricsPage />);
    await userEvent.click(screen.getByTestId("metrics-row-revenue"));
    await userEvent.click(screen.getByTestId("metrics-new-button"));
    expectHoldsNoneOf(panel(), VALUES);
  });

  it("holds none of them after the metric was edited and the edit cancelled", async () => {
    render(<MetricsPage />);
    await userEvent.click(screen.getByTestId("metrics-row-revenue"));
    await userEvent.click(screen.getByTestId("metric-detail-edit-revenue"));
    await userEvent.click(screen.getByTestId("metric-cancel-button"));
    await userEvent.click(screen.getByTestId("metrics-new-button"));
    expectHoldsNoneOf(panel(), VALUES);
  });
});

describe("Duplicate, in a metric's panel head", () => {
  it("is beside Edit and Delete, named Duplicate", async () => {
    render(<MetricsPage />);
    await userEvent.click(screen.getByTestId("metrics-row-revenue"));
    expect(screen.getByTestId("metric-detail-duplicate-revenue")).toHaveAccessibleName("Duplicate");
  });

  it("opens the create form filled from the metric, under a new name", async () => {
    render(<MetricsPage />);
    await userEvent.click(screen.getByTestId("metrics-row-revenue"));
    await userEvent.click(screen.getByTestId("metric-detail-duplicate-revenue"));
    expect(screen.getByTestId("metrics-split-title")).toHaveTextContent("New Metric");
    const form = within(screen.getByTestId("metric-create-card"));
    expect(form.getByTestId("metric-name-input")).toHaveValue("revenue-copy");
    expect(form.getByTestId("metric-expression-input")).toHaveValue("SUM(orders.amount)");
  });

  it("creates the copy with the whole definition, as a create", async () => {
    upsertMetric.mockResolvedValue({ success: true, message: "ok" });
    render(<MetricsPage />);
    await userEvent.click(screen.getByTestId("metrics-row-revenue"));
    await userEvent.click(screen.getByTestId("metric-detail-duplicate-revenue"));
    await userEvent.click(screen.getByTestId("metric-save-button"));
    expect(upsertMetric).toHaveBeenCalledWith(
      expect.objectContaining({
        name: "revenue-copy",
        expression: "SUM(orders.amount)",
        datatype: "numeric",
        description: "Total revenue",
        aiContext: "Money taken, net of refunds",
      }),
      true,
    );
  });
});
