// Copyright (c) 2026 Kenneth Stott
// Canary: 9b4e2d71-0c5a-4f38-a6e9-2d8f1c7b3e50
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-2243: a dataset's panel lists the governed metrics it uses, each with its description, AI
// definition and the definition and calculation method of the term it names; never its formula.

import { describe, it, expect, vi } from "vitest";
import { render, screen } from "../../../test-utils/render";
import type { DatasetMetric } from "../../../types/admin";

const read: { metrics: DatasetMetric[] | undefined } = { metrics: undefined };

vi.mock("../../../hooks/useAdminQueries", async (importOriginal) => ({
  ...(await importOriginal<typeof import("../../../hooks/useAdminQueries")>()),
  useTableMetrics: () => ({ metrics: read.metrics, loading: false, error: undefined }),
}));

import { TableMetrics } from "../TableMetrics";

describe("TableMetrics", () => {
  it("lists each metric with the definition and method of the term it names", () => {
    read.metrics = [
      {
        name: "net_revenue",
        description: "Net revenue",
        aiContext: "Gross minus refunds.",
        term: "net revenue",
        termDefinition: "Revenue kept after refunds.",
        termCalculationMethod: "Gross order value less refunds.",
      },
      {
        name: "order_count",
        description: null,
        aiContext: null,
        term: null,
        termDefinition: null,
        termCalculationMethod: null,
      },
    ];
    render(<TableMetrics tableId={1} />);
    const net = screen.getByTestId("table-metric-net_revenue");
    expect(net).toHaveTextContent("Net revenue");
    expect(net).toHaveTextContent("Gross minus refunds.");
    expect(screen.getByTestId("table-metric-net_revenue-term")).toHaveTextContent(
      "net revenue: Revenue kept after refunds.",
    );
    expect(screen.getByTestId("table-metric-net_revenue-method")).toHaveTextContent(
      "Calculated as: Gross order value less refunds.",
    );
    expect(screen.getByTestId("table-metric-order_count")).toBeInTheDocument();
    expect(screen.queryByTestId("table-metric-order_count-term")).toBeNull();
  });

  it("says when no metric uses the table", () => {
    read.metrics = [];
    render(<TableMetrics tableId={1} />);
    expect(screen.getByText("No metric uses this table")).toBeInTheDocument();
  });
});
