// Copyright (c) 2026 Kenneth Stott
// Canary: 38962fc0-29ed-4a67-868f-517683f81ddc
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// A metric's panel shows every value of the metric under its form's labels, an empty one as "—".

import { describe, it, expect, vi } from "vitest";
import { render, screen, within } from "../../../test-utils/render";
import type { Metric } from "../../../types/admin";
import { MetricDetailPanel } from "../MetricDetailPanel";

const revenue: Metric = {
  name: "revenue",
  expression: "SUM(amount)",
  datatype: "decimal",
  description: "Booked revenue",
  aiContext: "Excludes refunds",
  visibleTo: ["analyst", "finance"],
  fromFact: "orders",
  term: null,
};

function fields(name: string): [string, string][] {
  return within(screen.getByTestId(`metric-read-${name}`))
    .queryAllByRole("term")
    .map((dt) => [dt.textContent ?? "", dt.nextElementSibling?.textContent ?? ""]);
}

const show = (m: Metric, dependentViews: string[] = []) =>
  render(
    <MetricDetailPanel
      m={m}
      dependentViews={dependentViews}
      onEdit={vi.fn()}
      onDuplicate={vi.fn()}
      onDelete={vi.fn()}
    />,
  );

describe("a metric's read view", () => {
  it("shows every value of the metric, under the form's labels", () => {
    show(revenue, ["sales_by_month"]);
    expect(fields("revenue")).toEqual([
      ["Name", "revenue"],
      ["Expression", "SUM(amount)"],
      ["Datatype", "decimal"],
      ["Description", "Booked revenue"],
      ["AI Context", "Excludes refunds"],
      ["Glossary term", "—"],
      ["Tags", ""],
      ["Source fact", "orders"],
      ["Visible to", "analyst, finance"],
      ["Dependent metric views", "sales_by_month"],
    ]);
  });

  it("shows an empty value as a dash, and says what a missing fact or grant means", () => {
    show({
      ...revenue,
      datatype: null,
      description: null,
      aiContext: null,
      fromFact: null,
      visibleTo: null,
    });
    const shown = Object.fromEntries(fields("revenue"));
    expect(shown["Datatype"]).toBe("—");
    expect(shown["Description"]).toBe("—");
    expect(shown["AI Context"]).toBe("—");
    expect(shown["Source fact"]).toBe("hand-authored");
    expect(shown["Visible to"]).toBe("not shown");
    expect(shown["Dependent metric views"]).toBe("—");
  });
});
