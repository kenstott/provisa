// Copyright (c) 2026 Kenneth Stott
// Canary: e3b06819-849e-4bea-802c-51e97d1700a3
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.

// REQ-2099: a relationship whose target takes parameters (a code table's function arguments)
// shows one choice per parameter: the source column that feeds it, or none. A target with no
// parameters shows nothing.

import { describe, it, expect, vi } from "vitest";
import { render, screen } from "@testing-library/react";
import { MantineProvider } from "@mantine/core";
import { ParametersPanel } from "../components/relationships/ParametersPanel";
import { EMPTY_FORM, type RelForm } from "../components/relationships/relationship-types";
import type { RegisteredTable } from "../types/admin";

const TABLES = [
  {
    tableName: "churn_features",
    alias: null,
    columns: [
      { columnName: "customer_id", nativeFilterType: null },
      { columnName: "plan", nativeFilterType: null },
      { columnName: "tenure_months", nativeFilterType: null },
    ],
  },
  {
    tableName: "churn_risk",
    alias: null,
    columns: [
      { columnName: "_nf_customer_id", nativeFilterType: "query_arg" },
      { columnName: "_nf_plan", nativeFilterType: "query_arg" },
      { columnName: "customer_id", nativeFilterType: null },
      { columnName: "score", nativeFilterType: null },
    ],
  },
  { tableName: "orders", alias: null, columns: [{ columnName: "id", nativeFilterType: null }] },
] as unknown as RegisteredTable[];

function mount(form: RelForm) {
  render(
    <MantineProvider theme={{ components: {} }}>
      <ParametersPanel form={form} setForm={vi.fn()} tables={TABLES} testIdPrefix="rel-edit" />
    </MantineProvider>,
  );
}

describe("REQ-2099 parameters a relationship feeds", () => {
  it("offers a source column for each of the target's parameters", () => {
    mount({
      ...EMPTY_FORM,
      sourceTableId: "churn_features",
      targetTableId: "churn_risk",
      parameters: [{ parameter: "customer_id", sourceColumn: "customer_id" }],
    });
    expect(screen.getByTestId("rel-edit-parameters-panel")).toBeTruthy();
    const fed = screen.getByTestId("rel-edit-parameter-customer_id") as HTMLInputElement;
    expect(fed.value).toBe("customer_id");
    const unfed = screen.getByTestId("rel-edit-parameter-plan") as HTMLInputElement;
    expect(unfed.value).toBe("");
    expect(screen.queryByTestId("rel-edit-parameter-score")).toBeNull();
  });

  it("shows nothing for a target without parameters", () => {
    mount({ ...EMPTY_FORM, sourceTableId: "churn_features", targetTableId: "orders" });
    expect(screen.queryByTestId("rel-edit-parameters-panel")).toBeNull();
  });
});
