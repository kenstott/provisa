// Copyright (c) 2026 Kenneth Stott
// Canary: f8cac15c-69b9-4be6-945a-bc2880a14c1f
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-1986: the table page lists a table's parameters, each with its type, how it reaches the
// source and the required switch -- on and locked where the source requires the parameter.

import { describe, expect, it, vi } from "vitest";
import { fireEvent, render, screen, within } from "@testing-library/react";
import { MantineProvider } from "@mantine/core";
import "../../../i18n";
import { ParametersPanel } from "../ParametersPanel";
import type { TableColumn } from "../../../types/admin";

function column(columnName: string, patch: Partial<TableColumn> = {}): TableColumn {
  return {
    id: 0,
    columnName,
    visibleTo: [],
    masks: [],
    alias: null,
    computedSqlAlias: columnName,
    computedGqlAlias: columnName,
    description: null,
    dataType: "varchar",
    nativeFilterType: null,
    isPrimaryKey: false,
    isForeignKey: false,
    isAlternateKey: false,
    scope: "domain",
    isImplicitMeasure: false,
    isImplicitDimension: false,
    ...patch,
  };
}

const COLUMNS = [
  column("id", { dataType: "integer" }),
  column("_nf_id", {
    dataType: "integer",
    nativeFilterType: "path_param",
    nativeFilterRequired: true,
  }),
  column("_nf_region", { nativeFilterType: "query_param", nativeFilterRequired: false }),
  column("_nf_status", {
    nativeFilterType: "graphql_arg",
    nativeFilterRequired: false,
    requiredBySteward: true,
  }),
];

function renderPanel(columns: TableColumn[], onRequiredChange?: (i: number, r: boolean) => void) {
  render(
    <MantineProvider>
      <ParametersPanel columns={columns} onRequiredChange={onRequiredChange} />
    </MantineProvider>,
  );
}

const switchOf = (name: string) =>
  within(screen.getByTestId(`parameter-${name}`)).getByRole("switch", { hidden: true });

describe("ParametersPanel", () => {
  it("lists every parameter with its type and how it reaches the source", () => {
    renderPanel(COLUMNS, vi.fn());
    expect(screen.getByText("2 of 3 required")).toBeInTheDocument();
    const id = screen.getByTestId("parameter-id");
    expect(within(id).getByText("integer")).toBeInTheDocument();
    expect(within(id).getByText("Path parameter")).toBeInTheDocument();
    expect(
      within(screen.getByTestId("parameter-status")).getByText("GraphQL argument"),
    ).toBeInTheDocument();
    expect(screen.queryByTestId("parameter-_nf_id")).toBeNull();
  });

  it("locks the switch on for a parameter the source requires", () => {
    renderPanel(COLUMNS, vi.fn());
    expect(switchOf("id")).toBeChecked();
    expect(switchOf("id")).toBeDisabled();
    expect(
      within(screen.getByTestId("parameter-id")).getByText("Required by the source"),
    ).toBeInTheDocument();
  });

  it("lets a steward make an optional parameter required, and optional again", () => {
    const onRequiredChange = vi.fn();
    renderPanel(COLUMNS, onRequiredChange);
    expect(switchOf("region")).not.toBeChecked();
    fireEvent.click(switchOf("region"));
    expect(onRequiredChange).toHaveBeenLastCalledWith(2, true);
    expect(switchOf("status")).toBeChecked();
    fireEvent.click(switchOf("status"));
    expect(onRequiredChange).toHaveBeenLastCalledWith(3, false);
  });

  it("only shows the switches where nothing can be changed", () => {
    renderPanel(COLUMNS);
    expect(switchOf("region")).toBeDisabled();
    expect(switchOf("status")).toBeChecked();
  });

  it("is absent for a table with no parameters", () => {
    renderPanel([column("id")]);
    expect(screen.queryByTestId("parameters-panel")).toBeNull();
  });
});
