// Copyright (c) 2026 Kenneth Stott
// Canary: 6ae7c922-00dc-4006-bbc4-4f0a309b6aeb
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// The header's profile icon samples the table and opens Quality, where the profile table is.

import { useState } from "react";
import { describe, it, expect, vi } from "vitest";
import { fireEvent, render, screen, within } from "../../../test-utils/render";
vi.mock("../TableClassTerm", () => ({ TableClassTerm: () => <span>class term</span> }));
import { TableReadView } from "../TableReadView";
import type { RegisteredTable } from "../../../types/admin";

function makeTable(overrides: Partial<RegisteredTable> = {}): RegisteredTable {
  return {
    id: 2,
    sourceId: "wh",
    domainId: "sales",
    schemaName: "public",
    tableName: "orders",
    alias: null,
    description: null,
    cacheTtl: null,
    roleTtl: [],
    pagingKind: null,
    pagination: null,
    pagingCeilingRows: null,
    replicate: null,
    region: null,
    draft: false,
    rowMaterialize: false,
    loadProtected: null,
    offPeakWindow: null,
    offPeakTz: null,
    refreshPolicySummary: null,
    gqlNamingConvention: null,
    watermarkColumn: null,
    changeSignal: null,
    probeQuery: null,
    probeType: null,
    columns: [],
    implicitMeasures: [],
    implicitDimensions: [],
    apiEndpoint: null,
    apiEndpointError: null,
    viewSql: null,
    dqContract: null,
    materialize: false,
    mvRefreshInterval: 300,
    mvDebounceQuiet: 0,
    mvDebounceMaxDelay: 5,
    pushDebounceQuiet: 0,
    pushDebounceMaxDelay: 5,
    mvConsistency: "shared",
    mvPreprocess: null,
    mvHoldOnFailingChecks: false,
    businessKey: [],
    validTime: null,
    recordedTime: null,
    mvPersist: "replace",
    mvPrimaryKey: [],
    mvIncremental: false,
    mvCalendar: null,
    mvGrain: null,
    mvAllowedLateness: 0,
    mvExpectedEvents: null,
    mvBusinessDayGrain: false,
    productId: null,
    enableAggregates: false,
    enableGroupBy: false,
    canDeployToDb: false,
    writeOps: ["delete", "insert", "update"],
    live: null,
    writeGrants: [],
    uniqueConstraints: [],
    graphqlFieldName: null,
    timeTravel: null,
    dqDataset: null,
    qualityCheckers: [],
    ...overrides,
  };
}

const PROFILE = {
  columns: ["id", "name"],
  rows: [
    [1, "a"],
    [2, null],
  ],
  rowCount: 2,
};

function Harness({ initial = {} }: { initial?: Record<number, unknown> }) {
  const [profiles, setProfiles] = useState<Record<number, unknown>>(initial);
  const table = makeTable({ id: 7, sourceId: "pg" });
  return (
    <TableReadView
      t={table}
      hidingOnly={false}
      dataProducts={[]}
      navigate={vi.fn()}
      viewsOnly={false}
      deploying={{}}
      setDeploying={vi.fn()}
      deployMsg={{}}
      setDeployMsg={vi.fn()}
      tableProfiles={profiles as never}
      deployViewToDb={vi.fn()}
      reload={vi.fn()}
      startEditing={vi.fn()}
      handleDelete={vi.fn()}
      handleProfile={() => setProfiles((p) => ({ ...p, 7: PROFILE }))}
      onPreview={vi.fn()}
      relationships={[]}
    />
  );
}

describe("the header's profile icon", () => {
  it("runs the profile and opens Quality with the profile table", () => {
    render(<Harness />);
    expect(screen.getByTestId("table-section-identity-panel")).toBeInTheDocument();
    fireEvent.click(screen.getByTestId("table-read-view-profile"));
    expect(screen.getByTestId("table-section-quality")).toHaveAttribute("aria-selected", "true");
    const result = screen.getByTestId("table-read-view-profile-result");
    expect(within(result).getByText("Profile: 2 sampled rows")).toBeInTheDocument();
    for (const h of ["Column", "Nulls", "Blanks", "Distinct", "Min", "Max", "Mean", "Top values"])
      expect(within(result).getByText(h)).toBeInTheDocument();
  });

  it("shows the run's progress on Quality at once", () => {
    render(<Harness initial={{ 7: "loading" }} />);
    fireEvent.click(screen.getByTestId("table-section-quality"));
    expect(screen.getByTestId("table-read-view-profiling")).toBeInTheDocument();
  });

  it("opens a profile already taken on Quality without hiding it", () => {
    render(<Harness initial={{ 7: PROFILE }} />);
    expect(screen.queryByTestId("table-read-view-profile-result")).not.toBeInTheDocument();
    fireEvent.click(screen.getByTestId("table-read-view-profile"));
    expect(screen.getByTestId("table-section-quality")).toHaveAttribute("aria-selected", "true");
    expect(screen.getByTestId("table-read-view-profile-result")).toBeInTheDocument();
  });
});
