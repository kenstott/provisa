// Copyright (c) 2026 Kenneth Stott
// Canary: 513a0343-5dc1-40d6-98a8-122fee1060f4
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-1986: a parameterized table is obvious. Its Tables list row is marked PARAMETERIZED, or
// NEEDS PARAMS in the warning colour when a read must give a parameter; its Identity section lists
// each parameter under the query links, by the name a caller gives it.

import { describe, expect, it } from "vitest";
import { render, screen, within } from "../../../test-utils/render";
import type { TableColumn } from "../../../types/admin";
import { ParameterizedBadge } from "../ParameterizedBadge";
import { IdentityParameters } from "../IdentityParameters";

const column = (over: Partial<TableColumn>): TableColumn =>
  ({
    id: 1,
    columnName: "name",
    alias: null,
    dataType: "text",
    description: null,
    nativeFilterType: null,
    nativeFilterRequired: null,
    ...over,
  }) as TableColumn;

const DATA = column({ id: 1, columnName: "name" });
const OWNER = column({
  id: 2,
  columnName: "_nf_owner",
  nativeFilterType: "path_param",
  nativeFilterRequired: true,
  description: "The account that owns the repositories",
});
const SINCE = column({
  id: 3,
  columnName: "_nf_since",
  dataType: "timestamp",
  nativeFilterType: "query_param",
  nativeFilterRequired: false,
});

describe("the Tables list's parameterized badge", () => {
  it("marks a table whose parameters are all optional PARAMETERIZED", () => {
    render(<ParameterizedBadge tableName="repos" columns={[DATA, SINCE]} />);
    const badge = screen.getByTestId("tables-parameterized-repos");
    expect(badge).toHaveTextContent("parameterized");
    expect(badge).toHaveAttribute("title", "Parameterized: a read may give since");
    expect(screen.queryByTestId("tables-needs-params-repos")).toBeNull();
  });

  it("marks a table a read must give a parameter NEEDS PARAMS, in the warning colour", () => {
    render(<ParameterizedBadge tableName="repos" columns={[DATA, OWNER, SINCE]} />);
    const badge = screen.getByTestId("tables-needs-params-repos");
    expect(badge).toHaveTextContent("needs params");
    expect(badge).toHaveAttribute("title", "Needs parameters: a read must give owner");
    expect(badge.getAttribute("style") ?? "").toContain("yellow");
    expect(screen.queryByTestId("tables-parameterized-repos")).toBeNull();
  });

  it("counts a parameter a steward requires as required", () => {
    render(
      <ParameterizedBadge
        tableName="repos"
        columns={[DATA, { ...SINCE, requiredBySteward: true }]}
      />,
    );
    expect(screen.getByTestId("tables-needs-params-repos")).toBeInTheDocument();
  });

  it("marks a table without parameters with nothing", () => {
    render(<ParameterizedBadge tableName="repos" columns={[DATA]} />);
    expect(screen.queryByTestId("tables-parameterized-repos")).toBeNull();
    expect(screen.queryByTestId("tables-needs-params-repos")).toBeNull();
  });
});

describe("a table's Identity parameters", () => {
  it("lists each parameter by its name, with its type, whether required, and description", () => {
    render(<IdentityParameters columns={[DATA, OWNER, SINCE]} />);
    const block = screen.getByTestId("table-identity-parameters");
    expect(within(block).getByText("Parameters")).toBeInTheDocument();
    const owner = screen.getByTestId("table-identity-parameter-owner");
    expect(owner).toHaveTextContent("owner");
    expect(owner).not.toHaveTextContent("_nf_");
    expect(owner).toHaveTextContent("text");
    expect(screen.getByTestId("table-identity-parameter-owner-required")).toHaveTextContent(
      "Required",
    );
    expect(owner).toHaveTextContent("The account that owns the repositories");
    const since = screen.getByTestId("table-identity-parameter-since");
    expect(since).toHaveTextContent("timestamp");
    expect(screen.getByTestId("table-identity-parameter-since-required")).toHaveTextContent(
      "Optional",
    );
    // The data column is no parameter.
    expect(screen.queryByTestId("table-identity-parameter-name")).toBeNull();
  });

  it("shows nothing for a table without parameters", () => {
    render(<IdentityParameters columns={[DATA]} />);
    expect(screen.queryByTestId("table-identity-parameters")).toBeNull();
  });
});
