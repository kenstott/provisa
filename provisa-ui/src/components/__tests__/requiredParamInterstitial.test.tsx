// Copyright (c) 2026 Kenneth Stott
// Canary: fc2e1a61-0ff5-4077-b90b-93a50650a8d2
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// #204: Preview and Profile of a table with a required parameter ask for its value first. The
// registration records which parameters are required; a remote GraphQL table's required
// argument is a query_param, which the kind alone does not tell from an optional one.

import { describe, it, expect, vi, beforeEach } from "vitest";
import { render, screen, fireEvent, waitFor } from "../../test-utils/render";
import type { RegisteredTable } from "../../types/admin";
import { runSql } from "../../api/admin";

vi.mock("../../api/admin", () => ({ runSql: vi.fn() }));

import { GovernedTableViewer } from "../GovernedTableViewer";
import { NativeParamsModal } from "../NativeParamsModal";

function param(columnName: string, nativeFilterRequired: boolean) {
  return {
    columnName,
    alias: null,
    dataType: "varchar",
    nativeFilterType: "query_param",
    nativeFilterRequired,
  };
}

const TABLE = {
  id: 9,
  sourceId: "gh",
  domainId: "code",
  schemaName: "graphql",
  tableName: "repository",
  alias: null,
  description: null,
  apiEndpoint: null,
  viewSql: null,
  columns: [param("_nf_owner", true), param("_nf_first", false)],
} as unknown as RegisteredTable;

describe("a table with a required GraphQL argument", () => {
  beforeEach(() => {
    vi.mocked(runSql).mockReset();
    localStorage.clear();
  });

  it("is not previewed until the required value is given", async () => {
    vi.mocked(runSql).mockResolvedValue({ columns: ["id"], rows: [{ id: 1 }] });
    render(<GovernedTableViewer table={TABLE} />);
    expect(screen.getByTestId("table-preview-params-hint")).toBeInTheDocument();
    expect(screen.getByTestId("preview-param-_nf_owner")).toBeRequired();
    expect(screen.getByTestId("preview-param-_nf_first")).not.toBeRequired();
    expect(screen.getByTestId("preview-run-btn")).toBeDisabled();
    await new Promise((r) => setTimeout(r, 400)); // past the viewer's debounce
    expect(runSql).not.toHaveBeenCalled();

    fireEvent.change(screen.getByTestId("preview-param-_nf_owner"), {
      target: { value: "octocat" },
    });
    fireEvent.click(screen.getByTestId("preview-run-btn"));
    await waitFor(() => expect(runSql).toHaveBeenCalled());
    const sent = vi.mocked(runSql).mock.calls[0][0] as { sql: string; params: unknown[] };
    expect(sent.sql).toContain('"_nf_owner" =');
    expect(sent.params).toEqual(["octocat"]);
  });

  it("is profiled only once the dialog has the required value", () => {
    const onSubmit = vi.fn();
    render(<NativeParamsModal table={TABLE} onClose={vi.fn()} onSubmit={onSubmit} />);
    expect(screen.getByTestId("native-param-_nf_owner")).toBeRequired();
    expect(screen.getByTestId("native-param-_nf_first")).not.toBeRequired();
    expect(screen.getByTestId("native-params-run-btn")).toBeDisabled();
    fireEvent.change(screen.getByTestId("native-param-_nf_owner"), {
      target: { value: "octocat" },
    });
    fireEvent.click(screen.getByTestId("native-params-run-btn"));
    expect(onSubmit).toHaveBeenCalledWith({ _nf_owner: "octocat" });
  });
});
