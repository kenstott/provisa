// Copyright (c) 2026 Kenneth Stott
// Canary: e1fb2d2b-96ea-4253-a127-340510539764
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// A result the server landed in the results store comes back as a handle, not rows. The viewer
// says so, with where it is: never "No rows", and never a crash on the column list it did not get.

import { afterEach, describe, expect, it, vi } from "vitest";
import { render, screen } from "../../test-utils/render";
import type { RegisteredTable } from "../../types/admin";
import { GovernedTableViewer } from "../GovernedTableViewer";

const TABLE = {
  id: 1,
  sourceId: "provisa-admin",
  domainId: "ops",
  schemaName: "ops",
  tableName: "usage_ranking",
  alias: null,
  description: null,
  apiEndpoint: null,
  viewSql: null,
  draft: false,
  columns: [],
} as unknown as RegisteredTable;

afterEach(() => vi.unstubAllGlobals());

describe("GovernedTableViewer — a result landed in the results store", () => {
  it("says where the result is and how many rows it holds", async () => {
    vi.stubGlobal(
      "fetch",
      vi.fn().mockResolvedValue(
        new Response(
          JSON.stringify({
            data: { sql: null },
            redirect: {
              redirect_url: "https://results.example/r/abc.parquet",
              row_count: 2500000,
              expires_in: 3600,
              content_type: "application/vnd.apache.parquet",
            },
          }),
          { status: 200, headers: { "Content-Type": "application/json" } },
        ),
      ),
    );
    render(<GovernedTableViewer table={TABLE} role="analyst" />);
    const notice = await screen.findByTestId("result-redirected");
    expect(notice).toHaveTextContent("2,500,000");
    expect(screen.getByRole("link", { name: /download/i })).toHaveAttribute(
      "href",
      "https://results.example/r/abc.parquet",
    );
    expect(screen.queryByText("No rows.")).toBeNull();
  });
});
