// Copyright (c) 2026 Kenneth Stott
// Canary: 642ba708-b40f-4b54-939c-d3546bb5903a
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-788: a files source's table reads the glob it spans and the column naming each row's file,
// under the labels it was registered with; a table of any other source has neither.

import { describe, it, expect, vi } from "vitest";
// REQ-2277 rule 6: names link to their panels for a viewer who opens them (EntityLink).
vi.mock("../../../context/AuthContext", () => ({
  useAuth: () => ({
    capabilities: ["access_config", "org_settings", "source_registration", "table_registration"],
  }),
}));
import { render, screen } from "../../../test-utils/render";
import type { RegisteredTable, Source } from "../../../types/admin";
import { IdentityRead } from "../TableSectionsRead";

const TABLE = {
  id: 7,
  sourceId: "lake",
  schemaName: "public",
  tableName: "events",
  domainId: "ops",
  alias: null,
  gqlNamingConvention: null,
  description: null,
  draft: false,
  region: null,
  apiEndpoint: null,
  fileGlob: "events/2026-*.parquet",
  sourceFileColumn: null,
  columns: [],
} as unknown as RegisteredTable;

describe("IdentityRead", () => {
  it("reads a files source's glob, and a source-file column it does not set as empty", () => {
    render(<IdentityRead table={TABLE} source={{ id: "lake", type: "Parquet" } as Source} />);
    const identity = screen.getByTestId("table-identity");
    expect(screen.getByText("events/2026-*.parquet")).toBeInTheDocument();
    const column = screen.getByText("Source-file column").nextElementSibling;
    expect(column).toHaveTextContent("—");
    expect(identity).toHaveTextContent("File glob");
  });

  it("has no file rows for a table of another source", () => {
    render(<IdentityRead table={TABLE} source={{ id: "pg", type: "postgresql" } as Source} />);
    expect(screen.getByTestId("table-identity")).not.toHaveTextContent("File glob");
  });
});
