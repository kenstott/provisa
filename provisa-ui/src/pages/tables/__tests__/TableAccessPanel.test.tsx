// Copyright (c) 2026 Kenneth Stott
// Canary: db68a663-7a82-4334-93a0-1d893749676a
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-2162, as its scenario sets it: claims.diagnosis is granted to data_steward and to
// clinical_reviewer through analyst; choosing diagnosis lists both with the grant, inheritance and
// reach behind each.

import { describe, expect, it, vi } from "vitest";
import { fireEvent, render, screen, within } from "../../../test-utils/render";
import { TableAccessPanel } from "../TableAccessPanel";
import type { TableAccess } from "../../../api/roleRights";

const ACCESS: TableAccess = {
  table_id: 1,
  domain: "claims",
  name: "claims.claims",
  table_name: "claims",
  columns: [
    {
      name: "amount",
      scope: "domain",
      access: [
        {
          role: "clinical_reviewer",
          standard: false,
          read: true,
          write: [],
          mask: { type: "regex" },
          row_rule: null,
          granted_by: "viewer",
          via: ["clinical_reviewer", "viewer"],
          reach: "domain",
        },
      ],
    },
    {
      name: "diagnosis",
      scope: "restricted",
      access: [
        {
          role: "clinical_reviewer",
          standard: false,
          read: true,
          write: ["update"],
          mask: null,
          row_rule: "region = 'eu'",
          granted_by: "analyst",
          via: ["clinical_reviewer", "analyst"],
          reach: "domain",
        },
        {
          role: "data_steward",
          standard: true,
          read: true,
          write: [],
          mask: null,
          row_rule: null,
          granted_by: "data_steward",
          via: ["data_steward"],
          reach: "domain",
        },
      ],
    },
  ],
};

vi.mock("../../../api/roleRights", () => ({
  fetchTableAccess: (id: number) => Promise.resolve({ ...ACCESS, table_id: id }),
}));

describe("who can read a table's columns (REQ-2162)", () => {
  it("lists, for the chosen column, each role with the grant behind it", async () => {
    render(<TableAccessPanel tableId={1} />);
    // The first column is shown first: Viewer's grant, masked for the reviewer.
    const first = await screen.findByTestId("table-access-roles");
    expect(within(first).getByTestId("table-access-role-clinical_reviewer")).toHaveTextContent(
      "viewer",
    );
    fireEvent.click(screen.getByTestId("table-access-column"));
    fireEvent.click(await screen.findByRole("option", { name: "diagnosis", hidden: true }));
    const reviewer = await screen.findByTestId("table-access-role-clinical_reviewer");
    expect(reviewer).toHaveTextContent("analyst");
    expect(reviewer).toHaveTextContent("update");
    expect(reviewer).toHaveTextContent("region = 'eu'");
    const steward = screen.getByTestId("table-access-role-data_steward");
    expect(steward).toHaveTextContent("standard role");
  });

  it("asks nothing of a table not yet saved", () => {
    render(<TableAccessPanel tableId={null} />);
    expect(screen.getByTestId("table-access-unsaved")).toBeInTheDocument();
  });
});
