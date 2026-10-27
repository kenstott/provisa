// Copyright (c) 2026 Kenneth Stott
// Canary: 2ca63a5b-eca1-4831-ba95-6c290a16ba4d
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-2161: a role's Data access tab -- every table it reaches, each column with its scope, access
// and the grant that serves it; the page's domain filter narrows the tables.

import { describe, expect, it, vi } from "vitest";
import { render, screen, within } from "../../../test-utils/render";
import { RoleDataAccess } from "../RoleDataAccess";
import type { RoleEffective } from "../../../api/roleRights";

const checked = vi.hoisted(() => ({ domains: new Set<string>() }));
vi.mock("../../../context/DomainFilterContext", () => ({
  useDomainFilter: () => ({ checkedDomains: checked.domains }),
}));

const column = (name: string, over = {}) => ({
  name,
  scope: "domain",
  read: true,
  write: [] as ("insert" | "update" | "delete")[],
  mask: null,
  row_rule: null,
  granted_by: "viewer",
  via: ["dq_lead", "viewer"],
  reach: "domain" as const,
  ...over,
});

const VIEW = {
  role: "dq_lead",
  data: [
    {
      table_id: 1,
      domain: "claims",
      name: "claims.claims",
      table_name: "claims",
      reach: "domain",
      columns: [
        column("diagnosis", {
          scope: "restricted",
          granted_by: "analyst",
          via: ["dq_lead", "dq_steward", "analyst"],
        }),
      ],
    },
    {
      table_id: 2,
      domain: "billing",
      name: "billing.inv",
      table_name: "invoices",
      reach: "across",
      columns: [column("total", { scope: "public", reach: "public" })],
    },
  ],
} as unknown as RoleEffective;

vi.mock("../../../api/roleRights", () => ({ fetchRoleEffective: () => Promise.resolve(VIEW) }));

describe("a role's Data access tab (REQ-2161)", () => {
  it("lists each table it reaches and each column's grant, with the path it inherits it by", async () => {
    render(<RoleDataAccess roleId="dq_lead" />);
    const diagnosis = await screen.findByTestId("role-data-column-1-diagnosis");
    expect(diagnosis).toHaveTextContent("restricted");
    expect(diagnosis).toHaveTextContent("analyst");
    expect(diagnosis).toHaveTextContent("through dq_steward");
    expect(
      within(screen.getByTestId("role-data-table-2")).getByText("across domains"),
    ).toBeTruthy();
  });

  it("narrows to the domains the page's filter keeps", async () => {
    checked.domains = new Set(["billing"]);
    render(<RoleDataAccess roleId="dq_lead" />);
    await screen.findByTestId("role-data-table-2");
    expect(screen.queryByTestId("role-data-table-1")).toBeNull();
    checked.domains = new Set();
  });
});
