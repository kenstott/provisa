// Copyright (c) 2026 Kenneth Stott
// Canary: a2b26616-ddce-46d1-b58c-4974ff16d526
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-2161, as its scenario sets it: dq_lead inherits Data quality steward and Data product
// owner; its panel lists each capability with the standard role it comes from.

import { describe, expect, it, vi } from "vitest";
import { render, screen, within } from "../../../test-utils/render";
import { RoleCapabilities } from "../RoleCapabilities";
import type { RoleEffective } from "../../../api/roleRights";

const VIEW: RoleEffective = {
  role: "dq_lead",
  standard: false,
  chain: ["dq_lead", "dq_steward", "viewer", "dp_owner"],
  parents: {
    dq_lead: ["dq_steward", "dp_owner"],
    dq_steward: ["viewer"],
    viewer: [],
    dp_owner: ["viewer"],
  },
  adds: {
    viewer: ["read"],
    dq_steward: ["dq_rw"],
    dp_owner: ["data_product_owned_rw"],
    dq_lead: ["export"],
  },
  standards: ["dq_steward", "viewer", "dp_owner"],
  capabilities: [
    // Entering in two branches, it comes from both.
    { right: "data_product_owned_rw", from: ["dp_owner", "dq_steward"] },
    { right: "dq_rw", from: ["dq_steward"] },
    { right: "export", from: ["dq_lead"] },
    { right: "read", from: ["viewer"] },
  ],
};

const asked = vi.hoisted(() => ({ roles: [] as string[] }));
vi.mock("../../../api/roleRights", () => ({
  fetchRoleEffective: (id: string) => {
    asked.roles.push(id);
    return Promise.resolve(VIEW);
  },
}));
// The diagram is the lineage page's own component, held by its own tests.
vi.mock("../../../components/lineage/LineageDag", () => ({
  LineageDag: ({ graph }: { graph: { nodes: unknown[] } }) => (
    <div data-testid="lineage-dag" data-nodes={graph.nodes.length} />
  ),
}));

describe("a role's Capabilities tab (REQ-2161)", () => {
  it("lists each capability with the role it comes from, standard roles marked", async () => {
    render(<RoleCapabilities roleId="dq_lead" />);
    const list = await screen.findByTestId("role-capabilities-list");
    expect(asked.roles).toEqual(["dq_lead"]);
    const row = (right: string) => within(list).getByTestId(`role-capability-${right}`);
    expect(row("dq_rw")).toHaveTextContent("dq_steward");
    expect(row("dq_rw")).toHaveTextContent("standard role");
    expect(row("data_product_owned_rw")).toHaveTextContent("dp_owner");
    expect(row("data_product_owned_rw")).toHaveTextContent("dq_steward");
    expect(row("export")).toHaveTextContent("its own");
    expect(row("export")).not.toHaveTextContent("standard role");
    // The lineage: a box per role, a kind row each plus what each adds and the accumulated set.
    expect(screen.getByTestId("lineage-dag")).toHaveAttribute("data-nodes", String(4 + 3 + 4));
  });
});
