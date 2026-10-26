// Copyright (c) 2026 Kenneth Stott
// Canary: 30fdfba7-c534-47f6-89ec-c92943791462
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-2161: a role's lineage as the lineage diagram draws it -- a box per role from the standard
// roles to the selected one, each listing what it adds and the last the accumulated set, joined
// parent to child.

import { describe, expect, it } from "vitest";
import { roleLineageGraph } from "../roleLineage";
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
    { right: "data_product_owned_rw", from: "dp_owner" },
    { right: "dq_rw", from: "dq_steward" },
    { right: "export", from: "dq_lead" },
    { right: "read", from: "viewer" },
  ],
};

const kind = (k: "standard" | "custom" | "meta") => `kind:${k}`;

describe("a role's lineage", () => {
  const g = roleLineageGraph(VIEW, kind);
  const rows = (role: string) => g.nodes.filter((n) => n.relation === role).map((n) => n.column);

  it("lists what each role adds, and the selected role the accumulated set", () => {
    expect(rows("viewer")).toEqual(["kind:standard", "read"]);
    expect(rows("dq_steward")).toEqual(["kind:standard", "dq_rw"]);
    expect(rows("dq_lead")).toEqual([
      "kind:custom",
      "data_product_owned_rw",
      "dq_rw",
      "export",
      "read",
    ]);
  });

  it("joins each parent to its child, from the standard roles down", () => {
    const pairs = g.edges.map((e) => [e.source.split("::")[0], e.target.split("::")[0]]);
    expect(pairs).toEqual(
      expect.arrayContaining([
        ["dq_steward", "dq_lead"],
        ["dp_owner", "dq_lead"],
        ["viewer", "dq_steward"],
        ["viewer", "dp_owner"],
      ]),
    );
    expect(pairs).toHaveLength(4);
    expect(g.outputs).toEqual(g.nodes.filter((n) => n.relation === "dq_lead").map((n) => n.id));
  });

  it("names a meta-role's kind, its members its branches", () => {
    const meta = roleLineageGraph(
      {
        ...VIEW,
        role: "meta:dp_owner+dq_steward",
        chain: ["meta:dp_owner+dq_steward", "dp_owner", "viewer", "dq_steward"],
        parents: { ...VIEW.parents, "meta:dp_owner+dq_steward": ["dp_owner", "dq_steward"] },
        adds: { ...VIEW.adds, "meta:dp_owner+dq_steward": [] },
      },
      kind,
    );
    expect(meta.nodes.find((n) => n.relation === "meta:dp_owner+dq_steward")?.column).toBe(
      "kind:meta",
    );
  });
});
