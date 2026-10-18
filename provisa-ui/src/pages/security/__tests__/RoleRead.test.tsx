// Copyright (c) 2026 Kenneth Stott
// Canary: 3fd2105a-f149-49fa-ba07-dec64bcc060c
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// A role's panel shows every value of the role under its form's labels, label beside value as
// every detail panel lays them out, an empty one as "—".

import { describe, it, expect } from "vitest";
import { render, screen, within } from "../../../test-utils/render";
import type { Role } from "../../../types/auth";
import { RoleRead } from "../RoleRead";

const steward: Role = {
  id: "data_steward",
  description: "What data_steward is for.",
  capabilities: ["data_residency", "masking_config"],
  demonstrated: ["create_relationship"],
  domain_access: ["*"],
  parentRoleId: "analyst",
  residencyValues: ["eu", "no_region"],
  sessionVars: [
    { name: "tier", value: "gold" },
    { name: "desk", value: "7" },
  ],
  rateLimit: { requestsPerSecond: 20, maxQueryComplexity: 50, maxQueryTimeMs: 30000 },
  inactivityDays: 30, // REQ-2077
};

function fields(testId: string): [string, string][] {
  return within(screen.getByTestId(testId))
    .queryAllByRole("term")
    .map((dt) => [dt.textContent ?? "", dt.nextElementSibling?.textContent ?? ""]);
}

describe("a role's read view", () => {
  it("shows every value of the role, under the form's labels", () => {
    render(<RoleRead role={steward} />);
    expect(fields("role-read-fields")).toEqual([
      ["Role ID", "data_steward"],
      ["Description", "What data_steward is for."],
      ["Capabilities", "data_residency, masking_config"],
      ["Demonstrated capabilities", "create_relationship"],
      ["Region values covered", "eu, no_region"],
      ["Domain Access", "All Domains"],
      ["Inherits from", "analyst"],
      ["Session values", "tier = golddesk = 7"],
    ]);
    // REQ-1682: one name = value to a line.
    expect(
      Array.from(
        screen
          .getByTestId("role-session-values-data_steward")
          .querySelectorAll(".role-session-value"),
      ).map((line) => line.textContent),
    ).toEqual(["tier = gold", "desk = 7"]);
    expect(fields("role-read-limits")).toEqual([
      ["Requests / sec", "20"],
      ["Max query complexity", "50"],
      ["Max query time (ms)", "30000"],
      ["Remove grants unused for (days)", "30"],
    ]);
    // The grid every detail panel uses, not a bold label run into its value.
    expect(screen.getByTestId("role-read-fields")).toHaveClass("detail-fields");
    expect(screen.getByTestId("role-read-fields").querySelector("strong")).toBeNull();
  });

  it("shows an empty value as a dash, and a role with no domain as reaching no data", () => {
    render(
      <RoleRead
        role={{
          id: "guest",
          description: "What guest is for.",
          capabilities: [],
          demonstrated: [],
          domain_access: [],
          parentRoleId: null,
          rateLimit: null,
        }}
      />,
    );
    expect(fields("role-read-fields")).toEqual([
      ["Role ID", "guest"],
      ["Description", "What guest is for."],
      ["Capabilities", "—"],
      ["Demonstrated capabilities", "—"],
      ["Region values covered", "—"],
      ["Domain Access", "no domains — reaches no data"],
      ["Inherits from", "—"],
      ["Session values", "—"],
    ]);
    const limits = fields("role-read-limits");
    for (const [, value] of limits.slice(0, -1)) expect(value).toBe("—");
    // REQ-2077: a role that removes no grant for going unused says so.
    expect(limits.at(-1)).toEqual(["Remove grants unused for (days)", "never"]);
  });
});
