// Copyright (c) 2026 Kenneth Stott
// Canary: 4e9b7052-cd6a-4f38-e1b4-62ad5fc09b43
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-1327: local users are the org's people, on Team, with the org's roles and domains.
import { readFileSync } from "fs";
import { resolve } from "path";
import { describe, expect, it, vi } from "vitest";
import { render, screen } from "../test-utils/render";

vi.mock("../hooks/useAdminQueries", () => ({
  useRoles: () => ({ roles: [{ id: "analyst" }] }),
  useDomains: () => ({ domains: [{ id: "" }, { id: "sales" }] }),
}));
vi.mock("../components/admin/LocalUsersTab", () => ({
  LocalUsersTab: ({ allRoles, allDomains }: { allRoles: string[]; allDomains: string[] }) => (
    <div>{`${allRoles.join()}|${allDomains.join()}`}</div>
  ),
}));

import { TeamLocalUsers } from "../components/admin/TeamLocalUsers";

describe("Team's Local users section", () => {
  it("assigns the org's roles and named domains", () => {
    render(<TeamLocalUsers />);
    expect(screen.getByText("analyst|sales")).toBeInTheDocument();
  });

  it("is a section of the Team page, under its user_management gate", () => {
    const team = readFileSync(resolve(__dirname, "..", "pages/TeamPage.tsx"), "utf-8");
    expect(team).toContain('value="local-users"');
    expect(team).toContain("<TeamLocalUsers />");
    const app = readFileSync(resolve(__dirname, "..", "App.tsx"), "utf-8");
    const route = app.slice(app.indexOf('path="/team"')).slice(0, 300);
    expect(route).toContain('capability="user_management"');
  });
});
