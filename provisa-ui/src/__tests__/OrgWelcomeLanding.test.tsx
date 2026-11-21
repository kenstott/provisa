// Copyright (c) 2026 Kenneth Stott
// Canary: 8c14e7a2-5f90-4d3b-b6e1-2a7d90c4f315
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-1327: the welcome's continue action opens the acting role's first page (landingPath), not a
// fixed /query that a role without the query right would meet as a refusal.
import { describe, it, expect, vi, beforeEach } from "vitest";
import userEvent from "@testing-library/user-event";
import { useLocation } from "react-router-dom";
import { render, screen } from "../test-utils/render";
import { OrgWelcomePage } from "../pages/OrgWelcomePage";
import { landingPath } from "../components/navGroups";

const mockAuth = {
  activeOrgId: "carolco",
  billing: false,
  capabilities: [] as string[],
  orgMemberships: [{ org_id: "carolco", role_id: "viewer" }],
};
vi.mock("../context/AuthContext", async (importOriginal) => ({
  ...(await importOriginal<typeof import("../context/AuthContext")>()),
  useAuth: () => mockAuth,
}));

function Where() {
  return <div data-testid="where">{useLocation().pathname}</div>;
}

async function continueFromWelcome() {
  render(
    <>
      <OrgWelcomePage />
      <Where />
    </>,
    { initialEntries: ["/welcome"] },
  );
  await userEvent.setup().click(screen.getByTestId("onboard-welcome-continue"));
}

beforeEach(() => {
  mockAuth.capabilities = [];
});

describe("the welcome's continue action", () => {
  it("opens the first page of a role that has no query page", async () => {
    mockAuth.capabilities = [];
    const first = landingPath([], false);
    expect(first).not.toMatch(/^\/query/);
    await continueFromWelcome();
    expect(screen.getByTestId("where").textContent).toBe(first);
  });

  it("opens Explore, the query pages' group, for a query-capable role", async () => {
    mockAuth.capabilities = ["query_development"];
    // Explore is the query role's first group, and its first entry is the schema page.
    const first = landingPath(["query_development"], false);
    expect(first).toBe("/schema");
    await continueFromWelcome();
    expect(screen.getByTestId("where").textContent).toBe(first);
  });
});
