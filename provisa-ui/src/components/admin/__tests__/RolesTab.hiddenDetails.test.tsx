// Copyright (c) 2026 Kenneth Stott
// Canary: 9c4e2a71-8d36-4b05-a1f9-3e7b5d0c6842
// A role whose definition the server withheld shows its id and a plain note, with no edit or delete
// control; a role the caller may read shows its capabilities and the delete control.

import { describe, it, expect, vi } from "vitest";
import { screen, waitFor } from "@testing-library/react";
import { render } from "../../../test-utils/render";
import * as api from "../../../api/admin";
import { normalizeRole } from "../../../lib/roles";
import { RolesTab } from "../RolesTab";

vi.mock("../../../api/admin", async (orig) => ({
  ...(await orig<typeof import("../../../api/admin")>()),
  fetchOrgRoles: vi.fn(),
}));

describe("normalizeRole", () => {
  it("marks a role arriving as an id alone as hidden", () => {
    expect(normalizeRole({ id: "org_admin", origin: "admin" })).toMatchObject({
      id: "org_admin",
      detailsHidden: true,
    });
    expect(normalizeRole({ id: "x", origin: "admin", capabilities: null, domainAccess: null }).detailsHidden).toBe(
      true,
    );
  });

  it("keeps a full definition and reads either spelling of domain access", () => {
    const role = normalizeRole({ id: "a", origin: "admin", capabilities: ["usage"], domainAccess: ["sales"] });
    expect(role.detailsHidden).toBeUndefined();
    expect(role.capabilities).toEqual(["usage"]);
    expect(role.domain_access).toEqual(["sales"]);
  });
});

describe("RolesTab", () => {
  it("shows a hidden role by id with a note and no controls", async () => {
    vi.mocked(api.fetchOrgRoles).mockResolvedValue([
      normalizeRole({ id: "analyst", origin: "admin", capabilities: ["usage"], domain_access: ["sales"] }),
      normalizeRole({ id: "org_admin", origin: "admin" }),
    ]);
    render(<RolesTab orgId="acme" />);

    expect(await screen.findByText("org_admin")).toBeTruthy();
    await waitFor(() => expect(screen.getByTestId("role-details-hidden-org_admin")).toBeTruthy());
    expect(screen.getByTestId("role-details-hidden-org_admin").textContent).toBe(
      "Details require user management.",
    );
    expect(screen.queryByTestId("delete-role-org_admin")).toBeNull();

    expect(screen.getByText("usage")).toBeTruthy();
    expect(screen.getByTestId("delete-role-analyst")).toBeTruthy();
  });
});
