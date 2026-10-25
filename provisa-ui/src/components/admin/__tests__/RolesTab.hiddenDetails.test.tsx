// Copyright (c) 2026 Kenneth Stott
// Canary: 9c4e2a71-8d36-4b05-a1f9-3e7b5d0c6842
// A role whose definition the server withheld shows its id and a plain note, with no edit or delete
// control; a role the caller may read shows its capabilities and the delete control.

import { describe, it, expect, vi } from "vitest";
import { fireEvent, screen, waitFor, within } from "@testing-library/react";
import { render } from "../../../test-utils/render";
import * as api from "../../../api/admin";
import { normalizeRole } from "../../../lib/roles";
import { RolesTab } from "../RolesTab";

vi.mock("../../../api/admin", async (orig) => ({
  ...(await orig<typeof import("../../../api/admin")>()),
  fetchOrgRoles: vi.fn(),
  deleteOrgRole: vi.fn(async () => undefined),
}));

describe("normalizeRole", () => {
  it("marks a role arriving as an id alone as hidden", () => {
    expect(
      normalizeRole({ id: "org_admin", description: "What org_admin is for.", standard: true }),
    ).toMatchObject({
      id: "org_admin",
      detailsHidden: true,
    });
    expect(
      normalizeRole({
        id: "x",
        description: "What x is for.",
        standard: false,
        capabilities: null,
        domainAccess: null,
      }).detailsHidden,
    ).toBe(true);
  });

  it("keeps a full definition and reads either spelling of domain access", () => {
    const role = normalizeRole({
      id: "a",
      description: "What a is for.",
      standard: false,
      capabilities: ["usage"],
      domainAccess: ["sales"],
    });
    expect(role.detailsHidden).toBeUndefined();
    expect(role.capabilities).toEqual(["usage"]);
    expect(role.domain_access).toEqual(["sales"]);
  });
});

describe("RolesTab", () => {
  it("shows a hidden role by id with a note and no controls", async () => {
    vi.mocked(api.fetchOrgRoles).mockResolvedValue([
      normalizeRole({
        id: "shop_analyst",
        description: "What shop_analyst is for.",
        standard: false,
        capabilities: ["usage"],
        domain_access: ["sales"],
      }),
      normalizeRole({ id: "org_admin", description: "What org_admin is for.", standard: true }),
    ]);
    render(<RolesTab orgId="acme" />);

    expect(await screen.findByText("org_admin")).toBeTruthy();
    await waitFor(() => expect(screen.getByTestId("role-details-hidden-org_admin")).toBeTruthy());
    expect(screen.getByTestId("role-details-hidden-org_admin").textContent).toBe(
      "Details require user management.",
    );
    expect(screen.queryByTestId("delete-role-org_admin")).toBeNull();

    expect(screen.getByText("usage")).toBeTruthy();
    expect(screen.getByTestId("delete-role-shop_analyst")).toBeTruthy();
  });

  it("asks before deleting a role, and deletes only on confirm", async () => {
    vi.mocked(api.fetchOrgRoles).mockResolvedValue([
      normalizeRole({
        id: "shop_analyst",
        description: "What shop_analyst is for.",
        standard: false,
        capabilities: ["usage"],
        domain_access: ["sales"],
      }),
    ]);
    render(<RolesTab orgId="acme" />);

    fireEvent.click(await screen.findByTestId("delete-role-shop_analyst"));
    const dialog = await screen.findByRole("dialog");
    expect(within(dialog).getByText('Delete role "shop_analyst"?')).toBeTruthy();
    expect(api.deleteOrgRole).not.toHaveBeenCalled();

    fireEvent.click(within(dialog).getByRole("button", { name: "Confirm" }));
    await waitFor(() => expect(api.deleteOrgRole).toHaveBeenCalledWith("acme", "shop_analyst"));
  });
});
