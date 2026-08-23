// Copyright (c) 2026 Kenneth Stott
// Canary: 6b2d4f18-91c7-4a52-8e30-7d5a9c1f2b64
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-1559: Admin's entries stand in a vertical rail on an admin route, and nowhere else. What is
// worth testing is that the rail shows exactly the surfaces the person's rights reach — a link to
// one they do not hold leads straight to a permission error.

import { describe, it, expect, vi, beforeEach } from "vitest";
import { MemoryRouter } from "react-router-dom";
import { cleanup, render, screen } from "../test-utils/render";
import { AdminRail } from "../components/AdminRail";

const auth = {
  loading: false,
  capabilities: ["org_settings"] as string[],
  billing: false,
};
vi.mock("../context/AuthContext", () => ({ useAuth: () => auth }));

beforeEach(() => {
  auth.capabilities = ["org_settings"];
  auth.billing = false;
});

function renderAt(path: string) {
  render(
    <MemoryRouter initialEntries={[path]}>
      <AdminRail />
    </MemoryRouter>,
  );
}

describe("AdminRail", () => {
  it("stands on an admin route", () => {
    renderAt("/admin/tags");
    expect(screen.getByTestId("admin-rail")).toBeInTheDocument();
    expect(screen.getByRole("link", { name: "Tags" })).toHaveAttribute("href", "/admin/tags");
  });

  it("is absent everywhere else, so no other page loses its width to it", () => {
    renderAt("/query");
    expect(screen.queryByTestId("admin-rail")).toBeNull();
  });

  it("offers an org administrator no deployment-wide surface", () => {
    // REQ-1573: the seeded org_admin carries the environments right alongside org_settings.
    auth.capabilities = ["org_settings", "environment_management"];
    renderAt("/admin/tags");
    // Security is platform_settings; an org admin holding only org_settings must not see a link
    // that answers with "You do not have permission to view this page."
    expect(screen.queryByRole("link", { name: "Security" })).toBeNull();
    expect(screen.getByRole("link", { name: "Environments" })).toBeInTheDocument();
  });

  it("offers Environments on its own right and not on org settings", () => {
    // REQ-1573: a developer manages environments while holding no org settings at all, and an
    // org_settings holder without the right sees no link to a page that would refuse them.
    auth.capabilities = ["environment_management"];
    renderAt("/admin/tags");
    expect(screen.getByRole("link", { name: "Environments" })).toBeInTheDocument();
    cleanup();
    auth.capabilities = ["org_settings"];
    renderAt("/admin/tags");
    expect(screen.queryByRole("link", { name: "Environments" })).toBeNull();
  });

  it("offers a platform administrator the deployment-wide surfaces", () => {
    auth.capabilities = ["platform_settings"];
    renderAt("/admin/security");
    expect(screen.getByRole("link", { name: "Security" })).toHaveAttribute(
      "href",
      "/admin/security",
    );
  });

  it("carries no entry of its own for Secrets, which is a Security sub-tab", () => {
    auth.capabilities = ["org_settings", "platform_settings"];
    renderAt("/admin/tags");
    expect(screen.queryByRole("link", { name: "Secrets" })).toBeNull();
  });
});
