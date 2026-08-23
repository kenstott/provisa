// Copyright (c) 2026 Kenneth Stott
// Canary: 0c1f5f04-1b26-4f6b-9c33-6a3fa9f2b7ad
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-1571: an invite link opens a sign-in, not the product. The app shell — navigation, org and
// environment pickers, admin rail — belongs to a session that has already identified itself, so
// /login and /register render the standalone landing card instead, however the visitor arrives.
import { describe, it, expect, vi } from "vitest";
import { MemoryRouter } from "react-router-dom";
import { render, screen, waitFor } from "../test-utils/render";
import { IdentityRoutes } from "../App";

vi.mock("../pages/LandingPage", () => ({
  LandingPage: () => <div data-testid="landing-card">sign in</div>,
}));

const shell = <div data-testid="app-shell">navbar and pages</div>;

function renderAt(path: string) {
  return render(
    <MemoryRouter initialEntries={[path]}>
      <IdentityRoutes onLoginSuccess={vi.fn()} authDisabled={false}>
        {shell}
      </IdentityRoutes>
    </MemoryRouter>,
  );
}

describe("the routes that establish who someone is", () => {
  it("renders the invite registration without the app shell", async () => {
    renderAt("/register?invite=94dcdf60-0e74-4d3b-ab95-83185b57844c");
    await waitFor(() => expect(screen.getByTestId("landing-card")).toBeTruthy());
    expect(screen.queryByTestId("app-shell")).toBeNull();
  });

  it("renders the sign-in without the app shell", async () => {
    renderAt("/login");
    await waitFor(() => expect(screen.getByTestId("landing-card")).toBeTruthy());
    expect(screen.queryByTestId("app-shell")).toBeNull();
  });

  it("leaves every other route to the app shell", () => {
    renderAt("/query");
    expect(screen.getByTestId("app-shell")).toBeTruthy();
    expect(screen.queryByTestId("landing-card")).toBeNull();
  });
});
