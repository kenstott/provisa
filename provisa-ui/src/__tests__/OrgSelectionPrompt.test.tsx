// Copyright (c) 2026 Kenneth Stott
// Canary: 5763a54c-c1e0-425e-9830-19072406aa7b
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-1935: a request naming no org is refused with 401 and a stable code; the app answers that
// refusal with one prompt to select an org rather than an error on every page.

import { describe, it, expect, vi, beforeEach, afterEach } from "vitest";
import { act, fireEvent, render, screen } from "../test-utils/render";
import { installAuthFetch } from "../lib/authFetch";
import {
  ORG_SELECTION_REQUIRED,
  clearOrgSelectionRequired,
  orgSelectionRequired,
} from "../lib/orgSelection";
import { OrgSelectionPrompt } from "../components/OrgSelectionPrompt";

vi.mock("../lib/firebase", () => ({ currentFirebaseToken: async () => null }));
vi.mock("../context/AuthContext", () => ({ useAuth: vi.fn() }));

import { useAuth } from "../context/AuthContext";

const mockUseAuth = vi.mocked(useAuth);

function refusal(code: string): Response {
  return new Response(JSON.stringify({ detail: "Org selection required", code }), {
    status: 401,
    headers: { "Content-Type": "application/json" },
  });
}

function auth(memberships: { org_id: string; org_name: string }[], selectOrg = vi.fn()) {
  mockUseAuth.mockReturnValue({
    orgMemberships: memberships,
    selectOrg,
  } as unknown as ReturnType<typeof useAuth>);
  return selectOrg;
}

describe("REQ-1935 org selection prompt", () => {
  let nativeFetch: typeof globalThis.fetch;

  beforeEach(() => {
    clearOrgSelectionRequired();
    localStorage.clear();
    localStorage.setItem("provisa_token", "t");
    nativeFetch = window.fetch;
  });

  afterEach(() => {
    window.fetch = nativeFetch;
    clearOrgSelectionRequired();
    vi.restoreAllMocks();
  });

  it("the server's org-selection refusal is recorded, and the response passes through", async () => {
    window.fetch = vi.fn().mockResolvedValue(refusal(ORG_SELECTION_REQUIRED)) as typeof fetch;
    installAuthFetch();

    const res = await window.fetch("/admin/graphql");
    expect(res.status).toBe(401);
    expect(orgSelectionRequired()).toBe(true);
  });

  it("any other 401 is not that refusal", async () => {
    window.fetch = vi.fn().mockResolvedValue(refusal("auth.expired")) as typeof fetch;
    installAuthFetch();

    await window.fetch("/admin/graphql");
    expect(orgSelectionRequired()).toBe(false);
  });

  it("prompts with the user's own orgs and selecting one clears it", async () => {
    window.fetch = vi.fn().mockResolvedValue(refusal(ORG_SELECTION_REQUIRED)) as typeof fetch;
    installAuthFetch();
    const selectOrg = auth([{ org_id: "acme", org_name: "Acme" }]);
    render(<OrgSelectionPrompt />);
    expect(screen.queryByTestId("org-selection-option-acme")).toBeNull(); // not prompted yet

    await act(async () => {
      await window.fetch("/data/graphql");
    });
    fireEvent.click(await screen.findByTestId("org-selection-option-acme"));
    expect(selectOrg).toHaveBeenCalledWith("acme");
  });

  it("says so when the user belongs to no org", async () => {
    window.fetch = vi.fn().mockResolvedValue(refusal(ORG_SELECTION_REQUIRED)) as typeof fetch;
    installAuthFetch();
    auth([]);
    render(<OrgSelectionPrompt />);

    await act(async () => {
      await window.fetch("/data/graphql");
    });
    expect(await screen.findByTestId("org-selection-none")).toBeTruthy();
  });
});
