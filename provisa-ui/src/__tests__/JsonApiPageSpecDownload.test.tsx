// Copyright (c) 2026 Kenneth Stott
// Canary: 2f6a9d48-7b13-4e5c-a0d9-3c8e1f5b7a62
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// The JSON:API page's spec download is the ACTIVE roles' spec (REQ-1620, REQ-273). It used to be a
// bare link: the browser followed it with no role header and no credential, so the server answered
// with whatever role it resolved by default. The page now fetches it as it fetches everything else
// — the active roles in X-Provisa-Role — and hands the answer over as a file; a refusal is shown.

import { describe, it, expect, vi, beforeEach } from "vitest";
import { render, screen, waitFor } from "../test-utils/render";

vi.mock("../context/AuthContext", () => ({
  useAuth: () => ({
    role: { id: "analyst" },
    selectedRoles: [{ id: "analyst" }, { id: "org_admin" }],
  }),
}));
vi.mock("../context/DomainFilterContext", () => ({
  useDomainFilter: () => ({ checkedDomains: new Set<string>(["pet-store"]) }),
}));
vi.mock("../hooks/useAdminQueries", () => ({
  useDomains: () => ({ domains: [{ id: "pet-store", description: "Pet store" }] }),
  useTables: () => ({ tables: [] }),
  useAllRelationships: () => ({ relationships: [] }),
}));

import "../i18n";
import { JsonApiPage } from "../pages/JsonApiPage";

interface Call {
  url: string;
  role: string | null;
}
const calls: Call[] = [];
let specResponse: () => Response;
const saved: { href: string; download: string }[] = [];

beforeEach(() => {
  calls.length = 0;
  saved.length = 0;
  specResponse = () =>
    new Response('{"openapi":"3.0.3"}', {
      status: 200,
      headers: { "content-type": "application/json" },
    });
  vi.stubGlobal(
    "fetch",
    vi.fn(async (url: string, init?: RequestInit) => {
      calls.push({ url: String(url), role: new Headers(init?.headers).get("x-provisa-role") });
      return String(url).startsWith("/data/jsonapi/openapi.json")
        ? specResponse()
        : new Response("[]", { status: 200, headers: { "content-type": "application/json" } });
    }),
  );
  URL.createObjectURL = vi.fn(() => "blob:spec");
  URL.revokeObjectURL = vi.fn();
  vi.spyOn(HTMLAnchorElement.prototype, "click").mockImplementation(function (
    this: HTMLAnchorElement,
  ) {
    saved.push({ href: this.href, download: this.download });
  });
});

describe("JsonApiPage — spec download", () => {
  it("is not a bare link: nothing the browser could follow without the role", () => {
    render(<JsonApiPage />);
    const control = screen.getByTestId("jsonapi-spec-download");
    expect(control.tagName).toBe("BUTTON");
    expect(control.getAttribute("href")).toBeNull();
  });

  it("fetches the active role set's spec and saves it as a file", async () => {
    render(<JsonApiPage />);
    screen.getByTestId("jsonapi-spec-download").click();
    await waitFor(() => expect(saved).toHaveLength(1));
    const spec = calls.find((c) => c.url.startsWith("/data/jsonapi/openapi.json"))!;
    expect(spec.role).toBe("analyst,org_admin");
    const params = new URLSearchParams(spec.url.split("?")[1]);
    expect(params.get("role")).toBe("analyst,org_admin");
    expect(params.get("domains")).toBe("pet-store");
    expect(saved[0]).toEqual({ href: "blob:spec", download: "jsonapi-openapi.json" });
  });

  it("shows the server's refusal and saves nothing", async () => {
    specResponse = () =>
      new Response(
        JSON.stringify({
          detail: "Role 'org_admin' is not assigned to this user",
          code: "auth.role_not_assigned",
          params: { role_id: "org_admin" },
        }),
        { status: 403, headers: { "content-type": "application/json" } },
      );
    render(<JsonApiPage />);
    screen.getByTestId("jsonapi-spec-download").click();
    expect(await screen.findByText(/org_admin.*not assigned/i)).toBeInTheDocument();
    expect(saved).toHaveLength(0);
  });
});
