// Copyright (c) 2026 Kenneth Stott
// Canary: ff1a403a-6fec-4e6e-a241-f0e9682ae8e6
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-2006: a live demo's link opens its sandbox for every browser, signed in or not. Driven through
// the real App, since what this holds is which of App's branches /demos/<name> reaches: a browser
// holding a login must not get the shell (its tour offer, its gates) before the sandbox opens.
import { describe, it, expect, vi, beforeEach, afterEach } from "vitest";
import { act, render, screen, waitFor } from "@testing-library/react";
import { MantineProvider } from "@mantine/core";
import { I18nextProvider } from "react-i18next";
import App from "../App";
import i18n from "../i18n";
import { theme } from "../theme/theme";
import { fetchMe } from "../api/admin";
import { fetchSetupStatus, type SetupStatus } from "../api/setup";
import { openDemoSandbox } from "../api/demoSandbox";
import { TOUR_OFFERED_KEY } from "../tour/tourKeys";
import { installAuthFetch } from "../lib/authFetch";
import {
  clearOrgSelectionRequired,
  ORG_SELECTION_REQUIRED,
  orgSelectionRequired,
} from "../lib/orgSelection";
import { onDeclaredBaseDomain } from "./declaredBaseDomain";

vi.mock("../api/setup", async (importOriginal) => ({
  ...(await importOriginal<typeof import("../api/setup")>()),
  fetchSetupStatus: vi.fn(),
}));
vi.mock("../api/admin", async (importOriginal) => ({
  ...(await importOriginal<typeof import("../api/admin")>()),
  fetchMe: vi.fn(),
  fetchBootstrapStatus: vi.fn().mockResolvedValue(false),
}));
vi.mock("../api/demoSandbox", async (importOriginal) => ({
  ...(await importOriginal<typeof import("../api/demoSandbox")>()),
  openDemoSandbox: vi.fn(),
}));
vi.mock("../pages/LandingPage", () => ({
  LandingPage: () => <div data-testid="landing-card" />,
}));

const SANDBOX_ADDRESS = "https://sbx0123456789ab.provisa.dev/#provisa_sandbox=provisa_pat_abc";

// App holds its own router, so the shared render helper's cannot wrap it.
function renderApp() {
  render(
    <MantineProvider theme={theme} defaultColorScheme="dark">
      <I18nextProvider i18n={i18n}>
        <App />
      </I18nextProvider>
    </MantineProvider>,
  );
}

const assign = vi.fn();
const realLocation = window.location;
// cloud.provisa.dev, which declares provisa.dev: a sandbox is reached at its own subdomain of it.
onDeclaredBaseDomain("provisa.dev");
const nativeFetch = window.fetch;

function at(pathname: string) {
  Object.defineProperty(window, "location", {
    configurable: true,
    value: {
      ...realLocation,
      assign,
      pathname,
      search: "",
      hash: "",
      hostname: "cloud.provisa.dev",
      protocol: "https:",
      port: "",
    },
  });
}

beforeEach(() => {
  vi.clearAllMocks();
  localStorage.clear();
  sessionStorage.clear();
  vi.mocked(fetchSetupStatus).mockResolvedValue({
    needs_setup: false,
    demo_mode: false,
    auth_enabled: true,
    multitenancy: true,
  } as SetupStatus);
  vi.mocked(openDemoSandbox).mockResolvedValue({
    org_id: "sbx0123456789ab",
    demo: "weblogs",
    token: "provisa_pat_abc",
  });
  vi.mocked(fetchMe).mockResolvedValue({
    user_id: "u1",
    email: null,
    display_name: null,
    given_name: null,
    family_name: null,
    dev_mode: false,
    billing: false,
    active_org_id: "acme",
    control_plane_id: "cp-1",
    assignments: [],
    org_memberships: [{ org_id: "acme", org_name: "Acme" }],
    env_capabilities: [],
    derived_roles: [],
  });
});

afterEach(() => {
  Object.defineProperty(window, "location", { configurable: true, value: realLocation });
  window.fetch = nativeFetch;
  clearOrgSelectionRequired();
});

describe("the live demo link", () => {
  it("opens the sandbox for a signed-in browser, before the shell or its tour", async () => {
    localStorage.setItem("provisa_token", "login-token");
    at("/demos/weblogs");
    renderApp();
    await waitFor(() => expect(assign).toHaveBeenCalledWith(SANDBOX_ADDRESS));
    expect(openDemoSandbox).toHaveBeenCalledWith("weblogs");
    // What mounting the shell does on its first render, before any modal shows.
    expect(sessionStorage.getItem(TOUR_OFFERED_KEY)).toBeNull();
    expect(screen.queryByTestId("demo-open-loading")).toBeInTheDocument();
    expect(document.querySelector("main")).toBeNull();
    expect(localStorage.getItem("provisa_token")).toBe("login-token");
  });

  it("opens the sandbox for an anonymous browser", async () => {
    at("/demos/weblogs");
    renderApp();
    await waitFor(() => expect(assign).toHaveBeenCalledWith(SANDBOX_ADDRESS));
    expect(screen.queryByTestId("landing-card")).not.toBeInTheDocument();
  });

  it("leaves an anonymous browser's other routes on the landing page", async () => {
    at("/query");
    renderApp();
    expect(await screen.findByTestId("landing-card")).toBeInTheDocument();
    expect(openDemoSandbox).not.toHaveBeenCalled();
  });

  // REQ-1935 / REQ-2006: the browser's own login is not who opens a sandbox. A login acting in no
  // org (a platform operator, or one whose org has gone) is refused by every org-scoped request, and
  // that refusal opens the select-an-org prompt -- which covered the page for the whole time the
  // next demo's sandbox was being composed (cloud-dev 2026-11-05, /demos/streaming).
  it("composes the sandbox without the login's org-scoped requests or the org prompt", async () => {
    localStorage.setItem("provisa_token", "operator-token");
    vi.mocked(fetchMe).mockResolvedValue({
      user_id: "operator",
      email: null,
      display_name: null,
      given_name: null,
      family_name: null,
      dev_mode: false,
      billing: false,
      active_org_id: null,
      control_plane_id: "cp-1",
      assignments: [],
      org_memberships: [],
      env_capabilities: [],
      derived_roles: [],
    });
    const server = vi.fn(async (input: RequestInfo | URL) => {
      const url = typeof input === "string" ? input : input instanceof URL ? input.href : input.url;
      if (url.includes("/admin/graphql")) {
        return new Response(JSON.stringify({ code: ORG_SELECTION_REQUIRED }), {
          status: 401,
          headers: { "Content-Type": "application/json" },
        });
      }
      return new Response("{}", { status: 200, headers: { "Content-Type": "application/json" } });
    });
    window.fetch = server as typeof fetch;
    installAuthFetch();
    let composed: (opened: { org_id: string; demo: string; token: string }) => void = () => {};
    vi.mocked(openDemoSandbox).mockReturnValue(
      new Promise((resolve) => {
        composed = resolve;
      }),
    );
    at("/demos/weblogs");
    renderApp();

    // The sandbox is being composed: whatever the app does while it waits, it has done by now.
    expect(await screen.findByTestId("demo-open-loading")).toBeInTheDocument();
    await act(() => new Promise((resolve) => setTimeout(resolve, 300)));
    const asked = server.mock.calls.map(([input]) => String(input));
    expect(asked.filter((url) => url.includes("/admin/graphql"))).toEqual([]);
    expect(orgSelectionRequired()).toBe(false);
    expect(screen.queryByTestId("org-selection-prompt")).not.toBeInTheDocument();

    await act(async () => {
      composed({ org_id: "sbx0123456789ab", demo: "weblogs", token: "provisa_pat_abc" });
    });
    await waitFor(() => expect(assign).toHaveBeenCalledWith(SANDBOX_ADDRESS));
    expect(screen.queryByTestId("org-selection-prompt")).not.toBeInTheDocument();
  });
});
