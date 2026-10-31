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
import { render, screen, waitFor } from "@testing-library/react";
import { MantineProvider } from "@mantine/core";
import { I18nextProvider } from "react-i18next";
import App from "../App";
import i18n from "../i18n";
import { theme } from "../theme/theme";
import { fetchMe } from "../api/admin";
import { fetchSetupStatus, type SetupStatus } from "../api/setup";
import { openDemoSandbox } from "../api/demoSandbox";
import { TOUR_OFFERED_KEY } from "../tour/tourKeys";

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
});
