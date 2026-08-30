// Copyright (c) 2026 Kenneth Stott
// Canary: 6c1a94f7-0d28-4b53-9e61-af73208d5cb4
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-1487: choosing which branch of the org's model this browser reads.
//
// The switcher is the ONLY place an environment is chosen, so what is asserted here is the two
// things that make the choice hold: the name it leaves in localStorage — which is what the fetch
// interceptor and the Apollo link both read — and the cache reset that keeps the environment being
// left from surviving in Apollo's store.

import { describe, it, expect, vi, beforeEach } from "vitest";
import { render, screen, waitFor, fireEvent } from "../test-utils/render";
import { EnvSwitcher } from "../components/EnvSwitcher";
import { modelReplaced } from "../apolloClient";
import { ENV_STORAGE_KEY } from "../lib/authFetch";
import { ENVIRONMENTS_CHANGED_EVENT } from "../api/environments";
import type { BranchSync, Environment } from "../api/environments";

// REQ-1573: being served by an environment other than prod is a right, so the switcher reads the
// caller's capabilities as well as their org.
const auth = {
  activeOrgId: "acme" as string | null,
  capabilities: ["environment_switch"] as string[],
  // REQ-1602: rights the caller is SHOWN without holding them.
  demonstrated: [] as string[],
  loading: false,
};
vi.mock("../context/AuthContext", () => ({ useAuth: () => auth }));
// The store reset is the switch's other half, so what is asserted is that it happens. A real
// ApolloClient here would put a network stack behind a menu click and test that instead.
vi.mock("../apolloClient", () => ({ modelReplaced: vi.fn().mockResolvedValue(undefined) }));
vi.mock("../api/environments", () => ({
  fetchEnvironments: vi.fn(),
  fetchBranchSync: vi.fn(),
  undoEnvironment: vi.fn(),
  redoEnvironment: vi.fn(),
  ENVIRONMENTS_CHANGED_EVENT: "provisa:environments-changed",
}));

import {
  fetchBranchSync,
  fetchEnvironments,
  redoEnvironment,
  undoEnvironment,
} from "../api/environments";
const mockFetch = vi.mocked(fetchEnvironments);
const mockSync = vi.mocked(fetchBranchSync);
const mockUndo = vi.mocked(undoEnvironment);
const mockRedo = vi.mocked(redoEnvironment);

function syncOf(overrides: Partial<BranchSync> = {}): BranchSync {
  return {
    local: "aaaaaaa",
    remote: "aaaaaaa",
    ahead: 0,
    behind: 0,
    diverged: false,
    unsynced: false,
    ...overrides,
  };
}

function env(name: string, overrides: Partial<Environment> = {}): Environment {
  return {
    name,
    created_at: null,
    created_by: null,
    expires_at: null,
    protected: false,
    drifted: false,
    branched_from: name === "prod" ? null : "prod",
    can_undo: true,
    can_redo: false,
    deployed_sha: "aaaaaaa",
    ...overrides,
  };
}

const reload = vi.fn();

beforeEach(() => {
  vi.clearAllMocks();
  localStorage.clear();
  auth.activeOrgId = "acme";
  auth.capabilities = ["environment_switch"];
  auth.demonstrated = [];
  auth.loading = false;
  mockSync.mockResolvedValue({ remote_configured: true, branches: { prod: syncOf() } });
  Object.defineProperty(window, "location", {
    configurable: true,
    value: { ...window.location, reload },
  });
});

describe("EnvSwitcher", () => {
  it("names the environment being read, which is prod when none is selected", async () => {
    mockFetch.mockResolvedValue([env("prod"), env("dev")]);
    render(<EnvSwitcher />);
    expect(await screen.findByTestId("env-switcher-trigger")).toHaveTextContent("Env: prod");
  });

  it("writes the chosen name where both clients read it", async () => {
    mockFetch.mockResolvedValue([env("prod"), env("dev")]);
    render(<EnvSwitcher />);
    fireEvent.click(await screen.findByTestId("env-switcher-trigger"));
    fireEvent.click(await screen.findByText("dev"));
    await waitFor(() => expect(localStorage.getItem(ENV_STORAGE_KEY)).toBe("dev"));
  });

  it("resets the store, so no other environment's model survives the switch", async () => {
    // And WITHOUT reloading the page: the reload threw the whole application away and put the
    // loading splash back on screen in the middle of a two-click gesture.
    mockFetch.mockResolvedValue([env("prod"), env("dev")]);
    render(<EnvSwitcher />);
    fireEvent.click(await screen.findByTestId("env-switcher-trigger"));
    fireEvent.click(await screen.findByText("dev"));
    await waitFor(() => expect(modelReplaced).toHaveBeenCalled());
    expect(reload).not.toHaveBeenCalled();
  });

  it("clears the key for prod rather than sending its name", async () => {
    // A request naming no environment is served prod, so selecting prod is clearing the selection.
    localStorage.setItem(ENV_STORAGE_KEY, "dev");
    mockFetch.mockResolvedValue([env("prod"), env("dev")]);
    render(<EnvSwitcher />);
    fireEvent.click(await screen.findByTestId("env-switcher-trigger"));
    fireEvent.click(await screen.findByText("prod"));
    await waitFor(() => expect(localStorage.getItem(ENV_STORAGE_KEY)).toBeNull());
  });

  it("drops a selection the org no longer has", async () => {
    // An unknown environment is answered 404 by design and never falls back to prod, so a name the
    // list does not carry would make every request fail until it was cleared by hand.
    localStorage.setItem(ENV_STORAGE_KEY, "deleted-branch");
    mockFetch.mockResolvedValue([env("prod")]);
    render(<EnvSwitcher />);
    await waitFor(() => expect(localStorage.getItem(ENV_STORAGE_KEY)).toBeNull());
    expect(reload).toHaveBeenCalled();
  });

  it("re-reads the list when a branch is created elsewhere", async () => {
    // The admin page that creates a branch is a different tree; without the announcement this menu
    // keeps showing the list it read at mount and the new branch cannot be selected at all.
    mockFetch.mockResolvedValue([env("prod")]);
    render(<EnvSwitcher />);
    await screen.findByTestId("env-switcher-trigger");
    mockFetch.mockResolvedValue([env("prod"), env("dev")]);
    fireEvent(window, new Event(ENVIRONMENTS_CHANGED_EVENT));
    fireEvent.click(await screen.findByTestId("env-switcher-trigger"));
    expect(await screen.findByText("dev")).toBeInTheDocument();
  });

  it("is a menu even when the org has one environment", async () => {
    mockFetch.mockResolvedValue([env("prod")]);
    render(<EnvSwitcher />);
    const trigger = await screen.findByTestId("env-switcher-trigger");
    expect(trigger).toHaveTextContent("Env: prod");
    fireEvent.click(trigger);
    expect(await screen.findByText("prod")).toBeInTheDocument();
  });

  // REQ-1552: what the branch being worked in owes the remote, said where the work happens.
  it("says how far the environment being read is from the remote", async () => {
    mockFetch.mockResolvedValue([env("prod")]);
    mockSync.mockResolvedValue({
      remote_configured: true,
      branches: { prod: syncOf({ ahead: 2, behind: 0 }) },
    });
    render(<EnvSwitcher />);
    const badge = await screen.findByTestId("env-switcher-state");
    expect(badge).toHaveAttribute("data-state", "ahead");
    expect(badge).toHaveTextContent("2 to push");
  });

  it("says nothing when the branch owes the remote nothing", async () => {
    // A badge shown in every state is a badge that says nothing in the state that matters.
    mockFetch.mockResolvedValue([env("prod")]);
    render(<EnvSwitcher />);
    await screen.findByTestId("env-switcher-trigger");
    await waitFor(() => expect(mockSync).toHaveBeenCalled());
    expect(screen.queryByTestId("env-switcher-state")).toBeNull();
  });

  it("leaves whether the org mirrors anywhere to the environments page", async () => {
    // That is a property of the org, not of the branch being worked in, and this switcher is on
    // every screen -- it says what this environment owes, and nothing about the org's setup.
    mockFetch.mockResolvedValue([env("prod")]);
    mockSync.mockResolvedValue({ remote_configured: false, branches: {} });
    render(<EnvSwitcher />);
    await screen.findByTestId("env-switcher-trigger");
    await waitFor(() => expect(mockSync).toHaveBeenCalled());
    expect(screen.queryByTestId("env-switcher-state")).toBeNull();
    fireEvent.click(screen.getByTestId("env-switcher-trigger"));
    expect(await screen.findByTestId("env-switcher-undo")).toBeInTheDocument();
    expect(screen.queryByTestId("env-switcher-no-remote")).toBeNull();
  });

  // REQ-1543: the change was made in this environment, so the way back is offered beside its name.
  it("steps the environment being read back along its own history", async () => {
    localStorage.setItem(ENV_STORAGE_KEY, "dev");
    mockFetch.mockResolvedValue([env("prod"), env("dev")]);
    mockUndo.mockResolvedValue({
      report: { added: 0, changed: 0, removed: 0 },
      deployed_sha: "bbbbbbbcccc",
      redo_sha: "aaaaaaa",
      refreshed: true,
    });
    render(<EnvSwitcher />);
    fireEvent.click(await screen.findByTestId("env-switcher-trigger"));
    fireEvent.click(await screen.findByTestId("env-switcher-undo"));
    await waitFor(() => expect(mockUndo).toHaveBeenCalledWith("acme", "dev"));
    await waitFor(() => expect(modelReplaced).toHaveBeenCalled());
    expect(reload).not.toHaveBeenCalled();
  });

  // REQ-1553: neither end of the line is offered into a refusal.
  it("withholds undo at the first commit and redo when nothing was stepped back from", async () => {
    mockFetch.mockResolvedValue([env("prod", { can_undo: false, can_redo: false })]);
    render(<EnvSwitcher />);
    fireEvent.click(await screen.findByTestId("env-switcher-trigger"));
    expect(await screen.findByTestId("env-switcher-undo")).toHaveAttribute("data-disabled", "true");
    expect(screen.getByTestId("env-switcher-redo")).toHaveAttribute("data-disabled", "true");
    fireEvent.click(screen.getByTestId("env-switcher-undo"));
    expect(mockUndo).not.toHaveBeenCalled();
  });

  // REQ-1553: editing the model moves the cursor server-side and tells this menu nothing, so what
  // was true at mount is not what to show when the menu is opened.
  it("re-reads the row when the menu opens, so a change made since mount lights undo", async () => {
    mockFetch.mockResolvedValueOnce([env("prod", { can_undo: false })]);
    render(<EnvSwitcher />);
    const trigger = await screen.findByTestId("env-switcher-trigger");
    await waitFor(() => expect(mockFetch).toHaveBeenCalledTimes(1));
    mockFetch.mockResolvedValue([env("prod", { can_undo: true })]);
    fireEvent.click(trigger);
    await waitFor(() =>
      expect(screen.getByTestId("env-switcher-undo")).not.toHaveAttribute("data-disabled"),
    );
  });

  it("offers redo once the environment has stepped back from something", async () => {
    mockFetch.mockResolvedValue([env("prod", { can_redo: true })]);
    mockRedo.mockResolvedValue({
      report: { added: 0, changed: 0, removed: 0 },
      deployed_sha: "cccccccdddd",
      redo_sha: null,
      refreshed: true,
    });
    render(<EnvSwitcher />);
    fireEvent.click(await screen.findByTestId("env-switcher-trigger"));
    fireEvent.click(await screen.findByTestId("env-switcher-redo"));
    await waitFor(() => expect(mockRedo).toHaveBeenCalledWith("acme", "prod"));
  });

  it("renders nothing before an org is chosen", () => {
    auth.activeOrgId = null;
    render(<EnvSwitcher />);
    expect(screen.queryByTestId("env-switcher-trigger")).toBeNull();
    expect(mockFetch).not.toHaveBeenCalled();
  });

  // REQ-1573: switching environments is org_admin's and developer's; an analyst works in prod.
  it("is not shown to a caller who may not be served another environment", async () => {
    auth.capabilities = ["usage", "query_development"];
    mockFetch.mockResolvedValue([env("prod"), env("dev")]);
    render(<EnvSwitcher />);
    await waitFor(() => expect(mockFetch).not.toHaveBeenCalled());
    expect(screen.queryByTestId("env-switcher-trigger")).toBeNull();
  });

  // A role shown this right without holding it (the sandbox, REQ-1608) gets an inert stub: the
  // environments it would name are never fetched (there is nothing real behind the control), but
  // the trigger stays on the page, badged and disabled, to illustrate that the control exists.
  it("shows an inert stub to a role that is only shown the right, not holding it", async () => {
    auth.capabilities = ["usage"];
    auth.demonstrated = ["environment_switch"];
    mockFetch.mockResolvedValue([env("prod"), env("dev")]);
    render(<EnvSwitcher />);
    await waitFor(() => expect(mockFetch).not.toHaveBeenCalled());
    const trigger = await screen.findByTestId("env-switcher-trigger");
    const wrapper = screen.getByTestId("demonstrated-children");
    expect(wrapper).toContainElement(trigger);
    expect(wrapper).toHaveAttribute("aria-disabled", "true");
    expect(wrapper.closest("[inert]")).not.toBeNull();
    expect(screen.getByTestId("demonstrated-badge")).toBeInTheDocument();
  });

  it("is shown to the platform administrator, who bypasses every right", async () => {
    auth.capabilities = ["admin"];
    mockFetch.mockResolvedValue([env("prod"), env("dev")]);
    render(<EnvSwitcher />);
    expect(await screen.findByTestId("env-switcher-trigger")).toBeInTheDocument();
  });

  it("drops a selection made before the right was withdrawn", async () => {
    // Every request would otherwise keep carrying the name, and the server answers each one 403.
    localStorage.setItem(ENV_STORAGE_KEY, "dev");
    auth.capabilities = ["usage"];
    mockFetch.mockResolvedValue([env("prod"), env("dev")]);
    render(<EnvSwitcher />);
    await waitFor(() => expect(localStorage.getItem(ENV_STORAGE_KEY)).toBeNull());
    expect(reload).toHaveBeenCalled();
  });

  it("keeps the selection while the bootstrap is still in flight", async () => {
    // A caller carries no capabilities until /auth/me answers, and reading that as a withdrawal
    // would clear a legitimate selection and reload the page on every cold start.
    localStorage.setItem(ENV_STORAGE_KEY, "dev");
    auth.capabilities = [];
    auth.loading = true;
    mockFetch.mockResolvedValue([env("prod"), env("dev")]);
    render(<EnvSwitcher />);
    await waitFor(() => expect(mockFetch).not.toHaveBeenCalled());
    expect(localStorage.getItem(ENV_STORAGE_KEY)).toBe("dev");
    expect(reload).not.toHaveBeenCalled();
  });
});
