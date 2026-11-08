// Copyright (c) 2026 Kenneth Stott
// Canary: bbfac309-4e92-43be-b90b-b469dd9c2403
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-2006: a demo sandbox is opened from /demos/<name> with no sign-in and handed to its own
// address with the visitor's token; its welcome notice names every limit it runs under, each with
// the value the settings that enforce it hold.
import { beforeEach, describe, expect, it, vi } from "vitest";
import userEvent from "@testing-library/user-event";
import { Route, Routes } from "react-router-dom";
import { render, screen, waitFor } from "../test-utils/render";
import type { OrgMembership } from "../types/auth";

vi.mock("../api/demoSandbox", async (importOriginal) => ({
  ...(await importOriginal<typeof import("../api/demoSandbox")>()),
  openDemoSandbox: vi.fn(),
  fetchDemoStatus: vi.fn(),
  fetchSandboxLimits: vi.fn(),
}));
vi.mock("../api/admin", async (importOriginal) => ({
  ...(await importOriginal<typeof import("../api/admin")>()),
  acknowledgeJoin: vi.fn(),
  leaveOrg: vi.fn(),
}));
const mockAuth: { orgMemberships: OrgMembership[]; refresh: ReturnType<typeof vi.fn> } = {
  orgMemberships: [],
  refresh: vi.fn(),
};
vi.mock("../context/AuthContext", async (importOriginal) => ({
  ...(await importOriginal<typeof import("../context/AuthContext")>()),
  useAuth: () => mockAuth,
}));

import {
  fetchDemoStatus,
  fetchSandboxLimits,
  openDemoSandbox,
  SandboxRefused,
} from "../api/demoSandbox";
import { JoinNotice } from "../components/JoinNotice";
import { DemoOpenPage } from "../pages/demos/DemoOpenPage";
import {
  adoptSandboxTokenFromLocation,
  sandboxDestination,
  sandboxToken,
} from "../lib/sandboxSession";
import { storedToken } from "../lib/sessionToken";
import { endVisit } from "../lib/browserStorage";
import { onDeclaredBaseDomain } from "./declaredBaseDomain";

const assign = vi.fn();
// cloud.provisa.dev, which declares provisa.dev: a sandbox is reached at its own subdomain of it.
onDeclaredBaseDomain("provisa.dev");

beforeEach(() => {
  vi.clearAllMocks();
  vi.mocked(openDemoSandbox).mockReset();
  vi.mocked(fetchDemoStatus).mockReset();
  Object.defineProperty(window, "location", {
    configurable: true,
    value: {
      ...window.location,
      assign,
      hostname: "cloud.provisa.dev",
      protocol: "https:",
      port: "",
    },
  });
  endVisit();
});

function openAt(name: string) {
  render(
    <Routes>
      <Route path="/demos/:name" element={<DemoOpenPage />} />
    </Routes>,
    { initialEntries: [`/demos/${name}`] },
  );
}

describe("the live demo link", () => {
  it("opens a sandbox and goes to its own address with the visitor's token", async () => {
    vi.mocked(openDemoSandbox).mockResolvedValue({
      org_id: "sbx0123456789ab",
      demo: "weblogs",
      token: "provisa_pat_abc",
    });
    openAt("weblogs");
    await waitFor(() =>
      expect(assign).toHaveBeenCalledWith(
        "https://sbx0123456789ab.provisa.dev/#provisa_sandbox=provisa_pat_abc",
      ),
    );
    expect(openDemoSandbox).toHaveBeenCalledWith("weblogs");
  });

  it("shows a full house as the server words it, and tries again on request", async () => {
    vi.mocked(openDemoSandbox).mockRejectedValueOnce(
      new SandboxRefused("Every demo sandbox is in use. Try again shortly.", "sandbox.at_capacity"),
    );
    vi.mocked(openDemoSandbox).mockResolvedValueOnce({
      org_id: "sbx0123456789ab",
      demo: "weblogs",
      token: "t",
    });
    openAt("weblogs");
    expect(await screen.findByTestId("demo-open-refused")).toHaveTextContent("in use");
    await userEvent.click(screen.getByTestId("demo-open-retry"));
    await waitFor(() => expect(assign).toHaveBeenCalled());
  });

  it("waits for a demo still starting, asking its status, and opens it once it is ready", async () => {
    vi.useFakeTimers({ shouldAdvanceTime: true });
    try {
      vi.mocked(openDemoSandbox).mockRejectedValueOnce(
        new SandboxRefused("Weblogs is still being prepared.", "sandbox.demo_not_ready"),
      );
      vi.mocked(openDemoSandbox).mockResolvedValueOnce({
        org_id: "sbx0123456789ab",
        demo: "weblogs",
        token: "t",
      });
      const status = { demo: "weblogs", state: "building", places_free: 3 };
      vi.mocked(fetchDemoStatus)
        .mockResolvedValueOnce({ ...status, ready: false })
        .mockResolvedValueOnce({ ...status, state: "ready", ready: true });
      openAt("weblogs");
      expect(await screen.findByTestId("demo-open-starting")).toHaveTextContent(
        "Starting this demo",
      );
      expect(screen.queryByTestId("demo-open-refused")).toBeNull();
      await vi.advanceTimersByTimeAsync(5000);
      expect(fetchDemoStatus).toHaveBeenCalledTimes(1);
      expect(openDemoSandbox).toHaveBeenCalledTimes(1);
      await vi.advanceTimersByTimeAsync(5000);
      await waitFor(() => expect(assign).toHaveBeenCalled());
      expect(fetchDemoStatus).toHaveBeenCalledWith("weblogs");
      expect(openDemoSandbox).toHaveBeenCalledTimes(2);
    } finally {
      vi.useRealTimers();
    }
  });

  it("stops waiting on a real error from the status, naming it", async () => {
    vi.useFakeTimers({ shouldAdvanceTime: true });
    try {
      vi.mocked(openDemoSandbox).mockRejectedValueOnce(
        new SandboxRefused("Weblogs is still being prepared.", "sandbox.demo_not_ready"),
      );
      vi.mocked(fetchDemoStatus).mockRejectedValueOnce(
        new SandboxRefused("There is no published demo named weblogs.", "sandbox.unknown_demo"),
      );
      openAt("weblogs");
      await screen.findByTestId("demo-open-starting");
      await vi.advanceTimersByTimeAsync(5000);
      expect(await screen.findByTestId("demo-open-refused")).toHaveTextContent(
        "no published demo named weblogs",
      );
      expect(screen.queryByTestId("demo-open-retry")).toBeNull();
      expect(assign).not.toHaveBeenCalled();
    } finally {
      vi.useRealTimers();
    }
  });

  it("stops waiting on a demo whose stores could not be started, offering a retry", async () => {
    vi.useFakeTimers({ shouldAdvanceTime: true });
    try {
      vi.mocked(openDemoSandbox).mockRejectedValueOnce(
        new SandboxRefused("Weblogs is still being prepared.", "sandbox.demo_not_ready"),
      );
      vi.mocked(fetchDemoStatus).mockRejectedValueOnce(
        new SandboxRefused(
          "Weblogs could not be started; try again in a few minutes",
          "sandbox.demo_stores_failed",
        ),
      );
      vi.mocked(openDemoSandbox).mockResolvedValueOnce({
        org_id: "sbx0123456789ab",
        demo: "weblogs",
        token: "t",
      });
      openAt("weblogs");
      await screen.findByTestId("demo-open-starting");
      await vi.advanceTimersByTimeAsync(5000);
      expect(await screen.findByTestId("demo-open-refused")).toHaveTextContent(
        "could not be started",
      );
      expect(screen.queryByTestId("demo-open-starting")).toBeNull();
      await userEvent.click(screen.getByTestId("demo-open-retry"));
      await waitFor(() => expect(assign).toHaveBeenCalled());
      expect(openDemoSandbox).toHaveBeenCalledTimes(2);
    } finally {
      vi.useRealTimers();
    }
  });

  it.each([
    ["front_door.coordinator_waking", "The server is waking up"],
    ["front_door.coordinator_updating", "The server is being updated"],
    ["sandbox.starting", "The demo server is starting"],
    ["sandbox.demo_not_ready", "Starting this demo"],
  ])("keeps waiting across %s, saying what is happening", async (code, said) => {
    vi.useFakeTimers({ shouldAdvanceTime: true });
    try {
      vi.mocked(openDemoSandbox).mockRejectedValueOnce(new SandboxRefused("Wait.", code));
      vi.mocked(openDemoSandbox).mockResolvedValueOnce({
        org_id: "sbx0123456789ab",
        demo: "weblogs",
        token: "t",
      });
      const status = { demo: "weblogs", state: "ready", places_free: 3 };
      vi.mocked(fetchDemoStatus)
        .mockRejectedValueOnce(new SandboxRefused("Wait.", code))
        .mockResolvedValueOnce({ ...status, ready: true });
      openAt("weblogs");
      expect(await screen.findByText(new RegExp(said))).toBeInTheDocument();
      await vi.advanceTimersByTimeAsync(5000);
      expect(screen.getByText(new RegExp(said))).toBeInTheDocument();
      expect(screen.queryByTestId("demo-open-refused")).toBeNull();
      expect(screen.getByTestId("demo-open-cold-start")).toHaveTextContent("about five minutes");
      expect(screen.getByTestId("demo-open-progress")).toBeInTheDocument();
      await vi.advanceTimersByTimeAsync(5000);
      await waitFor(() => expect(assign).toHaveBeenCalled());
    } finally {
      vi.useRealTimers();
    }
  });

  it("moves on as the wait moves from one stage to the next, and counts the time", async () => {
    vi.useFakeTimers({ shouldAdvanceTime: true });
    try {
      vi.mocked(openDemoSandbox).mockRejectedValueOnce(
        new SandboxRefused("Wait.", "front_door.coordinator_waking"),
      );
      vi.mocked(fetchDemoStatus)
        .mockRejectedValueOnce(new SandboxRefused("Wait.", "sandbox.starting"))
        .mockResolvedValue({ demo: "w", state: "building", places_free: 3, ready: false });
      openAt("weblogs");
      await screen.findByText(/The server is waking up/);
      await vi.advanceTimersByTimeAsync(5000);
      expect(await screen.findByText(/The demo server is starting/)).toBeInTheDocument();
      await vi.advanceTimersByTimeAsync(5000);
      expect(await screen.findByText(/Starting this demo/)).toBeInTheDocument();
      expect(screen.getByTestId("demo-open-elapsed")).toHaveTextContent("0:10");
    } finally {
      vi.useRealTimers();
    }
  });

  it("waits through a connection lost while the server restarts", async () => {
    vi.useFakeTimers({ shouldAdvanceTime: true });
    try {
      vi.mocked(openDemoSandbox).mockRejectedValueOnce(new TypeError("Failed to fetch"));
      vi.mocked(fetchDemoStatus).mockRejectedValueOnce(
        new SandboxRefused("API unavailable", null, 502),
      );
      vi.mocked(fetchDemoStatus).mockResolvedValueOnce({
        demo: "w",
        state: "ready",
        places_free: 3,
        ready: true,
      });
      vi.mocked(openDemoSandbox).mockResolvedValueOnce({
        org_id: "sbx0123456789ab",
        demo: "weblogs",
        token: "t",
      });
      openAt("weblogs");
      await screen.findByText(/Reaching the server/);
      expect(screen.queryByTestId("demo-open-refused")).toBeNull();
      await vi.advanceTimersByTimeAsync(10000);
      await waitFor(() => expect(assign).toHaveBeenCalled());
    } finally {
      vi.useRealTimers();
    }
  });

  it("opens again now and then, whatever the status says", async () => {
    vi.useFakeTimers({ shouldAdvanceTime: true });
    try {
      vi.mocked(openDemoSandbox).mockRejectedValue(
        new SandboxRefused("Wait.", "sandbox.demo_not_ready"),
      );
      vi.mocked(fetchDemoStatus).mockResolvedValue({
        demo: "w",
        state: "unknown",
        places_free: 3,
        ready: false,
      });
      openAt("weblogs");
      await screen.findByTestId("demo-open-starting");
      await vi.advanceTimersByTimeAsync(61000);
      expect(vi.mocked(openDemoSandbox).mock.calls.length).toBeGreaterThanOrEqual(2);
    } finally {
      vi.useRealTimers();
    }
  });

  it("names a server that failed to start, with a retry", async () => {
    vi.mocked(openDemoSandbox).mockRejectedValueOnce(
      new SandboxRefused("The demo server could not be started.", "sandbox.failed_to_start"),
    );
    openAt("weblogs");
    expect(await screen.findByTestId("demo-open-refused")).toHaveTextContent(
      "could not be started",
    );
    expect(screen.getByTestId("demo-open-retry")).toBeInTheDocument();
  });

  it("names a demo that could not be prepared, with a retry", async () => {
    vi.mocked(openDemoSandbox).mockRejectedValueOnce(
      new SandboxRefused("This demo could not be prepared.", "sandbox.demo_failed"),
    );
    openAt("weblogs");
    expect(await screen.findByTestId("demo-open-refused")).toHaveTextContent("could not be");
    expect(screen.getByTestId("demo-open-retry")).toBeInTheDocument();
  });

  it("does not give up inside the real wait, and names it when it does", async () => {
    vi.useFakeTimers({ shouldAdvanceTime: true });
    try {
      vi.mocked(openDemoSandbox).mockRejectedValue(new SandboxRefused("Wait.", "sandbox.starting"));
      vi.mocked(fetchDemoStatus).mockRejectedValue(new SandboxRefused("Wait.", "sandbox.starting"));
      openAt("weblogs");
      await screen.findByTestId("demo-open-starting");
      await vi.advanceTimersByTimeAsync(29 * 60 * 1000);
      expect(screen.queryByTestId("demo-open-refused")).toBeNull();
      await vi.advanceTimersByTimeAsync(2 * 60 * 1000);
      expect(await screen.findByTestId("demo-open-refused")).toHaveTextContent("did not open");
      expect(screen.getByTestId("demo-open-retry")).toBeInTheDocument();
    } finally {
      vi.useRealTimers();
    }
  }, 30000);

  it("offers no retry for a demo that does not exist", async () => {
    vi.mocked(openDemoSandbox).mockRejectedValue(
      new SandboxRefused("There is no published demo named nope.", "sandbox.unknown_demo"),
    );
    openAt("nope");
    expect(await screen.findByTestId("demo-open-refused")).toHaveTextContent("nope");
    expect(screen.queryByTestId("demo-open-retry")).toBeNull();
  });
});

describe("the sandbox's own address", () => {
  it("keeps the token from the fragment as its bearer, and clears the fragment", () => {
    const replace = vi.spyOn(window.history, "replaceState");
    const location = { hash: "#provisa_sandbox=provisa_pat_xyz", pathname: "/", search: "" };
    expect(adoptSandboxTokenFromLocation(location as Location)).toBe(true);
    expect(sandboxToken()).toBe("provisa_pat_xyz");
    expect(storedToken()).toBe("provisa_pat_xyz");
    expect(replace).toHaveBeenCalledWith(null, "", "/");
  });

  it("on a host that names no org, names the sandbox's org beside the token", () => {
    expect(sandboxDestination("sbx01", "tok", null)).toBe(
      "/#provisa_sandbox=tok&provisa_sandbox_org=sbx01",
    );
  });
});

describe("the demo sandbox's welcome", () => {
  it("names every limit with the value its setting holds", async () => {
    vi.mocked(fetchSandboxLimits).mockResolvedValue({
      engine: "duckdb",
      memory_limit: 256 * 1024 ** 2,
      disk_limit: 512 * 1024 ** 2,
      idle_ttl: 3600,
      max_concurrent: 10,
    });
    mockAuth.orgMemberships = [
      {
        org_id: "sbx0123456789ab",
        org_name: "Web shop logs sandbox",
        joined_via: "admin",
        acknowledged: false,
      } as OrgMembership,
    ];
    render(<JoinNotice />);
    expect(await screen.findByTestId("join-notice-limit-memory")).toHaveTextContent("256");
    expect(screen.getByTestId("join-notice-limit-disk")).toHaveTextContent("512");
    expect(screen.getByTestId("join-notice-limit-idle")).toHaveTextContent("1 hour");
    expect(screen.getByTestId("join-notice-demo-sandbox")).toHaveTextContent("DuckDB");
    expect(screen.getByTestId("join-notice-demo-sandbox")).toHaveTextContent("read-only");
    expect(screen.getByTestId("join-notice-demo-sandbox")).toHaveTextContent("invite");
    expect(screen.getByTestId("join-notice-demo-sandbox")).toHaveTextContent("conflict");
    expect(screen.queryByTestId("join-notice-leave")).toBeNull();
  });
});
