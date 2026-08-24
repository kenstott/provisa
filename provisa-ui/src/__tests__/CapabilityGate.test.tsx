// Copyright (c) 2026 Kenneth Stott
// Canary: f8be57dd-826e-4cdd-863b-83a5f63a018c
// Canary: placeholder
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

import { describe, it, expect, vi, beforeEach } from "vitest";
import { screen } from "@testing-library/react";
import { render } from "../test-utils/render";
import { CapabilityGate } from "../components/CapabilityGate";
import type { Capability } from "../types/auth";

// REQ-1430: the gate reads `loading` and the resolved capability set off the auth context. Stub the
// context rather than mounting a provider, which would fetch /auth/me. The capabilities are the
// real ones the gate decides on -- the decision itself is not mocked, because REQ-1361 turns on
// which set answers which entry.
const authState: { loading: boolean; capabilities: string[] } = {
  loading: false,
  capabilities: [],
};
vi.mock("../context/AuthContext", () => ({
  useAuth: () => authState,
}));

/** Grant `caps` for the next render. */
function holding(...caps: string[]) {
  authState.capabilities = caps;
}

describe("CapabilityGate", () => {
  beforeEach(() => {
    authState.loading = false;
    authState.capabilities = [];
  });

  it("says credentials are being checked while the bootstrap is in flight", () => {
    authState.loading = true;
    holding("usage");

    render(
      <CapabilityGate
        capability={"observability" as Capability}
        fallback={<div>Not Authorized</div>}
      >
        <span>Reports</span>
      </CapabilityGate>,
    );

    expect(screen.getByTestId("capability-gate-checking")).toBeInTheDocument();
    expect(screen.queryByText("Not Authorized")).not.toBeInTheDocument();
    expect(screen.queryByText("Reports")).not.toBeInTheDocument();
  });

  it("stays silent while loading when it has no fallback", () => {
    // An inline gate — a nav link, a toolbar button — has no region of its own to fill. A spinner
    // per gate covers the page in them.
    authState.loading = true;
    holding("usage");

    const { container } = render(
      <CapabilityGate capability={"admin" as Capability}>
        <span>Admin Only</span>
      </CapabilityGate>,
    );

    expect(screen.queryByTestId("capability-gate-checking")).not.toBeInTheDocument();
    expect(screen.queryByText("Admin Only")).not.toBeInTheDocument();
    expect(container.querySelectorAll(":not(style)")).toHaveLength(0);
  });

  it("shows the denial only once the bootstrap has settled", () => {
    authState.loading = false;
    holding("usage");

    render(
      <CapabilityGate
        capability={"observability" as Capability}
        fallback={<div>Not Authorized</div>}
      >
        <span>Reports</span>
      </CapabilityGate>,
    );

    expect(screen.queryByTestId("capability-gate-checking")).not.toBeInTheDocument();
    expect(screen.getByText("Not Authorized")).toBeInTheDocument();
  });

  it("renders children when the capability is allowed", () => {
    holding("admin");

    render(
      <CapabilityGate capability={"query_development" as Capability}>
        <span>Protected Content</span>
      </CapabilityGate>,
    );

    expect(screen.getByText("Protected Content")).toBeInTheDocument();
  });

  it("renders nothing when capability is not allowed and no fallback provided", () => {
    holding("usage");

    const { container } = render(
      <CapabilityGate capability={"admin" as Capability}>
        <span>Admin Only</span>
      </CapabilityGate>,
    );

    expect(screen.queryByText("Admin Only")).not.toBeInTheDocument();
    // The gate contributes no markup of its own; the provider's injected <style> tags are all
    // that remain in the container.
    expect(container.querySelectorAll(":not(style)")).toHaveLength(0);
  });

  it("renders fallback when capability is not allowed and fallback provided", () => {
    holding("usage");

    render(
      <CapabilityGate
        capability={"access_config" as Capability}
        fallback={<div>Not Authorized</div>}
      >
        <span>Security Settings</span>
      </CapabilityGate>,
    );

    expect(screen.queryByText("Security Settings")).not.toBeInTheDocument();
    expect(screen.getByText("Not Authorized")).toBeInTheDocument();
  });

  it("does not render fallback when capability is allowed", () => {
    holding("admin");

    render(
      <CapabilityGate
        capability={"source_registration" as Capability}
        fallback={<div>Not Authorized</div>}
      >
        <span>Sources</span>
      </CapabilityGate>,
    );

    expect(screen.getByText("Sources")).toBeInTheDocument();
    expect(screen.queryByText("Not Authorized")).not.toBeInTheDocument();
  });

  it("decides on the capability it was given, not on another the caller happens to hold", () => {
    holding("observability");

    render(
      <CapabilityGate capability={"approve_view" as Capability}>
        <span>Approvals</span>
      </CapabilityGate>,
    );

    expect(screen.queryByText("Approvals")).not.toBeInTheDocument();
  });

  // REQ-1361: `strict` refuses the platform wildcard, and `orCapability` opens the same entry on a
  // second right. An org's secrets are the case both exist for: the server refuses `admin` there by
  // name, and what a platform admin does reach at that route is the deployment's secrets service.
  it("refuses the platform wildcard on a strict gate", () => {
    holding("admin");

    const { container } = render(
      <CapabilityGate capability={"org_settings" as Capability} strict>
        <span>Org Secrets</span>
      </CapabilityGate>,
    );

    expect(screen.queryByText("Org Secrets")).not.toBeInTheDocument();
    expect(container.querySelectorAll(":not(style)")).toHaveLength(0);
  });

  it("admits the literal right on a strict gate", () => {
    holding("org_settings");

    render(
      <CapabilityGate capability={"org_settings" as Capability} strict>
        <span>Org Secrets</span>
      </CapabilityGate>,
    );

    expect(screen.getByText("Org Secrets")).toBeInTheDocument();
  });

  it("opens on the second right when the strict one is absent", () => {
    holding("admin", "platform_settings");

    render(
      <CapabilityGate
        capability={"org_settings" as Capability}
        strict
        orCapability={"platform_settings" as Capability}
      >
        <span>Secrets service</span>
      </CapabilityGate>,
    );

    expect(screen.getByText("Secrets service")).toBeInTheDocument();
  });
});
