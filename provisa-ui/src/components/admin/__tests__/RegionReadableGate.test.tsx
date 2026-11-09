// Copyright (c) 2026 Kenneth Stott
// Canary: 2c7f5e30-ab48-4d16-c9f2-408b3dae7f21
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-1922, REQ-1327: Federation with regions is read by the org and a demo sandbox; without regions
// it is the platform's alone, its deployment check asked of everyone.
import { beforeEach, describe, expect, it, vi } from "vitest";
import { render, screen } from "../../../test-utils/render";
import { routeRequirement } from "../../../lib/capabilities";

const auth = vi.hoisted(() => ({
  loading: false,
  capabilities: [] as string[],
  demonstrated: [] as string[],
  availableRoles: [] as { id: string; effectiveCapabilities: string[] }[],
  activeOrgId: "acme" as string | null,
}));
const choices = vi.hoisted(() => ({
  value: { regions: [] as string[], connected: null as string | null, loading: false as boolean },
}));
vi.mock("../../../context/AuthContext", () => ({ useAuth: () => auth }));
vi.mock("../../../hooks/useRegionQueries", () => ({ useRegionChoices: () => choices.value }));
vi.mock("../../../api/demoSandbox", () => ({
  isDemoSandbox: (org: string | null) => org === "sandbox-1",
}));
vi.mock("../../RoleSwitchOffer", () => ({
  RoleSwitchOffer: ({ children }: { children: React.ReactNode }) => <>{children}</>,
}));

import { RegionReadableGate } from "../RegionReadableGate";

function gate() {
  render(
    <RegionReadableGate
      requirement={routeRequirement("platform_settings")}
      regionsCapability="org_settings"
      fallback={<div>refused</div>}
    >
      <div>the page</div>
    </RegionReadableGate>,
  );
}

describe("RegionReadableGate", () => {
  beforeEach(() => {
    auth.capabilities = [];
    auth.activeOrgId = "acme";
    choices.value = { regions: [], connected: null, loading: false };
  });

  it("is the platform's alone with no regions", () => {
    auth.capabilities = ["org_settings"];
    gate();
    expect(screen.getByText("refused")).toBeInTheDocument();
  });

  it("does not open to a sandbox with no regions", () => {
    auth.activeOrgId = "sandbox-1";
    gate();
    expect(screen.getByText("refused")).toBeInTheDocument();
  });

  it("asks the whole deployment check with no regions", () => {
    auth.capabilities = ["platform_settings"];
    gate();
    expect(screen.getByText("refused")).toBeInTheDocument();
  });

  it("opens to the platform with no regions", () => {
    auth.capabilities = ["platform_settings", "cross_org"];
    gate();
    expect(screen.getByText("the page")).toBeInTheDocument();
  });

  describe("with regions", () => {
    beforeEach(() => {
      choices.value = { regions: ["eu"], connected: "eu", loading: false };
    });

    it("opens to org_settings, to read", () => {
      auth.capabilities = ["org_settings"];
      gate();
      expect(screen.getByText("the page")).toBeInTheDocument();
    });

    it("opens to the platform", () => {
      auth.capabilities = ["platform_settings", "cross_org"];
      gate();
      expect(screen.getByText("the page")).toBeInTheDocument();
    });

    it("opens to a demo sandbox's visitor, whatever it holds", () => {
      auth.activeOrgId = "sandbox-1";
      gate();
      expect(screen.getByText("the page")).toBeInTheDocument();
    });

    it("refuses a caller with neither right", () => {
      auth.capabilities = ["usage"];
      gate();
      expect(screen.getByText("refused")).toBeInTheDocument();
    });
  });

  it("says nothing until the regions are known", () => {
    choices.value = { regions: [], connected: null, loading: true };
    auth.capabilities = ["org_settings"];
    gate();
    expect(screen.queryByText("refused")).toBeNull();
    expect(screen.queryByText("the page")).toBeNull();
  });
});
