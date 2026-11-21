// Copyright (c) 2026 Kenneth Stott
// Canary: 3d8a6f41-bc59-4e27-d0a3-519c4ebf8a32
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-1327: the Observability page is the org's and its controls ask what their routes ask; the
// exporter, the engine's lifecycle and org-wide debug tracing are the platform's page.
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { render, screen } from "../../../test-utils/render";
import type { PlatformSettings } from "../../../api/admin";
import i18n from "../../../i18n";

const t = i18n.getFixedT("en");

const auth = vi.hoisted(() => ({
  capabilities: [] as string[],
  activeOrgId: "acme" as string | null,
}));
vi.mock("../../../context/AuthContext", () => ({ useAuth: () => auth }));
vi.mock("../DebugTracePanel", () => ({
  DebugTracePanel: ({ orgId }: { orgId?: string }) => (
    <div data-testid="panel">{orgId ?? "platform"}</div>
  ),
}));

import { ObservabilityTab, PlatformObservabilityTab } from "../ObservabilityTab";

beforeEach(() => {
  auth.capabilities = ["observability"];
  auth.activeOrgId = "acme";
  vi.stubGlobal(
    "fetch",
    vi.fn(async () => new Response(JSON.stringify({ traces: [] }))),
  );
});
afterEach(() => vi.unstubAllGlobals());

describe("the org's Observability page", () => {
  it("shows an observability holder its traces and no org control", () => {
    render(<ObservabilityTab />);
    expect(screen.getByTestId("trace-feed")).toBeInTheDocument();
    expect(screen.queryByTestId("observability-recompute-clusters")).toBeNull();
    expect(screen.queryByTestId("panel")).toBeNull();
  });

  it("gives org_settings the org's debug tracing and the cluster recompute", () => {
    auth.capabilities = ["observability", "org_settings"];
    render(<ObservabilityTab />);
    expect(screen.getByTestId("panel")).toHaveTextContent("acme");
    expect(screen.getByTestId("observability-recompute-clusters")).toBeInTheDocument();
  });

  it("holds none of the platform's controls, whoever opens it", () => {
    auth.capabilities = ["observability", "org_settings", "platform_settings", "cross_org"];
    render(<ObservabilityTab />);
    expect(screen.queryByRole("button", { name: t("observabilityTab.restartEngine") })).toBeNull();
    expect(screen.queryByRole("button", { name: t("observabilityTab.reloadCatalog") })).toBeNull();
    expect(screen.queryByTestId("observability-save")).toBeNull();
  });
});

describe("the platform's Observability page", () => {
  it("holds the query engine's lifecycle and not the org's cluster recompute", () => {
    render(<PlatformObservabilityTab settings={{} as PlatformSettings} setSettings={vi.fn()} />);
    expect(
      screen.getByRole("button", { name: t("observabilityTab.restartEngine") }),
    ).toBeInTheDocument();
    expect(
      screen.getByRole("button", { name: t("observabilityTab.reloadCatalog") }),
    ).toBeInTheDocument();
    expect(screen.queryByTestId("observability-recompute-clusters")).toBeNull();
  });
});
