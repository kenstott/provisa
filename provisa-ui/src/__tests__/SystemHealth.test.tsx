// Copyright (c) 2026 Kenneth Stott
// Canary: 0d7a4b92-61ce-4e0f-8a35-c7f1e92b3d14
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// The System health table: a socket listener's row names its port, an HTTP surface's its path.

import { describe, it, expect, vi } from "vitest";
import { fireEvent, render, screen, within } from "../test-utils/render";
import i18n from "../i18n";
import { SystemHealth } from "../components/admin/SystemHealth";
import type { ProtocolHealth } from "../api/admin";

const t = i18n.getFixedT("en");

const protocols: ProtocolHealth[] = [
  { name: "pgwire", status: "running", port: 5433, path: null },
  { name: "SPARQL", status: "running", port: null, path: "/data/sparql" },
  { name: "GraphQL", status: "running", port: null, path: "/data/graphql" },
  { name: "Cypher over HTTP", status: "running", port: null, path: "/data/cypher" },
  { name: "REST", status: "disabled", port: null, path: "/data/rest" },
  { name: "JSON:API", status: "running", port: null, path: "/data/jsonapi" },
  { name: "OData", status: "running", port: null, path: "/data/odata" },
];

vi.mock("../hooks/useAdminOpsQueries", () => ({
  useSystemHealth: () => ({
    systemHealth: {
      engineConnected: true,
      engineWorkerCount: 1,
      engineActiveWorkers: 1,
      metadataPoolSize: 1,
      metadataPoolFree: 1,
      metadataDialect: "postgresql",
      cacheMode: "embedded",
      cacheConnected: true,
      protocols,
      mvRefreshLoopRunning: true,
    },
    refetch: vi.fn(),
  }),
}));

const row = (name: string) => within(screen.getByRole("cell", { name }).closest("tr")!);

describe("SystemHealth", () => {
  it.each([
    ["SPARQL", "/data/sparql"],
    ["GraphQL", "/data/graphql"],
    ["REST", "/data/rest"],
    ["JSON:API", "/data/jsonapi"],
    ["OData", "/data/odata"],
  ])("shows %s with its path", (name, path) => {
    render(<SystemHealth />);
    expect(row(name).getByText(path)).toBeInTheDocument();
  });

  it("copies the whole URL of a surface", async () => {
    const writeText = vi.fn().mockResolvedValue(undefined);
    Object.defineProperty(navigator, "clipboard", { value: { writeText }, configurable: true });
    render(<SystemHealth />);
    fireEvent.click(row("OData").getByRole("button", { name: t("systemHealth.copyUrl") }));
    expect(writeText).toHaveBeenCalledWith(`${window.location.origin}/data/odata`);
    await screen.findByRole("button", { name: t("copyButton.copied") });
  });

  it("offers no copy on a listener, which has a port and no URL", () => {
    render(<SystemHealth />);
    expect(row("pgwire").queryByTestId("copy-button")).toBeNull();
  });

  it("names Cypher over HTTP in the viewer's language, with its path", () => {
    render(<SystemHealth />);
    const cypher = row(t("systemHealth.cypherOverHttp"));
    expect(cypher.getByText("/data/cypher")).toBeInTheDocument();
  });

  it("says whether each surface is serving", () => {
    render(<SystemHealth />);
    expect(row("SPARQL").getByText(t("systemHealth.running"))).toBeInTheDocument();
    expect(row("REST").getByText(t("systemHealth.disabled"))).toBeInTheDocument();
  });

  it("still names a listener's port", () => {
    render(<SystemHealth />);
    expect(row("pgwire").getByText(t("systemHealth.port", { port: 5433 }))).toBeInTheDocument();
  });
});
