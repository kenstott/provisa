// Copyright (c) 2026 Kenneth Stott
// Canary: 9c1d47ea-2f30-4b6d-8a15-6c0f3b7de241
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

import { describe, it, expect, vi, beforeEach } from "vitest";
import { render, screen, waitFor, fireEvent } from "../test-utils/render";
import { MemoryRouter } from "react-router-dom";
import { OrgEngineTab } from "../components/admin/OrgEngineTab";
import type { OrgEngineState } from "../api/admin";

vi.mock("../api/admin", () => ({
  fetchOrgEngine: vi.fn(),
  setOrgEngine: vi.fn(),
}));

import { fetchOrgEngine, setOrgEngine } from "../api/admin";
const mockFetch = vi.mocked(fetchOrgEngine);
const mockSet = vi.mocked(setOrgEngine);

function state(overrides: Partial<OrgEngineState> = {}): OrgEngineState {
  return {
    org_id: "acme",
    mode: "shared",
    external_host: null,
    external_port: null,
    engine_kind: null,
    external_url_set: false,
    external_kinds: [
      { key: "trino-byo", label: "Trino", description: "A Trino you run.", addressing: "endpoint" },
      {
        key: "databricks",
        label: "Databricks",
        description: "A Databricks SQL warehouse.",
        addressing: "url",
      },
    ],
    isolated_available: false,
    isolated_entitled: true,
    engine_name: "trino",
    plan: null,
    plan_derived: false,
    engine_size: null,
    isolated_engine: null,
    ...overrides,
  };
}

describe("OrgEngineTab", () => {
  beforeEach(() => {
    mockFetch.mockReset();
    mockSet.mockReset();
    mockSet.mockResolvedValue({ success: true, mode: "external" });
  });

  // REQ-1418: which address the org supplies follows the KIND it picked, as reported by the
  // server — a URL-addressed warehouse must never be asked for a coordinator host/port.
  it("asks for a DSN when the chosen kind is URL-addressed", async () => {
    mockFetch.mockResolvedValue(
      state({ mode: "external", engine_kind: "databricks", external_url_set: true }),
    );
    render(<OrgEngineTab />);
    expect(await screen.findByTestId("org-engine-url")).toBeInTheDocument();
    expect(screen.queryByTestId("org-engine-host")).not.toBeInTheDocument();
    expect(screen.queryByTestId("org-engine-port")).not.toBeInTheDocument();
  });

  it("asks for host and port when the chosen kind is endpoint-addressed", async () => {
    mockFetch.mockResolvedValue(
      state({
        mode: "external",
        engine_kind: "trino-byo",
        external_host: "trino.acme.example.com",
        external_port: 8443,
      }),
    );
    render(<OrgEngineTab />);
    expect(await screen.findByTestId("org-engine-host")).toBeInTheDocument();
    expect(screen.getByTestId("org-engine-port")).toBeInTheDocument();
    expect(screen.queryByTestId("org-engine-url")).not.toBeInTheDocument();
  });

  it("sends the kind with the lane so an org can run an engine the deployment does not", async () => {
    mockFetch.mockResolvedValue(
      state({ mode: "external", engine_kind: "databricks", external_url_set: false }),
    );
    render(<OrgEngineTab />);
    const url = await screen.findByTestId("org-engine-url");
    fireEvent.change(url, { target: { value: "databricks://token:T@dbx?http_path=/sql/1.0/w/x" } });
    fireEvent.click(screen.getByTestId("org-engine-save-button"));
    await waitFor(() => expect(mockSet).toHaveBeenCalled());
    expect(mockSet.mock.calls[0][0]).toMatchObject({
      mode: "external",
      engine_kind: "databricks",
      external_url: "databricks://token:T@dbx?http_path=/sql/1.0/w/x",
      external_host: null,
      external_port: null,
    });
  });

  it("leaves a stored DSN alone when it is not re-entered", async () => {
    mockFetch.mockResolvedValue(
      state({ mode: "external", engine_kind: "databricks", external_url_set: true }),
    );
    render(<OrgEngineTab />);
    await screen.findByTestId("org-engine-url");
    fireEvent.click(screen.getByTestId("org-engine-save-button"));
    await waitFor(() => expect(mockSet).toHaveBeenCalled());
    // Null, not an empty string: the server reads "unchanged" and keeps the DSN it holds, which
    // the tab could not resend — the GET never returns a value carrying a warehouse token.
    expect(mockSet.mock.calls[0][0].external_url).toBeNull();
  });

  // REQ-1412: the isolated lane is a coordinator the platform runs and bills for, so a plan that
  // does not include one cannot select it — refused at the choice, not at the save.
  it("disables the isolated lane when the org's plan does not include it", async () => {
    mockFetch.mockResolvedValue(state({ isolated_available: true, isolated_entitled: false }));
    render(<OrgEngineTab />);
    const isolated = await screen.findByTestId("org-engine-mode-isolated");
    expect(isolated).toBeDisabled();
    expect(screen.getByText(/part of the Pro plan/)).toBeInTheDocument();
  });

  it("offers the isolated lane when the plan includes it", async () => {
    mockFetch.mockResolvedValue(state({ isolated_available: true, isolated_entitled: true }));
    render(<OrgEngineTab />);
    expect(await screen.findByTestId("org-engine-mode-isolated")).not.toBeDisabled();
  });

  it("blocks saving an external lane with no engine kind chosen", async () => {
    mockFetch.mockResolvedValue(state({ mode: "external", engine_kind: null }));
    render(<OrgEngineTab />);
    await waitFor(() => expect(screen.getByTestId("org-engine-save-button")).toBeDisabled());
    expect(screen.queryByTestId("org-engine-url")).not.toBeInTheDocument();
    expect(screen.queryByTestId("org-engine-host")).not.toBeInTheDocument();
  });

  // REQ-1512: on a hosted deployment the plan decides the lane and the size (REQ-1510), so the tab
  // reports what the org runs on and offers no controls at all — absent, not disabled, because an
  // org-operated engine is sold on no hosted plan and is not something to offer and then refuse.
  it("reports the engine and offers no controls when the plan decides it", async () => {
    mockFetch.mockResolvedValue(
      state({
        mode: "isolated",
        plan: "pro_m",
        plan_derived: true,
        engine_size: {
          label: "Pro M",
          machine_type: "n2-highmem-8",
          vcpu: 8,
          memory_gib: 64,
          query_max_memory_gb: 40,
        },
        isolated_engine: { state: "ready" },
      }),
    );
    render(
      <MemoryRouter>
        <OrgEngineTab />
      </MemoryRouter>,
    );
    expect(await screen.findByTestId("org-engine-hosted")).toBeInTheDocument();
    expect(screen.getByTestId("org-engine-hosted-size")).toHaveTextContent("n2-highmem-8");
    expect(screen.getByTestId("org-engine-hosted-plan")).toHaveTextContent("pro_m");
    expect(screen.getByTestId("org-engine-hosted-state")).toHaveTextContent(/running/i);
    expect(screen.queryByTestId("org-engine-mode-isolated")).not.toBeInTheDocument();
    expect(screen.queryByTestId("org-engine-mode-external")).not.toBeInTheDocument();
    expect(screen.queryByTestId("org-engine-kind")).not.toBeInTheDocument();
    expect(screen.queryByTestId("org-engine-url")).not.toBeInTheDocument();
    expect(screen.queryByTestId("org-engine-host")).not.toBeInTheDocument();
    expect(screen.queryByTestId("org-engine-save-button")).not.toBeInTheDocument();
  });

  it("names the Billing page as where a plan-decided engine is changed", async () => {
    mockFetch.mockResolvedValue(
      state({ mode: "shared", plan: "starter", plan_derived: true, isolated_engine: null }),
    );
    render(
      <MemoryRouter>
        <OrgEngineTab />
      </MemoryRouter>,
    );
    const change = await screen.findByTestId("org-engine-hosted-change");
    expect(change).toHaveTextContent(/Billing page/);
    expect(change.querySelector("a")).toHaveAttribute("href", "/admin/billing");
    // A shared-lane org has no dedicated engine, so there is no engine state to report.
    expect(screen.queryByTestId("org-engine-hosted-state")).not.toBeInTheDocument();
  });

  it("keeps the controls where nothing derives the lane", async () => {
    mockFetch.mockResolvedValue(state({ plan: null, plan_derived: false }));
    render(<OrgEngineTab />);
    expect(await screen.findByTestId("org-engine-save-button")).toBeInTheDocument();
    expect(screen.queryByTestId("org-engine-hosted")).not.toBeInTheDocument();
  });
});
