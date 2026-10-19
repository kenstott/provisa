// Copyright (c) 2026 Kenneth Stott
// Canary: 55c070e2-2d55-4271-b1e9-30f7c4dacf7b
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

/**
 * REQ-2087: the redaction rules surface. The org edits its own rules; the deployment's are
 * editable only with platform_settings; the automatic rule's secrets are shown by name only.
 */

import { describe, it, expect, vi, beforeEach } from "vitest";
import { render, screen, waitFor } from "../test-utils/render";
import userEvent from "@testing-library/user-event";
import { RedactionTab } from "../components/admin/RedactionTab";
import type { RedactionState } from "../api/admin";

vi.mock("../api/admin", () => ({
  fetchRedaction: vi.fn(),
  saveRedactionRules: vi.fn(),
}));

import { fetchRedaction, saveRedactionRules } from "../api/admin";
const mockFetch = vi.mocked(fetchRedaction);
const mockSave = vi.mocked(saveRedactionRules);

function state(overrides: Partial<RedactionState> = {}): RedactionState {
  return {
    builtins: [
      { name: "us_ssn", pattern: "\\d{3}-\\d{2}-\\d{4}", reference: null, source: "builtin" },
    ],
    deployment: [
      { name: "ops_token", pattern: null, reference: "${env:OPS_TOKEN}", source: "deployment" },
    ],
    org: [{ name: "employee_id", pattern: "EMP-\\d{6}", reference: null, source: "org" }],
    automatic: ["warehouse_password"],
    resolved_at_use: 2,
    unresolved: [],
    can_edit_deployment: false,
    ...overrides,
  };
}

beforeEach(() => {
  vi.clearAllMocks();
  mockFetch.mockResolvedValue(state());
  mockSave.mockImplementation(async (_scope, rules) => state({ org: rules }));
});

describe("RedactionTab (REQ-2087)", () => {
  it("lists the built-ins, the automatic names and says redaction is best effort", async () => {
    render(<RedactionTab />);
    expect(await screen.findByTestId("redaction-builtins")).toHaveTextContent("us_ssn");
    expect(screen.getByTestId("redaction-automatic")).toHaveTextContent("warehouse_password");
    expect(screen.getByTestId("redaction-best-effort")).toHaveTextContent(/best effort/i);
    expect(screen.getByTestId("redaction-best-effort")).toHaveTextContent(/starter set/i);
  });

  it("shows the deployment's rules read-only without platform_settings", async () => {
    render(<RedactionTab />);
    expect(await screen.findByTestId("redaction-deployment-table")).toHaveTextContent(
      "${env:OPS_TOKEN}",
    );
    expect(screen.queryByTestId("redaction-deployment-editor")).toBeNull();
  });

  it("lets a platform_settings holder edit the deployment's rules", async () => {
    mockFetch.mockResolvedValue(state({ can_edit_deployment: true }));
    render(<RedactionTab />);
    expect(await screen.findByTestId("redaction-deployment-editor")).toBeInTheDocument();
    expect(screen.getByTestId("redaction-deployment-name-0")).toHaveValue("ops_token");
  });

  it("names the reference rules that do not resolve", async () => {
    mockFetch.mockResolvedValue(state({ unresolved: ["ops_token"] }));
    render(<RedactionTab />);
    expect(await screen.findByTestId("redaction-unresolved")).toHaveTextContent("ops_token");
  });

  it("saves an added org rule as a pattern", async () => {
    const user = userEvent.setup();
    render(<RedactionTab />);
    await user.click(await screen.findByTestId("redaction-org-add"));
    await user.type(screen.getByTestId("redaction-org-name-1"), "badge");
    await user.type(screen.getByTestId("redaction-org-value-1"), "B-\\d+");
    await user.click(screen.getByTestId("redaction-org-save"));
    await waitFor(() => expect(mockSave).toHaveBeenCalled());
    expect(mockSave).toHaveBeenCalledWith("org", [
      { name: "employee_id", pattern: "EMP-\\d{6}", reference: null },
      { name: "badge", pattern: "B-\\d+", reference: null },
    ]);
  });

  it("removes an org rule before saving", async () => {
    const user = userEvent.setup();
    render(<RedactionTab />);
    await user.click(await screen.findByTestId("redaction-org-remove-0"));
    await user.click(screen.getByTestId("redaction-org-save"));
    await waitFor(() => expect(mockSave).toHaveBeenCalledWith("org", []));
  });
});
