// Copyright (c) 2026 Kenneth Stott
// Canary: f2ed1594-aafa-40df-87d1-42ceb1f588de
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

/**
 * REQ-1574: the org key surface shows a fingerprint and offers no way to see the key.
 *
 * The absence tests here are the point of the requirement: a reveal control that reappears in a
 * later edit is exactly the regression this file exists to fail on.
 */

import { describe, it, expect, vi, beforeEach } from "vitest";
import { render, screen, waitFor } from "../test-utils/render";
import userEvent from "@testing-library/user-event";
import { OrgEncryptionTab } from "../components/admin/OrgEncryptionTab";
import type { OrgEncryptionState } from "../api/admin";

vi.mock("../api/admin", () => ({
  fetchOrgEncryption: vi.fn(),
  setOrgEncryption: vi.fn(),
}));

import { fetchOrgEncryption, setOrgEncryption } from "../api/admin";
const mockFetch = vi.mocked(fetchOrgEncryption);
const mockSet = vi.mocked(setOrgEncryption);

const KEY = "MDEyMzQ1Njc4OWFiY2RlZjAxMjM0NTY3ODlhYmNkZWY=";

function state(overrides: Partial<OrgEncryptionState> = {}): OrgEncryptionState {
  return {
    org_id: "acme",
    configured: true,
    key_id: "k2",
    fingerprint: "3f9a1c88b0e47d25",
    supplied: false,
    created_at: "2026-08-21T10:00:00Z",
    created_by: "uid-admin",
    retired_count: 1,
    ...overrides,
  };
}

beforeEach(() => {
  vi.clearAllMocks();
  mockFetch.mockResolvedValue(state());
  mockSet.mockResolvedValue({ key_id: "k3", fingerprint: "aa11bb22cc33dd44" } as never);
});

describe("OrgEncryptionTab (REQ-1574)", () => {
  it("shows the fingerprint, the key id and the retired count", async () => {
    render(<OrgEncryptionTab />);
    expect(await screen.findByTestId("org-encryption-fingerprint")).toHaveTextContent(
      "3f9a1c88b0e47d25",
    );
    expect(screen.getByTestId("org-encryption-status")).toHaveTextContent("k2");
    expect(screen.getByTestId("org-encryption-retired")).toHaveTextContent("1");
  });

  it("offers no control that reveals or copies the key", async () => {
    render(<OrgEncryptionTab />);
    await screen.findByTestId("org-encryption-status");
    for (const name of [/show/i, /reveal/i, /copy/i, /download/i]) {
      expect(screen.queryByRole("button", { name })).toBeNull();
    }
  });

  it("says the org holds no key of its own when it does not", async () => {
    mockFetch.mockResolvedValue({ org_id: "acme", configured: false });
    render(<OrgEncryptionTab />);
    expect(await screen.findByTestId("org-encryption-unset")).toBeInTheDocument();
    expect(screen.queryByTestId("org-encryption-status")).toBeNull();
    expect(screen.getByTestId("org-encryption-apply")).toHaveTextContent(/set key/i);
  });

  it("generating asks the server for a key and never handles key material", async () => {
    const user = userEvent.setup();
    render(<OrgEncryptionTab />);
    await screen.findByTestId("org-encryption-status");

    await user.click(screen.getByTestId("org-encryption-apply"));

    await waitFor(() => expect(mockSet).toHaveBeenCalledWith({ key_b64: null }));
    // The applied message names the new key and its fingerprint. Anything more would be the value.
    expect(await screen.findByText(/aa11bb22cc33dd44/)).toBeInTheDocument();
    expect(mockFetch).toHaveBeenCalledTimes(2);
  });

  it("a supplied key is sent once and left nowhere on the page", async () => {
    const user = userEvent.setup();
    render(<OrgEncryptionTab />);
    await screen.findByTestId("org-encryption-status");

    await user.click(screen.getByTestId("org-encryption-mode-supply"));
    const input = screen.getByTestId("org-encryption-key-input");
    await user.type(input, KEY);
    await user.click(screen.getByTestId("org-encryption-apply"));

    await waitFor(() => expect(mockSet).toHaveBeenCalledWith({ key_b64: KEY }));
    await waitFor(() => expect(input).toHaveValue(""));
  });

  it("will not submit an empty supplied key", async () => {
    const user = userEvent.setup();
    render(<OrgEncryptionTab />);
    await screen.findByTestId("org-encryption-status");

    await user.click(screen.getByTestId("org-encryption-mode-supply"));

    expect(screen.getByTestId("org-encryption-apply")).toBeDisabled();
    expect(mockSet).not.toHaveBeenCalled();
  });

  it("surfaces the server's refusal of a key that is not a key", async () => {
    const user = userEvent.setup();
    mockSet.mockRejectedValue(new Error("key must decode to 32 bytes (AES-256), got 16"));
    render(<OrgEncryptionTab />);
    await screen.findByTestId("org-encryption-status");

    await user.click(screen.getByTestId("org-encryption-apply"));

    expect(await screen.findByText(/must decode to 32 bytes/)).toBeInTheDocument();
  });

  it("says plainly that rotating is not revoking", async () => {
    render(<OrgEncryptionTab />);
    await screen.findByTestId("org-encryption-status");
    expect(screen.getByText(/not a revocation/i)).toBeInTheDocument();
    expect(screen.getByTestId("org-encryption-apply")).toHaveTextContent(/rotate key/i);
  });
});
