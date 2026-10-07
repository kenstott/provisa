// Copyright (c) 2026 Kenneth Stott
// Canary: 08271a19-9e6e-4abc-a006-c8fea7711348
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

import { describe, it, expect, vi, beforeEach } from "vitest";
import { render, screen, waitFor } from "../test-utils/render";
import userEvent from "@testing-library/user-event";
import { EncryptionTab } from "../components/admin/EncryptionTab";
import type { EncryptionState } from "../api/admin";

vi.mock("../api/admin", () => ({
  fetchEncryption: vi.fn(),
  setEncryption: vi.fn(),
  generateEncryptionKey: vi.fn(),
}));

import { fetchEncryption, generateEncryptionKey, setEncryption } from "../api/admin";
const mockFetch = vi.mocked(fetchEncryption);
const mockGenerate = vi.mocked(generateEncryptionKey);
const mockSet = vi.mocked(setEncryption);

function state(overrides: Partial<EncryptionState> = {}): EncryptionState {
  return {
    provider: "null",
    key_id: null,
    key_present: null,
    key_fingerprint: null,
    providers: [
      {
        key: "null",
        label: "None (passthrough)",
        description: "Plaintext.",
        available: true,
        config_fields: [],
      },
      {
        key: "local",
        label: "Local keychain",
        description: "On-host key.",
        available: true,
        config_fields: [],
      },
      {
        key: "aws_kms",
        label: "AWS KMS",
        description: "Planned.",
        available: false,
        config_fields: [
          { config_key: "key_arn", label: "KMS key ARN", type: "string", required: true },
          { config_key: "region", label: "AWS region", type: "string", required: false },
        ],
      },
    ],
    config: {},
    // REQ-1575: per provider, per secret field, whether a value is stored. Never the value.
    secret_set: {},
    restart_required_note: "Restart to apply.",
    ...overrides,
  };
}

describe("EncryptionTab", () => {
  beforeEach(() => {
    mockFetch.mockReset();
    mockSet.mockReset();
    mockGenerate.mockReset();
    mockSet.mockResolvedValue({ success: true, restart_required: true });
  });

  it("renders generic config fields declared by a provider registry entry", async () => {
    // Start already on aws_kms so its fields render (available:false still shows fields for reference).
    mockFetch.mockResolvedValue(state({ provider: "aws_kms" }));
    render(<EncryptionTab />);
    await waitFor(() => expect(screen.getByTestId("encryption-field-key_arn")).toBeInTheDocument());
    expect(screen.getByTestId("encryption-field-region")).toBeInTheDocument();
  });

  it("blocks save for an unavailable provider", async () => {
    mockFetch.mockResolvedValue(state({ provider: "aws_kms" }));
    render(<EncryptionTab />);
    await waitFor(() => expect(screen.getByTestId("encryption-unavailable")).toBeInTheDocument());
    expect(screen.getByTestId("save-encryption-button")).toBeDisabled();
  });

  it("persists provider config on save for an available provider", async () => {
    mockFetch.mockResolvedValue(
      state({
        provider: "local",
        config: { local: {} },
      }),
    );
    render(<EncryptionTab />);
    await waitFor(() => expect(screen.getByTestId("save-encryption-button")).toBeEnabled());
    await userEvent.click(screen.getByTestId("save-encryption-button"));
    await waitFor(() => expect(mockSet).toHaveBeenCalled());
    expect(mockSet.mock.calls[0][0]).toMatchObject({ provider: "local" });
  });

  // REQ-918: a master key is provisioned, never replaced.
  it("offers to generate a master key when the deployment has recorded none", async () => {
    mockFetch.mockResolvedValue(state({ provider: "local", key_present: false }));
    mockGenerate.mockResolvedValue({ stored: true, key_id: "master" });
    render(<EncryptionTab />);
    const button = await screen.findByTestId("generate-key-button");
    expect(button).toHaveTextContent("Generate master key");
    expect(screen.queryByTestId("master-key-fingerprint")).not.toBeInTheDocument();
    await userEvent.click(button);
    await waitFor(() => expect(mockGenerate).toHaveBeenCalledWith({ key_id: null }));
  });

  it("shows the recorded key's fingerprint and offers no way to replace it", async () => {
    mockFetch.mockResolvedValue(
      state({ provider: "local", key_present: true, key_fingerprint: "fb98f0cf0a1b2c3d" }),
    );
    render(<EncryptionTab />);
    expect(await screen.findByTestId("master-key-fingerprint")).toHaveTextContent(
      "fb98f0cf0a1b2c3d",
    );
    expect(screen.queryByTestId("generate-key-button")).not.toBeInTheDocument();
    expect(screen.queryByText(/rotat/i)).not.toBeInTheDocument();
  });
});
