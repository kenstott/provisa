// Copyright (c) 2026 Kenneth Stott
// Canary: 3ed69a3b-3224-4a8f-8469-46390925da06
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-1558: names go in, values never come back out. What is worth testing on this page is mostly
// what it does NOT do — no value on screen, no read call, and no create button at all when a
// central service owns the names.
//
// REQ-1560 adds the second claim: WHOSE. Every call names a vault, the personal surface names the
// caller's without any user id crossing the wire, and the platform `admin` wildcard — which
// satisfies every other gate in this UI — does not open the org's list of names.

import { describe, it, expect, vi, beforeEach } from "vitest";
import { cleanup } from "@testing-library/react";
import { render, screen, waitFor, fireEvent } from "../test-utils/render";
import { SecretsTab, MySecretsTab } from "../components/admin/SecretsTab";

const auth = { activeOrgId: "acme" as string | null, capabilities: ["org_settings"] };
vi.mock("../context/AuthContext", () => ({ useAuth: () => auth }));
vi.mock("../api/secrets", () => ({
  fetchSecrets: vi.fn(),
  putSecret: vi.fn(),
  deleteSecret: vi.fn(),
  fetchSecretsService: vi.fn(),
  setSecretsService: vi.fn(),
}));

import {
  fetchSecrets,
  putSecret,
  deleteSecret,
  fetchSecretsService,
  setSecretsService,
} from "../api/secrets";

const mockFetch = vi.mocked(fetchSecrets);
const mockPut = vi.mocked(putSecret);
const mockDelete = vi.mocked(deleteSecret);
const mockService = vi.mocked(fetchSecretsService);
const mockSetService = vi.mocked(setSecretsService);

const PROVIDERS = {
  provider: "provisa",
  providers: [
    {
      key: "provisa",
      label: "Provisa (built-in, encrypted)",
      description: "Held per-org in the control plane.",
      available: true,
      requires: null,
      writable: true,
      config_fields: [],
    },
    {
      key: "hashicorp_vault",
      label: "HashiCorp Vault (KV v2)",
      description: "Reads names out of a Vault KV v2 mount.",
      available: false,
      requires: "hvac",
      writable: false,
      config_fields: [
        { config_key: "url", label: "Vault address", type: "string", required: true },
        { config_key: "token", label: "Vault token", type: "string", required: true },
      ],
    },
    {
      key: "aws_secrets_manager",
      label: "AWS Secrets Manager",
      description: "Reads names out of AWS Secrets Manager.",
      available: true,
      requires: "boto3",
      writable: false,
      config_fields: [
        { config_key: "region", label: "AWS region", type: "string", required: false },
      ],
    },
  ],
  config: {},
  // REQ-1575: the server sends this bit and never the value it is about.
  secret_set: {},
};

const SECRET = {
  name: "GIT_TOKEN",
  description: "Push access",
  created_at: "2026-08-01T00:00:00Z",
  updated_at: "2026-08-20T00:00:00Z",
  updated_by: "uid-admin",
  reference: "${secret:GIT_TOKEN}",
  scope: "org" as const,
};

const BUILT_IN = {
  provider: { key: "provisa", label: "Provisa (built-in, encrypted)", writable: true },
  secrets: [SECRET],
};

beforeEach(() => {
  vi.clearAllMocks();
  mockFetch.mockResolvedValue(BUILT_IN);
  mockService.mockResolvedValue(PROVIDERS);
  auth.capabilities = ["org_settings"];
});

describe("SecretsTab", () => {
  it("lists the name and the reference to paste, and no value", async () => {
    render(<SecretsTab />);
    await waitFor(() => expect(screen.getByTestId("secret-row-GIT_TOKEN")).toBeInTheDocument());
    expect(screen.getByText("${secret:GIT_TOKEN}")).toBeInTheDocument();
    expect(screen.getByText("Push access")).toBeInTheDocument();
    // Nothing in the module can even ask for a value: no export reads one back.
    const exported = Object.keys(await import("../api/secrets"));
    expect(exported).toContain("fetchSecrets");
    expect(exported.filter((n) => /read|reveal|show|value|get/i.test(n))).toEqual([]);
  });

  it("stores a new secret under the name that was typed", async () => {
    mockPut.mockResolvedValue({ ...SECRET, name: "SLACK_WEBHOOK" });
    render(<SecretsTab />);
    await waitFor(() => expect(screen.getByTestId("secrets-tab")).toBeInTheDocument());
    fireEvent.click(screen.getByTestId("secrets-add"));
    fireEvent.change(await screen.findByLabelText("Name"), {
      target: { value: "SLACK_WEBHOOK" },
    });
    fireEvent.change(screen.getByLabelText("Value"), { target: { value: "https://hooks" } });
    fireEvent.click(screen.getByTestId("secret-submit"));
    await waitFor(() =>
      expect(mockPut).toHaveBeenCalledWith("acme", "org", "SLACK_WEBHOOK", {
        value: "https://hooks",
        description: null,
      }),
    );
  });

  it("replacing keeps the name, because the name is the identity", async () => {
    mockPut.mockResolvedValue(SECRET);
    render(<SecretsTab />);
    await waitFor(() => expect(screen.getByTestId("secret-row-GIT_TOKEN")).toBeInTheDocument());
    fireEvent.click(screen.getByTestId("secret-replace-GIT_TOKEN"));
    const nameField = await screen.findByLabelText("Name");
    expect(nameField).toHaveValue("GIT_TOKEN");
    expect(nameField).toBeDisabled();
    fireEvent.change(screen.getByLabelText("Value"), { target: { value: "ghp_rotated" } });
    fireEvent.click(screen.getByTestId("secret-submit"));
    await waitFor(() =>
      expect(mockPut).toHaveBeenCalledWith("acme", "org", "GIT_TOKEN", {
        value: "ghp_rotated",
        description: "Push access",
      }),
    );
  });

  it("will not save a secret with no value", async () => {
    render(<SecretsTab />);
    await waitFor(() => expect(screen.getByTestId("secrets-tab")).toBeInTheDocument());
    fireEvent.click(screen.getByTestId("secrets-add"));
    fireEvent.change(await screen.findByLabelText("Name"), { target: { value: "EMPTY" } });
    expect(screen.getByTestId("secret-submit")).toBeDisabled();
    expect(mockPut).not.toHaveBeenCalled();
  });

  it("deletes only after the consequence is confirmed", async () => {
    mockDelete.mockResolvedValue({ deleted: "GIT_TOKEN" });
    render(<SecretsTab />);
    await waitFor(() => expect(screen.getByTestId("secret-row-GIT_TOKEN")).toBeInTheDocument());
    fireEvent.click(screen.getByTestId("secret-delete-GIT_TOKEN"));
    expect(mockDelete).not.toHaveBeenCalled();
    fireEvent.click(await screen.findByRole("button", { name: "Confirm" }));
    await waitFor(() => expect(mockDelete).toHaveBeenCalledWith("acme", "org", "GIT_TOKEN"));
  });

  it("offers nothing to create when a central service owns the names", async () => {
    mockFetch.mockResolvedValue({
      provider: { key: "hashicorp_vault", label: "HashiCorp Vault (KV v2)", writable: false },
      secrets: [],
    });
    render(<SecretsTab />);
    await waitFor(() => expect(screen.getByTestId("secrets-tab")).toBeInTheDocument());
    expect(screen.queryByTestId("secrets-add")).toBeNull();
    expect(screen.getAllByText(/HashiCorp Vault/).length).toBeGreaterThan(0);
  });

  it("shows an org admin the names and not the deployment's choice of service", async () => {
    render(<SecretsTab />);
    await waitFor(() => expect(screen.getByTestId("secrets-tab")).toBeInTheDocument());
    expect(screen.queryByTestId("secrets-service")).toBeNull();
    expect(mockService).not.toHaveBeenCalled();
  });
});

describe("SecretsTab secrets-service panel", () => {
  beforeEach(() => {
    auth.capabilities = ["platform_settings"];
    localStorage.clear();
  });

  /** The panel starts collapsed; open it the way a person does. */
  async function openService() {
    render(<SecretsTab />);
    fireEvent.click(await screen.findByTestId("secrets-service-toggle"));
    // Mantine hides the collapsing region from the accessibility tree until the transition
    // finishes, so wait for a control inside it rather than for the container.
    await screen.findByRole("radio", { name: /Provisa/ });
  }

  it("starts collapsed and reads nothing until it is opened", async () => {
    render(<SecretsTab />);
    expect(await screen.findByTestId("secrets-service-toggle")).toBeInTheDocument();
    expect(screen.queryByTestId("secrets-service")).toBeNull();
    expect(mockService).not.toHaveBeenCalled();
  });

  it("remembers that it was opened, and restores that on the next visit", async () => {
    await openService();
    expect(localStorage.getItem("provisa.panel.secretsService")).toBe(JSON.stringify("service"));
    cleanup();
    render(<SecretsTab />);
    await waitFor(() => expect(screen.getByTestId("secrets-service")).toBeInTheDocument());
  });

  it("remembers that it was closed again", async () => {
    await openService();
    fireEvent.click(screen.getByTestId("secrets-service-toggle"));
    await waitFor(() => expect(screen.queryByTestId("secrets-service")).toBeNull());
    cleanup();
    render(<SecretsTab />);
    expect(await screen.findByTestId("secrets-service-toggle")).toBeInTheDocument();
    expect(screen.queryByTestId("secrets-service")).toBeNull();
  });

  it("lists every backend, greying out one whose library is missing", async () => {
    await openService();
    expect(screen.getByTestId("secrets-provider-provisa")).toBeInTheDocument();
    const vault = screen.getByTestId("secrets-provider-hashicorp_vault");
    expect(vault).toHaveAttribute("data-unavailable", "true");
    expect(screen.getByTestId("secrets-provider-requires-hashicorp_vault")).toHaveTextContent(
      "(requires hvac import)",
    );
    expect(screen.getByRole("radio", { name: /HashiCorp Vault/ })).toBeDisabled();
    // An installed backend says nothing about its library — there is nothing to install.
    expect(screen.queryByTestId("secrets-provider-requires-aws_secrets_manager")).toBeNull();
  });

  it("changes the deployment's secrets service", async () => {
    mockSetService.mockResolvedValue({ success: true, provider: "aws_secrets_manager" });
    await openService();
    fireEvent.click(screen.getByRole("radio", { name: /AWS Secrets Manager/ }));
    fireEvent.change(await screen.findByLabelText("AWS region"), {
      target: { value: "us-east-1" },
    });
    fireEvent.click(screen.getByTestId("secrets-service-save"));
    await waitFor(() =>
      expect(mockSetService).toHaveBeenCalledWith({
        provider: "aws_secrets_manager",
        config: { region: "us-east-1" },
      }),
    );
  });

  it("will not submit a backend whose required config is blank", async () => {
    mockService.mockResolvedValue({
      ...PROVIDERS,
      providers: [
        { ...PROVIDERS.providers[1], available: true },
        ...PROVIDERS.providers.filter((p) => p.key !== "hashicorp_vault"),
      ],
      provider: "hashicorp_vault",
    });
    await openService();
    expect(screen.getByTestId("secrets-service-save")).toBeDisabled();
    fireEvent.change(screen.getByTestId("secrets-service-field-url"), {
      target: { value: "https://vault.internal:8200" },
    });
    fireEvent.change(screen.getByTestId("secrets-service-field-token"), {
      target: { value: "${env:VAULT_TOKEN}" },
    });
    expect(screen.getByTestId("secrets-service-save")).not.toBeDisabled();
  });

  it("shows a platform admin no org secret names at all", async () => {
    await openService();
    expect(mockFetch).not.toHaveBeenCalled();
    expect(screen.queryByTestId("secrets-table")).toBeNull();
  });
});

// REQ-1560: the second surface. Same list, same form, different vault — and the difference travels
// as the vault name in the URL, never as a user id the browser could edit.
describe("MySecretsTab", () => {
  const MINE = {
    provider: { key: "provisa", label: "Provisa (built-in, encrypted)", writable: true },
    secrets: [
      {
        ...SECRET,
        name: "MY_GIT_TOKEN",
        reference: "${user:MY_GIT_TOKEN}",
        scope: "user" as const,
      },
    ],
  };

  beforeEach(() => {
    mockFetch.mockResolvedValue(MINE);
    // An analyst: `usage` and nothing else. Holding a credential of your own is not a grant.
    auth.capabilities = ["usage", "query_development"];
  });

  it("reads the caller's own vault, with no user id to name anybody else's", async () => {
    render(<MySecretsTab />);
    await waitFor(() => expect(screen.getByTestId("secret-row-MY_GIT_TOKEN")).toBeInTheDocument());
    expect(mockFetch).toHaveBeenCalledWith("acme", "user");
    expect(mockFetch.mock.calls[0]).toHaveLength(2);
    expect(screen.getByText("${user:MY_GIT_TOKEN}")).toBeInTheDocument();
  });

  it("stores into the personal vault", async () => {
    mockPut.mockResolvedValue({ ...MINE.secrets[0], name: "MY_PAT" });
    render(<MySecretsTab />);
    await waitFor(() => expect(screen.getByTestId("my-secrets-tab")).toBeInTheDocument());
    fireEvent.click(screen.getByTestId("secrets-add"));
    fireEvent.change(await screen.findByLabelText("Name"), { target: { value: "MY_PAT" } });
    fireEvent.change(screen.getByLabelText("Value"), { target: { value: "ghp_mine" } });
    fireEvent.click(screen.getByTestId("secret-submit"));
    await waitFor(() =>
      expect(mockPut).toHaveBeenCalledWith("acme", "user", "MY_PAT", {
        value: "ghp_mine",
        description: null,
      }),
    );
  });

  it("never offers the deployment's choice of secrets service", async () => {
    render(<MySecretsTab />);
    await waitFor(() => expect(screen.getByTestId("my-secrets-tab")).toBeInTheDocument());
    expect(screen.queryByTestId("secrets-service-toggle")).toBeNull();
    expect(mockService).not.toHaveBeenCalled();
  });
});

// REQ-1560, REQ-1361: `admin` is the platform wildcard and satisfies every other capability gate in
// this UI. The org vault is the exception, and it has to be: the names an org keeps are themselves
// a statement about what that org connects to. The server refuses the same call, so the wildcard
// bought a page that 403s — this asserts the browser does not even ask.
describe("SecretsTab and the platform wildcard", () => {
  it("shows a platform admin the service chooser and none of the org's names", async () => {
    auth.capabilities = ["admin", "platform_settings"];
    render(<SecretsTab />);
    expect(await screen.findByTestId("secrets-service-toggle")).toBeInTheDocument();
    expect(mockFetch).not.toHaveBeenCalled();
    expect(screen.queryByTestId("secrets-table")).toBeNull();
  });
});
