// Copyright (c) 2026 Kenneth Stott
// Canary: 8b40d6c2-1f57-4e93-a0d8-63c2b915ef7a
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-1527: the remote is kept verbatim, secret reference and all, and a cleared field is the org
// saying it no longer mirrors anywhere — which is a null, not an empty string.

import { describe, it, expect, vi, beforeEach } from "vitest";
import { render, screen, waitFor, fireEvent } from "../test-utils/render";
import { RepoIntegrationPanel } from "../components/admin/RepoIntegrationPanel";

vi.mock("../api/environments", () => ({
  fetchRepoIntegration: vi.fn(),
  saveRepoIntegration: vi.fn(),
  probeRepoRemote: vi.fn(),
  createRepoRemote: vi.fn(),
}));

import {
  fetchRepoIntegration,
  saveRepoIntegration,
  probeRepoRemote,
  createRepoRemote,
} from "../api/environments";

const mockLoad = vi.mocked(fetchRepoIntegration);
const mockSave = vi.mocked(saveRepoIntegration);
const mockProbe = vi.mocked(probeRepoRemote);
const mockCreate = vi.mocked(createRepoRemote);

const MISSING = {
  exists: false,
  kind: "GitHub repository",
  creatable: true,
  target: "github.com/acme/model",
  detail: "the API answered 404",
};

const REMOTE = "https://${env:GIT_TOKEN}@github.com/acme/model.git";

beforeEach(() => {
  vi.clearAllMocks();
  mockLoad.mockResolvedValue({ remote: REMOTE, status_webhook: null, configured: true });
});

describe("RepoIntegrationPanel", () => {
  it("shows the stored remote with its secret reference intact", async () => {
    render(<RepoIntegrationPanel orgId="acme" />);
    await waitFor(() => expect(screen.getByTestId("repo-remote-input")).toHaveValue(REMOTE));
  });

  it("sends a cleared field as null rather than an empty string", async () => {
    mockSave.mockResolvedValue({ remote: null, status_webhook: null, configured: false });
    render(<RepoIntegrationPanel orgId="acme" />);
    const input = await screen.findByTestId("repo-remote-input");
    await waitFor(() => expect(input).toBeEnabled());
    fireEvent.change(input, { target: { value: "   " } });
    fireEvent.click(screen.getByTestId("repo-integration-save"));
    await waitFor(() =>
      expect(mockSave).toHaveBeenCalledWith("acme", { remote: null, status_webhook: null }),
    );
  });

  it("saves both halves together", async () => {
    const hook = "https://ci.example.com/hooks/provisa";
    mockSave.mockResolvedValue({ remote: REMOTE, status_webhook: hook, configured: true });
    render(<RepoIntegrationPanel orgId="acme" />);
    const webhook = await screen.findByTestId("repo-webhook-input");
    await waitFor(() => expect(webhook).toBeEnabled());
    fireEvent.change(webhook, { target: { value: hook } });
    fireEvent.click(screen.getByTestId("repo-integration-save"));
    await waitFor(() =>
      expect(mockSave).toHaveBeenCalledWith("acme", { remote: REMOTE, status_webhook: hook }),
    );
  });

  it("withholds the fields until the stored value has been read", () => {
    render(<RepoIntegrationPanel orgId="acme" />);
    expect(screen.getByTestId("repo-remote-input")).toBeDisabled();
    expect(screen.getByTestId("repo-integration-save")).toBeDisabled();
  });
});

// REQ-1537: the check happens while the field is still open, because that is the only moment its
// answer can change anything — a push that discovers the repository missing has already failed.
describe("RepoIntegrationPanel remote probe", () => {
  it("probes the address on screen rather than the stored one", async () => {
    const typed = "https://github.com/acme/other.git";
    mockProbe.mockResolvedValue({ ...MISSING, exists: true, target: "github.com/acme/other" });
    render(<RepoIntegrationPanel orgId="acme" />);
    const input = await screen.findByTestId("repo-remote-input");
    await waitFor(() => expect(input).toBeEnabled());
    fireEvent.change(input, { target: { value: typed } });
    fireEvent.click(screen.getByTestId("repo-remote-probe"));
    await waitFor(() => expect(mockProbe).toHaveBeenCalledWith("acme", typed));
    expect(await screen.findByTestId("repo-remote-found")).toHaveTextContent(
      "github.com/acme/other",
    );
  });

  it("offers to create a repository that is not there, and does not create it unasked", async () => {
    mockProbe.mockResolvedValue(MISSING);
    render(<RepoIntegrationPanel orgId="acme" />);
    await waitFor(() => expect(screen.getByTestId("repo-remote-probe")).toBeEnabled());
    fireEvent.click(screen.getByTestId("repo-remote-probe"));
    expect(await screen.findByTestId("repo-remote-missing")).toHaveTextContent(
      "the API answered 404",
    );
    expect(mockCreate).not.toHaveBeenCalled();
    fireEvent.click(screen.getByTestId("repo-remote-create"));
    await waitFor(() => expect(mockCreate).toHaveBeenCalledWith("acme", REMOTE, true));
  });

  it("says so rather than offering when this remote is one Provisa cannot create", async () => {
    mockProbe.mockResolvedValue({
      ...MISSING,
      creatable: false,
      detail: "no credential in the URL",
    });
    render(<RepoIntegrationPanel orgId="acme" />);
    await waitFor(() => expect(screen.getByTestId("repo-remote-probe")).toBeEnabled());
    fireEvent.click(screen.getByTestId("repo-remote-probe"));
    expect(await screen.findByTestId("repo-remote-uncreatable")).toHaveTextContent(
      "no credential in the URL",
    );
    expect(screen.queryByTestId("repo-remote-create")).toBeNull();
  });

  it("drops a verdict as soon as the address it was about is edited", async () => {
    // Otherwise "Create it" would offer to create something other than what is on screen.
    mockProbe.mockResolvedValue(MISSING);
    render(<RepoIntegrationPanel orgId="acme" />);
    await waitFor(() => expect(screen.getByTestId("repo-remote-probe")).toBeEnabled());
    fireEvent.click(screen.getByTestId("repo-remote-probe"));
    expect(await screen.findByTestId("repo-remote-missing")).toBeInTheDocument();
    fireEvent.change(screen.getByTestId("repo-remote-input"), {
      target: { value: "https://github.com/acme/typo.git" },
    });
    await waitFor(() => expect(screen.queryByTestId("repo-remote-missing")).toBeNull());
  });

  it("re-checks the remote it just saved", async () => {
    mockSave.mockResolvedValue({ remote: REMOTE, status_webhook: null, configured: true });
    mockProbe.mockResolvedValue({ ...MISSING, exists: true });
    render(<RepoIntegrationPanel orgId="acme" />);
    await waitFor(() => expect(screen.getByTestId("repo-integration-save")).toBeEnabled());
    fireEvent.click(screen.getByTestId("repo-integration-save"));
    await waitFor(() => expect(mockProbe).toHaveBeenCalledWith("acme", REMOTE));
  });
});
