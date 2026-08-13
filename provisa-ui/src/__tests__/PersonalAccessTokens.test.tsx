// Copyright (c) 2026 Kenneth Stott
// Canary: 87962365-0c22-44d7-bc77-d6ed5b4ac7d0
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-1263: a personal access token is the credential every non-browser protocol accepts, so the
// person who holds it must be able to mint and revoke it themselves. These pin the two properties
// the server cannot recover from a mistake in: the secret is shown exactly once at issuance, and a
// revoked token stops offering a revoke control.

import { describe, it, expect, vi, beforeEach } from "vitest";
import { render, screen, fireEvent, waitFor } from "../test-utils/render";
import { PersonalAccessTokens } from "../components/PersonalAccessTokens";

const listSpy = vi.fn();
const issueSpy = vi.fn();
const revokeSpy = vi.fn();

vi.mock("../api/admin", () => ({
  listPersonalAccessTokens: (...a: unknown[]) => listSpy(...(a as [])),
  issuePersonalAccessToken: (...a: unknown[]) => issueSpy(...(a as [])),
  revokePersonalAccessToken: (...a: unknown[]) => revokeSpy(...(a as [])),
}));

const LIVE = {
  token_hash: "hash-live",
  prefix: "provisa_pat_abcd1234",
  name: "laptop",
  role_id: null,
  scopes: [],
  created_at: "2026-01-01T00:00:00Z",
  expires_at: null,
  last_used_at: null,
  revoked_at: null,
};

const REVOKED = {
  ...LIVE,
  token_hash: "hash-dead",
  prefix: "provisa_pat_dead0000",
  name: "old",
  revoked_at: "2026-02-01T00:00:00Z",
};

describe("PersonalAccessTokens", () => {
  beforeEach(() => {
    listSpy.mockReset().mockResolvedValue([LIVE, REVOKED]);
    issueSpy.mockReset();
    revokeSpy.mockReset().mockResolvedValue(undefined);
  });

  it("lists the caller's tokens by their display prefix, never a secret", async () => {
    render(<PersonalAccessTokens />);
    expect(await screen.findByTestId(`profile-pat-row-${LIVE.prefix}`)).toHaveTextContent("laptop");
    expect(screen.queryByTestId("profile-pat-minted")).not.toBeInTheDocument();
  });

  it("shows the minted secret once, and only after issuance", async () => {
    issueSpy.mockResolvedValue({
      token: "provisa_pat_THE-SECRET",
      prefix: "provisa_pat_THE-SECR",
      name: "ci",
      expires_at: null,
    });
    render(<PersonalAccessTokens />);

    fireEvent.change(await screen.findByTestId("profile-pat-name"), { target: { value: "ci" } });
    fireEvent.click(screen.getByTestId("profile-pat-issue"));

    expect(await screen.findByTestId("profile-pat-secret")).toHaveTextContent(
      "provisa_pat_THE-SECRET",
    );
    await waitFor(() =>
      expect(issueSpy).toHaveBeenCalledWith({ name: "ci", expires_in_days: null }),
    );

    // Dismissing removes it from the DOM: the server stores only a hash, so nothing can re-show it.
    fireEvent.click(screen.getByTestId("profile-pat-dismiss"));
    await waitFor(() => expect(screen.queryByTestId("profile-pat-secret")).not.toBeInTheDocument());
  });

  it("refuses to issue an unnamed token", async () => {
    render(<PersonalAccessTokens />);
    expect(await screen.findByTestId("profile-pat-issue")).toBeDisabled();
  });

  it("revokes by the token's hash and reloads the listing", async () => {
    render(<PersonalAccessTokens />);
    fireEvent.click(await screen.findByTestId(`profile-pat-revoke-${LIVE.prefix}`));
    await waitFor(() => expect(revokeSpy).toHaveBeenCalledWith("hash-live"));
    expect(listSpy).toHaveBeenCalledTimes(2);
  });

  it("offers no revoke control for an already revoked token", async () => {
    render(<PersonalAccessTokens />);
    expect(await screen.findByTestId(`profile-pat-revoked-${REVOKED.prefix}`)).toBeInTheDocument();
    expect(screen.queryByTestId(`profile-pat-revoke-${REVOKED.prefix}`)).not.toBeInTheDocument();
  });

  it("surfaces a server refusal rather than swallowing it", async () => {
    issueSpy.mockRejectedValue(new Error("expires_in_days must be between 1 and 366"));
    render(<PersonalAccessTokens />);
    fireEvent.change(await screen.findByTestId("profile-pat-name"), {
      target: { value: "forever" },
    });
    fireEvent.click(screen.getByTestId("profile-pat-issue"));
    expect(await screen.findByTestId("profile-pat-error")).toHaveTextContent("between 1 and 366");
  });
});
