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
// revoked token stops offering a revoke control. A token's scopes are the roles it may act as: the
// form picks them from the roles the user holds, requires one, and the list shows each token's.

import { describe, it, expect, vi, beforeEach } from "vitest";
import userEvent from "@testing-library/user-event";
import { render, screen, fireEvent, waitFor, within } from "../test-utils/render";
import { PersonalAccessTokens } from "../components/PersonalAccessTokens";

const listSpy = vi.fn();
const issueSpy = vi.fn();
const revokeSpy = vi.fn();

// The roles the signed-in user holds: what the form offers.
const auth = { availableRoles: [{ id: "analyst" }, { id: "steward" }] };
vi.mock("../context/AuthContext", () => ({ useAuth: () => auth }));

vi.mock("../api/admin", () => ({
  listPersonalAccessTokens: (...a: unknown[]) => listSpy(...(a as [])),
  issuePersonalAccessToken: (...a: unknown[]) => issueSpy(...(a as [])),
  revokePersonalAccessToken: (...a: unknown[]) => revokeSpy(...(a as [])),
}));

const LIVE = {
  token_hash: "hash-live",
  prefix: "provisa_pat_abcd1234",
  name: "laptop",
  env_name: "dev",
  scopes: ["analyst", "steward"],
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

/** Pick ``role`` in the form's role picker, the standard MultiSelect. */
async function pickRole(role: string) {
  await userEvent.click(await screen.findByRole("textbox", { name: "Roles" }));
  await userEvent.click(await screen.findByRole("option", { name: role, hidden: true }));
}

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

  it("says which environment each token addresses, and which a new one will", async () => {
    // REQ-1503: a token works in the environment it was issued into and no other.
    render(<PersonalAccessTokens />);
    expect(await screen.findByTestId(`pat-env-${LIVE.token_hash}`)).toHaveTextContent("dev");
    expect(screen.getByTestId("profile-pat-env-help")).toHaveTextContent(
      "A new token will work in prod",
    );
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
    await pickRole("analyst");
    fireEvent.click(screen.getByTestId("profile-pat-issue"));

    expect(await screen.findByTestId("profile-pat-secret")).toHaveTextContent(
      "provisa_pat_THE-SECRET",
    );
    await waitFor(() =>
      expect(issueSpy).toHaveBeenCalledWith({
        name: "ci",
        scopes: ["analyst"],
        expires_in_days: null,
      }),
    );

    // Dismissing removes it from the DOM: the server stores only a hash, so nothing can re-show it.
    fireEvent.click(screen.getByTestId("profile-pat-dismiss"));
    await waitFor(() => expect(screen.queryByTestId("profile-pat-secret")).not.toBeInTheDocument());
  });

  it("refuses to issue an unnamed token", async () => {
    render(<PersonalAccessTokens />);
    expect(await screen.findByTestId("profile-pat-issue")).toBeDisabled();
  });

  it("requires a role: a named token with none picked is not issued", async () => {
    render(<PersonalAccessTokens />);
    fireEvent.change(await screen.findByTestId("profile-pat-name"), { target: { value: "ci" } });
    // The standard picker marks a required field on its label.
    expect(screen.getByText("Roles", { selector: "label" })).toHaveTextContent("Roles *");
    expect(screen.getByTestId("profile-pat-issue")).toBeDisabled();
    await pickRole("steward");
    expect(screen.getByTestId("profile-pat-issue")).toBeEnabled();
  });

  it("offers the roles the user holds, and no other", async () => {
    render(<PersonalAccessTokens />);
    await userEvent.click(await screen.findByRole("textbox", { name: "Roles" }));
    const offered = (await screen.findAllByRole("option", { hidden: true })).map(
      (o) => o.textContent,
    );
    expect(offered).toEqual(["analyst", "steward"]);
  });

  it("issues a token for every role picked, in the order picked", async () => {
    issueSpy.mockResolvedValue({ token: "provisa_pat_S", prefix: "p", name: "ci", expires_at: null });
    render(<PersonalAccessTokens />);
    fireEvent.change(await screen.findByTestId("profile-pat-name"), { target: { value: "ci" } });
    await pickRole("steward");
    await pickRole("analyst");
    fireEvent.click(screen.getByTestId("profile-pat-issue"));
    await waitFor(() =>
      expect(issueSpy).toHaveBeenCalledWith({
        name: "ci",
        scopes: ["steward", "analyst"],
        expires_in_days: null,
      }),
    );
  });

  it("shows the roles each token may act as", async () => {
    render(<PersonalAccessTokens />);
    const row = await screen.findByTestId(`profile-pat-row-${LIVE.prefix}`);
    expect(within(row).getByTestId(`pat-roles-${LIVE.token_hash}`)).toHaveTextContent(
      "analyst, steward",
    );
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
    await pickRole("analyst");
    fireEvent.click(screen.getByTestId("profile-pat-issue"));
    expect(await screen.findByTestId("profile-pat-error")).toHaveTextContent("between 1 and 366");
  });
});
