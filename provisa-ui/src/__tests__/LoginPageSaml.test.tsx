// Copyright (c) 2026 Kenneth Stott
// Canary: cbf6f210-3817-4a2d-a0ae-72e29e8933e9
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-1265: under the SAML provider the sign-in page offers single sign-on, and the session
// token the service provider hands back in the URL fragment starts the session.

import { describe, it, expect, vi, beforeEach, afterEach } from "vitest";
import { render, screen, waitFor } from "../test-utils/render";
import { LoginPage } from "../pages/LoginPage";

vi.mock("../api/admin", async (importOriginal) => ({
  ...(await importOriginal<typeof import("../api/admin")>()),
  fetchProviderType: vi.fn().mockResolvedValue("saml"),
  fetchBootstrapStatus: vi.fn().mockResolvedValue(false),
  claimBootstrap: vi.fn().mockResolvedValue(true),
}));

describe("SAML sign-in (REQ-1265)", () => {
  const onLoginSuccess = vi.fn();

  beforeEach(() => {
    onLoginSuccess.mockReset();
    window.history.replaceState(null, "", "/login");
  });

  afterEach(() => {
    localStorage.removeItem("provisa_token");
    window.history.replaceState(null, "", "/login");
  });

  it("offers single sign-on instead of a password form", async () => {
    render(<LoginPage onLoginSuccess={onLoginSuccess} authDisabled={false} />);
    const sso = await screen.findByTestId("saml-signin-button");
    expect(sso).toHaveAttribute("href", "/auth/saml/login");
    expect(screen.queryByTestId("password-input")).not.toBeInTheDocument();
    expect(screen.getByTestId("operator-signin-toggle")).toBeInTheDocument();
  });

  it("starts the session from the token in the fragment and clears it from the address", async () => {
    window.history.replaceState(null, "", "/login#saml_token=tok%2Eabc");
    render(<LoginPage onLoginSuccess={onLoginSuccess} authDisabled={false} />);
    await waitFor(() => expect(onLoginSuccess).toHaveBeenCalledWith("tok.abc"));
    expect(window.location.hash).toBe("");
  });

  it("says sign-in failed when the service provider refused the response", async () => {
    window.history.replaceState(null, "", "/login#saml_error=rejected");
    render(<LoginPage onLoginSuccess={onLoginSuccess} authDisabled={false} />);
    expect(await screen.findByTestId("login-error")).toHaveTextContent(/single sign-on/i);
    expect(onLoginSuccess).not.toHaveBeenCalled();
  });
});
