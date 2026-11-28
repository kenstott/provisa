// Copyright (c) 2026 Kenneth Stott
// Canary: 3d8968ec-b52b-44ab-ab4c-5d0724028eec
// Canary: placeholder
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-1264: the break-glass account's forced first-sign-in rotation, in the browser. The operator
// sign-in answers 403 auth.password_change_required until the generated password is replaced, and
// the sign-in page answers that with a form calling POST /auth/superuser-password.

import { describe, it, expect, vi, beforeEach, afterEach } from "vitest";
import type { MockInstance } from "vitest";
import { render, screen, fireEvent, waitFor } from "../test-utils/render";
import { LoginPage } from "../pages/LoginPage";
import { SUPERUSER_TOKEN_KEY } from "../lib/sessionToken";

// Spread the real module: vmThreads + fileParallelism:false share one module registry.
vi.mock("../api/admin", async (importOriginal) => ({
  ...(await importOriginal<typeof import("../api/admin")>()),
  // The operator sign-in is reached from an IdP's sign-in card.
  fetchProviderType: vi.fn().mockResolvedValue("firebase"),
  fetchBootstrapStatus: vi.fn().mockResolvedValue(false),
  claimBootstrap: vi.fn().mockResolvedValue(true),
}));

vi.mock("../lib/firebase", () => ({
  signInWithGoogle: vi.fn(),
  signInWithGithub: vi.fn(),
  signInWithMicrosoft: vi.fn(),
  signInWithEmailPassword: vi.fn(),
  registerWithEmailPassword: vi.fn(),
}));

type FetchSpy = MockInstance<typeof fetch>;

const USER = "platform-admin";
const GENERATED = "generated-pw-0001";
const NEW_PW = "a-new-password-42";

function jsonResponse(status: number, body: unknown): Response {
  return {
    ok: status >= 200 && status < 300,
    status,
    statusText: "",
    json: async () => body,
  } as Response;
}

const REFUSED_UNTIL_CHANGED = jsonResponse(403, {
  detail: "The break-glass password must be changed before it signs in",
  code: "auth.password_change_required",
  params: {},
});

async function openOperatorForm() {
  fireEvent.click(await screen.findByTestId("operator-signin-toggle"));
  fireEvent.change(screen.getByTestId("operator-username-input"), { target: { value: USER } });
  fireEvent.change(screen.getByTestId("operator-password-input"), {
    target: { value: GENERATED },
  });
}

async function reachChangeForm(fetchSpy: FetchSpy) {
  fetchSpy.mockResolvedValueOnce(REFUSED_UNTIL_CHANGED);
  await openOperatorForm();
  fireEvent.click(screen.getByTestId("operator-login-button"));
  await screen.findByTestId("breakglass-new-password");
}

function typeNew(newPw: string, confirm: string) {
  fireEvent.change(screen.getByTestId("breakglass-new-password"), { target: { value: newPw } });
  fireEvent.change(screen.getByTestId("breakglass-confirm-password"), {
    target: { value: confirm },
  });
}

function changeCalls(fetchSpy: FetchSpy) {
  return fetchSpy.mock.calls.filter(([url]: Parameters<typeof fetch>) => String(url).endsWith("/auth/superuser-password"));
}

describe("LoginPage break-glass password change (REQ-1264)", () => {
  const onLoginSuccess = vi.fn();
  let fetchSpy: FetchSpy;

  beforeEach(() => {
    onLoginSuccess.mockReset();
    fetchSpy = vi.spyOn(globalThis, "fetch");
  });

  afterEach(() => {
    fetchSpy.mockRestore();
    localStorage.removeItem(SUPERUSER_TOKEN_KEY);
    localStorage.removeItem("provisa_token");
  });

  it("opens the change-password form on 403 auth.password_change_required", async () => {
    render(<LoginPage onLoginSuccess={onLoginSuccess} authDisabled={false} />);
    await reachChangeForm(fetchSpy);

    expect(
      screen.getByRole("heading", { name: "Change the operator password" }),
    ).toBeInTheDocument();
    // The password just refused is the one being replaced.
    expect(screen.getByTestId("breakglass-current-password")).toHaveValue(GENERATED);
    expect(screen.queryByTestId("login-error")).not.toBeInTheDocument();
    expect(onLoginSuccess).not.toHaveBeenCalled();
  });

  it("keeps another 403 on the sign-in form as an error", async () => {
    fetchSpy.mockResolvedValueOnce(
      jsonResponse(403, { detail: "Forbidden", code: "auth.forbidden", params: {} }),
    );
    render(<LoginPage onLoginSuccess={onLoginSuccess} authDisabled={false} />);
    await openOperatorForm();
    fireEvent.click(screen.getByTestId("operator-login-button"));

    expect(await screen.findByTestId("login-error")).toHaveTextContent("Forbidden");
    expect(screen.queryByTestId("breakglass-new-password")).not.toBeInTheDocument();
  });

  it("marks empty required fields on submit and sends nothing", async () => {
    render(<LoginPage onLoginSuccess={onLoginSuccess} authDisabled={false} />);
    await reachChangeForm(fetchSpy);

    fireEvent.click(screen.getByTestId("breakglass-password-submit"));

    await waitFor(() => {
      expect(screen.getAllByText("Required")).toHaveLength(2);
    });
    expect(screen.getByTestId("breakglass-new-password")).toHaveFocus();
    expect(changeCalls(fetchSpy)).toHaveLength(0);
  });

  it("refuses a new password shorter than 12 characters", async () => {
    render(<LoginPage onLoginSuccess={onLoginSuccess} authDisabled={false} />);
    await reachChangeForm(fetchSpy);

    typeNew("short-pw", "short-pw");
    fireEvent.click(screen.getByTestId("breakglass-password-submit"));

    expect(await screen.findByText("Use at least 12 characters")).toBeInTheDocument();
    expect(changeCalls(fetchSpy)).toHaveLength(0);
  });

  it("refuses a new password equal to the current one", async () => {
    render(<LoginPage onLoginSuccess={onLoginSuccess} authDisabled={false} />);
    await reachChangeForm(fetchSpy);

    typeNew(GENERATED, GENERATED);
    fireEvent.click(screen.getByTestId("breakglass-password-submit"));

    expect(
      await screen.findByText("The new password must differ from the current one"),
    ).toBeInTheDocument();
    expect(changeCalls(fetchSpy)).toHaveLength(0);
  });

  it("refuses a confirmation that does not match", async () => {
    render(<LoginPage onLoginSuccess={onLoginSuccess} authDisabled={false} />);
    await reachChangeForm(fetchSpy);

    typeNew(NEW_PW, `${NEW_PW}x`);
    fireEvent.click(screen.getByTestId("breakglass-password-submit"));

    expect(await screen.findByText("The passwords do not match")).toBeInTheDocument();
    expect(changeCalls(fetchSpy)).toHaveLength(0);
  });

  it("changes the password and signs in with the returned session", async () => {
    render(<LoginPage onLoginSuccess={onLoginSuccess} authDisabled={false} />);
    await reachChangeForm(fetchSpy);
    fetchSpy.mockResolvedValueOnce(
      jsonResponse(200, { changed: true, access_token: "rotated-token", token_type: "bearer" }),
    );

    typeNew(NEW_PW, NEW_PW);
    fireEvent.click(screen.getByTestId("breakglass-password-submit"));

    await waitFor(() => expect(onLoginSuccess).toHaveBeenCalledWith("rotated-token"));
    const calls = changeCalls(fetchSpy);
    expect(calls).toHaveLength(1);
    const init = calls[0][1] as RequestInit;
    expect(init.method).toBe("POST");
    expect(JSON.parse(String(init.body))).toEqual({
      username: USER,
      current_password: GENERATED,
      new_password: NEW_PW,
    });
    // REQ-1472: an operator session lives under the operator key.
    expect(localStorage.getItem(SUPERUSER_TOKEN_KEY)).toBe("rotated-token");
  });

  it("shows the server's message when the change is refused", async () => {
    render(<LoginPage onLoginSuccess={onLoginSuccess} authDisabled={false} />);
    await reachChangeForm(fetchSpy);
    fetchSpy.mockResolvedValueOnce(
      jsonResponse(401, {
        detail: "Invalid credentials",
        code: "auth.invalid_credentials",
        params: {},
      }),
    );

    typeNew(NEW_PW, NEW_PW);
    fireEvent.click(screen.getByTestId("breakglass-password-submit"));

    expect(await screen.findByTestId("breakglass-password-error")).toHaveTextContent(
      "Invalid credentials",
    );
    expect(screen.getByTestId("breakglass-new-password")).toBeInTheDocument();
    expect(onLoginSuccess).not.toHaveBeenCalled();
  });

  it("returns to the operator sign-in when the server changed it without a session", async () => {
    render(<LoginPage onLoginSuccess={onLoginSuccess} authDisabled={false} />);
    await reachChangeForm(fetchSpy);
    fetchSpy.mockResolvedValueOnce(jsonResponse(200, { changed: true }));

    typeNew(NEW_PW, NEW_PW);
    fireEvent.click(screen.getByTestId("breakglass-password-submit"));

    expect(await screen.findByTestId("breakglass-password-changed")).toBeInTheDocument();
    expect(screen.getByTestId("operator-password-input")).toHaveValue("");
    expect(onLoginSuccess).not.toHaveBeenCalled();
  });
});
