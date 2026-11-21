// Copyright (c) 2026 Kenneth Stott
// Canary: 2b7f4e90-c31a-4d86-a5e2-9f08d6c3b174
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-2197: QuickBooks Online is in the source pick list under Enterprise; its form asks for the
// company's realm id, sandbox or production, the Intuit app's client id and secret, and the
// refresh token, which is kept in the mapping beside the environment.

import { beforeEach, describe, expect, it, vi } from "vitest";
import { fireEvent, renderInForm, screen, waitFor } from "../../../test-utils/render";
import { SignInRefused } from "../../../lib/sourceSignIn";
import { QuickBooksFields } from "../QuickBooksFields";
import { SOURCE_TYPES } from "../constants";
import {
  quickbooksFieldsFromMapping,
  quickbooksMappingJson,
  quickbooksRealmIdValid,
} from "../quickbooks";
import { MARKS } from "../../../components/sourceLogoMarks";

const signIn = vi.hoisted(() => ({ signInSource: vi.fn() }));
vi.mock("../../../lib/sourceSignIn", async (original) => ({
  ...(await original<typeof import("../../../lib/sourceSignIn")>()),
  signInSource: signIn.signInSource,
}));

const FORM = {
  id: "books",
  database: "9341453456789012",
  username: "ABcd123",
  password: "${secret:qb_client_secret}",
};

describe("the source pick list", () => {
  it("offers QuickBooks Online under Enterprise, with its mark", () => {
    expect(SOURCE_TYPES.find((s) => s.value === "quickbooks")).toMatchObject({
      label: "QuickBooks Online",
      category: "Enterprise",
    });
    expect(MARKS.quickbooks).toBeDefined();
  });
});

describe("quickbooksRealmIdValid", () => {
  it("takes the company's number or a reference to it", () => {
    expect(quickbooksRealmIdValid("9341453456789012")).toBe(true);
    expect(quickbooksRealmIdValid("${env:QUICKBOOKS_REALM_ID}")).toBe(true);
  });

  it("refuses anything else", () => {
    expect(quickbooksRealmIdValid("acme")).toBe(false);
    expect(quickbooksRealmIdValid("")).toBe(false);
  });
});

describe("the mapping", () => {
  it("holds the environment, sandbox unless production is chosen, and the refresh token", () => {
    expect(JSON.parse(quickbooksMappingJson({}))).toEqual({ environment: "sandbox" });
    expect(
      JSON.parse(
        quickbooksMappingJson({ environment: "production", refresh_token: " ${secret:qb} " }),
      ),
    ).toEqual({ environment: "production", refresh_token: "${secret:qb}" });
  });

  it("is read back into the form's fields", () => {
    expect(
      quickbooksFieldsFromMapping('{"environment":"production","refresh_token":"${secret:qb}"}'),
    ).toEqual({ environment: "production", refresh_token: "${secret:qb}" });
  });
});

describe("QuickBooksFields", () => {
  it("asks for the realm id, client id and secret in database, username, password", () => {
    renderInForm(
      <QuickBooksFields form={FORM} setForm={vi.fn()} fields={{}} setFields={vi.fn()} />,
    );
    expect(screen.getByTestId("quickbooks-realm-id-input")).toHaveValue(FORM.database);
    expect(screen.getByTestId("quickbooks-client-id-input")).toHaveValue(FORM.username);
    expect(screen.getByTestId("quickbooks-client-secret-input")).toHaveValue(FORM.password);
    expect(screen.getByTestId("quickbooks-environment-select")).toHaveValue(
      "Sandbox (test company)",
    );
  });

  it("keeps the refresh token in the source's fields", () => {
    const setFields = vi.fn();
    renderInForm(
      <QuickBooksFields form={FORM} setForm={vi.fn()} fields={{}} setFields={setFields} />,
    );
    fireEvent.change(screen.getByTestId("quickbooks-refresh-token-input"), {
      target: { value: "${secret:qb_refresh}" },
    });
    expect(setFields).toHaveBeenLastCalledWith({ refresh_token: "${secret:qb_refresh}" });
  });

  it("says a realm id that is not a number is not one", () => {
    renderInForm(
      <QuickBooksFields
        form={{ ...FORM, database: "acme" }}
        setForm={vi.fn()}
        fields={{}}
        setFields={vi.fn()}
      />,
    );
    expect(
      screen.getByText("The company ID is a number, for example 9341453456789012"),
    ).toBeInTheDocument();
  });
});

describe("Sign in with QuickBooks", () => {
  beforeEach(() => {
    signIn.signInSource.mockReset();
  });

  it("sits beside the refresh token and waits for the source's name and its app", () => {
    for (const missing of ["id", "username", "password"] as const) {
      const { unmount } = renderInForm(
        <QuickBooksFields
          form={{ ...FORM, [missing]: " " }}
          setForm={vi.fn()}
          fields={{}}
          setFields={vi.fn()}
        />,
      );
      expect(screen.getByTestId("quickbooks-sign-in")).toBeDisabled();
      unmount();
    }
    renderInForm(
      <QuickBooksFields form={FORM} setForm={vi.fn()} fields={{}} setFields={vi.fn()} />,
    );
    expect(screen.getByTestId("quickbooks-sign-in")).toBeEnabled();
    expect(screen.getByTestId("quickbooks-sign-in")).toHaveTextContent("Sign in with QuickBooks");
    // Either signs the source in: the token entered on the form is no longer required.
    expect(screen.getByTestId("quickbooks-refresh-token-input")).not.toBeRequired();
  });

  it("signs in with the source's own app and sets the company Intuit names", async () => {
    signIn.signInSource.mockResolvedValue({
      source_id: "books",
      account: "4620816365",
      connected: true,
    });
    const setForm = vi.fn();
    renderInForm(
      <QuickBooksFields
        form={FORM}
        setForm={setForm}
        fields={{ environment: "production", refresh_token: "rt-on-the-form" }}
        setFields={vi.fn()}
      />,
    );
    fireEvent.click(screen.getByTestId("quickbooks-sign-in"));
    await waitFor(() => expect(setForm).toHaveBeenCalled());
    expect(signIn.signInSource).toHaveBeenCalledWith({
      source_id: "books",
      kind: "quickbooks",
      account: FORM.database,
      // The settings the server asks Intuit's scope for: never the token on the form.
      mapping: { environment: "production" },
      client: { client_id: FORM.username, client_secret: FORM.password },
    });
    expect(setForm).toHaveBeenCalledWith({ ...FORM, database: "4620816365" });
    expect(await screen.findByTestId("quickbooks-signed-in")).toHaveTextContent(
      "Signed in to company 4620816365. Save the source to keep it.",
    );
  });

  it("says why a sign-in was refused, in the reader's language", async () => {
    signIn.signInSource.mockImplementation(async () => {
      throw new SignInRefused({ code: "source_sign_in.client_missing" });
    });
    renderInForm(
      <QuickBooksFields form={FORM} setForm={vi.fn()} fields={{}} setFields={vi.fn()} />,
    );
    fireEvent.click(screen.getByTestId("quickbooks-sign-in"));
    expect(await screen.findByTestId("quickbooks-sign-in-problem")).toHaveTextContent(
      "Enter the app's client ID and client secret, then sign in.",
    );
  });
});
