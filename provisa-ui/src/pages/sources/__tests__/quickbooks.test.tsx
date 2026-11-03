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

import { describe, expect, it, vi } from "vitest";
import { fireEvent, renderInForm, screen } from "../../../test-utils/render";
import { QuickBooksFields } from "../QuickBooksFields";
import { SOURCE_TYPES } from "../constants";
import {
  quickbooksFieldsFromMapping,
  quickbooksMappingJson,
  quickbooksRealmIdValid,
} from "../quickbooks";
import { MARKS } from "../../../components/sourceLogoMarks";

const FORM = { database: "9341453456789012", username: "ABcd123", password: "s3cret" };

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
    expect(screen.getByTestId("quickbooks-client-secret-input")).toHaveValue("s3cret");
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
