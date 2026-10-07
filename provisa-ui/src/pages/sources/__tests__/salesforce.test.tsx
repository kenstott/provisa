// Copyright (c) 2026 Kenneth Stott
// Canary: 2a9c4e61-7d03-4b58-8f1e-5c6d0a3b9e47
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-1946: Salesforce is in the source pick list under Enterprise; its form shows the chosen
// credential set's fields and saves only those.

import { describe, expect, it, vi } from "vitest";
import { render, screen } from "../../../test-utils/render";
import { SalesforceFields } from "../SalesforceFields";
import { SOURCE_TYPES } from "../constants";
import {
  salesforceFieldsFromMapping,
  salesforceLoginUrlValid,
  salesforceMappingJson,
} from "../salesforce";
import tour from "../../../i18n/locales/en/tour.json";

const FORM = { host: "https://acme.my.salesforce.com", username: "key", password: "secret" };

describe("the source pick list", () => {
  it("offers Salesforce under Enterprise", () => {
    expect(SOURCE_TYPES.find((s) => s.value === "salesforce")).toMatchObject({
      label: "Salesforce",
      category: "Enterprise",
    });
  });

  it("is counted by the tour", () => {
    const categories = new Set(SOURCE_TYPES.map((s) => s.category)).size;
    expect(tour.tour.steps.step3.title).toBe(
      `${SOURCE_TYPES.length} source types, ${categories} categories`,
    );
  });
});

describe("salesforceMappingJson", () => {
  it("saves the client-credentials set as its type alone", () => {
    expect(JSON.parse(salesforceMappingJson({ sf_username: "left over" }))).toEqual({
      auth_type: "CLIENT_CREDENTIALS",
    });
  });

  it("saves the username-password set and the API version", () => {
    expect(
      JSON.parse(
        salesforceMappingJson({
          auth_type: "USERNAME_PASSWORD",
          sf_username: "ops@acme.com",
          sf_password: "${secret:sf_pw}",
          security_token: "tok",
          access_token: "left over",
          api_version: " v61.0 ",
        }),
      ),
    ).toEqual({
      auth_type: "USERNAME_PASSWORD",
      sf_username: "ops@acme.com",
      sf_password: "${secret:sf_pw}",
      security_token: "tok",
      api_version: "v61.0",
    });
  });

  it("saves the access-token set", () => {
    expect(
      JSON.parse(
        salesforceMappingJson({
          auth_type: "ACCESS_TOKEN",
          access_token: "00D",
          instance_url: "https://acme.my.salesforce.com",
          sf_password: "left over",
        }),
      ),
    ).toEqual({
      auth_type: "ACCESS_TOKEN",
      access_token: "00D",
      instance_url: "https://acme.my.salesforce.com",
    });
  });

  it("reads back what it saved", () => {
    const fields = { auth_type: "ACCESS_TOKEN", access_token: "00D", instance_url: "https://x" };
    expect(salesforceFieldsFromMapping(salesforceMappingJson(fields))).toEqual(fields);
  });
});

describe("salesforceLoginUrlValid", () => {
  it("takes the full https My Domain URL or a reference to it", () => {
    expect(salesforceLoginUrlValid("https://acme.my.salesforce.com")).toBe(true);
    expect(salesforceLoginUrlValid("${secret:sf_login_url}")).toBe(true);
    expect(salesforceLoginUrlValid("${env:SF_LOGIN_URL}")).toBe(true);
  });

  it("refuses a bare host and any other scheme", () => {
    expect(salesforceLoginUrlValid("acme.my.salesforce.com")).toBe(false);
    expect(salesforceLoginUrlValid("http://acme.my.salesforce.com")).toBe(false);
    expect(salesforceLoginUrlValid("https://")).toBe(false);
  });
});

describe("SalesforceFields", () => {
  const show = (fields: Record<string, string>, form = FORM) =>
    render(<SalesforceFields form={form} setForm={vi.fn()} fields={fields} setFields={vi.fn()} />);

  it("says a login URL without https:// is not the full My Domain URL", () => {
    show({}, { ...FORM, host: "acme.my.salesforce.com" });
    expect(
      screen.getByText("Enter the full My Domain URL, starting with https://"),
    ).toBeInTheDocument();
  });

  it("says nothing about a full login URL or an empty one", () => {
    show({});
    expect(screen.queryByText(/starting with https/)).toBeNull();
  });

  it("asks for the login URL, the connected app and an API version by default", () => {
    show({});
    expect(screen.getByTestId("salesforce-login-url-input")).toHaveValue(FORM.host);
    expect(screen.getByTestId("salesforce-consumer-key-input")).toHaveValue("key");
    expect(screen.getByTestId("salesforce-consumer-secret-input")).toBeInTheDocument();
    expect(screen.getByTestId("salesforce-api-version-input")).toBeInTheDocument();
    expect(screen.queryByTestId("salesforce-username-input")).toBeNull();
    expect(screen.queryByTestId("salesforce-access-token-input")).toBeNull();
  });

  it("adds the user's credentials for username-password", () => {
    show({ auth_type: "USERNAME_PASSWORD" });
    expect(screen.getByTestId("salesforce-consumer-key-input")).toBeInTheDocument();
    expect(screen.getByTestId("salesforce-username-input")).toBeInTheDocument();
    expect(screen.getByTestId("salesforce-password-input")).toBeInTheDocument();
    expect(screen.getByTestId("salesforce-security-token-input")).toBeInTheDocument();
  });

  it("asks for the token and its instance URL, not the connected app, for an access token", () => {
    show({ auth_type: "ACCESS_TOKEN" });
    expect(screen.getByTestId("salesforce-access-token-input")).toBeInTheDocument();
    expect(screen.getByTestId("salesforce-instance-url-input")).toBeInTheDocument();
    expect(screen.queryByTestId("salesforce-consumer-key-input")).toBeNull();
  });
});
