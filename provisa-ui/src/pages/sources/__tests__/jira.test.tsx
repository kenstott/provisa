// Copyright (c) 2026 Kenneth Stott
// Canary: 6260e9da-44a2-4a58-b8e7-9584f175bf23
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-2069: Jira is in the source pick list under Enterprise; its form asks for the site's
// address, the Atlassian account's email and its API token.

import { describe, expect, it, vi } from "vitest";
import { fireEvent, renderInForm, screen } from "../../../test-utils/render";
import { JiraFields } from "../JiraFields";
import { SOURCE_TYPES } from "../constants";
import { jiraSiteUrlValid } from "../jira";

const FORM = { host: "https://acme.atlassian.net", username: "ada@acme.example", password: "t" };

describe("the source pick list", () => {
  it("offers Jira under Enterprise", () => {
    expect(SOURCE_TYPES.find((s) => s.value === "jira")).toMatchObject({
      label: "Jira",
      category: "Enterprise",
    });
  });
});

describe("jiraSiteUrlValid", () => {
  it("takes the full https address or a reference to it", () => {
    expect(jiraSiteUrlValid("https://acme.atlassian.net")).toBe(true);
    expect(jiraSiteUrlValid("${env:JIRA_BASE_URL}")).toBe(true);
  });

  it("refuses a bare host and any other scheme", () => {
    expect(jiraSiteUrlValid("acme.atlassian.net")).toBe(false);
    expect(jiraSiteUrlValid("http://acme.atlassian.net")).toBe(false);
  });
});

describe("JiraFields", () => {
  it("asks for the site, the account's email and its API token, in host, username, password", () => {
    renderInForm(<JiraFields form={FORM} setForm={vi.fn()} />);
    expect(screen.getByTestId("jira-site-input")).toHaveValue(FORM.host);
    expect(screen.getByTestId("jira-email-input")).toHaveValue(FORM.username);
    expect(screen.getByTestId("jira-token-input")).toHaveValue("t");
  });

  it("keeps the email in the source's username", () => {
    const setForm = vi.fn();
    renderInForm(<JiraFields form={FORM} setForm={setForm} />);
    fireEvent.change(screen.getByTestId("jira-email-input"), {
      target: { value: "bo@acme.example" },
    });
    expect(setForm).toHaveBeenLastCalledWith({ ...FORM, username: "bo@acme.example" });
  });

  it("says a site address without https:// is not the full address", () => {
    renderInForm(<JiraFields form={{ ...FORM, host: "acme.atlassian.net" }} setForm={vi.fn()} />);
    expect(
      screen.getByText("Enter the full site address, starting with https://"),
    ).toBeInTheDocument();
  });
});
