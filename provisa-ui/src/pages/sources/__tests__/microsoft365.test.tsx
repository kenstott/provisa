// Copyright (c) 2026 Kenneth Stott
// Canary: 1783089d-6da4-47e1-954c-49dc4a58b987
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-1923: a Microsoft 365 source's setup, as the whole Sources form shows it. The person
// adding a source is asked for the mailbox, is told what is read, and is given one button;
// nothing about how Provisa signs in to Microsoft is in front of them.

import { beforeEach, describe, expect, it, vi } from "vitest";
import { fireEvent, render, screen, waitFor } from "../../../test-utils/render";
import en from "../../../i18n/locales/en/microsoft365Fields.json";
import { BRAND_CARRIER, SOURCE_TYPES } from "../constants";
import { SourceFormFieldsExtended } from "../SourceFormFieldsExtended";
import type { SourceFormFieldsProps } from "../SourceFormFields";
import { backendType } from "../sourceHelpers";
import * as m365 from "../microsoft365";
import {
  M365_SCOPES,
  m365ConnectMissing,
  m365FieldsFromMapping,
  m365Mailboxes,
  m365Mapping,
} from "../microsoft365";

const signIn = vi.hoisted(() => ({
  signInStatus: vi.fn(),
  signInSource: vi.fn(),
}));
vi.mock("../../../lib/sourceSignIn", async (original) => ({
  ...(await original<typeof import("../../../lib/sourceSignIn")>()),
  signInStatus: signIn.signInStatus,
  signInSource: signIn.signInSource,
}));

const FILLED = { m365_account: "megan@contoso.test" };

function form(authFields: Record<string, string>, id = "mail") {
  const setAuthFields = vi.fn();
  const props = {
    // The Sources page's empty form (SourcesPage.tsx), as a Microsoft 365 source.
    form: {
      id,
      type: "microsoft_365",
      host: "",
      port: 0,
      database: "",
      username: "",
      password: "",
      gqlNamingConvention: "",
      cacheTtl: "",
      cacheEnabled: true,
      replicate: null,
      region: null,
      loadProtected: false,
      offPeakWindow: "",
      offPeakTz: "UTC",
      changeSignal: "ttl",
      sentinelPath: "",
      freshnessGate: false,
      maxLiveConcurrency: "",
      path: "",
      description: "",
    },
    setForm: vi.fn(),
    authType: "none",
    setAuthType: vi.fn(),
    authFields,
    setAuthFields,
    domains: [],
  } as unknown as SourceFormFieldsProps;
  render(<SourceFormFieldsExtended {...props} />);
  return setAuthFields;
}

const shown = (key: string) => screen.queryByTestId(`microsoft-365-${key}`);

beforeEach(() => {
  signIn.signInStatus.mockReset().mockResolvedValue({ configured: true, may_configure: false });
  signIn.signInSource.mockReset();
});

describe("the source pick list", () => {
  it("offers Microsoft 365 as a source type of its own", () => {
    expect(SOURCE_TYPES.find((s) => s.value === "microsoft_365")).toMatchObject({
      label: "Microsoft 365 (Outlook, Exchange)",
    });
    expect(backendType("microsoft_365")).toBe("microsoft_365");
    expect(BRAND_CARRIER.microsoft_365).toBeUndefined();
  });
});

describe("what the form asks", () => {
  it("asks for the mailbox, says what is read, and offers one button", async () => {
    form({});
    expect(shown("m365_account")).toBeInTheDocument();
    expect(shown("reads-mail")).toBeChecked();
    await waitFor(() => expect(shown("connect")).toBeInTheDocument());
    expect(shown("connect")).toBeDisabled(); // no mailbox named yet
    // Nothing about how Provisa signs in to Microsoft is asked of the person adding a source.
    for (const word of ["redirect", "client", "tenant", "secret"]) {
      expect(document.body.textContent?.toLowerCase()).not.toContain(word);
    }
  });

  it("asks Microsoft for reading mail and nothing that changes it", () => {
    expect(M365_SCOPES).toEqual([
      "https://graph.microsoft.com/Mail.Read",
      "offline_access",
      "https://graph.microsoft.com/User.Read",
    ]);
    expect(m365ConnectMissing({}, "mail")).toEqual(["m365_account"]);
    expect(m365ConnectMissing(FILLED, " ")).toEqual(["source_id"]);
    expect(m365ConnectMissing(FILLED, "mail")).toEqual([]);
  });
});

describe("connecting the mailbox", () => {
  it("asks for the owner's approval and keeps what it gave", async () => {
    signIn.signInSource.mockResolvedValue({
      source_id: "mail",
      account: "megan@contoso.test",
      refresh_token: "${secret:source_mail_refresh_token}",
    });
    const setAuthFields = form(FILLED);
    await waitFor(() => expect(shown("connect")).toBeEnabled());
    fireEvent.click(shown("connect")!);
    await waitFor(() => expect(setAuthFields).toHaveBeenCalled());
    expect(signIn.signInSource).toHaveBeenCalledWith({
      source_id: "mail",
      kind: "microsoft_365",
      account: "megan@contoso.test",
      scopes: M365_SCOPES,
    });
    expect(setAuthFields).toHaveBeenCalledWith({
      ...FILLED,
      m365_refresh_token: "${secret:source_mail_refresh_token}",
      m365_connected_as: "megan@contoso.test",
    });
  });

  it("says who it is connected as", async () => {
    form({ ...FILLED, m365_refresh_token: "${secret:t}", m365_connected_as: "megan@contoso.test" });
    expect(await screen.findByTestId("microsoft-365-connected")).toHaveTextContent(
      "Connected as megan@contoso.test",
    );
  });

  it("asks again when another mailbox is named", () => {
    const setAuthFields = form({
      ...FILLED,
      m365_refresh_token: "${secret:t}",
      m365_connected_as: "megan@contoso.test",
    });
    fireEvent.change(shown("m365_account")!, { target: { value: "other@contoso.test" } });
    expect(setAuthFields).toHaveBeenCalledWith({
      m365_account: "other@contoso.test",
      m365_refresh_token: "",
      m365_connected_as: "",
    });
  });

  it("says why when the sign-in is refused", async () => {
    signIn.signInSource.mockRejectedValue(new Error("closed"));
    form(FILLED);
    await waitFor(() => expect(shown("connect")).toBeEnabled());
    fireEvent.click(shown("connect")!);
    expect(await screen.findByTestId("microsoft-365-problem")).toHaveTextContent(
      en.microsoft365Fields.connectFailed,
    );
  });
});

describe("an organisation that has not connected Microsoft 365", () => {
  it("tells an administrator in one line and links to where it is set up", async () => {
    signIn.signInStatus.mockResolvedValue({ configured: false, may_configure: true });
    form(FILLED);
    const line = await screen.findByTestId("microsoft-365-not-set-up");
    expect(line).toHaveTextContent(en.microsoft365Fields.notSetUp);
    expect(shown("set-up")).toHaveAttribute("href", "/admin/email");
    expect(shown("set-up")).toHaveTextContent("Set it up under Admin › Email");
    expect(shown("connect")).toBeNull();
  });

  it("tells anyone else to ask an administrator, with no link they cannot use", async () => {
    signIn.signInStatus.mockResolvedValue({ configured: false, may_configure: false });
    form(FILLED);
    const line = await screen.findByTestId("microsoft-365-not-set-up");
    expect(line).toHaveTextContent("Ask an administrator to set it up under Admin › Email.");
    expect(shown("set-up")).toBeNull();
    expect(shown("connect")).toBeNull();
  });
});

describe("the source's mapping", () => {
  it("keeps the mailbox, what is read and the approval, and no client", () => {
    const mapping = m365Mapping({ ...FILLED, m365_refresh_token: "${secret:t}" });
    expect(mapping).toEqual({
      accounts: ["megan@contoso.test"],
      resources: ["mail"],
      refresh_token: "${secret:t}",
    });
    expect(m365FieldsFromMapping(JSON.stringify(mapping))).toEqual({
      m365_whose: "one",
      m365_account: "megan@contoso.test",
      m365_refresh_token: "${secret:t}",
      m365_connected_as: "megan@contoso.test",
    });
  });
});

const ORG = { m365_whose: "organisation" };
const allowed = { configured: true, may_configure: false, organisation_mailboxes: true };

describe("a source of the organisation's mailboxes", () => {
  it("asks which mailboxes and nothing about a person's sign-in", async () => {
    signIn.signInStatus.mockResolvedValue(allowed);
    form({ ...ORG, m365_choice: "group" });
    expect(shown("m365_choice")).toBeInTheDocument();
    expect(shown("m365_group")).toBeInTheDocument();
    expect(shown("m365_account")).toBeNull();
    expect(shown("connect")).toBeNull();
    expect(shown("every-mailbox")).toHaveTextContent(en.microsoft365Fields.everyMailbox);
    await waitFor(() => expect(shown("check")).toBeInTheDocument());
    expect(shown("check")).toBeDisabled(); // no group named yet
  });

  it("shows the list of addresses only when a list is chosen", () => {
    signIn.signInStatus.mockResolvedValue(allowed);
    form({ ...ORG, m365_choice: "list" });
    expect(shown("m365_list")).toBeInTheDocument();
    expect(shown("m365_group")).toBeNull();
  });

  it("says how many mailboxes a choice names", async () => {
    signIn.signInStatus.mockResolvedValue(allowed);
    const check = vi.spyOn(m365, "m365CheckMailboxes").mockResolvedValue(12);
    form({ ...ORG, m365_choice: "everyone" });
    await waitFor(() => expect(shown("check")).toBeEnabled());
    fireEvent.click(shown("check")!);
    expect(await screen.findByTestId("microsoft-365-found")).toHaveTextContent(
      "Mailboxes found: 12",
    );
    expect(check).toHaveBeenCalledWith({ everyone: true });
    check.mockRestore();
  });

  it("says why when the count is refused", async () => {
    signIn.signInStatus.mockResolvedValue(allowed);
    const check = vi.spyOn(m365, "m365CheckMailboxes").mockRejectedValue(new Error("down"));
    form({ ...ORG, m365_choice: "everyone" });
    await waitFor(() => expect(shown("check")).toBeEnabled());
    fireEvent.click(shown("check")!);
    expect(await screen.findByTestId("microsoft-365-problem")).toBeInTheDocument();
    check.mockRestore();
  });

  it("tells an administrator in one line when the organisation has not allowed it", async () => {
    signIn.signInStatus.mockResolvedValue({ ...allowed, may_configure: true, organisation_mailboxes: false });
    form({ ...ORG, m365_choice: "everyone" });
    const line = await screen.findByTestId("microsoft-365-not-allowed");
    expect(line).toHaveTextContent(en.microsoft365Fields.notAllowed);
    expect(shown("allow")).toHaveAttribute("href", "/admin/email");
    expect(shown("check")).toBeNull();
  });

  it("tells anyone else to ask an administrator", async () => {
    signIn.signInStatus.mockResolvedValue({ ...allowed, organisation_mailboxes: false });
    form({ ...ORG, m365_choice: "everyone" });
    const line = await screen.findByTestId("microsoft-365-not-allowed");
    expect(line).toHaveTextContent("Ask an administrator to allow it under Admin › Email.");
    expect(shown("allow")).toBeNull();
    expect(shown("check")).toBeNull();
  });

  it("keeps only which mailboxes in the source's mapping", () => {
    expect(m365Mailboxes({ ...ORG, m365_choice: "group", m365_group: " " })).toBeNull();
    const cases: [Record<string, string>, Record<string, unknown>][] = [
      [{ m365_choice: "everyone" }, { everyone: true }],
      [{ m365_choice: "group", m365_group: " sales@contoso.test " }, { group: "sales@contoso.test" }],
      [{ m365_choice: "list", m365_list: "a@contoso.test\n\n b@contoso.test " }, { list: ["a@contoso.test", "b@contoso.test"] }],
    ];
    for (const [fields, mailboxes] of cases) {
      const mapping = m365Mapping({ ...ORG, ...fields, m365_refresh_token: "left over" });
      expect(mapping).toEqual({ resources: ["mail"], mailboxes });
      const back = m365FieldsFromMapping(JSON.stringify(mapping));
      expect(back.m365_whose).toBe("organisation");
      expect(m365Mapping(back)).toEqual(mapping);
    }
  });
});
