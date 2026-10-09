// Copyright (c) 2026 Kenneth Stott
// Canary: e9c2db72-1e2d-48d8-9a6f-0acb959c67fa
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-1923: a Google Workspace source's setup, as the whole Sources form shows it. The person
// adding a source is asked for the mailbox, how much to read and which mail, and is given one
// button; nothing about how Provisa signs in to Google is in front of them.

import { beforeEach, describe, expect, it, vi } from "vitest";
import { fireEvent, render, screen, waitFor } from "../../../test-utils/render";
import en from "../../../i18n/locales/en/googleWorkspaceFields.json";
import { BRAND_CARRIER, SOURCE_TYPES } from "../constants";
import { SourceFormFieldsExtended } from "../SourceFormFieldsExtended";
import type { SourceFormFieldsProps } from "../SourceFormFields";
import { backendType } from "../sourceHelpers";
import {
  gwConnectMissing,
  gwFieldsFromMapping,
  gwMapping,
  gwMissing,
  gwScopes,
} from "../googleWorkspace";

const signIn = vi.hoisted(() => ({
  signInStatus: vi.fn(),
  signInSource: vi.fn(),
}));
vi.mock("../../../lib/sourceSignIn", async (original) => ({
  ...(await original<typeof import("../../../lib/sourceSignIn")>()),
  signInStatus: signIn.signInStatus,
  signInSource: signIn.signInSource,
}));

const READONLY = "https://www.googleapis.com/auth/gmail.readonly";
const FILLED = { gw_account: "ada@example.test", gw_mail_content: "full" };

function form(authFields: Record<string, string>, id = "mail") {
  const setAuthFields = vi.fn();
  const props = {
    // The Sources page's empty form (SourcesPage.tsx), as a Google Workspace source.
    form: {
      id,
      type: "google_workspace",
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

const shown = (key: string) => screen.queryByTestId(`google-workspace-${key}`);
const connectReady = () => waitFor(() => expect(shown("connect")).toBeEnabled());

beforeEach(() => {
  signIn.signInStatus.mockReset().mockResolvedValue({ configured: true, may_configure: false });
  signIn.signInSource.mockReset();
});

describe("the source pick list", () => {
  it("offers Google Workspace as a source type of its own", () => {
    expect(SOURCE_TYPES.find((s) => s.value === "google_workspace")).toMatchObject({
      label: "Google Workspace (Gmail)",
    });
    expect(backendType("google_workspace")).toBe("google_workspace");
    expect(BRAND_CARRIER.google_workspace).toBeUndefined();
  });
});

describe("what a Google Workspace setup asks", () => {
  it("asks for the mailbox, how much to read and which mail, and offers one button", async () => {
    form(FILLED);
    await connectReady();
    for (const key of ["gw_account", "gw_mail_content", "gw_mail_search", "gw_mail_labels"]) {
      expect(shown(key)).toBeInTheDocument();
    }
    expect(signIn.signInStatus).toHaveBeenCalledWith("google_workspace");
  });

  it("puts nothing about how Provisa signs in to Google in front of the person", async () => {
    form(FILLED);
    await connectReady();
    // No client, no secret, no key, no token, no address to copy or type.
    expect(
      screen.queryByLabelText(/client|secret|key|token|redirect|address to|sign[- ]?in/i),
    ).toBeNull();
    expect(screen.queryByText(/redirect|client id|client secret|service account/i)).toBeNull();
    expect(document.querySelectorAll('input[type="password"]')).toHaveLength(0);
    expect(screen.queryByTestId("brand-token-input")).toBeNull();
  });

  it("shows nothing it cannot read: no calendar, no tasks, nothing marked as coming", async () => {
    form(FILLED);
    await connectReady();
    expect(screen.queryByText(/calendar|tasks|not yet|coming|not supported/i)).toBeNull();
    expect(screen.queryByRole("checkbox", { name: /calendar|tasks/i })).toBeNull();
  });

  it("does not offer a search over mail read as headers and labels only", () => {
    form({ ...FILLED, gw_mail_content: "headers" });
    expect(shown("gw_mail_search")).toBeNull();
    expect(shown("gw_mail_since")).toBeNull();
    expect(shown("gw_mail_labels")).toBeInTheDocument();
  });

  it("asks which mail to hold in Gmail's own terms", () => {
    form(FILLED);
    expect(screen.getByText(/as you would type it in Gmail's search box/)).toBeInTheDocument();
    expect(shown("gw_mail_since")).toHaveAttribute("type", "date");
  });
});

describe("an organisation that has not connected Google Workspace", () => {
  it("tells an administrator in one line and links to where it is set up", async () => {
    signIn.signInStatus.mockResolvedValue({ configured: false, may_configure: true });
    form(FILLED);
    const line = await screen.findByTestId("google-workspace-not-set-up");
    expect(line).toHaveTextContent(en.googleWorkspaceFields.notSetUp);
    expect(shown("set-up")).toHaveAttribute("href", "/admin/email");
    expect(shown("set-up")).toHaveTextContent("Set it up under Admin › Email");
    expect(shown("connect")).toBeNull();
  });

  it("tells anyone else to ask an administrator, with no link they cannot use", async () => {
    signIn.signInStatus.mockResolvedValue({ configured: false, may_configure: false });
    form(FILLED);
    const line = await screen.findByTestId("google-workspace-not-set-up");
    expect(line).toHaveTextContent("Ask an administrator to set it up under Admin › Email.");
    expect(shown("set-up")).toBeNull();
    expect(shown("connect")).toBeNull();
  });

  it("says why when it cannot tell", async () => {
    signIn.signInStatus.mockRejectedValue(new Error("unreachable"));
    form(FILLED);
    expect(await screen.findByTestId("google-workspace-problem")).toHaveTextContent(
      en.googleWorkspaceFields.connectFailed,
    );
    expect(shown("connect")).toBeDisabled();
  });
});

describe("connecting a Google account", () => {
  it("cannot be asked for until Google can be told whose mailbox and what for", async () => {
    form({ gw_account: "ada@example.test" });
    await waitFor(() => expect(signIn.signInStatus).toHaveBeenCalled());
    expect(shown("connect")).toBeDisabled();
    expect(gwConnectMissing(FILLED, "")).toEqual(["source_id"]);
    expect(gwConnectMissing(FILLED, "mail")).toEqual([]);
  });

  it("sends Google's scope for what is read, and no client, and keeps only a reference", async () => {
    signIn.signInSource.mockResolvedValue({
      source_id: "mail",
      account: "ada@example.test",
      refresh_token: "${secret:source_mail__refresh_token}",
    });
    const setAuthFields = form(FILLED);
    await connectReady();
    fireEvent.click(shown("connect")!);
    await waitFor(() => expect(setAuthFields).toHaveBeenCalled());
    expect(signIn.signInSource).toHaveBeenCalledWith({
      source_id: "mail",
      kind: "google_workspace",
      account: "ada@example.test",
      scopes: [READONLY],
    });
    expect(setAuthFields).toHaveBeenLastCalledWith({
      ...FILLED,
      gw_refresh_token: "${secret:source_mail__refresh_token}",
      gw_connected_as: "ada@example.test",
    });
  });

  it("says who approved once connected", async () => {
    form({ ...FILLED, gw_refresh_token: "${secret:t}", gw_connected_as: "ada@example.test" });
    await connectReady();
    expect(shown("connected")).toHaveTextContent("Connected as ada@example.test");
  });

  it("asks again when what Google was asked for changes", () => {
    const setAuthFields = form({
      ...FILLED,
      gw_refresh_token: "${secret:t}",
      gw_connected_as: "ada@example.test",
    });
    fireEvent.change(shown("gw_account")!, { target: { value: "bo@example.test" } });
    expect(setAuthFields).toHaveBeenLastCalledWith(
      expect.objectContaining({
        gw_account: "bo@example.test",
        gw_refresh_token: "",
        gw_connected_as: "",
      }),
    );
  });

  it("shows a refusal and connects nothing", async () => {
    signIn.signInSource.mockRejectedValue(new Error("refused"));
    const setAuthFields = form(FILLED);
    await connectReady();
    fireEvent.click(shown("connect")!);
    expect(await screen.findByTestId("google-workspace-problem")).toBeInTheDocument();
    expect(setAuthFields).not.toHaveBeenCalled();
    expect(shown("connected")).toBeNull();
  });
});

describe("what a Google Workspace setup saves", () => {
  const connected = { ...FILLED, gw_refresh_token: "${secret:t}" };

  it("is the settings the server reads, by their names, and no client", () => {
    expect(
      gwMapping({
        ...connected,
        gw_mail_search: " from:bo@example.test ",
        gw_mail_labels: "Clients\n\n Invoices ",
        gw_mail_since: "2026-01-01",
        gw_mail_include_spam_trash: "true",
      }),
    ).toEqual({
      accounts: ["ada@example.test"],
      resources: ["mail"],
      sign_in: "google_account",
      mail_content: "full",
      refresh_token: "${secret:t}",
      mail_search: "from:bo@example.test",
      mail_labels: ["Clients", "Invoices"],
      mail_since: "2026-01-01",
      mail_include_spam_trash: true,
    });
  });

  it("leaves out a search Google would refuse for headers-only mail", () => {
    const mapping = gwMapping({
      ...connected,
      gw_mail_content: "headers",
      gw_mail_search: "is:unread",
      gw_mail_since: "2026-01-01",
    });
    expect(mapping).not.toHaveProperty("mail_search");
    expect(mapping).not.toHaveProperty("mail_since");
    expect(gwScopes({ gw_mail_content: "headers" })).toEqual([
      "https://www.googleapis.com/auth/gmail.metadata",
    ]);
  });

  it("is not complete until the account is connected", () => {
    expect(gwMissing({})).toEqual(["gw_account", "gw_mail_content", "gw_refresh_token"]);
    expect(gwMissing(FILLED)).toEqual(["gw_refresh_token"]);
    expect(gwMissing(connected)).toEqual([]);
  });

  it("reads back into the form what was saved", () => {
    const saved = gwMapping({ ...connected, gw_mail_labels: "Clients" });
    const fields = gwFieldsFromMapping(JSON.stringify(saved));
    expect(fields).toMatchObject({
      ...connected,
      gw_mail_labels: "Clients",
      gw_connected_as: "ada@example.test",
    });
    expect(gwMapping(fields)).toEqual(saved);
  });

  it("keeps a source set up by hand with a service account as it is, and offers no button", async () => {
    const byHand = {
      accounts: ["ada@example.test"],
      resources: ["mail"],
      sign_in: "service_account",
      service_account_key: "${secret:k}",
      mail_content: "full",
    };
    const fields = gwFieldsFromMapping(JSON.stringify(byHand));
    expect(gwMapping(fields)).toEqual(byHand);
    expect(gwMissing(fields)).toEqual([]);
    form(fields);
    expect(shown("connect")).toBeNull();
    expect(signIn.signInStatus).not.toHaveBeenCalled();
  });
});
