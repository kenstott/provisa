// Copyright (c) 2026 Kenneth Stott
// Canary: e9c2db72-1e2d-48d8-9a6f-0acb959c67fa
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-1923: a Google Workspace source's setup, as the whole Sources form shows it: what is
// asked, in whose terms, and only when its answer is used.

import { beforeEach, describe, expect, it, vi } from "vitest";
import { fireEvent, render, screen, waitFor } from "../../../test-utils/render";
import en from "../../../i18n/locales/en/googleWorkspaceFields.json";
import { BRAND_CARRIER, SOURCE_TYPES } from "../constants";
import { SourceFormFieldsExtended } from "../SourceFormFieldsExtended";
import { backendType } from "../sourceHelpers";
import type { SourceFormFieldsProps } from "../SourceFormFields";
import {
  gwConnectMissing,
  gwFieldsFromMapping,
  gwMapping,
  gwMissing,
  gwScopes,
} from "../googleWorkspace";

const signIn = vi.hoisted(() => ({
  redirectAddress: vi.fn(),
  signInSource: vi.fn(),
}));
vi.mock("../../../lib/sourceSignIn", async (original) => ({
  ...(await original<typeof import("../../../lib/sourceSignIn")>()),
  redirectAddress: signIn.redirectAddress,
  signInSource: signIn.signInSource,
}));

const REDIRECT = "https://cloud.provisa.test/source-sign-in.html";
const READONLY = "https://www.googleapis.com/auth/gmail.readonly";
const APPROVAL = {
  gw_account: "ada@example.test",
  gw_mail_content: "full",
  gw_sign_in: "google_account",
  gw_client_id: "client-1",
  gw_client_secret: "made-up-client-secret",
};

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

beforeEach(() => {
  signIn.redirectAddress.mockReset().mockResolvedValue(REDIRECT);
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
  it("asks whose mailbox and how to sign in, and for no credential until that is chosen", () => {
    form({});
    expect(shown("gw_account")).toBeInTheDocument();
    expect(shown("gw_sign_in")).toBeInTheDocument();
    expect(shown("gw_mail_content")).toBeInTheDocument();
    for (const key of ["gw_client_id", "gw_client_secret", "gw_service_account_key", "connect"]) {
      expect(shown(key)).toBeNull();
    }
    expect(screen.queryByTestId("brand-token-input")).toBeNull();
    expect(signIn.redirectAddress).not.toHaveBeenCalled();
  });

  it("offers mail, and names calendar and tasks as not yet available", () => {
    form({});
    expect(shown("reads-mail")).toBeChecked();
    for (const later of ["calendar", "tasks"]) {
      expect(shown(`reads-${later}`)).not.toBeChecked();
      expect(shown(`reads-${later}`)).toBeDisabled();
    }
    expect(screen.getAllByText(en.googleWorkspaceFields.readsLater)).toHaveLength(2);
  });

  it("for the owner's approval asks for the client, shows the address to give Google, and no key", async () => {
    form({ gw_sign_in: "google_account" });
    await waitFor(() => expect(shown("redirect-address")).toHaveValue(REDIRECT));
    expect(shown("redirect-address")).toHaveAttribute("readonly");
    expect(shown("gw_client_id")).toBeInTheDocument();
    expect(shown("gw_client_secret")).toHaveAttribute("type", "password");
    expect(shown("gw_service_account_key")).toBeNull();
    // The refresh token is Google's to hand over; the operator is never asked to paste one.
    expect(screen.queryByLabelText(/refresh|token/i)).toBeNull();
  });

  it("for a service account asks for its key and nothing about a client", () => {
    form({ gw_sign_in: "service_account" });
    expect(shown("gw_service_account_key")).toHaveAttribute("type", "password");
    for (const key of ["gw_client_id", "gw_client_secret", "connect", "redirect-address"]) {
      expect(shown(key)).toBeNull();
    }
    expect(signIn.redirectAddress).not.toHaveBeenCalled();
  });

  it("does not offer a search over mail read as headers and labels only", () => {
    form({ gw_mail_content: "headers" });
    expect(shown("gw_mail_search")).toBeNull();
    expect(shown("gw_mail_since")).toBeNull();
    expect(shown("gw_mail_labels")).toBeInTheDocument();
  });

  it("asks which mail to hold in Gmail's own terms", () => {
    form({ gw_mail_content: "full" });
    expect(shown("gw_mail_search")).toBeInTheDocument();
    expect(screen.getByText(/as you would type it in Gmail's search box/)).toBeInTheDocument();
    expect(shown("gw_mail_since")).toHaveAttribute("type", "date");
  });

  it("says why the address could not be stated", async () => {
    signIn.redirectAddress.mockRejectedValue(new Error("unreachable"));
    form({ gw_sign_in: "google_account" });
    expect(await screen.findByTestId("google-workspace-problem")).toHaveTextContent(
      en.googleWorkspaceFields.connectFailed,
    );
  });
});

describe("connecting a Google account", () => {
  it("cannot be asked for until Google can be told what for", () => {
    form({ ...APPROVAL, gw_client_secret: "" });
    expect(shown("connect")).toBeDisabled();
    expect(gwConnectMissing(APPROVAL, "")).toEqual(["source_id"]);
    expect(gwConnectMissing(APPROVAL, "mail")).toEqual([]);
  });

  it("asks Google for the scope of what is read and keeps only references", async () => {
    signIn.signInSource.mockResolvedValue({
      source_id: "mail",
      account: "ada@example.test",
      client_secret: "${secret:source_mail__client_secret}",
      refresh_token: "${secret:source_mail__refresh_token}",
    });
    const setAuthFields = form(APPROVAL);
    fireEvent.click(shown("connect")!);
    await waitFor(() => expect(setAuthFields).toHaveBeenCalled());
    expect(signIn.signInSource).toHaveBeenCalledWith({
      source_id: "mail",
      kind: "google_workspace",
      account: "ada@example.test",
      scopes: [READONLY],
      client_id: "client-1",
      client_secret: "made-up-client-secret",
    });
    expect(setAuthFields).toHaveBeenLastCalledWith({
      ...APPROVAL,
      gw_client_secret: "${secret:source_mail__client_secret}",
      gw_refresh_token: "${secret:source_mail__refresh_token}",
      gw_connected_as: "ada@example.test",
    });
  });

  it("says who approved once connected", () => {
    form({ ...APPROVAL, gw_refresh_token: "${secret:t}", gw_connected_as: "ada@example.test" });
    expect(shown("connected")).toHaveTextContent("Connected as ada@example.test");
  });

  it("asks again when what Google was asked for changes", () => {
    const setAuthFields = form({
      ...APPROVAL,
      gw_refresh_token: "${secret:t}",
      gw_connected_as: "ada@example.test",
    });
    fireEvent.change(shown("gw_client_id")!, { target: { value: "client-2" } });
    expect(setAuthFields).toHaveBeenLastCalledWith(
      expect.objectContaining({
        gw_client_id: "client-2",
        gw_refresh_token: "",
        gw_connected_as: "",
      }),
    );
  });

  it("shows a refusal and connects nothing", async () => {
    signIn.signInSource.mockRejectedValue(new Error("refused"));
    const setAuthFields = form(APPROVAL);
    fireEvent.click(shown("connect")!);
    expect(await screen.findByTestId("google-workspace-problem")).toBeInTheDocument();
    expect(setAuthFields).not.toHaveBeenCalled();
    expect(shown("connected")).toBeNull();
  });
});

describe("what a Google Workspace setup saves", () => {
  const connected = {
    ...APPROVAL,
    gw_client_secret: "${secret:s}",
    gw_refresh_token: "${secret:t}",
  };

  it("is the settings the server reads, by their names", () => {
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
      client_id: "client-1",
      client_secret: "${secret:s}",
      refresh_token: "${secret:t}",
      mail_search: "from:bo@example.test",
      mail_labels: ["Clients", "Invoices"],
      mail_since: "2026-01-01",
      mail_include_spam_trash: true,
    });
  });

  it("carries only the chosen sign-in's credential", () => {
    const mapping = gwMapping({
      ...connected,
      gw_sign_in: "service_account",
      gw_service_account_key: "${secret:k}",
    });
    expect(mapping).toMatchObject({
      sign_in: "service_account",
      service_account_key: "${secret:k}",
    });
    expect(mapping).not.toHaveProperty("client_secret");
    expect(mapping).not.toHaveProperty("refresh_token");
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

  it("is not complete until the account is connected or the key given", () => {
    expect(gwMissing({})).toEqual(["gw_account", "gw_sign_in", "gw_mail_content"]);
    expect(gwMissing(APPROVAL)).toEqual(["gw_refresh_token"]);
    expect(gwMissing(connected)).toEqual([]);
    expect(gwMissing({ ...APPROVAL, gw_sign_in: "service_account" })).toEqual([
      "gw_service_account_key",
    ]);
  });

  it("reads back into the form what was saved", () => {
    const fields = gwFieldsFromMapping(
      JSON.stringify(gwMapping({ ...connected, gw_mail_labels: "Clients" })),
    );
    expect(fields).toMatchObject({
      ...connected,
      gw_mail_labels: "Clients",
      gw_connected_as: "ada@example.test",
    });
    expect(gwMapping(fields)).toEqual(gwMapping({ ...connected, gw_mail_labels: "Clients" }));
  });
});
