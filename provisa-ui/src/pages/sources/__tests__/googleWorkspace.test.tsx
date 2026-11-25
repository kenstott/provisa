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

import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { fireEvent, renderInForm, screen, waitFor } from "../../../test-utils/render";

// SourceFormFieldsExtended calls useAuth() unconditionally; stub it so the form
// renders without an AuthProvider (the Google Workspace tests have no connector/script panel).
vi.mock("../../../context/AuthContext", async (importOriginal) => ({
  ...(await importOriginal<typeof import("../../../context/AuthContext")>()),
  useAuth: () => ({ activeOrgId: null }),
}));
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
  gwShapes,
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

const FILLED = { gw_account: "ada@example.test", gw_mail_content: "full" };
// The same, in a form that has already settled on one mailbox (a new form settles when it
// learns what the organisation's entry can do).
const ON_ONE = { ...FILLED, gw_whose: "one" };

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
  renderInForm(<SourceFormFieldsExtended {...props} />);
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

  it("offers mail, calendars and tasks, a new source starting on mail", async () => {
    form(FILLED);
    await connectReady();
    expect(shown("resource-mail")).toBeChecked();
    expect(shown("resource-calendar")).not.toBeChecked();
    expect(shown("resource-tasks")).not.toBeChecked();
    // Whether attendees are told is a setting of calendars, asked once they are read.
    expect(shown("send-updates")).toBeNull();
  });

  it("asks whether attendees are told once calendars are read, and nothing of mail without it", () => {
    form({ ...FILLED, gw_resources: "calendar,tasks" });
    expect(shown("send-updates")).not.toBeChecked();
    expect(shown("gw_mail_content")).toBeNull();
    expect(shown("gw_mail_labels")).toBeNull();
  });

  it("ticking calendars is another approval: the one given is asked again", () => {
    const setAuthFields = form({ ...FILLED, gw_connected_as: "ada@example.test" });
    fireEvent.click(shown("resource-calendar")!);
    expect(setAuthFields).toHaveBeenLastCalledWith({
      ...FILLED,
      gw_resources: "mail,calendar",
      gw_connected_as: "",
      gw_connected_at: "",
      gw_earlier_approval: "",
    });
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

  it("sends Google's scope for what is read, and no client, and holds no approval", async () => {
    signIn.signInSource.mockResolvedValue({
      source_id: "mail",
      account: "ada@example.test",
      connected: true,
    });
    const setAuthFields = form(FILLED);
    await connectReady();
    fireEvent.click(shown("connect")!);
    await waitFor(() => expect(setAuthFields).toHaveBeenCalled());
    expect(signIn.signInSource).toHaveBeenCalledWith({
      source_id: "mail",
      kind: "google_workspace",
      account: "ada@example.test",
      // REQ-1994: what the source reads; the server asks Google for its scopes.
      mapping: gwMapping(FILLED),
    });
    // The server keeps the approval; the form holds who connected and when, and no token.
    const kept = setAuthFields.mock.calls.at(-1)![0] as Record<string, string>;
    expect(kept).toMatchObject({
      ...FILLED,
      gw_connected_as: "ada@example.test",
      gw_earlier_approval: "",
    });
    expect(Number.isNaN(Date.parse(kept.gw_connected_at))).toBe(false);
    expect(Object.keys(kept)).not.toContain("gw_refresh_token");
  });

  it("says who approved once connected", async () => {
    form({ ...FILLED, gw_connected_as: "ada@example.test" });
    await connectReady();
    expect(shown("connected")).toHaveTextContent("Connected as ada@example.test");
  });

  it("asks the server whether this mailbox's owner has signed in, and says since when", async () => {
    signIn.signInStatus.mockImplementation(async (_kind: string, of?: object) =>
      of
        ? {
            configured: true,
            may_configure: false,
            connected: true,
            connected_at: "2026-10-01T09:00:00Z",
          }
        : { configured: true, may_configure: false },
    );
    const setAuthFields = form(ON_ONE);
    await waitFor(() =>
      expect(signIn.signInStatus).toHaveBeenCalledWith("google_workspace", {
        source_id: "mail",
        account: "ada@example.test",
      }),
    );
    await waitFor(() =>
      expect(setAuthFields).toHaveBeenLastCalledWith({
        ...ON_ONE,
        gw_connected_as: "ada@example.test",
        gw_connected_at: "2026-10-01T09:00:00Z",
      }),
    );
  });

  it("says when it was connected where the server said", async () => {
    form({
      ...FILLED,
      gw_connected_as: "ada@example.test",
      gw_connected_at: "2026-10-01T09:00:00Z",
    });
    await connectReady();
    expect(shown("connected")).toHaveTextContent(
      `Connected as ada@example.test on ${new Date("2026-10-01T09:00:00Z").toLocaleDateString()}`,
    );
  });

  it("asks again when what Google was asked for changes", () => {
    const setAuthFields = form({
      ...FILLED,
      gw_earlier_approval: "${secret:t}",
      gw_connected_as: "ada@example.test",
      gw_connected_at: "2026-10-01T09:00:00Z",
    });
    fireEvent.change(shown("gw_account")!, { target: { value: "bo@example.test" } });
    expect(setAuthFields).toHaveBeenLastCalledWith(
      expect.objectContaining({
        gw_account: "bo@example.test",
        gw_connected_as: "",
        gw_connected_at: "",
        gw_earlier_approval: "",
      }),
    );
  });

  it("shows a refusal and connects nothing", async () => {
    signIn.signInSource.mockRejectedValue(new Error("refused"));
    const setAuthFields = form(ON_ONE);
    await connectReady();
    fireEvent.click(shown("connect")!);
    expect(await screen.findByTestId("google-workspace-problem")).toBeInTheDocument();
    expect(setAuthFields).not.toHaveBeenCalled();
    expect(shown("connected")).toBeNull();
  });
});

describe("what a Google Workspace setup saves", () => {
  const connected = { ...FILLED, gw_connected_as: "ada@example.test" };

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
    expect(mapping.mail_content).toBe("headers");
  });

  it("is not complete until the account is connected", () => {
    expect(gwMissing({})).toEqual(["gw_account", "gw_mail_content", "gw_connected_as"]);
    expect(gwMissing(FILLED)).toEqual(["gw_connected_as"]);
    expect(gwMissing(connected)).toEqual([]);
  });

  it("holds no approval: the server keeps it", () => {
    expect(gwMapping(connected)).not.toHaveProperty("refresh_token");
  });

  it("carries through the approval of a source connected the earlier way, unchanged", () => {
    const saved = {
      accounts: ["ada@example.test"],
      resources: ["mail"],
      sign_in: "google_account",
      mail_content: "full",
      refresh_token: "${secret:source_mail__refresh_token}",
    };
    const fields = gwFieldsFromMapping(JSON.stringify(saved));
    expect(fields).toMatchObject({
      gw_earlier_approval: "${secret:source_mail__refresh_token}",
      gw_connected_as: "ada@example.test",
    });
    expect(gwMissing(fields)).toEqual([]);
    // An edit of anything else does not drop it before the server has taken it.
    expect(gwMapping(fields)).toEqual(saved);
  });

  it("reads back into the form what was saved", () => {
    const saved = gwMapping({ ...connected, gw_mail_labels: "Clients" });
    const fields = gwFieldsFromMapping(JSON.stringify(saved));
    // Whether its owner has signed in is asked of the server when the form opens.
    expect(fields).toMatchObject({ ...FILLED, gw_mail_labels: "Clients" });
    expect(fields).not.toHaveProperty("gw_connected_as");
    expect(gwMapping(fields)).toEqual(saved);
  });

  it("holds no service account key: the key is the organisation's alone", () => {
    const fields = gwFieldsFromMapping(
      JSON.stringify({
        accounts: ["ada@example.test"],
        resources: ["mail"],
        sign_in: "service_account",
        service_account_key: "${secret:k}",
        mail_content: "full",
      }),
    );
    expect(JSON.stringify(gwMapping(fields))).not.toContain("service_account");
    expect(gwMissing(fields)).toEqual(["gw_connected_as"]);
  });
});

describe("what a search costs", () => {
  it("is said under the field once a search is typed, in place of how to write one", async () => {
    form({ ...FILLED, gw_whose: "one" });
    await connectReady();
    expect(screen.getByText(en.googleWorkspaceFields.mailSearchHelp)).toBeInTheDocument();
    expect(screen.queryByText(en.googleWorkspaceFields.mailSearchCost)).toBeNull();
  });

  it("a source that searches is read again whole on every refresh", async () => {
    form({ ...FILLED, gw_whose: "one", gw_mail_search: "from:orders@example.com" });
    await connectReady();
    expect(screen.getByText(en.googleWorkspaceFields.mailSearchCost)).toHaveTextContent(
      "A source that searches is read again whole on every refresh.",
    );
    expect(screen.queryByText(en.googleWorkspaceFields.mailSearchHelp)).toBeNull();
  });
});

describe("what the form offers follows what the organisation's Google entry can do", () => {
  // `admin`: whether the person at the form may set the entry up (Admin › Email).
  const entry = (configured: boolean, organisation_mailboxes: boolean, admin = false) => ({
    configured,
    may_configure: admin,
    organisation_mailboxes,
  });
  const offered = async () => {
    const input = await screen.findByTestId("google-workspace-gw_whose");
    fireEvent.click(input);
    const list = document.getElementById(input.getAttribute("aria-controls")!)!;
    return [...list.querySelectorAll('[role="option"]')].map((o) => o.textContent);
  };
  const ONE = en.googleWorkspaceFields.whoseOne;
  const ORGANISATION = en.googleWorkspaceFields.whoseOrganisation;

  it("no sign-in client, the organisation's mailboxes allowed: a new source opens on those", async () => {
    signIn.signInStatus.mockResolvedValue(entry(false, true));
    const setAuthFields = form({});
    await waitFor(() => expect(setAuthFields).toHaveBeenCalledWith({ gw_whose: "organisation" }));
    expect(gwShapes(entry(false, true), "")).toEqual({
      offered: ["organisation"],
      opensOn: "organisation",
    });
  });

  it("and that form has no word of nothing being set up, and offers no one mailbox", async () => {
    signIn.signInStatus.mockResolvedValue(entry(false, true));
    form({ gw_whose: "organisation" });
    expect(await offered()).toEqual([ORGANISATION]);
    expect(shown("not-set-up")).toBeNull();
    expect(shown("not-allowed")).toBeNull();
    expect(shown("gw_choice")).toBeInTheDocument();
  });

  it("a client, the organisation's mailboxes not allowed: one mailbox only", async () => {
    signIn.signInStatus.mockResolvedValue(entry(true, false));
    const setAuthFields = form({});
    await waitFor(() => expect(setAuthFields).toHaveBeenCalledWith({ gw_whose: "one" }));
    expect(await offered()).toEqual([ONE]);
    expect(shown("not-set-up")).toBeNull();
  });

  it("an entry that can do both offers both and opens on one mailbox, as before", async () => {
    signIn.signInStatus.mockResolvedValue(entry(true, true));
    const setAuthFields = form({});
    await waitFor(() => expect(setAuthFields).toHaveBeenCalledWith({ gw_whose: "one" }));
    expect(await offered()).toEqual([ONE, ORGANISATION]);
  });

  it("an entry that can do neither offers both and says what is missing", async () => {
    signIn.signInStatus.mockResolvedValue(entry(false, false));
    form({});
    expect(await screen.findByTestId("google-workspace-not-set-up")).toBeInTheDocument();
    expect(await offered()).toEqual([ONE, ORGANISATION]);
  });

  it("someone who may set the entry up is still offered what it cannot do yet, and told where", async () => {
    // The form still opens on what works; the other choice is where they learn it exists.
    signIn.signInStatus.mockResolvedValue(entry(false, true, true));
    const setAuthFields = form({});
    await waitFor(() => expect(setAuthFields).toHaveBeenCalledWith({ gw_whose: "organisation" }));
    expect(await offered()).toEqual([ONE, ORGANISATION]);
    expect(gwShapes(entry(true, false, true), "").offered).toEqual(["one", "organisation"]);
  });

  it("a stored source keeps the shape it was saved with, whatever the entry can do now", async () => {
    signIn.signInStatus.mockResolvedValue(entry(false, true));
    const setAuthFields = form({ ...FILLED, gw_whose: "one" });
    expect(await offered()).toEqual([ONE, ORGANISATION]);
    // It is not moved to another shape; it is told what its own shape lacks.
    expect(await screen.findByTestId("google-workspace-not-set-up")).toBeInTheDocument();
    expect(setAuthFields).not.toHaveBeenCalledWith(
      expect.objectContaining({ gw_whose: "organisation" }),
    );
    expect(gwShapes(entry(true, false), "organisation").offered).toEqual(["one", "organisation"]);
  });
});

describe("a source of the organisation's mailboxes", () => {
  const ORG = { gw_whose: "organisation", gw_mail_content: "full" };
  const allowed = () =>
    signIn.signInStatus.mockResolvedValue({
      configured: true,
      may_configure: false,
      organisation_mailboxes: true,
    });
  // The form's own questions of the server go on as they do; the check's answer is set here.
  const check = vi.fn();
  const answering = (reply: { ok: boolean; json: () => Promise<unknown> }) => {
    const real = globalThis.fetch;
    vi.stubGlobal("fetch", (url: string, init?: RequestInit) => {
      if (!String(url).endsWith("/mailboxes/check")) return real(url, init);
      check(url, init);
      return Promise.resolve(reply);
    });
  };

  beforeEach(() => check.mockReset());
  afterEach(() => vi.unstubAllGlobals());

  it("asks whose mail first, one mailbox being the choice a new source starts with", async () => {
    form(FILLED);
    await connectReady();
    expect(shown("gw_whose")).toHaveValue(en.googleWorkspaceFields.whoseOne);
    expect(shown("gw_choice")).toBeNull();
  });

  it("asks which mailboxes, and for no account and no approval", async () => {
    allowed();
    form({ ...ORG, gw_choice: "group" });
    await waitFor(() => expect(shown("check")).toBeInTheDocument());
    expect(shown("gw_group")).toBeInTheDocument();
    expect(shown("gw_account")).toBeNull();
    expect(shown("connect")).toBeNull();
    expect(shown("every-mailbox")).toHaveTextContent("reads every mailbox");
    // How much of each message is read is asked of either shape.
    expect(shown("gw_mail_content")).toBeInTheDocument();
  });

  it("saves which mailboxes and no credential at all", () => {
    expect(gwMapping({ ...ORG, gw_choice: "everyone" })).toEqual({
      resources: ["mail"],
      mailboxes: { everyone: true },
      mail_content: "full",
    });
    expect(gwMapping({ ...ORG, gw_choice: "group", gw_group: " sales@example.test " })).toEqual({
      resources: ["mail"],
      mailboxes: { group: "sales@example.test" },
      mail_content: "full",
    });
    const listed = gwMapping({ ...ORG, gw_choice: "list", gw_list: "a@x.test\n\n b@x.test " });
    expect(listed.mailboxes).toEqual({ list: ["a@x.test", "b@x.test"] });
    expect(JSON.stringify(listed)).not.toMatch(/token|accounts|sign_in/);
  });

  it("is not complete until it says which mailboxes", () => {
    expect(gwMissing(ORG)).toEqual(["gw_choice"]);
    expect(gwMissing({ ...ORG, gw_choice: "group" })).toEqual(["gw_choice"]);
    expect(gwMissing({ ...ORG, gw_choice: "list", gw_list: "a@x.test" })).toEqual([]);
    expect(gwMissing({ gw_whose: "organisation", gw_choice: "everyone" })).toEqual([
      "gw_mail_content",
    ]);
  });

  it("reads back into the form what was saved", () => {
    for (const fields of [
      { ...ORG, gw_choice: "everyone" },
      { ...ORG, gw_choice: "group", gw_group: "sales@example.test" },
      { ...ORG, gw_choice: "list", gw_list: "a@x.test\nb@x.test", gw_mail_labels: "Clients" },
    ]) {
      const saved = gwMapping(fields);
      const back = gwFieldsFromMapping(JSON.stringify(saved));
      expect(back).toMatchObject(fields);
      expect(gwMapping(back)).toEqual(saved);
    }
  });

  it("counts the mailboxes a choice names, and forgets the count when the choice changes", async () => {
    allowed();
    answering({ ok: true, json: async () => ({ count: 42 }) });
    const setAuthFields = form({ ...ORG, gw_choice: "everyone" });
    await waitFor(() => expect(shown("check")).toBeEnabled());
    fireEvent.click(shown("check")!);
    expect(await screen.findByTestId("google-workspace-found")).toHaveTextContent("42");
    expect(check).toHaveBeenCalledWith(
      "/admin/google-workspace/mailboxes/check",
      expect.objectContaining({ body: JSON.stringify({ mailboxes: { everyone: true } }) }),
    );
    expect(setAuthFields).not.toHaveBeenCalled();
  });

  it("shows a refusal of the check in its own words", async () => {
    allowed();
    answering({
      ok: false,
      json: async () => ({ code: "x.unknown", detail: "the scope is not delegated", params: {} }),
    });
    form({ ...ORG, gw_choice: "everyone" });
    await waitFor(() => expect(shown("check")).toBeEnabled());
    fireEvent.click(shown("check")!);
    expect(await screen.findByTestId("google-workspace-problem")).toHaveTextContent(
      "the scope is not delegated",
    );
    expect(shown("found")).toBeNull();
  });

  it("needs no client of the organisation's: its mailboxes are read with its own credential", async () => {
    signIn.signInStatus.mockResolvedValue({
      configured: false,
      may_configure: true,
      organisation_mailboxes: true,
    });
    form({ ...ORG, gw_choice: "everyone" });
    await waitFor(() => expect(shown("check")).toBeEnabled());
    expect(shown("not-set-up")).toBeNull();
    expect(shown("not-allowed")).toBeNull();
  });

  it("says so when the organisation has not allowed such sources, and offers no check", async () => {
    signIn.signInStatus.mockResolvedValue({
      configured: true,
      may_configure: true,
      organisation_mailboxes: false,
    });
    form({ ...ORG, gw_choice: "everyone" });
    expect(await screen.findByTestId("google-workspace-not-allowed")).toHaveTextContent(
      en.googleWorkspaceFields.notAllowed,
    );
    expect(shown("allow")).toHaveAttribute("href", "/admin/email");
    expect(shown("check")).toBeNull();
  });
});
