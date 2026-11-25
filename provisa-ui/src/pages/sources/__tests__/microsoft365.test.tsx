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
import { fireEvent, renderInForm, screen, waitFor } from "../../../test-utils/render";

// SourceFormFieldsExtended calls useAuth() unconditionally; stub it so the form
// renders without an AuthProvider (the M365 tests have no connector/script panel).
vi.mock("../../../context/AuthContext", async (importOriginal) => ({
  ...(await importOriginal<typeof import("../../../context/AuthContext")>()),
  useAuth: () => ({ activeOrgId: null }),
}));
import en from "../../../i18n/locales/en/microsoft365Fields.json";
import google from "../../../i18n/locales/en/googleWorkspaceFields.json";
import shared from "../../../i18n/locales/en/sourceFormFields.json";
import { BRAND_CARRIER, SOURCE_TYPES } from "../constants";
import { SourceFormFieldsExtended } from "../SourceFormFieldsExtended";
import type { SourceFormFieldsProps } from "../SourceFormFields";
import { backendType } from "../sourceHelpers";
import * as m365 from "../microsoft365";
import {
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
  renderInForm(<SourceFormFieldsExtended {...props} />);
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
    expect(shown("resource-mail")).toBeChecked();
    expect(shown("resource-calendar")).not.toBeChecked();
    await waitFor(() => expect(shown("connect")).toBeInTheDocument());
    expect(shown("connect")).toBeDisabled(); // no mailbox named yet
    // Nothing about how Provisa signs in to Microsoft is asked of the person adding a source.
    for (const word of ["redirect", "client", "tenant", "secret"]) {
      expect(document.body.textContent?.toLowerCase()).not.toContain(word);
    }
  });

  it("needs the mailbox, what is read, and for calendars the days a check looks at", () => {
    expect(m365ConnectMissing({}, "mail")).toEqual(["m365_account"]);
    expect(m365ConnectMissing({ ...FILLED, m365_resources: "" }, "mail")).toEqual([
      "m365_resources",
    ]);
    expect(m365ConnectMissing({ ...FILLED, m365_resources: "calendar" }, "mail")).toEqual([
      "m365_check_days",
    ]);
    expect(m365ConnectMissing(FILLED, " ")).toEqual(["source_id"]);
    expect(m365ConnectMissing(FILLED, "mail")).toEqual([]);
  });
});

describe("connecting the mailbox", () => {
  it("asks for the owner's approval and holds none of it", async () => {
    // The server keeps the approval for the source's reads; the answer says only who approved.
    signIn.signInSource.mockResolvedValue({
      source_id: "mail",
      account: "megan@contoso.test",
      connected: true,
    });
    const setAuthFields = form(FILLED);
    await waitFor(() => expect(shown("connect")).toBeEnabled());
    fireEvent.click(shown("connect")!);
    await waitFor(() => expect(setAuthFields).toHaveBeenCalled());
    expect(signIn.signInSource).toHaveBeenCalledWith({
      source_id: "mail",
      kind: "microsoft_365",
      account: "megan@contoso.test",
      // REQ-1994: what the source reads; the server asks Microsoft for its scopes.
      mapping: m365Mapping(FILLED),
    });
    const [kept] = setAuthFields.mock.calls.at(-1)!;
    expect(kept).toMatchObject({ ...FILLED, m365_connected_as: "megan@contoso.test" });
    expect(Date.parse(kept.m365_connected_at)).not.toBeNaN();
    expect(JSON.stringify(kept)).not.toContain("refresh_token");
  });

  it("learns from the server whether the mailbox's owner has signed in, and when", async () => {
    signIn.signInStatus.mockImplementation(async (_kind: string, of?: object) =>
      of
        ? {
            configured: true,
            may_configure: false,
            connected: true,
            connected_at: "2026-10-01T09:00:00+00:00",
          }
        : { configured: true, may_configure: false },
    );
    const setAuthFields = form(FILLED);
    await waitFor(() =>
      expect(signIn.signInStatus).toHaveBeenCalledWith("microsoft_365", {
        source_id: "mail",
        account: "megan@contoso.test",
      }),
    );
    await waitFor(() =>
      expect(setAuthFields).toHaveBeenCalledWith({
        ...FILLED,
        m365_connected_as: "megan@contoso.test",
        m365_connected_at: "2026-10-01T09:00:00+00:00",
      }),
    );
  });

  it("says who it is connected as", async () => {
    form({ ...FILLED, m365_connected_as: "megan@contoso.test" });
    expect(await screen.findByTestId("microsoft-365-connected")).toHaveTextContent(
      "Connected as megan@contoso.test",
    );
  });

  it("asks again when another mailbox is named", () => {
    const setAuthFields = form({ ...FILLED, m365_connected_as: "megan@contoso.test" });
    fireEvent.change(shown("m365_account")!, { target: { value: "other@contoso.test" } });
    expect(setAuthFields).toHaveBeenCalledWith({
      m365_account: "other@contoso.test",
      m365_earlier_approval: "",
      m365_connected_as: "",
      m365_connected_at: "",
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
  it("keeps the mailbox and what is read: no approval and no client", () => {
    const mapping = m365Mapping({ ...FILLED, m365_connected_as: "megan@contoso.test" });
    expect(mapping).toEqual({ accounts: ["megan@contoso.test"], resources: ["mail"] });
    expect(m365FieldsFromMapping(JSON.stringify(mapping))).toEqual({
      m365_whose: "one",
      m365_account: "megan@contoso.test",
      m365_resources: "mail",
      m365_send_updates: "",
    });
  });

  it("keeps calendars and tasks with the days a check looks at and who is told", () => {
    const fields = {
      ...FILLED,
      m365_resources: "calendar,tasks",
      m365_check_days: "30",
      m365_send_updates: "true",
      m365_mail_since: "2026-01-01",
    };
    const mapping = m365Mapping(fields);
    expect(mapping).toEqual({
      accounts: ["megan@contoso.test"],
      resources: ["calendar", "tasks"],
      calendar_check_days: 30,
      calendar_send_updates: true,
    });
    expect(m365FieldsFromMapping(JSON.stringify(mapping))).toMatchObject({
      m365_resources: "calendar,tasks",
      m365_check_days: "30",
      m365_send_updates: "true",
    });
  });

  it("carries through the reference a source connected the earlier way still names", () => {
    // Its reader takes that approval once and keeps it; until then an edit must not drop it.
    const stored = {
      accounts: ["megan@contoso.test"],
      resources: ["mail"],
      refresh_token: "${secret:source_mail__refresh_token}",
    };
    const fields = m365FieldsFromMapping(JSON.stringify(stored));
    expect(fields.m365_connected_as).toBe("megan@contoso.test");
    expect(m365Mapping({ ...FILLED, ...fields })).toEqual(stored);
  });
});

describe("nothing received before a date", () => {
  it.each([[FILLED], [{ m365_whose: "organisation" }]])(
    "offers the date and keeps what is typed, whichever mailboxes are read (%j)",
    async (whose) => {
      const setAuthFields = form(whose);
      const field = await screen.findByTestId("microsoft-365-m365_mail_since");
      expect(field).toHaveAttribute("type", "date");
      fireEvent.change(field, { target: { value: "2026-10-01" } });
      expect(setAuthFields).toHaveBeenCalledWith({ ...whose, m365_mail_since: "2026-10-01" });
    },
  );

  it("is the source's mail_since, as a Google Workspace source's is, for either shape", () => {
    const one = m365Mapping({ ...FILLED, m365_mail_since: "2026-10-01" });
    expect(one).toEqual({
      accounts: ["megan@contoso.test"],
      resources: ["mail"],
      mail_since: "2026-10-01",
    });
    const organisation = m365Mapping({
      m365_whose: "organisation",
      m365_choice: "everyone",
      m365_mail_since: "2026-10-01",
    });
    expect(organisation).toEqual({
      resources: ["mail"],
      mailboxes: { everyone: true },
      mail_since: "2026-10-01",
    });
    // A stored source shows its date again, whichever shape it is.
    expect(m365FieldsFromMapping(JSON.stringify(one)).m365_mail_since).toBe("2026-10-01");
    expect(m365FieldsFromMapping(JSON.stringify(organisation)).m365_mail_since).toBe("2026-10-01");
  });

  it("left empty, the source states no date and holds mail of any age", () => {
    expect(m365Mapping(FILLED)).toEqual({ accounts: ["megan@contoso.test"], resources: ["mail"] });
    expect(m365Mapping({ ...FILLED, m365_mail_since: "" })).not.toHaveProperty("mail_since");
    expect(m365FieldsFromMapping(JSON.stringify(m365Mapping(FILLED)))).not.toHaveProperty(
      "m365_mail_since",
    );
  });

  it("is the one field both mail sources show, with one label and one hint", async () => {
    form(FILLED);
    await screen.findByTestId("microsoft-365-m365_mail_since");
    expect(screen.getByLabelText(shared.sourceFormFields.mailSince)).toBeInTheDocument();
    expect(screen.getByText(shared.sourceFormFields.mailSinceHelp)).toBeInTheDocument();
    // Neither source keeps wording of its own for it.
    expect(en.microsoft365Fields).not.toHaveProperty("mailSince");
    expect(google.googleWorkspaceFields).not.toHaveProperty("mailSince");
  });
});

const ORG = { m365_whose: "organisation" };
describe("what the form offers follows what the organisation's Microsoft 365 entry can do", () => {
  // The organisation's client serves both shapes, so only the switch tells them apart.
  // `admin`: whether the person at the form may set the entry up (Admin › Email).
  const entry = (configured: boolean, organisation_mailboxes: boolean, admin = false) => ({
    configured,
    may_configure: admin,
    organisation_mailboxes,
  });
  const offered = async () => {
    const input = await screen.findByTestId("microsoft-365-m365_whose");
    fireEvent.click(input);
    const list = document.getElementById(input.getAttribute("aria-controls")!)!;
    return [...list.querySelectorAll('[role="option"]')].map((o) => o.textContent);
  };
  const ONE = en.microsoft365Fields.whoseOne;
  const ORGANISATION = en.microsoft365Fields.whoseOrganisation;

  it("connected, the organisation's mailboxes allowed: both, and a new source opens on one mailbox", async () => {
    signIn.signInStatus.mockResolvedValue(entry(true, true));
    const setAuthFields = form({});
    await waitFor(() => expect(setAuthFields).toHaveBeenCalledWith({ m365_whose: "one" }));
    expect(await offered()).toEqual([ONE, ORGANISATION]);
  });

  it("connected, not allowed: one mailbox only for someone who cannot allow it", async () => {
    signIn.signInStatus.mockResolvedValue(entry(true, false));
    form({});
    expect(await offered()).toEqual([ONE]);
    expect(shown("not-allowed")).toBeNull();
  });

  it("connected, not allowed: an administrator is still offered it, where the notice says where to allow it", async () => {
    signIn.signInStatus.mockResolvedValue(entry(true, false, true));
    form({});
    expect(await offered()).toEqual([ONE, ORGANISATION]);
    expect(m365.m365Shapes(entry(true, false, true), "").opensOn).toBe("one");
  });

  it("not connected: both are offered and the form says what is missing", async () => {
    signIn.signInStatus.mockResolvedValue(entry(false, false));
    form({});
    expect(await screen.findByTestId("microsoft-365-not-set-up")).toBeInTheDocument();
    expect(await offered()).toEqual([ONE, ORGANISATION]);
    // The switch alone reads nothing without the client.
    expect(m365.m365Shapes(entry(false, true), "")).toEqual({
      offered: ["one", "organisation"],
      opensOn: "one",
    });
  });

  it("a stored source keeps its shape and what it holds, whatever the entry can do now", async () => {
    signIn.signInStatus.mockResolvedValue(entry(true, false));
    const stored = { m365_whose: "organisation", m365_choice: "everyone" };
    const setAuthFields = form(stored);
    expect(await offered()).toEqual([ONE, ORGANISATION]);
    expect(await screen.findByTestId("microsoft-365-not-allowed")).toBeInTheDocument();
    expect(setAuthFields).not.toHaveBeenCalled();
  });

  it("a stored one-mailbox source connected the earlier way keeps its approval", async () => {
    signIn.signInStatus.mockResolvedValue(entry(true, true));
    const earlier = m365FieldsFromMapping(
      JSON.stringify({
        accounts: ["megan@contoso.test"],
        resources: ["mail"],
        refresh_token: "${secret:source_mail__refresh_token}",
      }),
    );
    expect(earlier.m365_whose).toBe("one");
    const setAuthFields = form(earlier, "");
    await screen.findByTestId("microsoft-365-m365_whose");
    await waitFor(() => expect(signIn.signInStatus).toHaveBeenCalled());
    // Nothing settles a shape for it, so nothing rewrites its fields.
    expect(setAuthFields).not.toHaveBeenCalled();
    expect(m365Mapping(earlier).refresh_token).toBe("${secret:source_mail__refresh_token}");
  });
});

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
    signIn.signInStatus.mockResolvedValue({
      ...allowed,
      may_configure: true,
      organisation_mailboxes: false,
    });
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
      [
        { m365_choice: "group", m365_group: " sales@contoso.test " },
        { group: "sales@contoso.test" },
      ],
      [
        { m365_choice: "list", m365_list: "a@contoso.test\n\n b@contoso.test " },
        { list: ["a@contoso.test", "b@contoso.test"] },
      ],
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
