// Copyright (c) 2026 Kenneth Stott
// Canary: 06d39d81-232a-4535-a09f-1267d092bd84
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-1576, REQ-1923: Admin › Email holds the mail Provisa sends (the deployment's) and the mail
// platforms the organisation's sources connect to (the organisation's), under two headings, each
// shown only to a holder of its own right. The platform section takes a client once, shows the
// address to register ready to copy, and never shows a secret back.

import { readFileSync } from "fs";
import { resolve } from "path";
import { beforeEach, describe, expect, it, vi } from "vitest";
import { fireEvent, render, screen, waitFor } from "../test-utils/render";
import en from "../i18n/locales/en/mailPlatforms.json";
import { meetsRequirement } from "../lib/capabilities";

const auth = vi.hoisted(() => ({
  capabilities: [] as string[],
  activeOrgId: "acme" as string | null,
}));
vi.mock("../context/AuthContext", () => ({ useAuth: () => auth }));
vi.mock("../components/admin/MailTab", () => ({
  MailTab: () => <div data-testid="outgoing-mail-form" />,
}));
vi.mock("../api/mailPlatforms", () => ({
  fetchMailPlatforms: vi.fn(),
  putMailPlatform: vi.fn(),
  deleteMailPlatform: vi.fn(),
}));

import { deleteMailPlatform, fetchMailPlatforms, putMailPlatform } from "../api/mailPlatforms";
import { EmailTab } from "../components/admin/EmailTab";
import { MailPlatformsSection } from "../components/admin/MailPlatformsSection";
import { platformMissing } from "../components/admin/mailPlatformDraft";
import { NAV_GROUPS } from "../components/navGroups";

const list = vi.mocked(fetchMailPlatforms);
const put = vi.mocked(putMailPlatform);
const remove = vi.mocked(deleteMailPlatform);

const REDIRECT = "http://localhost:3000/source-sign-in.html";
// An entry that holds a client holds its secret as entered, and the form is handed it.
const KEPT = "client-secret-as-entered";
const google = (over: Record<string, unknown> = {}) => ({
  platform: "google_workspace",
  settings_fields: [] as string[],
  configured: false,
  client_id: null as string | null,
  settings: {} as Record<string, string>,
  organisation_mailboxes: false,
  organisation_settings_fields: [] as string[],
  organisation_key_field: false,
  organisation_key: null as string | null,
  organisation_scopes: [] as string[],
  ...over,
  client_secret: (over.client_secret ?? (over.client_id ? KEPT : null)) as string | null,
});
const tid = (suffix: string) => `mail-platform-google_workspace${suffix}`;

beforeEach(() => {
  auth.capabilities = ["org_settings"];
  auth.activeOrgId = "acme";
  list.mockReset().mockResolvedValue({ redirect_address: REDIRECT, platforms: [google()] });
  put.mockReset().mockResolvedValue(google({ configured: true, client_id: "client-1" }));
  remove.mockReset().mockResolvedValue(undefined);
});

describe("Admin › Email", () => {
  it("shows an organisation administrator the mail platforms and not the mail Provisa sends", async () => {
    render(<EmailTab />);
    expect(await screen.findByTestId("email-mail-platforms")).toBeInTheDocument();
    expect(screen.getByText(en.mailPlatforms.platformsTitle)).toBeInTheDocument();
    expect(screen.queryByTestId("email-outgoing")).toBeNull();
    expect(screen.queryByTestId("outgoing-mail-form")).toBeNull();
  });

  it("shows a platform administrator the mail Provisa sends and not an organisation's platforms", () => {
    auth.capabilities = ["platform_settings", "cross_org"];
    render(<EmailTab />);
    expect(screen.getByTestId("email-outgoing")).toBeInTheDocument();
    expect(screen.getByText(en.mailPlatforms.sentTitle)).toBeInTheDocument();
    expect(screen.queryByTestId("email-mail-platforms")).toBeNull();
    expect(list).not.toHaveBeenCalled();
  });

  it("does not show the mail Provisa sends to a holder of platform_settings without cross_org", () => {
    // The API asks the deployment check (platform_settings AND cross_org); a single-tenant org
    // administrator holds the first alone and would see a section that answers 403.
    auth.capabilities = ["platform_settings"];
    render(<EmailTab />);
    expect(screen.queryByTestId("email-outgoing")).toBeNull();
    expect(screen.queryByTestId("outgoing-mail-form")).toBeNull();
  });

  it("shows both, each under its own heading and description, to a holder of both rights", async () => {
    auth.capabilities = ["platform_settings", "cross_org", "org_settings"];
    render(<EmailTab />);
    await screen.findByTestId(tid(""));
    expect(screen.getByText(en.mailPlatforms.sentTitle)).toBeInTheDocument();
    expect(screen.getByText(en.mailPlatforms.sentHelp)).toBeInTheDocument();
    expect(screen.getByText(en.mailPlatforms.platformsTitle)).toBeInTheDocument();
    expect(screen.getByText(en.mailPlatforms.platformsHelp)).toBeInTheDocument();
  });

  it("is reached from the menu by either half's right", () => {
    const entry = NAV_GROUPS.flatMap((g) => g.items).find((i) => i.to === "/admin/email");
    expect(entry).toMatchObject({
      capability: "org_settings",
      orAllOf: ["platform_settings", "cross_org"],
    });
    expect(meetsRequirement(["org_settings"], entry!)).toBe(true);
    expect(meetsRequirement(["platform_settings", "cross_org"], entry!)).toBe(true);
  });

  it("does not open on platform_settings alone, for which the page holds nothing", () => {
    // A single-tenant org administrator without org_settings would land on an empty page.
    const entry = NAV_GROUPS.flatMap((g) => g.items).find((i) => i.to === "/admin/email");
    expect(meetsRequirement(["platform_settings"], entry!)).toBe(false);
    expect(meetsRequirement(["cross_org"], entry!)).toBe(false);
    const source = readFileSync(resolve(__dirname, "../App.tsx"), "utf-8");
    const route = source.slice(source.indexOf('path="/admin/email"')).slice(0, 400);
    expect(route).toContain('capability="org_settings"');
    expect(route).toContain("orAllOf={[");
  });
});

describe("the organisation's mail platforms", () => {
  it("lists Google Workspace as not connected, for the caller's organisation", async () => {
    render(<MailPlatformsSection />);
    expect(await screen.findByTestId(tid("-state"))).toHaveTextContent(
      en.mailPlatforms.notConnected,
    );
    expect(screen.getByText("Google Workspace")).toBeInTheDocument();
    expect(list).toHaveBeenCalledWith("acme");
  });

  it("shows the redirect address ready to copy, with where it goes, and nothing to type", async () => {
    render(<MailPlatformsSection />);
    fireEvent.click(await screen.findByTestId(tid("-own-mailbox")));
    const address = screen.getByTestId(tid("-redirect"));
    expect(address).toHaveValue(REDIRECT);
    expect(address).toHaveAttribute("readonly");
    expect(screen.getByTestId(tid("-copy"))).toHaveTextContent(en.mailPlatforms.copy);
    // Where it goes is the field's help, behind its (?).
    expect(address.getAttribute("aria-description")).toMatch(
      /Google Cloud console, under Authorized redirect URIs/,
    );
  });

  it("says which address people must open Provisa at, and that another name will not work", async () => {
    render(<MailPlatformsSection />);
    fireEvent.click(await screen.findByTestId(tid("-own-mailbox")));
    const note = screen.getByTestId(tid("-same-address"));
    expect(note).toHaveTextContent("http://localhost:3000");
    expect(note).toHaveTextContent("127.0.0.1 in place of localhost");
  });

  it("says so when the deployment cannot state its address, and still takes the client", async () => {
    list.mockResolvedValue({
      redirect_address: null,
      redirect_problem: "source_sign_in.public_address_not_set",
      platforms: [google()],
    });
    render(<MailPlatformsSection />);
    fireEvent.click(await screen.findByTestId(tid("-own-mailbox")));
    expect(screen.getByTestId(tid("-no-redirect"))).toHaveTextContent(
      en.mailPlatforms.redirectUnknown,
    );
    expect(screen.getByTestId(tid("-client-id"))).toBeInTheDocument();
  });

  it("takes the client once: id and secret, the secret typed as a password", async () => {
    render(<MailPlatformsSection />);
    const save = await screen.findByTestId(tid("-save"));
    expect(save).toBeDisabled();
    // What the entry is for is chosen first; its fields follow.
    expect(screen.queryByTestId(tid("-client-id"))).toBeNull();
    fireEvent.click(screen.getByTestId(tid("-own-mailbox")));
    fireEvent.change(screen.getByTestId(tid("-client-id")), { target: { value: " client-1 " } });
    expect(save).toBeDisabled();
    const secret = screen.getByTestId(tid("-client-secret"));
    expect(secret).toHaveAttribute("type", "password");
    fireEvent.change(secret, { target: { value: "made-up-secret" } });
    fireEvent.click(save);
    await waitFor(() =>
      expect(put).toHaveBeenCalledWith("acme", "google_workspace", {
        client_id: "client-1",
        client_secret: "made-up-secret",
        settings: {},
        organisation_mailboxes: false,
      }),
    );
    expect(await screen.findByTestId(tid("-said"))).toHaveTextContent(en.mailPlatforms.saved);
  });

  it.each(["the-secret-as-typed", "${secret:my_google_client}"])(
    "shows the client's secret field again as it was entered (%s), behind the reveal control",
    async (entered) => {
      list.mockResolvedValue({
        redirect_address: REDIRECT,
        platforms: [google({ configured: true, client_id: "client-1", client_secret: entered })],
      });
      render(<MailPlatformsSection />);
      expect(await screen.findByTestId(tid("-state"))).toHaveTextContent(
        en.mailPlatforms.connected,
      );
      const field = screen.getByTestId(tid("-client-secret"));
      expect(field).toHaveValue(entered);
      expect(field).toHaveAttribute("type", "password");
      // Saved with another field changed, the secret goes back exactly as the field holds it.
      fireEvent.change(screen.getByTestId(tid("-client-id")), { target: { value: "client-2" } });
      fireEvent.click(screen.getByTestId(tid("-save")));
      await waitFor(() =>
        expect(put).toHaveBeenCalledWith("acme", "google_workspace", {
          client_id: "client-2",
          client_secret: entered,
          settings: {},
          organisation_mailboxes: false,
        }),
      );
    },
  );

  it("keeps a value typed over what the field held", async () => {
    list.mockResolvedValue({
      redirect_address: REDIRECT,
      platforms: [
        google({ configured: true, client_id: "client-1", client_secret: "${secret:earlier}" }),
      ],
    });
    render(<MailPlatformsSection />);
    const field = await screen.findByTestId(tid("-client-secret"));
    fireEvent.change(field, { target: { value: "typed-now" } });
    fireEvent.click(screen.getByTestId(tid("-save")));
    await waitFor(() =>
      expect(put).toHaveBeenCalledWith(
        "acme",
        "google_workspace",
        expect.objectContaining({ client_secret: "typed-now" }),
      ),
    );
  });

  it("removes a connected platform, and offers no removal of one that is not", async () => {
    render(<MailPlatformsSection />);
    await screen.findByTestId(tid("-save"));
    expect(screen.queryByTestId(tid("-remove"))).toBeNull();
    list.mockResolvedValue({
      redirect_address: REDIRECT,
      platforms: [google({ configured: true, client_id: "client-1" })],
    });
    render(<MailPlatformsSection />);
    fireEvent.click((await screen.findAllByTestId(tid("-remove")))[0]);
    await waitFor(() => expect(remove).toHaveBeenCalledWith("acme", "google_workspace"));
  });

  it("shows a refusal beside the platform it concerns", async () => {
    put.mockRejectedValue(new Error("google_workspace needs client_secret"));
    render(<MailPlatformsSection />);
    fireEvent.click(await screen.findByTestId(tid("-own-mailbox")));
    fireEvent.change(screen.getByTestId(tid("-client-id")), { target: { value: "c" } });
    fireEvent.change(screen.getByTestId(tid("-client-secret")), { target: { value: "s" } });
    fireEvent.click(screen.getByTestId(tid("-save")));
    expect(await screen.findByTestId(tid("-said"))).toHaveTextContent("needs client_secret");
  });

  it("shows Microsoft 365 by name with its tenant setting labelled and where its address goes", async () => {
    list.mockResolvedValue({
      redirect_address: REDIRECT,
      platforms: [google({ platform: "microsoft_365", settings_fields: ["tenant"] })],
    });
    render(<MailPlatformsSection />);
    expect(await screen.findByText("Microsoft 365")).toBeInTheDocument();
    fireEvent.click(screen.getByTestId("mail-platform-microsoft_365-own-mailbox"));
    expect(screen.getByTestId("mail-platform-microsoft_365-setting-tenant")).toBeInTheDocument();
    expect(screen.getByText(en.mailPlatforms.setting.tenant)).toBeInTheDocument();
    // Each explanation is its field's help, behind the (?).
    expect(screen.getByTestId("mail-platform-microsoft_365-setting-tenant")).toHaveAttribute(
      "aria-description",
      en.mailPlatforms.settingHelp.tenant,
    );
    expect(screen.getByTestId("mail-platform-microsoft_365-redirect")).toHaveAttribute(
      "aria-description",
      en.mailPlatforms.redirectHelp.microsoft_365,
    );
  });

  it("has a tick, off until an administrator turns it on, for reading the organisation's mailboxes", async () => {
    list.mockResolvedValue({
      redirect_address: REDIRECT,
      platforms: [google({ configured: true, client_id: "client-1" })],
    });
    render(<MailPlatformsSection />);
    const allow = await screen.findByTestId(tid("-organisation-mailboxes"));
    expect(allow).not.toBeChecked();
    expect(screen.getByText(en.mailPlatforms.organisationMailboxes)).toBeInTheDocument();
    fireEvent.click(allow);
    expect(put).not.toHaveBeenCalled(); // saved with the entry, not on the click
    fireEvent.click(screen.getByTestId(tid("-save")));
    await waitFor(() =>
      expect(put).toHaveBeenCalledWith("acme", "google_workspace", {
        client_id: "client-1",
        client_secret: KEPT,
        settings: {},
        organisation_mailboxes: true,
      }),
    );
  });

  it("says what the administrator grants at Microsoft for it, and nothing for a platform with no wording", async () => {
    list.mockResolvedValue({
      redirect_address: REDIRECT,
      platforms: [
        google({ platform: "another_platform" }),
        google({ platform: "microsoft_365", settings_fields: ["tenant"] }),
      ],
    });
    render(<MailPlatformsSection />);
    // The grant is said in the switch's help, behind its (?).
    const microsoft = await screen.findByTestId(
      "mail-platform-microsoft_365-organisation-mailboxes",
    );
    expect(microsoft.getAttribute("aria-description")).toContain("Mail.Read and User.Read.All");
    expect(
      screen.getByTestId("mail-platform-another_platform-organisation-mailboxes"),
    ).toHaveAttribute("aria-description", en.mailPlatforms.organisationMailboxesHelp);
  });

  describe("Google Workspace's own credential for the organisation's mailboxes", () => {
    const SCOPES = [
      "https://www.googleapis.com/auth/gmail.readonly",
      "https://www.googleapis.com/auth/admin.directory.user.readonly",
      "https://www.googleapis.com/auth/admin.directory.group.member.readonly",
    ];
    const workspace = (over = {}) =>
      google({
        configured: true,
        client_id: "client-1",
        organisation_settings_fields: ["admin_address"],
        organisation_key_field: true,
        organisation_scopes: SCOPES,
        ...over,
      });
    const KEY = '{"made-up": "service account key"}';

    it("asks for the key as a text field and the administrator address, and says what to authorise", async () => {
      list.mockResolvedValue({ redirect_address: REDIRECT, platforms: [workspace()] });
      render(<MailPlatformsSection />);
      fireEvent.click(await screen.findByTestId(tid("-organisation-mailboxes")));
      const key = screen.getByTestId(tid("-organisation-key"));
      // What was typed, never a file chosen: a reference or the key's JSON.
      expect(key).toHaveAttribute("type", "password");
      expect(document.querySelector('input[type="file"]')).toBeNull();
      expect(screen.getByText(en.mailPlatforms.setting.admin_address)).toBeInTheDocument();
      expect(key).toHaveAttribute("aria-description", en.mailPlatforms.organisationKeyHelp);
      expect(screen.getByTestId(tid("-organisation-scopes"))).toHaveValue(SCOPES.join(","));
      expect(screen.getByTestId(tid("-organisation-scopes-copy"))).toBeInTheDocument();
      const grant = screen
        .getByTestId(tid("-organisation-mailboxes"))
        .getAttribute("aria-description");
      expect(grant).toContain("Manage Domain Wide Delegation");
      // Said plainly: Google has no way to narrow the delegation.
      expect(grant).toContain("Google cannot limit this to some mailboxes");
      expect(grant).toContain("covers every user of the domain");
    });

    it("cannot be allowed before the key and the administrator address are given", () => {
      const on = {
        client_id: "c",
        client_secret: "s",
        settings: { admin_address: "" },
        organisation_mailboxes: true,
        own_mailbox: true,
        organisation_key: "",
      };
      expect(platformMissing(workspace(), on)).toEqual(["admin_address", "organisation_key"]);
      expect(platformMissing(workspace(), { ...on, organisation_mailboxes: false })).toEqual([]);
      const given = { ...on, settings: { admin_address: "admin@example.test" } };
      expect(platformMissing(workspace(), { ...given, organisation_key: KEY })).toEqual([]);
    });

    it("needs no client once such sources are allowed, and half a client is still refused", () => {
      const none = {
        client_id: "",
        client_secret: "",
        settings: { admin_address: "admin@example.test" },
        organisation_mailboxes: true,
        own_mailbox: false,
        organisation_key: KEY,
      };
      const fresh = workspace({ configured: false, client_id: null });
      expect(platformMissing(fresh, none)).toEqual([]);
      // With people connecting their own mailbox as well, the client is needed whole.
      const both = { ...none, own_mailbox: true };
      expect(platformMissing(fresh, { ...both, client_id: "c" })).toEqual(["client_secret"]);
      expect(platformMissing(fresh, { ...both, client_secret: "s" })).toEqual(["client_id"]);
      // Nothing chosen is nothing to save: nothing could read with the entry.
      expect(platformMissing(fresh, { ...none, organisation_mailboxes: false })).toEqual([
        "choice",
      ]);
      expect(platformMissing(fresh, { ...both, organisation_mailboxes: false })).toEqual([
        "client_id",
        "client_secret",
      ]);
      // An entry kept with no client holds no secret to keep.
      const kept = workspace({ client_id: null, organisation_key: KEY });
      expect(platformMissing(kept, { ...both, client_id: "c" })).toEqual(["client_secret"]);
      // A platform whose credential is its client always needs it.
      expect(platformMissing(google({ configured: false, client_id: null }), none)).toEqual([
        "client_id",
        "client_secret",
      ]);
    });

    it("tells a browser not to fill the client or the key", async () => {
      // A text box followed by a password box reads to a browser as a sign-in; a user name it
      // filled into Client ID made half a client, which cannot be saved.
      list.mockResolvedValue({ redirect_address: REDIRECT, platforms: [workspace()] });
      render(<MailPlatformsSection />);
      fireEvent.click(await screen.findByTestId(tid("-organisation-mailboxes")));
      expect(screen.getByTestId(tid("-client-id"))).toHaveAttribute("autocomplete", "off");
      expect(screen.getByTestId(tid("-client-secret"))).toHaveAttribute(
        "autocomplete",
        "new-password",
      );
      expect(screen.getByTestId(tid("-organisation-key"))).toHaveAttribute(
        "autocomplete",
        "new-password",
      );
    });

    it("shows only the fields of what is ticked, the two choices first", async () => {
      list.mockResolvedValue({
        redirect_address: REDIRECT,
        platforms: [
          workspace({
            client_id: null,
            organisation_key: KEY,
            organisation_mailboxes: true,
            settings: { admin_address: "admin@example.test" },
          }),
        ],
      });
      render(<MailPlatformsSection />);
      // An entry with no client is one nobody signs in to: only the organisation's fields show.
      expect(await screen.findByTestId(tid("-organisation-mailboxes"))).toBeChecked();
      expect(screen.getByTestId(tid("-own-mailbox"))).not.toBeChecked();
      expect(screen.getByText(en.mailPlatforms.ownMailbox)).toBeInTheDocument();
      expect(screen.getByTestId(tid("-setting-admin_address"))).toBeInTheDocument();
      expect(screen.getByTestId(tid("-organisation-key"))).toBeInTheDocument();
      expect(screen.getByTestId(tid("-organisation-scopes"))).toBeInTheDocument();
      expect(screen.queryByTestId(tid("-client-id"))).toBeNull();
      expect(screen.queryByTestId(tid("-client-secret"))).toBeNull();
      expect(screen.queryByTestId(tid("-redirect"))).toBeNull();
      // It is saved with an empty client, which the server keeps as no client.
      fireEvent.click(screen.getByTestId(tid("-save")));
      await waitFor(() =>
        expect(put).toHaveBeenCalledWith("acme", "google_workspace", {
          client_id: "",
          settings: { admin_address: "admin@example.test" },
          organisation_mailboxes: true,
          organisation_key: KEY,
        }),
      );
      // People connecting their own mailbox: the address to register and the client appear.
      fireEvent.click(screen.getByTestId(tid("-own-mailbox")));
      expect(screen.getByTestId(tid("-redirect"))).toHaveValue(REDIRECT);
      expect(screen.getByTestId(tid("-client-id"))).toBeRequired();
      expect(screen.getByTestId(tid("-save"))).toBeDisabled();
      // Neither: no field at all, and nothing to save.
      fireEvent.click(screen.getByTestId(tid("-own-mailbox")));
      fireEvent.click(screen.getByTestId(tid("-organisation-mailboxes")));
      expect(screen.queryByTestId(tid("-organisation-key"))).toBeNull();
      expect(screen.queryByTestId(tid("-setting-admin_address"))).toBeNull();
      expect(screen.getByTestId(tid("-save"))).toBeDisabled();
    });

    it("shows the client for the organisation's mailboxes where the platform has no credential of its own", async () => {
      list.mockResolvedValue({
        redirect_address: REDIRECT,
        platforms: [
          google({
            platform: "microsoft_365",
            settings_fields: ["tenant"],
            configured: true,
            client_id: "client-1",
            settings: { tenant: "contoso" },
            organisation_mailboxes: true,
          }),
        ],
      });
      render(<MailPlatformsSection />);
      const at = (suffix: string) => `mail-platform-microsoft_365${suffix}`;
      // The organisation's mailboxes are read with the client, and an entry that holds a client
      // is one people can sign in with: the second tick is on and cannot be taken off while the
      // first is.
      const own = await screen.findByTestId(at("-own-mailbox"));
      expect(own).toBeChecked();
      expect(own).toBeDisabled();
      expect(screen.getByTestId(at("-client-id"))).toHaveValue("client-1");
      expect(screen.getByTestId(at("-client-secret"))).toHaveValue(KEPT);
      expect(screen.getByTestId(at("-setting-tenant"))).toHaveValue("contoso");
      expect(screen.getByTestId(at("-redirect"))).toHaveValue(REDIRECT);
      expect(screen.getByTestId(at("-save"))).toBeEnabled();
      // With the first tick off it is the administrator's to choose again.
      fireEvent.click(screen.getByTestId(at("-organisation-mailboxes")));
      expect(screen.getByTestId(at("-own-mailbox"))).toBeEnabled();
    });

    it("sends the key as it was typed", async () => {
      list.mockResolvedValue({ redirect_address: REDIRECT, platforms: [workspace()] });
      render(<MailPlatformsSection />);
      fireEvent.click(await screen.findByTestId(tid("-organisation-mailboxes")));
      fireEvent.change(screen.getByTestId(tid("-organisation-key")), {
        target: { value: "${secret:my_google_key}" },
      });
      fireEvent.change(screen.getByTestId(tid("-setting-admin_address")), {
        target: { value: "admin@example.test" },
      });
      await waitFor(() => expect(screen.getByTestId(tid("-save"))).toBeEnabled());
      fireEvent.click(screen.getByTestId(tid("-save")));
      await waitFor(() =>
        expect(put).toHaveBeenCalledWith("acme", "google_workspace", {
          client_id: "client-1",
          client_secret: KEPT,
          settings: { admin_address: "admin@example.test" },
          organisation_mailboxes: true,
          organisation_key: "${secret:my_google_key}",
        }),
      );
    });

    it("sends a key file's text pasted into the field as the same JSON", async () => {
      // A key file as Google Cloud writes it: one member a line, the private key's own line
      // breaks escaped inside its string. A one-line field drops the breaks between members.
      const original = {
        type: "service_account",
        project_id: "made-up-project",
        private_key:
          "-----BEGIN PRIVATE KEY-----\nmade-up-line-1\nmade-up-line-2\n-----END PRIVATE KEY-----\n",
        client_email: "reader@made-up-project.iam.gserviceaccount.com",
        token_uri: "https://oauth2.googleapis.com/token",
      };
      const file = JSON.stringify(original, null, 2) + "\n";
      expect(file.split("\n").length).toBeGreaterThan(5);
      list.mockResolvedValue({
        redirect_address: REDIRECT,
        platforms: [
          workspace({
            organisation_mailboxes: true,
            settings: { admin_address: "admin@example.test" },
          }),
        ],
      });
      render(<MailPlatformsSection />);
      const field = await screen.findByTestId(tid("-organisation-key"));
      fireEvent.paste(field, { clipboardData: { getData: () => file } });
      fireEvent.change(field, { target: { value: file } });
      await waitFor(() => expect(screen.getByTestId(tid("-save"))).toBeEnabled());
      fireEvent.click(screen.getByTestId(tid("-save")));
      await waitFor(() => expect(put).toHaveBeenCalled());
      const sent = put.mock.calls[0][2].organisation_key as string;
      // What the input holds is one line, and it parses to the key file's own content.
      expect(sent).not.toContain("\n");
      expect(JSON.parse(sent)).toEqual(original);
    });

    it("shows again what the field holds, and sends it unchanged", async () => {
      list.mockResolvedValue({
        redirect_address: REDIRECT,
        platforms: [
          workspace({
            organisation_key: KEY,
            organisation_mailboxes: true,
            settings: { admin_address: "admin@example.test" },
          }),
        ],
      });
      render(<MailPlatformsSection />);
      expect(await screen.findByTestId(tid("-organisation-key"))).toHaveValue(KEY);
      fireEvent.click(screen.getByTestId(tid("-save")));
      await waitFor(() => expect(put).toHaveBeenCalled());
      expect(put.mock.calls[0][2]).toHaveProperty("organisation_key", KEY);
    });

    it("shows none of it for a platform whose credential is its client", async () => {
      render(<MailPlatformsSection />);
      await screen.findByTestId(tid("-save"));
      expect(screen.queryByTestId(tid("-organisation-key"))).toBeNull();
      expect(screen.queryByTestId(tid("-organisation-scopes"))).toBeNull();
    });
  });

  it("shows the switch as the organisation left it", async () => {
    list.mockResolvedValue({
      redirect_address: REDIRECT,
      platforms: [google({ configured: true, client_id: "c", organisation_mailboxes: true })],
    });
    render(<MailPlatformsSection />);
    expect(await screen.findByTestId(tid("-organisation-mailboxes"))).toBeChecked();
  });

  it("asks for each setting a platform declares, and needs it", () => {
    const tenanted = google({ platform: "tenanted", settings_fields: ["tenant"] });
    const draft = {
      client_id: "c",
      client_secret: "s",
      settings: { tenant: "" },
      organisation_mailboxes: false,
      own_mailbox: true,
      organisation_key: "",
    };
    expect(platformMissing(tenanted, draft)).toEqual(["tenant"]);
    expect(platformMissing(tenanted, { ...draft, settings: { tenant: "contoso" } })).toEqual([]);
    expect(platformMissing(google(), { ...draft, client_id: "", client_secret: "" })).toEqual([
      "client_id",
      "client_secret",
    ]);
  });
});
