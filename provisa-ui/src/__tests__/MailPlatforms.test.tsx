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

import { beforeEach, describe, expect, it, vi } from "vitest";
import { fireEvent, render, screen, waitFor } from "../test-utils/render";
import en from "../i18n/locales/en/mailPlatforms.json";

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
const google = (over = {}) => ({
  platform: "google_workspace",
  settings_fields: [] as string[],
  configured: false,
  client_id: null as string | null,
  settings: {} as Record<string, string>,
  ...over,
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
    auth.capabilities = ["platform_settings"];
    render(<EmailTab />);
    expect(screen.getByTestId("email-outgoing")).toBeInTheDocument();
    expect(screen.getByText(en.mailPlatforms.sentTitle)).toBeInTheDocument();
    expect(screen.queryByTestId("email-mail-platforms")).toBeNull();
    expect(list).not.toHaveBeenCalled();
  });

  it("shows both, each under its own heading and description, to a holder of both rights", async () => {
    auth.capabilities = ["platform_settings", "org_settings"];
    render(<EmailTab />);
    await screen.findByTestId(tid(""));
    expect(screen.getByText(en.mailPlatforms.sentTitle)).toBeInTheDocument();
    expect(screen.getByText(en.mailPlatforms.sentHelp)).toBeInTheDocument();
    expect(screen.getByText(en.mailPlatforms.platformsTitle)).toBeInTheDocument();
    expect(screen.getByText(en.mailPlatforms.platformsHelp)).toBeInTheDocument();
  });

  it("is reached from the menu by either right", () => {
    const entry = NAV_GROUPS.flatMap((g) => g.items).find((i) => i.to === "/admin/email");
    expect(entry).toMatchObject({ capability: "platform_settings", orCapability: "org_settings" });
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
    const address = await screen.findByTestId(tid("-redirect"));
    expect(address).toHaveValue(REDIRECT);
    expect(address).toHaveAttribute("readonly");
    expect(screen.getByTestId(tid("-copy"))).toHaveTextContent(en.mailPlatforms.copy);
    expect(
      screen.getByText(/Google Cloud console, under Authorized redirect URIs/),
    ).toBeInTheDocument();
  });

  it("says which address people must open Provisa at, and that another name will not work", async () => {
    render(<MailPlatformsSection />);
    const note = await screen.findByTestId(tid("-same-address"));
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
    expect(await screen.findByTestId(tid("-no-redirect"))).toHaveTextContent(
      en.mailPlatforms.redirectUnknown,
    );
    expect(screen.getByTestId(tid("-client-id"))).toBeInTheDocument();
  });

  it("takes the client once: id and secret, the secret typed as a password", async () => {
    render(<MailPlatformsSection />);
    const save = await screen.findByTestId(tid("-save"));
    expect(save).toBeDisabled();
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
      }),
    );
    expect(await screen.findByTestId(tid("-said"))).toHaveTextContent(en.mailPlatforms.saved);
  });

  it("never shows a stored secret, and leaving it empty keeps the one entered", async () => {
    list.mockResolvedValue({
      redirect_address: REDIRECT,
      platforms: [google({ configured: true, client_id: "client-1" })],
    });
    render(<MailPlatformsSection />);
    expect(await screen.findByTestId(tid("-state"))).toHaveTextContent(en.mailPlatforms.connected);
    expect(screen.getByTestId(tid("-client-secret"))).toHaveValue("");
    expect(screen.getByText(en.mailPlatforms.clientSecretKeep)).toBeInTheDocument();
    fireEvent.change(screen.getByTestId(tid("-client-id")), { target: { value: "client-2" } });
    fireEvent.click(screen.getByTestId(tid("-save")));
    await waitFor(() =>
      expect(put).toHaveBeenCalledWith("acme", "google_workspace", {
        client_id: "client-2",
        settings: {},
      }),
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
    fireEvent.change(await screen.findByTestId(tid("-client-id")), { target: { value: "c" } });
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
    expect(screen.getByTestId("mail-platform-microsoft_365-setting-tenant")).toBeInTheDocument();
    expect(screen.getByText(en.mailPlatforms.setting.tenant)).toBeInTheDocument();
    expect(screen.getByText(en.mailPlatforms.settingHelp.tenant)).toBeInTheDocument();
    expect(screen.getByText(en.mailPlatforms.redirectHelp.microsoft_365)).toBeInTheDocument();
  });

  it("asks for each setting a platform declares, and needs it", () => {
    const tenanted = google({ platform: "tenanted", settings_fields: ["tenant"] });
    const draft = { client_id: "c", client_secret: "s", settings: { tenant: "" } };
    expect(platformMissing(tenanted, draft)).toEqual(["tenant"]);
    expect(platformMissing(tenanted, { ...draft, settings: { tenant: "contoso" } })).toEqual([]);
    expect(platformMissing(google(), { client_id: "", client_secret: "", settings: {} })).toEqual([
      "client_id",
      "client_secret",
    ]);
  });
});
