// Copyright (c) 2026 Kenneth Stott
// Canary: 2a5f7c81-9b64-4de3-8071-c3e5b1d40f26
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-1576: the platform's mail transport. What is worth asserting is that the page offers every
// transport the build knows (including the one that needs a package, named and unselectable),
// that a stored credential is never rendered and never resent (REQ-1575), and that the delivery
// panel reports the record of real attempts — including a failure in the transport's own words.

import { describe, it, expect, vi, beforeEach } from "vitest";
import { render, screen, waitFor, fireEvent } from "../test-utils/render";
import { MailTab } from "../components/admin/MailTab";

vi.mock("../api/mail", () => ({
  fetchMail: vi.fn(),
  setMail: vi.fn(),
  fetchMailStats: vi.fn(),
  sendTestMail: vi.fn(),
}));

import { fetchMail, setMail, fetchMailStats, sendTestMail } from "../api/mail";

const mockFetch = vi.mocked(fetchMail);
const mockSet = vi.mocked(setMail);
const mockStats = vi.mocked(fetchMailStats);
const mockTest = vi.mocked(sendTestMail);

const STATE = {
  provider: "smtp",
  from_address: "provisa@example.com",
  base_url: "https://provisa.example.com",
  timeout_seconds: 10,
  providers: [
    {
      key: "smtp",
      label: "SMTP",
      description: "Any mail server that speaks SMTP.",
      available: true,
      requires: null,
      config_fields: [
        { config_key: "host", label: "Host", type: "string", required: true },
        { config_key: "port", label: "Port", type: "number", required: false },
        { config_key: "username", label: "Username", type: "string", required: false },
        {
          config_key: "password",
          label: "Password",
          type: "string",
          required: false,
          secret: true,
        },
      ],
    },
    {
      key: "resend",
      label: "Resend",
      description: "Resend's HTTP API.",
      available: true,
      requires: null,
      config_fields: [
        { config_key: "api_key", label: "API key", type: "string", required: true, secret: true },
      ],
    },
    {
      key: "ses",
      label: "Amazon SES",
      description: "Amazon Simple Email Service.",
      available: false,
      requires: "boto3",
      config_fields: [{ config_key: "region", label: "Region", type: "string", required: true }],
    },
  ],
  // REQ-1575: every field EXCEPT the secret ones. `password` is absent because it was never sent.
  config: { smtp: { host: "relay.internal", port: "587", username: "postmaster" }, resend: {} },
  secret_set: { smtp: { password: true }, resend: { api_key: false } },
};

const STATS = {
  total: { attempted: 9, delivered: 7, failed: 2 },
  windows: {
    day: { attempted: 3, delivered: 2, failed: 1 },
    week: { attempted: 9, delivered: 7, failed: 2 },
  },
  last_success: {
    sent_at: "2026-08-24T10:00:00+00:00",
    provider: "smtp",
    kind: "invite",
    recipient: "dana@example.com",
    org_id: "acme",
    succeeded: true,
    error: null,
    requested_by: "uid-admin",
  },
  last_failure: {
    sent_at: "2026-08-24T11:00:00+00:00",
    provider: "smtp",
    kind: "invite",
    recipient: "sam@example.com",
    org_id: "acme",
    succeeded: false,
    error: "550 sender domain not verified",
    requested_by: "uid-admin",
  },
  recent: [
    {
      sent_at: "2026-08-24T11:00:00+00:00",
      provider: "smtp",
      kind: "invite",
      recipient: "sam@example.com",
      org_id: "acme",
      succeeded: false,
      error: "550 sender domain not verified",
      requested_by: "uid-admin",
    },
  ],
};

beforeEach(() => {
  vi.clearAllMocks();
  mockFetch.mockResolvedValue(structuredClone(STATE));
  mockStats.mockResolvedValue(structuredClone(STATS));
  mockSet.mockResolvedValue({ success: true, provider: "smtp" });
});

describe("the transports on offer", () => {
  it("shows what the deployment is wired to, with its non-secret settings filled in", async () => {
    render(<MailTab />);
    await waitFor(() => expect(screen.getByTestId("mail-tab")).toBeInTheDocument());
    expect(screen.getByTestId("mail-provider")).toHaveValue("SMTP");
    expect(screen.getByTestId("mail-field-host")).toHaveValue("relay.internal");
    expect(screen.getByTestId("mail-from-address")).toHaveValue("provisa@example.com");
  });

  it("lists an uninstalled transport, naming the package rather than hiding the row", async () => {
    render(<MailTab />);
    await waitFor(() => expect(screen.getByTestId("mail-tab")).toBeInTheDocument());
    fireEvent.click(screen.getByTestId("mail-provider"));
    const option = await screen.findByText(/Amazon SES — install boto3/);
    expect(option).toBeInTheDocument();
  });
});

describe("credentials never come back out", () => {
  it("renders no stored credential — only that one is on file", async () => {
    render(<MailTab />);
    await waitFor(() => expect(screen.getByTestId("mail-tab")).toBeInTheDocument());
    const password = screen.getByTestId("mail-field-password");
    expect(password).toHaveValue("");
    expect(password).toHaveAttribute("placeholder", expect.stringMatching(/on file/i));
  });

  it("leaves an untouched credential out of the saved payload, so it stays as stored", async () => {
    render(<MailTab />);
    await waitFor(() => expect(screen.getByTestId("mail-tab")).toBeInTheDocument());
    fireEvent.change(screen.getByTestId("mail-field-host"), {
      target: { value: "relay2.internal" },
    });
    fireEvent.click(screen.getByTestId("mail-save"));
    await waitFor(() => expect(mockSet).toHaveBeenCalled());
    const body = mockSet.mock.calls[0][0];
    expect(body.config.host).toBe("relay2.internal");
    expect("password" in body.config).toBe(false);
  });

  it("sends a credential once it is typed in", async () => {
    render(<MailTab />);
    await waitFor(() => expect(screen.getByTestId("mail-tab")).toBeInTheDocument());
    fireEvent.change(screen.getByTestId("mail-field-password"), { target: { value: "hunter2" } });
    fireEvent.click(screen.getByTestId("mail-save"));
    await waitFor(() => expect(mockSet).toHaveBeenCalled());
    expect(mockSet.mock.calls[0][0].config.password).toBe("hunter2");
  });
});

describe("is mail going out", () => {
  it("reports attempted, delivered and failed for each window", async () => {
    render(<MailTab />);
    await waitFor(() => expect(screen.getByTestId("mail-stats")).toBeInTheDocument());
    const day = screen.getByTestId("mail-window-Last 24 hours");
    expect(day).toHaveTextContent("3");
    expect(day).toHaveTextContent("2");
    expect(day).toHaveTextContent("1");
  });

  it("shows the last failure in the transport's own words", async () => {
    render(<MailTab />);
    await waitFor(() => expect(screen.getByTestId("mail-stats")).toBeInTheDocument());
    expect(screen.getByTestId("mail-last-failure-error")).toHaveTextContent(
      "550 sender domain not verified",
    );
  });

  it("renders a refused test send as the mail server's answer, not as a broken page", async () => {
    mockTest.mockResolvedValue({ success: false, error: "550 relay access denied" });
    render(<MailTab />);
    await waitFor(() => expect(screen.getByTestId("mail-tab")).toBeInTheDocument());
    fireEvent.change(screen.getByTestId("mail-test-recipient"), {
      target: { value: "sam@example.com" },
    });
    fireEvent.click(screen.getByTestId("mail-test-send"));
    await waitFor(() =>
      expect(screen.getByTestId("mail-test-result")).toHaveTextContent("550 relay access denied"),
    );
  });

  it("re-reads the record after a test send, so the panel includes the attempt just made", async () => {
    mockTest.mockResolvedValue({ success: true });
    render(<MailTab />);
    await waitFor(() => expect(mockStats).toHaveBeenCalledTimes(1));
    fireEvent.change(screen.getByTestId("mail-test-recipient"), {
      target: { value: "sam@example.com" },
    });
    fireEvent.click(screen.getByTestId("mail-test-send"));
    await waitFor(() => expect(mockStats).toHaveBeenCalledTimes(2));
  });
});
