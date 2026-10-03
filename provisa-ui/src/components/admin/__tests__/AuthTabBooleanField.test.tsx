// Copyright (c) 2026 Kenneth Stott
// Canary: 893f5446-09de-4383-913f-fc27b4275691
// REQ-1265: a provider's true/false setting is a checkbox and is saved as a boolean, not as text.

import { describe, it, expect, vi, beforeEach } from "vitest";
import { render, screen, waitFor } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { MantineProvider } from "@mantine/core";
import "../../../i18n";
import * as api from "../../../api/admin";
import { AuthTab } from "../AuthTab";

vi.mock("../../../api/admin", async (orig) => ({
  ...(await orig<typeof import("../../../api/admin")>()),
  fetchAuthConfig: vi.fn(),
  setAuthConfig: vi.fn(),
}));

const STATE: api.AuthConfigState = {
  provider: "ldap",
  providers: [
    {
      key: "ldap",
      label: "LDAP",
      description: "Directory username and password.",
      config_fields: [
        { config_key: "server_url", label: "Server URL", type: "string", required: true },
        { config_key: "start_tls", label: "Use StartTLS", type: "boolean", required: false },
      ],
    },
  ],
  config: { ldap: { server_url: "ldap://directory.example:389", start_tls: false } },
  secret_set: { ldap: {} },
  common: {
    default_role: "analyst",
    assignments_source: "claims",
    trust_upstream: false,
    allow_simple_auth: false,
    allow_registration: true,
  },
  restart_required_note: "restart",
};

beforeEach(() => {
  vi.mocked(api.fetchAuthConfig).mockReset().mockResolvedValue(STATE);
  vi.mocked(api.setAuthConfig)
    .mockReset()
    .mockResolvedValue({ success: true, restart_required: true });
});

describe("AuthTab boolean provider field", () => {
  it("renders a checkbox and saves a boolean", async () => {
    const user = userEvent.setup();
    render(
      <MantineProvider>
        <AuthTab />
      </MantineProvider>,
    );

    const box = await screen.findByRole("checkbox", { name: "Use StartTLS" });
    expect(box).not.toBeChecked();
    await user.click(box);
    expect(box).toBeChecked();

    await user.click(screen.getByTestId("auth-save-button"));
    await waitFor(() => expect(api.setAuthConfig).toHaveBeenCalledTimes(1));
    expect(vi.mocked(api.setAuthConfig).mock.calls[0][0].config).toEqual({
      server_url: "ldap://directory.example:389",
      start_tls: true,
    });
  });

  it("saves false for a cleared checkbox rather than omitting it", async () => {
    vi.mocked(api.fetchAuthConfig).mockResolvedValue({
      ...STATE,
      config: { ldap: { server_url: "ldap://directory.example:389", start_tls: true } },
    });
    const user = userEvent.setup();
    render(
      <MantineProvider>
        <AuthTab />
      </MantineProvider>,
    );

    const box = await screen.findByRole("checkbox", { name: "Use StartTLS" });
    expect(box).toBeChecked();
    await user.click(box);
    await user.click(screen.getByTestId("auth-save-button"));
    await waitFor(() => expect(api.setAuthConfig).toHaveBeenCalledTimes(1));
    expect(vi.mocked(api.setAuthConfig).mock.calls[0][0].config).toMatchObject({
      start_tls: false,
    });
  });

  it("turns self-registration off for local accounts", async () => {
    vi.mocked(api.fetchAuthConfig).mockResolvedValue({
      ...STATE,
      provider: "basic",
      providers: [
        ...STATE.providers,
        { key: "basic", label: "Local accounts", description: "", config_fields: [] },
      ],
    });
    const user = userEvent.setup();
    render(
      <MantineProvider>
        <AuthTab />
      </MantineProvider>,
    );
    const box = await screen.findByTestId("auth-allow-registration");
    expect(box).toBeChecked();
    await user.click(box);
    await user.click(screen.getByTestId("auth-save-button"));
    await waitFor(() => expect(api.setAuthConfig).toHaveBeenCalledTimes(1));
    expect(vi.mocked(api.setAuthConfig).mock.calls[0][0].common).toMatchObject({
      allow_registration: false,
    });
  });
});
