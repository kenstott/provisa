// Copyright (c) 2026 Kenneth Stott
// Canary: 2e275e68-c803-453b-aac0-885378e1e6c8
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-1567: an auto-join rule that reaches past one exact domain is not refused and not saved
// quietly — it comes back to its author with the addresses it would admit, and is saved only once
// they say they accept that. The page's job is to ask that question rather than report a failure.
import { describe, it, expect, vi, beforeEach } from "vitest";
import userEvent from "@testing-library/user-event";
import { render, screen, waitFor } from "../test-utils/render";
import { OnboardOrgPage } from "../pages/OnboardOrgPage";

vi.mock("../api/admin", async (importOriginal) => ({
  ...(await importOriginal<typeof import("../api/admin")>()),
  createOrg: vi.fn(),
  fetchOrgStatus: vi.fn(),
  fetchMyInvites: vi.fn().mockResolvedValue([]),
  fetchAutoJoinOffers: vi.fn().mockResolvedValue([]),
  redeemInvite: vi.fn(),
}));
vi.mock("../api/billing", async (importOriginal) => ({
  ...(await importOriginal<typeof import("../api/billing")>()),
  fetchMyReservation: vi.fn().mockResolvedValue(null),
  fetchCatalog: vi.fn(),
}));
vi.mock("react-router-dom", async (importOriginal) => ({
  ...(await importOriginal<typeof import("react-router-dom")>()),
  useNavigate: () => vi.fn(),
}));

const mockAuth = { billing: false, selectOrg: vi.fn(), refresh: vi.fn() };
vi.mock("../context/AuthContext", async (importOriginal) => ({
  ...(await importOriginal<typeof import("../context/AuthContext")>()),
  useAuth: () => mockAuth,
}));

import { OrgError, createOrg, fetchOrgStatus } from "../api/admin";

const mockCreateOrg = vi.mocked(createOrg);
const mockFetchOrgStatus = vi.mocked(fetchOrgStatus);

const BREADTH = new OrgError(
  400,
  "orgs.auto_join_breadth_unacknowledged",
  "Review this auto-join rule carefully or you may admit people from outside your organization: " +
    "besides acme.com, it also accepts addresses such as someone@notacme.com.",
);

beforeEach(() => {
  vi.clearAllMocks();
  mockAuth.billing = false;
  mockFetchOrgStatus.mockResolvedValue({
    id: "carolco",
    name: "Carolco",
    provisioning_state: "ready",
  });
});

async function fillRuleAndSubmit(user: ReturnType<typeof userEvent.setup>) {
  await user.type(screen.getByTestId("onboard-org-id"), "carolco");
  await user.type(screen.getByTestId("onboard-org-name"), "Carolco");
  await user.type(screen.getByTestId("onboard-org-email-rule"), "acme\\.com$");
  await user.click(screen.getByTestId("onboard-org-auto-join"));
  await user.type(screen.getByTestId("onboard-org-auto-join-role"), "analyst");
  await user.click(screen.getByTestId("onboard-org-submit"));
}

describe("an auto-join rule wider than the org that wrote it", () => {
  it("asks the author to accept what the rule admits instead of reporting a failure", async () => {
    const user = userEvent.setup();
    mockCreateOrg.mockRejectedValue(BREADTH);
    render(<OnboardOrgPage />);

    await fillRuleAndSubmit(user);

    await waitFor(() => expect(screen.getByTestId("onboard-org-breadth-warning")).toBeTruthy());
    expect(screen.getByTestId("onboard-org-breadth-warning").textContent).toMatch(
      /someone@notacme\.com/,
    );
    // The rejection is the question, not an error — the red alert stays away.
    expect(screen.queryByTestId("onboard-org-error")).toBeNull();
    expect(screen.getByTestId("onboard-org-accept-risk")).toBeTruthy();
  });

  it("sends the acceptance with the second attempt", async () => {
    const user = userEvent.setup();
    mockCreateOrg.mockRejectedValueOnce(BREADTH).mockResolvedValue({
      id: "carolco",
      name: "Carolco",
      provisioning_state: "ready",
    });
    render(<OnboardOrgPage />);

    await fillRuleAndSubmit(user);
    await waitFor(() => expect(screen.getByTestId("onboard-org-accept-risk")).toBeTruthy());
    await user.click(screen.getByTestId("onboard-org-accept-risk"));
    await user.click(screen.getByTestId("onboard-org-submit"));

    await waitFor(() => expect(mockCreateOrg).toHaveBeenCalledTimes(2));
    expect(mockCreateOrg.mock.calls[0][3]).toMatchObject({ riskAcknowledged: false });
    expect(mockCreateOrg.mock.calls[1][3]).toMatchObject({ riskAcknowledged: true });
  });

  it("withdraws the acceptance when the rule is edited", async () => {
    const user = userEvent.setup();
    mockCreateOrg.mockRejectedValue(BREADTH);
    render(<OnboardOrgPage />);

    await fillRuleAndSubmit(user);
    await waitFor(() => expect(screen.getByTestId("onboard-org-accept-risk")).toBeTruthy());
    await user.click(screen.getByTestId("onboard-org-accept-risk"));
    // A different rule admits a different set of people, so the answer to the old one is void.
    await user.type(screen.getByTestId("onboard-org-email-rule"), "x");

    expect(screen.queryByTestId("onboard-org-breadth-warning")).toBeNull();
    await user.click(screen.getByTestId("onboard-org-submit"));
    await waitFor(() => expect(mockCreateOrg).toHaveBeenCalledTimes(2));
    expect(mockCreateOrg.mock.calls[1][3]).toMatchObject({ riskAcknowledged: false });
  });

  it("reports any other create failure as an error", async () => {
    const user = userEvent.setup();
    mockCreateOrg.mockRejectedValue(
      new OrgError(
        409,
        "orgs.auto_join_rule_taken",
        "Another organization already auto-joins here.",
      ),
    );
    render(<OnboardOrgPage />);

    await fillRuleAndSubmit(user);

    await waitFor(() => expect(screen.getByTestId("onboard-org-error")).toBeTruthy());
    expect(screen.getByTestId("onboard-org-error").textContent).toMatch(/already auto-joins/);
    expect(screen.queryByTestId("onboard-org-breadth-warning")).toBeNull();
  });
});
