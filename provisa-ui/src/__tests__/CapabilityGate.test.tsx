// Copyright (c) 2026 Kenneth Stott
// Canary: f8be57dd-826e-4cdd-863b-83a5f63a018c
// Canary: placeholder
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

import { describe, it, expect, vi, beforeEach } from "vitest";
import { screen, waitFor } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { render } from "../test-utils/render";
import { CapabilityGate } from "../components/CapabilityGate";
import type { Capability } from "../types/auth";

// REQ-1430: the gate reads `loading` and the resolved capability set off the auth context. Stub the
// context rather than mounting a provider, which would fetch /auth/me. The capabilities are the
// real ones the gate decides on -- the decision itself is not mocked, because REQ-1361 turns on
// which set answers which entry.
const authState: { loading: boolean; capabilities: string[]; demonstrated: string[] } = {
  loading: false,
  capabilities: [],
  // REQ-1602: rights the caller is SHOWN without holding. Empty for every case below except the
  // demonstration ones -- a role that simply lacks a right is shown nothing.
  demonstrated: [],
};
vi.mock("../context/AuthContext", () => ({
  useAuth: () => authState,
}));

/** Grant `caps` for the next render. */
function holding(...caps: string[]) {
  authState.capabilities = caps;
}

describe("CapabilityGate", () => {
  beforeEach(() => {
    authState.loading = false;
    authState.capabilities = [];
    authState.demonstrated = [];
  });

  it("says credentials are being checked while the bootstrap is in flight", () => {
    authState.loading = true;
    holding("usage");

    render(
      <CapabilityGate
        capability={"observability" as Capability}
        fallback={<div>Not Authorized</div>}
      >
        <span>Reports</span>
      </CapabilityGate>,
    );

    expect(screen.getByTestId("capability-gate-checking")).toBeInTheDocument();
    expect(screen.queryByText("Not Authorized")).not.toBeInTheDocument();
    expect(screen.queryByText("Reports")).not.toBeInTheDocument();
  });

  it("stays silent while loading when it has no fallback", () => {
    // An inline gate — a nav link, a toolbar button — has no region of its own to fill. A spinner
    // per gate covers the page in them.
    authState.loading = true;
    holding("usage");

    const { container } = render(
      <CapabilityGate capability={"admin" as Capability}>
        <span>Admin Only</span>
      </CapabilityGate>,
    );

    expect(screen.queryByTestId("capability-gate-checking")).not.toBeInTheDocument();
    expect(screen.queryByText("Admin Only")).not.toBeInTheDocument();
    expect(container.querySelectorAll(":not(style)")).toHaveLength(0);
  });

  it("shows the denial only once the bootstrap has settled", () => {
    authState.loading = false;
    holding("usage");

    render(
      <CapabilityGate
        capability={"observability" as Capability}
        fallback={<div>Not Authorized</div>}
      >
        <span>Reports</span>
      </CapabilityGate>,
    );

    expect(screen.queryByTestId("capability-gate-checking")).not.toBeInTheDocument();
    expect(screen.getByText("Not Authorized")).toBeInTheDocument();
  });

  it("renders children when the capability is allowed", () => {
    holding("admin");

    render(
      <CapabilityGate capability={"query_development" as Capability}>
        <span>Protected Content</span>
      </CapabilityGate>,
    );

    expect(screen.getByText("Protected Content")).toBeInTheDocument();
  });

  it("renders nothing when capability is not allowed and no fallback provided", () => {
    holding("usage");

    const { container } = render(
      <CapabilityGate capability={"admin" as Capability}>
        <span>Admin Only</span>
      </CapabilityGate>,
    );

    expect(screen.queryByText("Admin Only")).not.toBeInTheDocument();
    // The gate contributes no markup of its own; the provider's injected <style> tags are all
    // that remain in the container.
    expect(container.querySelectorAll(":not(style)")).toHaveLength(0);
  });

  it("renders fallback when capability is not allowed and fallback provided", () => {
    holding("usage");

    render(
      <CapabilityGate
        capability={"access_config" as Capability}
        fallback={<div>Not Authorized</div>}
      >
        <span>Security Settings</span>
      </CapabilityGate>,
    );

    expect(screen.queryByText("Security Settings")).not.toBeInTheDocument();
    expect(screen.getByText("Not Authorized")).toBeInTheDocument();
  });

  it("does not render fallback when capability is allowed", () => {
    holding("admin");

    render(
      <CapabilityGate
        capability={"source_registration" as Capability}
        fallback={<div>Not Authorized</div>}
      >
        <span>Sources</span>
      </CapabilityGate>,
    );

    expect(screen.getByText("Sources")).toBeInTheDocument();
    expect(screen.queryByText("Not Authorized")).not.toBeInTheDocument();
  });

  it("decides on the capability it was given, not on another the caller happens to hold", () => {
    holding("observability");

    render(
      <CapabilityGate capability={"approve_view" as Capability}>
        <span>Approvals</span>
      </CapabilityGate>,
    );

    expect(screen.queryByText("Approvals")).not.toBeInTheDocument();
  });

  // REQ-1361: `strict` refuses the platform wildcard, and `orCapability` opens the same entry on a
  // second right. An org's secrets are the case both exist for: the server refuses `admin` there by
  // name, and what a platform admin does reach at that route is the deployment's secrets service.
  it("refuses the platform wildcard on a strict gate", () => {
    holding("admin");

    const { container } = render(
      <CapabilityGate capability={"org_settings" as Capability} strict>
        <span>Org Secrets</span>
      </CapabilityGate>,
    );

    expect(screen.queryByText("Org Secrets")).not.toBeInTheDocument();
    expect(container.querySelectorAll(":not(style)")).toHaveLength(0);
  });

  it("admits the literal right on a strict gate", () => {
    holding("org_settings");

    render(
      <CapabilityGate capability={"org_settings" as Capability} strict>
        <span>Org Secrets</span>
      </CapabilityGate>,
    );

    expect(screen.getByText("Org Secrets")).toBeInTheDocument();
  });

  // REQ-1602: a right the role is shown but does not hold keeps its surface on the page, inert and
  // badged as belonging to the production system. The sandbox (REQ-1597) is org_admin minus six
  // rights, and a visitor holding it is being shown the product -- a feature that is simply absent
  // demonstrates nothing.
  describe("a demonstrated right", () => {
    it("renders the surface it withholds, inert and badged", () => {
      holding("usage");
      authState.demonstrated = ["user_management"];

      render(
        <CapabilityGate capability={"user_management" as Capability}>
          <button>Invite</button>
        </CapabilityGate>,
      );

      expect(screen.getByText("Invite")).toBeInTheDocument();
      expect(screen.getByTestId("demonstrated-badge")).toBeInTheDocument();
      expect(screen.getByTestId("demonstrated-children")).toHaveAttribute("aria-disabled", "true");
    });

    it("takes the fallback's place, so a route body demonstrates instead of refusing", () => {
      holding("usage");
      authState.demonstrated = ["observability"];

      render(
        <CapabilityGate
          capability={"observability" as Capability}
          fallback={<div>Not Authorized</div>}
        >
          <span>Reports</span>
        </CapabilityGate>,
      );

      expect(screen.getByText("Reports")).toBeInTheDocument();
      expect(screen.queryByText("Not Authorized")).not.toBeInTheDocument();
      expect(screen.getByTestId("demonstrated-banner")).toBeInTheDocument();
      expect(screen.getByTestId("demonstrated-children")).toHaveAttribute("aria-disabled", "true");
    });

    it("leaves a link into the region clickable", () => {
      // The nav entry is not the demonstration; the page it opens is. An entry that refuses its own
      // click leaves the visitor unable to see the feature at all.
      holding("usage");
      authState.demonstrated = ["environment_management"];

      render(
        <CapabilityGate capability={"environment_management" as Capability} navigable>
          <a href="/admin/environments">Environments</a>
        </CapabilityGate>,
      );

      expect(screen.getByText("Environments")).toBeInTheDocument();
      expect(screen.getByTestId("demonstrated-badge")).toBeInTheDocument();
      expect(screen.queryByTestId("demonstrated-children")).not.toBeInTheDocument();
    });

    // REQ-1624: the banner says the surface is not available; what it does not say is what the
    // surface would have been FOR. A right carrying an explanation opens it over the region on
    // arrival, so a visitor learns what they are looking at rather than only that it is closed.
    it("explains the purpose of a right that has an explanation", async () => {
      holding("usage");
      authState.demonstrated = ["environment_management"];

      render(
        <CapabilityGate
          capability={"environment_management" as Capability}
          fallback={<div>Not Authorized</div>}
        >
          <span>Environments</span>
        </CapabilityGate>,
      );

      const modal = await screen.findByTestId("demonstrated-purpose-modal");
      expect(modal).toHaveTextContent("separately governed copy of your model");
      // The region beneath is still the real one, still inert.
      expect(screen.getByTestId("demonstrated-children")).toHaveAttribute("aria-disabled", "true");
    });

    it("closes the explanation and leaves the region standing", async () => {
      holding("usage");
      authState.demonstrated = ["environment_management"];

      render(
        <CapabilityGate
          capability={"environment_management" as Capability}
          fallback={<div>Not Authorized</div>}
        >
          <span>Environments</span>
        </CapabilityGate>,
      );

      await userEvent.click(await screen.findByRole("button", { name: "Got it" }));

      // The Modal root outlives its own transition; what closes is the dialog inside it.
      await waitFor(() => expect(screen.queryByRole("dialog")).not.toBeInTheDocument());
      expect(screen.getByTestId("demonstrated-banner")).toBeInTheDocument();
    });

    it("opens no explanation for a right that has none", () => {
      // An absent entry is not an empty dialog: the modal appears only where something was
      // written to say.
      holding("usage");
      authState.demonstrated = ["observability"];

      render(
        <CapabilityGate
          capability={"observability" as Capability}
          fallback={<div>Not Authorized</div>}
        >
          <span>Reports</span>
        </CapabilityGate>,
      );

      expect(screen.queryByTestId("demonstrated-purpose-modal")).not.toBeInTheDocument();
      expect(screen.getByTestId("demonstrated-banner")).toBeInTheDocument();
    });

    it("says nothing about a right that is held", () => {
      // Being shown what you can already use explains nothing; the badge would only be wrong.
      holding("observability");
      authState.demonstrated = ["observability"];

      render(
        <CapabilityGate capability={"observability" as Capability}>
          <span>Reports</span>
        </CapabilityGate>,
      );

      expect(screen.getByText("Reports")).toBeInTheDocument();
      expect(screen.queryByTestId("demonstrated-badge")).not.toBeInTheDocument();
    });

    it("waits for the bootstrap before demonstrating anything", () => {
      // Mid-bootstrap the caller holds nothing yet, and badging every gate on the page as a
      // production feature would be a claim about rights nobody has resolved.
      authState.loading = true;
      authState.demonstrated = ["user_management"];

      const { container } = render(
        <CapabilityGate capability={"user_management" as Capability}>
          <button>Invite</button>
        </CapabilityGate>,
      );

      expect(screen.queryByTestId("demonstrated-badge")).not.toBeInTheDocument();
      expect(container.querySelectorAll(":not(style)")).toHaveLength(0);
    });

    it("demonstrates on either half of a two-right gate", () => {
      // The surface is the same surface whichever right would have opened it.
      holding("usage");
      authState.demonstrated = ["platform_settings"];

      render(
        <CapabilityGate
          capability={"org_settings" as Capability}
          strict
          orCapability={"platform_settings" as Capability}
        >
          <span>Secrets service</span>
        </CapabilityGate>,
      );

      expect(screen.getByText("Secrets service")).toBeInTheDocument();
      expect(screen.getByTestId("demonstrated-badge")).toBeInTheDocument();
    });
  });

  it("opens on the second right when the strict one is absent", () => {
    holding("admin", "platform_settings");

    render(
      <CapabilityGate
        capability={"org_settings" as Capability}
        strict
        orCapability={"platform_settings" as Capability}
      >
        <span>Secrets service</span>
      </CapabilityGate>,
    );

    expect(screen.getByText("Secrets service")).toBeInTheDocument();
  });
});
