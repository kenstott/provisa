// Copyright (c) 2026 Kenneth Stott
// Canary: b79bab43-071b-48df-89c5-b1278b075081
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-1361, REQ-1560: the Security section describes the DEPLOYMENT — its posture, encryption,
// authentication and local users — and nothing else. Secrets used to sit here as a fifth sub-tab
// carrying a second capability; REQ-1560 gave each vault its own surface, so what is tested here is
// that this section is four deployment tabs and that a deep link lands on the one it names.

import { describe, it, expect, vi, beforeEach } from "vitest";
import { render, screen } from "../test-utils/render";
import { SecurityManager } from "../components/admin/SecurityManager";

const auth = { capabilities: ["platform_settings"] as string[] };
vi.mock("../context/AuthContext", () => ({ useAuth: () => auth }));

// The panels themselves each fetch their own settings; this file is about which tabs exist.
vi.mock("../components/admin/SecurityTab", () => ({ SecurityTab: () => <div>posture panel</div> }));
vi.mock("../components/admin/EncryptionTab", () => ({
  EncryptionTab: () => <div>encryption panel</div>,
}));
vi.mock("../components/admin/AuthTab", () => ({ AuthTab: () => <div>auth panel</div> }));
vi.mock("../components/admin/LocalUsersTab", () => ({
  LocalUsersTab: () => <div>local users panel</div>,
}));

const DEPLOYMENT = ["posture", "encryption", "authentication", "localUsers"];

function tabs() {
  return DEPLOYMENT.filter((k) => screen.queryByTestId(`security-tab-${k}`));
}

describe("Security section membership", () => {
  beforeEach(() => {
    auth.capabilities = ["platform_settings"];
  });

  it("is the four deployment tabs, and secrets is not among them", () => {
    render(<SecurityManager allRoles={[]} allDomains={[]} />);
    expect(tabs()).toEqual(DEPLOYMENT);
    expect(screen.queryByTestId("security-tab-secrets")).toBeNull();
    expect(screen.getByText("posture panel")).toBeInTheDocument();
  });

  it("opens the sub-tab a legacy route deep-links to", () => {
    render(<SecurityManager allRoles={[]} allDomains={[]} initialTab="encryption" />);
    expect(screen.getByText("encryption panel")).toBeInTheDocument();
    expect(screen.queryByText("posture panel")).not.toBeInTheDocument();
  });

  it("opens on posture when no route named a sub-tab", () => {
    render(<SecurityManager allRoles={[]} allDomains={[]} />);
    expect(screen.getByText("posture panel")).toBeInTheDocument();
  });
});
