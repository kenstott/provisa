// Copyright (c) 2026 Kenneth Stott
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-1558, REQ-1361: Secrets shares the Security section with the deployment-wide tabs but not
// their capability. What is tested here is that the two sets stay apart — a platform administrator
// is not shown an org's secrets, and an org administrator is not shown the deployment's posture,
// encryption, authentication or local users.

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
vi.mock("../components/admin/SecretsTab", () => ({ SecretsTab: () => <div>secrets panel</div> }));

const DEPLOYMENT = ["posture", "encryption", "authentication", "localUsers"];

function tabs() {
  return DEPLOYMENT.concat("secrets").filter((k) => screen.queryByTestId(`security-tab-${k}`));
}

describe("Security section membership", () => {
  beforeEach(() => {
    auth.capabilities = ["platform_settings"];
  });

  it("shows a platform administrator the deployment tabs and no secrets", () => {
    render(<SecurityManager allRoles={[]} allDomains={[]} />);
    expect(tabs()).toEqual(DEPLOYMENT);
    expect(screen.getByText("posture panel")).toBeInTheDocument();
  });

  it("shows an org administrator secrets and none of the deployment tabs", () => {
    auth.capabilities = ["org_settings"];
    render(<SecurityManager allRoles={[]} allDomains={[]} />);
    expect(tabs()).toEqual(["secrets"]);
    expect(screen.getByText("secrets panel")).toBeInTheDocument();
  });

  it("opens the secrets sub-tab the /admin/secrets route deep-links to", () => {
    auth.capabilities = ["org_settings", "platform_settings"];
    render(<SecurityManager allRoles={[]} allDomains={[]} initialTab="secrets" />);
    expect(tabs()).toEqual(DEPLOYMENT.concat("secrets"));
    expect(screen.getByText("secrets panel")).toBeInTheDocument();
  });

  it("ignores a deep link to a sub-tab the person may not see", () => {
    auth.capabilities = ["org_settings"];
    render(<SecurityManager allRoles={[]} allDomains={[]} initialTab="encryption" />);
    expect(screen.queryByText("encryption panel")).not.toBeInTheDocument();
    expect(screen.getByText("secrets panel")).toBeInTheDocument();
  });
});
