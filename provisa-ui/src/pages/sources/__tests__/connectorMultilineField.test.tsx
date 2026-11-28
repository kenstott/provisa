// Copyright (c) 2026 Kenneth Stott
// Canary: 3fc27929-a8a7-430d-b258-06a02f489dff
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-1984: a connector's multi-line value is a plain text area. A connector takes no script
// (REQ-2225, amended 2026-11-25), so no value opens the code editor.

import { describe, expect, it, vi } from "vitest";
import { renderInForm, screen } from "../../../test-utils/render";

vi.mock("../../../context/AuthContext", () => ({ useAuth: () => ({ activeOrgId: "acme" }) }));
vi.mock("../../../api/secrets", () => ({ fetchSecrets: vi.fn(async () => ({ secrets: [] })) }));
vi.mock("../../../api/sparkCluster", () => ({
  fetchSparkSource: vi.fn(async () => ({
    host: {},
    default_cluster: { origin: "local_test", label: "local test cluster", target: "127.0.0.1" },
  })),
}));

import type { Connector } from "../../../api/connectors";
import { ConnectorSetupFields } from "../ConnectorSetupFields";

const LEDGER: Connector = {
  name: "ledger",
  git: "https://git.example/ledger.git",
  ref: "main",
  commit: "a".repeat(40),
  language: "python",
  image: "registry.example/data/ledger",
  digest: `sha256:${"b".repeat(64)}`,
  code_hash: "c".repeat(64),
  arch: "amd64",
  requires: [{ name: "notes", multiline: true }],
  description: "",
  icon: "",
  settings: [],
};

const FIELDS = {
  connector_host: "ledger-host.acme.internal",
  connector_port: "7443",
  connector_pairing_code: "fp.secret",
};

describe("a connector's multi-line value", () => {
  it("is a text area, not the code editor", () => {
    renderInForm(
      <ConnectorSetupFields connector={LEDGER} fields={FIELDS} setFields={() => undefined} />,
    );
    const notes = screen.getByTestId("connector-require-notes");
    expect(notes.tagName).toBe("TEXTAREA");
    expect(notes.closest(".cm-editor")).toBeNull();
  });
});
