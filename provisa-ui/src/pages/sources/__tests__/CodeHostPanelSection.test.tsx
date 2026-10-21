// Copyright (c) 2026 Kenneth Stott
// Canary: b4361e39-fc53-401f-a6ff-d3b9fce3e487
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-2098: a saved code source's Remote host panel is on the Connection tab, and on no other: a
// fold carries no section of its own, so the panel sits in the Connection section.

import { useState } from "react";
import { describe, expect, it, vi } from "vitest";
import { renderInForm, screen, waitFor } from "../../../test-utils/render";
import { FormSections } from "../../../components/form/FormLayout";
import { SOURCE_SECTIONS, type SourceSection } from "../sourceSections";

vi.mock("../../../context/AuthContext", () => ({ useAuth: () => ({ activeOrgId: "acme" }) }));
vi.mock("../../../api/codeUploads", () => ({
  uploadCodeArchive: vi.fn(),
  uploadLockfile: vi.fn(),
}));
vi.mock("../../../api/secrets", () => ({
  fetchSecrets: vi.fn().mockResolvedValue({
    provider: { key: "stored", label: "Stored", writable: true },
    secrets: [],
  }),
}));
vi.mock("../../../api/containerRegistries", () => ({
  fetchContainerRegistries: vi.fn().mockResolvedValue([]),
}));
vi.mock("../../../api/codeHosts", () => ({
  startBuild: vi.fn(),
  fetchBuild: vi.fn(),
  fetchBuilds: vi.fn().mockResolvedValue([]),
  archiveUrl: (o: string, s: string, b: string) => `/archive/${o}/${s}/${b}`,
  connectHost: vi.fn(),
  backToDev: vi.fn(),
  fetchHostStatus: vi.fn(),
  repin: vi.fn(),
  fetchExports: vi.fn().mockResolvedValue([]),
}));

import { CodeSourceFields } from "../CodeSourceFields";

function Saved({ active }: { active: SourceSection }) {
  const [fields, setFields] = useState<Record<string, string>>({
    code_origin: "package",
    code_mode: "dev",
    code_content_hash: "h".repeat(64),
  });
  return (
    <FormSections active={active} fallback="connection" onSections={() => {}}>
      <CodeSourceFields type="python" sourceId="rates" fields={fields} setFields={setFields} />
    </FormSections>
  );
}

describe("a saved code source's remote host panel", () => {
  it("is on the Connection tab", async () => {
    renderInForm(<Saved active="connection" />);
    expect(await screen.findByTestId("code-host")).toBeInTheDocument();
  });

  it.each(SOURCE_SECTIONS.filter((s) => s !== "connection"))("is not on the %s tab", async (s) => {
    renderInForm(<Saved active={s} />);
    await waitFor(() => expect(screen.queryByTestId("code-host")).toBeNull());
  });
});
