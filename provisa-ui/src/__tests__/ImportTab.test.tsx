// Copyright (c) 2026 Kenneth Stott
// Canary: 5b0e93c7-1a46-4d28-9f31-8c72d05ae4b6
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

import { describe, it, expect, vi, beforeEach } from "vitest";
import { render, screen, waitFor, fireEvent } from "../test-utils/render";
import { ImportTab } from "../components/admin/ImportTab";
import type { ImportPreview } from "../api/importer";

vi.mock("../api/importer", async () => {
  const actual = await vi.importActual<typeof import("../api/importer")>("../api/importer");
  return {
    ...actual,
    previewImport: vi.fn(),
    applyImport: vi.fn(),
    fileToBase64: vi.fn(async () => "Zm9v"),
  };
});

import { previewImport, applyImport } from "../api/importer";
const mockPreview = vi.mocked(previewImport);
const mockApply = vi.mocked(applyImport);

function preview(overrides: Partial<ImportPreview> = {}): ImportPreview {
  return {
    flavor: "hasura_v2",
    config_yaml: "sources:\n  - id: pg1\n",
    warnings: [{ category: "query_collections", message: "not converted", source_path: "" }],
    summary: {
      sources: 1,
      domains: 2,
      tables: 7,
      columns: 42,
      roles: 3,
      relationships: 4,
      rls_rules: 5,
      source_ids: ["pg1"],
      domain_ids: ["default", "sales"],
      role_ids: ["admin", "customer", "anon"],
    },
    ...overrides,
  };
}

/** Put a file on the hidden input FileButton renders, the way a picker would. */
function pickFile(name = "metadata.yaml") {
  const input = document.querySelector('input[type="file"]') as HTMLInputElement;
  const file = new File(["version: 3"], name, { type: "application/yaml" });
  Object.defineProperty(input, "files", { value: [file], configurable: true });
  fireEvent.change(input);
}

describe("ImportTab", () => {
  beforeEach(() => {
    mockPreview.mockReset();
    mockApply.mockReset();
    mockPreview.mockResolvedValue(preview());
    mockApply.mockResolvedValue({ summary: preview().summary, replace: false });
  });

  // REQ-1483: the conversion is a guess in places, so what it produced is shown before anything
  // is written — counts, warnings and the config itself.
  it("shows the summary and warnings after converting", async () => {
    render(<ImportTab />);
    pickFile();
    fireEvent.click(screen.getByRole("button", { name: "Convert and preview" }));

    await waitFor(() => expect(screen.getByText("Tables")).toBeInTheDocument());
    expect(screen.getByText("7")).toBeInTheDocument();
    expect(screen.getByText("query_collections")).toBeInTheDocument();
    expect(screen.getByText(/Conversion warnings \(1\)/)).toBeInTheDocument();
  });

  // The apply step is the approval: nothing reaches the org from the preview call itself.
  it("does not apply until the administrator applies", async () => {
    render(<ImportTab />);
    pickFile();
    fireEvent.click(screen.getByRole("button", { name: "Convert and preview" }));
    await waitFor(() => expect(screen.getByText("Tables")).toBeInTheDocument());
    expect(mockApply).not.toHaveBeenCalled();
  });

  // What is applied is what was reviewed — including edits made in the preview textarea.
  it("applies the edited YAML, not the server's original", async () => {
    render(<ImportTab />);
    pickFile();
    fireEvent.click(screen.getByRole("button", { name: "Convert and preview" }));
    await waitFor(() => expect(screen.getByText("Tables")).toBeInTheDocument());

    const editor = screen.getByLabelText("Generated configuration");
    fireEvent.change(editor, { target: { value: "sources:\n  - id: edited\n" } });
    fireEvent.click(screen.getByRole("button", { name: "Apply to this organization" }));

    await waitFor(() => expect(mockApply).toHaveBeenCalled());
    expect(mockApply).toHaveBeenCalledWith("sources:\n  - id: edited\n", false);
  });

  // Replace is destructive, so it is off unless the administrator turns it on.
  it("sends replace only when checked", async () => {
    render(<ImportTab />);
    pickFile();
    fireEvent.click(screen.getByRole("button", { name: "Convert and preview" }));
    await waitFor(() => expect(screen.getByText("Tables")).toBeInTheDocument());

    fireEvent.click(screen.getByLabelText("Replace the existing semantic layer"));
    fireEvent.click(screen.getByRole("button", { name: "Apply to this organization" }));

    await waitFor(() => expect(mockApply).toHaveBeenCalled());
    expect(mockApply.mock.calls[0][1]).toBe(true);
  });

  it("passes the domain mapping to the conversion", async () => {
    render(<ImportTab />);
    pickFile();
    fireEvent.click(screen.getByRole("button", { name: "Add mapping" }));
    fireEvent.change(screen.getByLabelText("Schema or subgraph"), { target: { value: "public" } });
    fireEvent.change(screen.getByLabelText("Domain"), { target: { value: "sales" } });
    fireEvent.click(screen.getByRole("button", { name: "Convert and preview" }));

    await waitFor(() => expect(mockPreview).toHaveBeenCalled());
    expect(mockPreview.mock.calls[0][0].domain_map).toEqual({ public: "sales" });
  });

  it("reports a conversion failure instead of showing a summary", async () => {
    mockPreview.mockRejectedValue(new Error("Import preview failed (400)"));
    render(<ImportTab />);
    pickFile();
    fireEvent.click(screen.getByRole("button", { name: "Convert and preview" }));

    await waitFor(() => expect(screen.getByText(/Import preview failed/)).toBeInTheDocument());
    expect(screen.queryByText("Tables")).not.toBeInTheDocument();
  });
});
