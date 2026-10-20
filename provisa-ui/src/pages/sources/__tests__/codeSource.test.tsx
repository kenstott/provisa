// Copyright (c) 2026 Kenneth Stott
// Canary: c01d8efb-d807-49f3-ab4f-e302f770f1ae
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-1984, REQ-1993: a Node or Python source is its code -- a package at an exact version (with a
// lockfile beside it), a Git repository at a commit, or an uploaded archive -- and, for Node, the
// runtime it runs on. What the form holds is the source's mapping, and an edit refills the form.
// REQ-2104: the org secrets its code may read, picked by name.
// REQ-2105: the monthly cap on what its calls to the LLM service cost.

import { useState } from "react";
import { describe, expect, it, vi } from "vitest";
import { fireEvent, renderInForm, screen, waitFor, within } from "../../../test-utils/render";

const auth = vi.hoisted(() => ({ activeOrgId: "acme" as string | null }));
vi.mock("../../../context/AuthContext", () => ({ useAuth: () => auth }));
vi.mock("../../../api/codeUploads", () => ({
  uploadCodeArchive: vi.fn().mockResolvedValue("code-hash-1"),
  uploadLockfile: vi.fn().mockResolvedValue("lock-sha-1"),
}));

vi.mock("../../../api/secrets", () => ({
  fetchSecrets: vi.fn().mockResolvedValue({
    provider: { key: "stored", label: "Stored", writable: true },
    secrets: [{ name: "openrouter" }, { name: "stripe" }],
  }),
}));

import { uploadCodeArchive, uploadLockfile } from "../../../api/codeUploads";
import { fetchSecrets } from "../../../api/secrets";
import { CodeSourceFields } from "../CodeSourceFields";
import { codeFieldsFromMapping, codeMappingJson, missingCodeFields } from "../codeSource";
import { NO_CONNECTION_TYPES, SOURCE_TYPES } from "../constants";

describe("the source pick list", () => {
  it("offers Node.js and Python as code sources, with no connection fields", () => {
    expect(SOURCE_TYPES.filter((s) => s.category === "Code").map((s) => s.value)).toEqual([
      "node",
      "python",
    ]);
    expect(NO_CONNECTION_TYPES.has("node") && NO_CONNECTION_TYPES.has("python")).toBe(true);
  });
});

describe("what a code source saves", () => {
  it("is a package at its version, with the lockfile given beside it", () => {
    const fields = {
      code_origin: "package",
      code_package: " acme-rates ",
      code_version: "1.2.0",
      lockfile_upload: "lock-sha-1",
      lockfile_filename: "requirements.txt",
      code_entry: "rates",
    };
    const mapping = JSON.parse(codeMappingJson("python", fields));
    expect(mapping).toEqual({
      runtime: "python",
      code: { kind: "pypi", package: "acme-rates", version: "1.2.0" },
      lockfile: { filename: "requirements.txt", upload: "lock-sha-1" },
      entry: "rates",
    });
    expect(codeFieldsFromMapping(JSON.stringify(mapping))).toMatchObject({
      code_origin: "package",
      code_package: "acme-rates",
      lockfile_upload: "lock-sha-1",
      code_entry: "rates",
    });
  });

  it("is a Git commit for Node on its runtime, with no lockfile beside kept code", () => {
    const fields = {
      code_runtime: "bun",
      code_origin: "git",
      code_repository: "https://github.com/acme/rates.git",
      code_commit: "a".repeat(40),
      code_subdirectory: "pkg",
      lockfile_upload: "stale",
    };
    expect(JSON.parse(codeMappingJson("node", fields))).toEqual({
      runtime: "bun",
      code: {
        kind: "git",
        repository: fields.code_repository,
        commit: fields.code_commit,
        subdirectory: "pkg",
      },
    });
  });

  it("declares org secrets by name, and keeps an ${env:...} reference as written", () => {
    const named = JSON.parse(
      codeMappingJson("python", {
        code_package: "acme-rates",
        code_version: "1.0.0",
        code_secrets: JSON.stringify({ openrouter: null }),
      }),
    );
    expect(named.secrets).toEqual(["openrouter"]);
    expect(codeFieldsFromMapping(JSON.stringify(named)).code_secrets).toBe(
      JSON.stringify({ openrouter: null }),
    );
    const written = { key: "${env:OPENROUTER_API_KEY}", openrouter: null };
    const both = JSON.parse(
      codeMappingJson("node", {
        ...codeFieldsFromMapping(JSON.stringify({ code: {}, secrets: written })),
      }),
    );
    expect(both.secrets).toEqual(written);
    expect(
      JSON.parse(codeMappingJson("python", { code_package: "a", code_version: "1" })),
    ).not.toHaveProperty("secrets");
  });

  it("keeps a monthly LLM cap, and none when the field is empty", () => {
    const capped = JSON.parse(
      codeMappingJson("python", { code_package: "a", code_version: "1", code_llm_cap: "12.5" }),
    );
    expect(capped.llm).toEqual({ cap_usd: 12.5 });
    expect(codeFieldsFromMapping(JSON.stringify(capped)).code_llm_cap).toBe("12.5");
    expect(
      codeFieldsFromMapping(JSON.stringify({ ...capped, llm: { cap_usd: 0 } })).code_llm_cap,
    ).toBe("0");
    expect(
      JSON.parse(
        codeMappingJson("python", { code_package: "a", code_version: "1", code_llm_cap: "" }),
      ),
    ).not.toHaveProperty("llm");
  });

  it("needs what its origin names, and a full commit", () => {
    expect(missingCodeFields({ code_origin: "package", code_package: "p" })).toEqual([
      "code_version",
    ]);
    expect(
      missingCodeFields({ code_origin: "git", code_repository: "r", code_commit: "v1.0" }),
    ).toEqual(["code_commit"]);
    expect(missingCodeFields({ code_origin: "upload", code_upload: "h" })).toEqual([]);
  });
});

function Harness({
  type,
  onFields,
}: {
  type: string;
  onFields: (f: Record<string, string>) => void;
}) {
  const [fields, setFields] = useState<Record<string, string>>({});
  return (
    <CodeSourceFields
      type={type}
      fields={fields}
      setFields={(f) => {
        onFields(f);
        setFields(f);
      }}
    />
  );
}

describe("the code source's fields", () => {
  it("offer Node's runtimes, and Python's entry module only for Python", () => {
    const { unmount } = renderInForm(<Harness type="node" onFields={() => {}} />);
    expect(screen.getByTestId("code-runtime")).toBeInTheDocument();
    expect(screen.queryByTestId("code-entry")).toBeNull();
    unmount();
    renderInForm(<Harness type="python" onFields={() => {}} />);
    expect(screen.queryByTestId("code-runtime")).toBeNull();
    expect(screen.getByTestId("code-entry")).toBeInTheDocument();
  });

  it("upload the archive and keep its content hash", async () => {
    const seen: Record<string, string>[] = [];
    renderInForm(<Harness type="python" onFields={(f) => seen.push(f)} />);
    fireEvent.click(screen.getByTestId("code-origin"));
    fireEvent.click(await screen.findByRole("option", { name: "Upload" }));
    expect(screen.getByTestId("code-archive")).toBeInTheDocument();
    const file = new File(["x"], "rates.zip", { type: "application/zip" });
    // Mantine keeps the file picker's own input beside the field; the only one shown here.
    const hidden = document.querySelector('input[type="file"]') as HTMLInputElement;
    fireEvent.change(hidden, { target: { files: [file] } });
    await waitFor(() => expect(uploadCodeArchive).toHaveBeenCalledWith("acme", file));
    await waitFor(() =>
      expect(seen.at(-1)).toMatchObject({
        code_upload: "code-hash-1",
        code_upload_filename: "rates.zip",
      }),
    );
  });

  it("pick the org secrets the code may read", async () => {
    const seen: Record<string, string>[] = [];
    renderInForm(<Harness type="python" onFields={(f) => seen.push(f)} />);
    await waitFor(() => expect(fetchSecrets).toHaveBeenCalledWith("acme", "org"));
    // Mantine MultiSelect in jsdom: floating-ui hides the detached dropdown (all rects are 0), so
    // the option is found through the input's aria-controls listbox with hidden: true.
    const picker = screen.getByTestId("code-secrets");
    fireEvent.click(picker);
    await waitFor(() => {
      if (!picker.getAttribute("aria-controls")) throw new Error("dropdown not open");
    });
    const listbox = document.getElementById(picker.getAttribute("aria-controls") as string);
    fireEvent.click(
      await within(listbox as HTMLElement).findByRole("option", {
        name: "openrouter",
        hidden: true,
      }),
    );
    await waitFor(() =>
      expect(seen.at(-1)).toMatchObject({
        code_secrets: JSON.stringify({ openrouter: null }),
      }),
    );
  });

  it("upload a lockfile beside a package", async () => {
    const seen: Record<string, string>[] = [];
    renderInForm(<Harness type="node" onFields={(f) => seen.push(f)} />);
    expect(screen.getByTestId("code-lockfile")).toBeInTheDocument();
    const file = new File(["{}"], "package-lock.json", { type: "application/json" });
    const hidden = document.querySelector('input[type="file"]') as HTMLInputElement;
    fireEvent.change(hidden, { target: { files: [file] } });
    await waitFor(() => expect(uploadLockfile).toHaveBeenCalledWith("acme", file));
    await waitFor(() =>
      expect(seen.at(-1)).toMatchObject({
        lockfile_upload: "lock-sha-1",
        lockfile_filename: "package-lock.json",
      }),
    );
  });
});
