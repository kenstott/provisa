// Copyright (c) 2026 Kenneth Stott
// Canary: 2b6f9d14-e8a3-4c70-95d1-a74e0c3b86f2
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-2237: Import ontology. The steward picks a file (an ontology, or a whole release as a zip),
// its format where the name does not say it, and their domains; the imports are listed with their
// items and alignments counted by status and what was left out of each file; picking one opens
// its review.

import { describe, it, expect, vi, beforeEach } from "vitest";
import { fireEvent, render, screen, waitFor, within } from "../test-utils/render";
import { IMPORT, REVIEW } from "./ontologyImportFixtures";

const api = vi.hoisted(() => ({
  listOntologyImports: vi.fn(),
  fetchOntologyImport: vi.fn(),
  importOntology: vi.fn(),
}));
vi.mock("../api/ontologyImport", async (importOriginal) => ({
  ...(await importOriginal<typeof import("../api/ontologyImport")>()),
  ...api,
}));

import { ImportOntologyDialog } from "../components/admin/ontologyImport/ImportOntologyDialog";
import { OntologyImportsView } from "../components/admin/ontologyImport/OntologyImportsView";

const DOMAINS = [
  { id: "sales", label: "sales" },
  { id: "finance", label: "finance" },
];

async function choose(testId: string, option: string) {
  const input = screen.getByTestId(testId);
  fireEvent.click(input);
  await waitFor(() => expect(input).toHaveAttribute("aria-controls"));
  const list = document.getElementById(input.getAttribute("aria-controls") as string);
  fireEvent.click(within(list as HTMLElement).getByRole("option", { name: option, hidden: true }));
}

function pick(file: File) {
  const input = document.querySelector<HTMLInputElement>('input[type="file"]');
  if (!input) throw new Error("no file input");
  fireEvent.change(input, { target: { files: [file] } });
}

beforeEach(() => {
  Object.values(api).forEach((fn) => fn.mockReset());
  api.listOntologyImports.mockResolvedValue([IMPORT]);
  api.fetchOntologyImport.mockResolvedValue(REVIEW);
});

describe("ImportOntologyDialog", () => {
  it("offers ontology files and a whole release as a zip", () => {
    render(
      <ImportOntologyDialog
        domainOptions={DOMAINS}
        domainsEnabled
        defaultDomains={["sales"]}
        onClose={() => undefined}
        onImported={() => undefined}
      />,
    );
    const accept = document.querySelector<HTMLInputElement>('input[type="file"]')?.accept ?? "";
    for (const ext of [".zip", ".ttl", ".owl", ".rdf", ".nt", ".jsonld", ".yaml"])
      expect(accept.split(",")).toContain(ext);
  });

  it("imports the file in its format and the steward's domains, then opens the review", async () => {
    api.importOntology.mockResolvedValue(REVIEW);
    const onImported = vi.fn();
    render(
      <ImportOntologyDialog
        domainOptions={DOMAINS}
        domainsEnabled
        defaultDomains={["sales"]}
        onClose={() => undefined}
        onImported={onImported}
      />,
    );
    expect(screen.getByTestId("ontology-import-run")).toBeDisabled();
    const file = new File(["@prefix : <x#> ."], "accounts.owl", { type: "text/plain" });
    pick(file);
    await choose("ontology-import-format", "Turtle");
    fireEvent.click(screen.getByTestId("ontology-import-run"));
    await waitFor(() => expect(onImported).toHaveBeenCalledWith(REVIEW));
    expect(api.importOntology).toHaveBeenCalledWith(file, "turtle", ["sales"]);
  });

  it("sends no format when the file's name says it, and no domains in single-domain mode", async () => {
    api.importOntology.mockResolvedValue(REVIEW);
    render(
      <ImportOntologyDialog
        domainOptions={[]}
        domainsEnabled={false}
        defaultDomains={[]}
        onClose={() => undefined}
        onImported={() => undefined}
      />,
    );
    const file = new File(["PK"], "fibo-release.zip", { type: "application/zip" });
    pick(file);
    fireEvent.click(screen.getByTestId("ontology-import-run"));
    await waitFor(() => expect(api.importOntology).toHaveBeenCalledWith(file, null, null));
  });

  it("says why the server refused the file, and stays open", async () => {
    api.importOntology.mockRejectedValue(
      new Error("'notes.txt' names no ontology format: give its format"),
    );
    const onImported = vi.fn();
    render(
      <ImportOntologyDialog
        domainOptions={DOMAINS}
        domainsEnabled
        defaultDomains={["sales"]}
        onClose={() => undefined}
        onImported={onImported}
      />,
    );
    pick(new File(["x"], "notes.txt"));
    fireEvent.click(screen.getByTestId("ontology-import-run"));
    expect(await screen.findByTestId("ontology-import-error")).toHaveTextContent(
      "names no ontology format",
    );
    expect(onImported).not.toHaveBeenCalled();
  });
});

describe("OntologyImportsView", () => {
  const view = () =>
    render(
      <OntologyImportsView
        terms={[]}
        domainOptions={["sales", "finance"]}
        domainsEnabled
        defaultDomains={["sales"]}
      />,
    );

  it("lists each import with its items and alignments by status, and what was left out", async () => {
    view();
    const row = await screen.findByTestId("ontology-import-row-7");
    expect(row).toHaveTextContent("FIBO Accounts");
    expect(row).toHaveTextContent("Accounts.ttl");
    expect(row).toHaveTextContent("Turtle");
    expect(screen.getByTestId("ontology-import-items-7")).toHaveTextContent("3 pending");
    expect(screen.getByTestId("ontology-import-items-7")).toHaveTextContent("1 excluded");
    expect(screen.getByTestId("ontology-import-alignments-7")).toHaveTextContent("3 proposed");
    expect(screen.getByTestId("ontology-import-alignments-7")).toHaveTextContent("1 approved");
    expect(screen.getByTestId("ontology-import-left-out-7")).toHaveTextContent("3");
  });

  it("opens an import's review in the detail panel, with what was left out by why", async () => {
    view();
    fireEvent.click(await screen.findByTestId("ontology-import-row-7"));
    await waitFor(() => expect(api.fetchOntologyImport).toHaveBeenCalledWith(7));
    const leftOut = await screen.findByTestId("ontology-import-summary");
    expect(leftOut).toHaveTextContent("2 restrictions");
    expect(leftOut).toHaveTextContent("1 imported modules");
    expect(leftOut).toHaveTextContent("4 items, 3 statements");
  });

  it("opens Import ontology, and the new import's review once it is read", async () => {
    api.importOntology.mockResolvedValue(REVIEW);
    api.listOntologyImports.mockResolvedValueOnce([]).mockResolvedValue([IMPORT]);
    view();
    expect(await screen.findByTestId("ontology-imports-empty")).toBeInTheDocument();
    fireEvent.click(screen.getByTestId("ontology-import-btn"));
    pick(new File(["x"], "Accounts.ttl"));
    fireEvent.click(screen.getByTestId("ontology-import-run"));
    expect(await screen.findByTestId("ontology-review")).toBeInTheDocument();
    expect(screen.queryByTestId("ontology-import-dialog")).toBeNull();
  });
});
