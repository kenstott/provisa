// The SPARQL tab's exports are SPARQL's own: a graph answer (a CONSTRUCT's triples) as RDF --
// Turtle by default, N-Triples or JSON-LD -- and a SELECT's rows as SPARQL Results JSON, CSV or
// TSV. Each is the frame's query asked of /data/sparql again in that format, so the server's one
// writer writes it, with IRIs as SPARQL answers them.

import { describe, it, expect, vi, beforeEach } from "vitest";
import { fireEvent, render, screen, waitFor } from "@testing-library/react";
import { MantineProvider } from "@mantine/core";
import "../../../i18n";

const downloads: { blob: Blob; name: string }[] = [];
vi.mock("../graph-export", async (importOriginal) => ({
  ...(await importOriginal<typeof import("../graph-export")>()),
  downloadBlob: (blob: Blob, name: string) => downloads.push({ blob, name }),
}));

import { SparqlExportModal } from "../SparqlExportModal";
import type { SparqlExportable } from "../sparqlExport";

const CONSTRUCT = "CONSTRUCT { ?s ?p ?o } WHERE { ?s ?p ?o }";
const SELECT = "SELECT ?s WHERE { ?s ?p ?o }";
const sent: { url: string; init?: RequestInit }[] = [];

function modal(frames: SparqlExportable[], role: string | null = "analyst") {
  render(
    <MantineProvider>
      <SparqlExportModal frames={frames} roleHeader={role} onClose={() => {}} />
    </MantineProvider>,
  );
}

const accepts = () => sent.map((s) => (s.init?.headers as Record<string, string>).Accept);

describe("the SPARQL export", () => {
  beforeEach(() => {
    sent.length = 0;
    downloads.length = 0;
    vi.stubGlobal(
      "fetch",
      vi.fn(async (url: string, init?: RequestInit) => {
        sent.push({ url: String(url), init });
        const accept = (init?.headers as Record<string, string>).Accept;
        if (accept === "text/csv")
          return new Response(JSON.stringify({ error: "No CSV today" }), {
            status: 406,
            headers: { "content-type": "application/json" },
          });
        return new Response("body", { headers: { "content-type": accept } });
      }),
    );
  });

  it("writes a graph answer as Turtle by default, asking /data/sparql for it", async () => {
    modal([{ id: "a", query: CONSTRUCT, form: "graph" }]);
    const formats = screen.getByTestId("sparql-export-format");
    expect(formats).toHaveTextContent("Turtle");
    expect(formats).toHaveTextContent("N-Triples");
    expect(formats).toHaveTextContent("JSON-LD");
    expect(formats).not.toHaveTextContent("CSV");
    fireEvent.click(screen.getByTestId("sparql-export-download"));
    await waitFor(() => expect(downloads).toHaveLength(1));
    const post = sent[0];
    expect(post.url).toBe("/data/sparql");
    expect(post.init?.method).toBe("POST");
    expect(post.init?.body).toBe(CONSTRUCT);
    const headers = post.init?.headers as Record<string, string>;
    expect(headers["Content-Type"]).toBe("application/sparql-query");
    expect(headers.Accept).toBe("text/turtle");
    expect(headers["X-Provisa-Role"]).toBe("analyst");
    expect(downloads[0].name).toBe("sparql-result.ttl");
    expect(await downloads[0].blob.text()).toBe("body");
  });

  it.each([
    ["N-Triples", "application/n-triples", "sparql-result.nt"],
    ["JSON-LD", "application/ld+json", "sparql-result.jsonld"],
  ])("writes a graph answer as %s", async (label, media, name) => {
    modal([{ id: "a", query: CONSTRUCT, form: "graph" }]);
    fireEvent.click(screen.getByLabelText(label));
    fireEvent.click(screen.getByTestId("sparql-export-download"));
    await waitFor(() => expect(downloads).toHaveLength(1));
    expect(accepts()).toEqual([media]);
    expect(downloads[0].name).toBe(name);
  });

  it("writes a SELECT's rows as SPARQL results, never as RDF", async () => {
    modal([{ id: "b", query: SELECT, form: "rows" }]);
    const formats = screen.getByTestId("sparql-export-format");
    expect(formats).toHaveTextContent("SPARQL Results JSON");
    expect(formats).toHaveTextContent("CSV");
    expect(formats).toHaveTextContent("TSV");
    expect(formats).not.toHaveTextContent("Turtle");
    fireEvent.click(screen.getByTestId("sparql-export-download"));
    await waitFor(() => expect(downloads).toHaveLength(1));
    expect(accepts()).toEqual(["application/sparql-results+json"]);
    expect(downloads[0].name).toBe("sparql-result.json");
    fireEvent.click(screen.getByLabelText("TSV"));
    fireEvent.click(screen.getByTestId("sparql-export-download"));
    await waitFor(() => expect(downloads).toHaveLength(2));
    expect(accepts()[1]).toBe("text/tab-separated-values");
    expect(downloads[1].name).toBe("sparql-result.tsv");
  });

  it("says a refusal as the server words it, and downloads nothing", async () => {
    modal([{ id: "b", query: SELECT, form: "rows" }]);
    fireEvent.click(screen.getByLabelText("CSV"));
    fireEvent.click(screen.getByTestId("sparql-export-download"));
    expect(await screen.findByTestId("sparql-export-error")).toHaveTextContent("No CSV today");
    expect(downloads).toHaveLength(0);
  });

  it("exports the newest result, or the one chosen, in its own form's formats", async () => {
    modal([
      { id: "b", query: SELECT, form: "rows" },
      { id: "a", query: CONSTRUCT, form: "graph" },
    ]);
    expect(screen.getByTestId("sparql-export-format")).toHaveTextContent("CSV");
    fireEvent.click(screen.getByTestId("sparql-export-frame"));
    fireEvent.click(await screen.findByRole("option", { name: CONSTRUCT }));
    expect(screen.getByTestId("sparql-export-format")).toHaveTextContent("Turtle");
    fireEvent.click(screen.getByTestId("sparql-export-download"));
    await waitFor(() => expect(downloads).toHaveLength(1));
    expect(sent[0].init?.body).toBe(CONSTRUCT);
    expect(accepts()).toEqual(["text/turtle"]);
  });

  it("sends no role header when none is chosen", async () => {
    modal([{ id: "a", query: CONSTRUCT, form: "graph" }], null);
    fireEvent.click(screen.getByTestId("sparql-export-download"));
    await waitFor(() => expect(downloads).toHaveLength(1));
    expect(sent[0].init?.headers).not.toHaveProperty("X-Provisa-Role");
  });

  it("says there is nothing to export before a query is answered", () => {
    modal([]);
    expect(screen.getByTestId("sparql-export-empty")).toBeInTheDocument();
    expect(screen.queryByTestId("sparql-export-download")).toBeNull();
  });
});
