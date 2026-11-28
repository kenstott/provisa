// Copyright (c) 2026 Kenneth Stott
// Canary: 2c7f9e41-8d36-4b05-a1e8-5f3b6d920c47
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-1159 (amended 2026-11-27), REQ-1993, REQ-2226: a command written in the command form as
// entered code. Saving keeps the code as the entered code of the Python code source of the
// command's name (createSource, then updateSource) and registers the declared function as the
// command by naming it, sending no arguments or columns; the command opens with its declared
// arguments and columns, and editing it reopens its code.

import { afterEach, beforeEach, describe, it, expect, vi } from "vitest";
import { act, fireEvent, render, screen, within } from "../../test-utils/render";
import userEvent from "@testing-library/user-event";
import { editorOf, typeInto } from "../../test-utils/codeEditor";
import type { TrackedFunction } from "../../api/actions";
import type { Source } from "../../types/admin";

const CODE = 'def greet(name: str) -> str:\n    """@query"""\n    return name\n';
const CHANGED = 'def greet(word: str) -> str:\n    """@query"""\n    return word\n';

const greet = (): TrackedFunction =>
  ({
    name: "greet",
    sourceId: "greet",
    schemaName: "code",
    functionName: "greet",
    returns: "",
    arguments: [{ name: "name", type: "text", argKind: "column_value" }],
    outputColumns: [{ name: "result", type: "text" }],
    visibleTo: ["admin"],
    domainId: "sales",
    description: "",
    kind: "query",
    implKind: "code_command",
    binding: {},
    languageBindings: {},
  }) as unknown as TrackedFunction;

const greetSource = (code: string): Source =>
  ({
    id: "greet",
    type: "python",
    host: "",
    port: 0,
    database: "",
    path: null,
    description: "",
    allowedDomains: [],
    mappingJson: JSON.stringify({
      runtime: "python",
      code: { kind: "entered", source: code, dependencies: ["httpx==0.27.0"] },
      workers: 2,
    }),
    changeSignal: "ttl",
    loadProtected: false,
    offPeakWindow: null,
    offPeakTz: "UTC",
    cacheEnabled: true,
    cacheTtl: null,
    replicate: null,
    gqlNamingConvention: null,
    maxLiveConcurrency: null,
    sentinelPath: null,
    downloadFrom: null,
    freshnessGate: false,
  }) as unknown as Source;

const held = vi.hoisted(() => ({
  actions: { functions: [] as unknown[], webhooks: [] as unknown[] },
  sources: [] as unknown[],
  // What the server holds after a save; the sources are read again from it.
  saved: [] as unknown[],
  // The commands the acting role is served, by name: what the server's graph schema answers now.
  served: [] as string[],
}));
const servedCommand = (name: string) => ({
  command: name,
  domain_id: "sales",
  graphql_root: "Query",
  graphql_field: `sales__${name}`,
  names: { sql: name, cypher: name, rest: name },
});
const createSource = vi.fn();
const updateSource = vi.fn();
const NO_DOMAINS = vi.hoisted(() => ({ domains: [], loading: false, refetch: () => undefined }));
const SOURCES = vi.hoisted(() => ({
  get sources() {
    return held.sources;
  },
  loading: false,
  refetch: async () => ({ data: { sources: held.saved } }),
}));

vi.mock("../../api/actions", async (importOriginal) => ({
  ...(await importOriginal<typeof import("../../api/actions")>()),
  fetchActions: vi.fn(async () => held.actions),
  saveFunction: vi.fn().mockResolvedValue(undefined),
}));

vi.mock("../../api/admin", async (importOriginal) => ({
  ...(await importOriginal<typeof import("../../api/admin")>()),
  fetchOrgRoles: vi.fn().mockResolvedValue([{ id: "admin" }]),
}));

vi.mock("../../context/AuthContext", async (importOriginal) => ({
  ...(await importOriginal<typeof import("../../context/AuthContext")>()),
  useAuth: () => ({
    role: "admin",
    selectedRoles: ["admin"],
    actingRoles: "admin",
    capabilities: ["admin", "table_registration"],
    domainAccess: ["*"],
    activeOrgId: "acme",
  }),
}));

vi.mock("../../hooks/useAdminQueries", async (importOriginal) => ({
  ...(await importOriginal<typeof import("../../hooks/useAdminQueries")>()),
  useSources: () => SOURCES,
  useDomains: () => NO_DOMAINS,
  useCreateSource: () => ({ createSource, loading: false }),
  useUpdateSource: () => ({ updateSource, loading: false }),
}));

import { CommandsPage } from "../CommandsPage";
import { saveFunction } from "../../api/actions";

const panel = () => screen.getByTestId("commands-split-panel");
const mappingSent = (call: unknown[]) =>
  JSON.parse((call[0] as { mappingJson: string }).mappingJson) as {
    runtime: string;
    code: { kind: string; source: string; dependencies: string[] };
    workers?: number;
  };

beforeEach(() => {
  held.actions = { functions: [], webhooks: [] };
  held.sources = [];
  held.saved = [];
  createSource.mockReset().mockResolvedValue({ success: true, message: "" });
  updateSource.mockReset().mockResolvedValue({ success: true, message: "" });
  vi.mocked(saveFunction).mockClear();
  held.served = [];
  vi.stubGlobal(
    "fetch",
    vi.fn(async (url: string) =>
      String(url).startsWith("/data/graph-schema")
        ? new Response(
            JSON.stringify({
              node_labels: [],
              relationship_types: [],
              root_fields: [],
              commands: held.served.map(servedCommand),
            }),
            { status: 200 },
          )
        : new Response("{}", { status: 404 }),
    ),
  );
});
afterEach(() => vi.unstubAllGlobals());

describe("+ Command, Code (entered)", () => {
  it("saves the code as entered code, then registers the function it names", async () => {
    render(<CommandsPage />);
    await userEvent.click(await screen.findByTestId("commands-toggle-form"));
    fireEvent.change(within(panel()).getByLabelText(/^Name/), { target: { value: "greet" } });
    fireEvent.click(screen.getByTestId("command-impl-kind-select"));
    fireEvent.click(await screen.findByRole("option", { name: "Code (entered)", hidden: true }));
    fireEvent.change(screen.getByTestId("command-entered-function"), {
      target: { value: "greet" },
    });
    // In act: the save reads the form as the typing left it.
    act(() => typeInto(editorOf("command-entered-code"), CODE));
    // The server holds the source and the command registered from its declaration.
    held.saved = [greetSource(CODE)];
    held.actions = { functions: [greet()], webhooks: [] };
    fireEvent.submit(panel().querySelector("form")!);

    await vi.waitFor(() => expect(saveFunction).toHaveBeenCalledTimes(1));
    expect(createSource).toHaveBeenCalledTimes(1);
    expect(updateSource).not.toHaveBeenCalled();
    expect(createSource.mock.calls[0][0]).toMatchObject({ id: "greet", type: "python" });
    expect(mappingSent(createSource.mock.calls[0])).toEqual({
      runtime: "python",
      code: { kind: "entered", source: CODE, dependencies: [] },
    });
    const [command, existing] = vi.mocked(saveFunction).mock.calls[0];
    expect(existing).toBeNull();
    expect(command).toMatchObject({
      name: "greet",
      sourceId: "greet",
      schemaName: "code",
      functionName: "greet",
      implKind: "code_command",
      arguments: [],
      outputColumns: null,
    });
    // It opens in edit with its code, and what its declaration says.
    await vi.waitFor(() =>
      expect(screen.getByTestId("command-entered-arguments")).toHaveTextContent(
        "name: text (value)",
      ),
    );
    expect(screen.getByTestId("command-entered-columns")).toHaveTextContent("result: text");
    expect(screen.getByTestId("command-entered-kind")).toHaveTextContent("Query");
    expect(editorOf("command-entered-code").state.doc.toString()).toBe(CODE);
  });

  it("shows the saved command's Query it line at once, read from what the role is served now", async () => {
    // Another command is read first, so what the role is served has been read once already.
    held.actions = {
      functions: [{ ...greet(), name: "haversine", sourceId: "haversine" }],
      webhooks: [],
    };
    held.served = ["haversine"];
    render(<CommandsPage />);
    await userEvent.click(await screen.findByTestId("commands-fn-row-haversine"));
    expect(await screen.findByTestId("command-query-links")).toBeInTheDocument();

    await userEvent.click(screen.getByTestId("commands-toggle-form"));
    fireEvent.change(within(panel()).getByLabelText(/^Name/), { target: { value: "greet" } });
    fireEvent.click(screen.getByTestId("command-impl-kind-select"));
    fireEvent.click(await screen.findByRole("option", { name: "Code (entered)", hidden: true }));
    fireEvent.change(screen.getByTestId("command-entered-function"), {
      target: { value: "greet" },
    });
    act(() => typeInto(editorOf("command-entered-code"), CODE));
    // The server rebuilt what the role is served before it answered the save.
    held.saved = [greetSource(CODE)];
    held.actions = {
      functions: [{ ...greet(), name: "haversine", sourceId: "haversine" }, greet()],
      webhooks: [],
    };
    held.served = ["haversine", "greet"];
    fireEvent.submit(panel().querySelector("form")!);
    await vi.waitFor(() => expect(saveFunction).toHaveBeenCalledTimes(1));
    await screen.findByTestId("command-entered-arguments");
    // Leaving the edit shows the command read, with its Query it line.
    await userEvent.click(within(panel()).getByRole("button", { name: "Cancel" }));
    const line = await screen.findByTestId("command-query-links");
    expect(within(line).getByTestId("command-query-sql")).toHaveAccessibleName(/greet/);
  });

  it("does not register the command when its code is refused", async () => {
    createSource.mockResolvedValue({ success: false, message: "greet: cannot be read" });
    render(<CommandsPage />);
    await userEvent.click(await screen.findByTestId("commands-toggle-form"));
    fireEvent.change(within(panel()).getByLabelText(/^Name/), { target: { value: "greet" } });
    fireEvent.click(screen.getByTestId("command-impl-kind-select"));
    fireEvent.click(await screen.findByRole("option", { name: "Code (entered)", hidden: true }));
    fireEvent.submit(panel().querySelector("form")!);
    expect(await screen.findByText(/greet: cannot be read/)).toBeInTheDocument();
    expect(saveFunction).not.toHaveBeenCalled();
  });
});

describe("Editing a command written as entered code", () => {
  beforeEach(() => {
    held.actions = { functions: [greet()], webhooks: [] };
    held.sources = [greetSource(CODE)];
  });

  it("reopens its code, its dependencies and the function it calls", async () => {
    render(<CommandsPage />);
    await userEvent.click(await screen.findByTestId("commands-fn-row-greet"));
    await userEvent.click(screen.getByRole("button", { name: "Edit greet" }));
    expect(screen.getByTestId("command-impl-kind-select")).toHaveValue("Code (entered)");
    expect(editorOf("command-entered-code").state.doc.toString()).toBe(CODE);
    expect(screen.getByTestId("command-entered-dependencies")).toHaveValue("httpx==0.27.0");
    expect(screen.getByTestId("command-entered-function")).toHaveValue("greet");
  });

  it("saves changed code into its source, keeping the source's other settings", async () => {
    render(<CommandsPage />);
    await userEvent.click(await screen.findByTestId("commands-fn-row-greet"));
    await userEvent.click(screen.getByRole("button", { name: "Edit greet" }));
    act(() => typeInto(editorOf("command-entered-code"), CHANGED));
    held.saved = [greetSource(CHANGED)];
    fireEvent.submit(panel().querySelector("form")!);

    await vi.waitFor(() => expect(saveFunction).toHaveBeenCalledTimes(1));
    expect(createSource).not.toHaveBeenCalled();
    expect(updateSource.mock.calls[0][0]).toMatchObject({ id: "greet", type: "python" });
    expect(mappingSent(updateSource.mock.calls[0])).toEqual({
      runtime: "python",
      code: { kind: "entered", source: CHANGED, dependencies: ["httpx==0.27.0"] },
      workers: 2,
    });
    const [command, existing] = vi.mocked(saveFunction).mock.calls[0];
    expect(existing).toBe("greet");
    expect(command).toMatchObject({ sourceId: "greet", implKind: "code_command", arguments: [] });
  });
});
