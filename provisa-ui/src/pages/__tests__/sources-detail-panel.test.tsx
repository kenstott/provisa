// Copyright (c) 2026 Kenneth Stott
// Canary: 8f50ab9d-0750-42bc-85dd-687949473530
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// The Sources page shows a source's detail in a panel at the right of the list, one at a time,
// as the Tables page does: a row selects, another row swaps, the row or the panel's close
// closes. A source's actions are in the panel's head. "+ Source" asks only what a source needs
// in order to exist; once saved, the new source is selected and its detail opens in edit.

import { beforeEach, describe, it, expect, vi } from "vitest";
import { render, screen, waitFor, within } from "../../test-utils/render";
import userEvent from "@testing-library/user-event";
import type { MutationResult, Source } from "../../types/admin";

vi.mock("../../api/admin", async (importOriginal) => ({
  ...(await importOriginal<typeof import("../../api/admin")>()),
  fetchSettings: vi.fn().mockResolvedValue({
    redirect: { enabled: false, threshold: 10000, default_format: "json", ttl: 3600 },
    sampling: { default_sample_size: 1000 },
    cache: { default_ttl: 300 },
    naming: { domain_prefix: false, convention: "none" },
  }),
  fetchSourceReach: vi.fn().mockResolvedValue(null),
}));

vi.mock("../../context/AuthContext", async (importOriginal) => ({
  ...(await importOriginal<typeof import("../../context/AuthContext")>()),
  useAuth: () => ({
    role: "admin",
    selectedRoles: ["admin"],
    capabilities: ["admin"],
    domainAccess: ["*"],
  }),
}));

const ok: MutationResult = { success: true, message: "" };
const source = (over: Partial<Source>): Source =>
  ({
    id: "sales-pg",
    type: "postgresql",
    host: "db",
    port: 5432,
    database: "sales",
    username: "svc",
    path: "",
    cacheEnabled: true,
    cacheTtl: null,
    changeSignal: "ttl",
    mappingJson: null,
    passwordRef: "pw",
    ...over,
  }) as unknown as Source;

let SOURCES: Source[] = [];
const createSource = vi.fn();
const refetch = vi.fn();

vi.mock("../../hooks/useAdminQueries", async (importOriginal) => ({
  ...(await importOriginal<typeof import("../../hooks/useAdminQueries")>()),
  useSources: () => ({ sources: SOURCES, loading: false, refetch }),
  useDomains: () => ({ domains: [], loading: false, refetch: vi.fn() }),
  useCreateSource: () => ({ createSource }),
  useUpdateSourceCache: () => ({ updateSourceCache: vi.fn().mockResolvedValue(ok) }),
  useUpdateSourceReplicate: () => ({ updateSourceReplicate: vi.fn().mockResolvedValue(ok) }),
  useUpdateSourceLoadProtection: () => ({
    updateSourceLoadProtection: vi.fn().mockResolvedValue(ok),
  }),
  useUpdateSourceNaming: () => ({ updateSourceNaming: vi.fn().mockResolvedValue(ok) }),
  useUpdateSourceAllowedDomains: () => ({
    updateSourceAllowedDomains: vi.fn().mockResolvedValue(ok),
  }),
}));

vi.mock("../../components/SchemaDiscovery", () => ({
  SchemaDiscovery: (props: { sourceId: string }) => (
    <div data-testid="discovery-stub">discovering {props.sourceId}</div>
  ),
}));

import { SourcesPage } from "../SourcesPage";
import { forgetSourceKinds } from "../sources/sourceKind";

const MAIL_KINDS = ["microsoft_365", "google_workspace"];
const panel = () => screen.queryByTestId("sources-split-panel");

beforeEach(() => {
  SOURCES = [
    source({}),
    source({ id: "events", type: "kafka", host: "broker", port: 9092, database: "" }),
  ];
  createSource.mockReset().mockResolvedValue(ok);
  refetch.mockReset().mockResolvedValue({ data: { sources: SOURCES } });
  forgetSourceKinds();
  vi.spyOn(globalThis, "fetch").mockImplementation(async (url) => {
    const asked = String(url);
    if (asked.includes("/admin/source-kinds/")) {
      const kind = decodeURIComponent(asked.split("/admin/source-kinds/")[1]);
      const mail = MAIL_KINDS.includes(kind);
      return new Response(
        JSON.stringify({ cache_ttl_required: mail, sentinel_path: !mail, tables_land: mail }),
      );
    }
    return new Response(JSON.stringify({ configured: true, organisation_mailboxes: true }));
  });
});

describe("a source's detail is a panel at the right of the list", () => {
  it("shows nothing until a row is chosen, then that source, one at a time", async () => {
    render(<SourcesPage />);
    await screen.findByTestId("sources-row-sales-pg");
    expect(panel()).toBeNull();

    await userEvent.click(screen.getByTestId("sources-row-sales-pg"));
    expect(screen.getByTestId("sources-split-title")).toHaveTextContent("sales-pg");
    expect(screen.getByTestId("sources-row-sales-pg")).toHaveClass("row-selected");
    // Nothing is shown under the row any more.
    expect(screen.getByTestId("sources-list").querySelector(".list-detail")).toBeNull();

    await userEvent.click(screen.getByTestId("sources-row-events"));
    expect(screen.getByTestId("sources-split-title")).toHaveTextContent("events");
    expect(screen.getByTestId("sources-row-sales-pg")).not.toHaveClass("row-selected");
  });

  it("closes by its own close, by Escape, and by the row again", async () => {
    render(<SourcesPage />);
    const row = await screen.findByTestId("sources-row-sales-pg");
    await userEvent.click(row);
    await userEvent.click(screen.getByTestId("sources-split-close"));
    expect(panel()).toBeNull();

    await userEvent.click(row);
    await userEvent.keyboard("{Escape}");
    expect(panel()).toBeNull();

    await userEvent.click(row);
    await userEvent.click(row);
    expect(panel()).toBeNull();
  });

  it("puts the source's actions in the panel's head, with the ids they had on its row", async () => {
    render(<SourcesPage />);
    // Not on the row: the list is what gives way when the panel opens.
    await screen.findByTestId("sources-row-events");
    expect(screen.queryByTestId("sources-discover-events")).toBeNull();
    await userEvent.click(screen.getByTestId("sources-row-events"));
    const head = within(panel()!);
    expect(head.getByTestId("sources-discover-events")).toBeInTheDocument();
    expect(head.getByTestId("source-detail-edit")).toBeInTheDocument();
    expect(head.getByTestId("source-detail-delete")).toBeInTheDocument();
  });

  it("opens schema discovery inside the panel, with a way back to the source", async () => {
    render(<SourcesPage />);
    await userEvent.click(await screen.findByTestId("sources-row-events"));
    await userEvent.click(screen.getByTestId("sources-discover-events"));
    expect(within(panel()!).getByTestId("discovery-stub")).toHaveTextContent("events");
    await userEvent.click(screen.getByTestId("sources-working-back"));
    expect(screen.queryByTestId("discovery-stub")).toBeNull();
    expect(screen.getByTestId("source-detail-edit")).toBeInTheDocument(); // the source again
  });

  it("does not close on Escape while a source is being edited", async () => {
    render(<SourcesPage />);
    await userEvent.click(await screen.findByTestId("sources-row-sales-pg"));
    await userEvent.click(screen.getByTestId("source-detail-edit"));
    await screen.findByTestId("sources-edit-submit");
    await userEvent.keyboard("{Escape}");
    expect(screen.getByTestId("sources-edit-submit")).toBeInTheDocument();
  });
});

/** "+ Source", then a type from the picker it opens: the create form, with that type. */
async function openCreate(type = "postgresql"): Promise<void> {
  await userEvent.click(await screen.findByTestId("sources-add-toggle"));
  // An option may be listed under more than one heading of the picker; any one picks the type.
  await userEvent.click((await screen.findAllByTestId(`source-type-option-${type}`))[0]);
}

describe("+ Source asks only what a source needs in order to exist", () => {
  it("opens the type picker first; a choice opens the form with that type, none opens nothing", async () => {
    render(<SourcesPage />);
    await userEvent.click(await screen.findByTestId("sources-add-toggle"));
    expect(await screen.findByTestId("source-type-picker")).toBeInTheDocument();
    expect(screen.queryByTestId("sources-id-input")).toBeNull();
    await userEvent.click(await screen.findByTestId("source-type-picker-close"));
    await waitFor(() => expect(screen.queryByTestId("source-type-picker-search")).toBeNull());
    expect(screen.queryByTestId("sources-id-input")).toBeNull();
    await openCreate("mysql");
    expect(await screen.findByTestId("sources-id-input")).toBeInTheDocument();
    expect(screen.getByTestId("sources-type-select")).toHaveValue("mysql");
  });

  it("asks a database source for its name, its type and how it is reached", async () => {
    render(<SourcesPage />);
    await openCreate();
    const step = within(panel()!);
    expect(step.getByTestId("sources-id-input")).toBeInTheDocument();
    expect(step.getByTestId("sources-type-select")).toBeInTheDocument();
    expect(step.getByLabelText(/^Host/)).toBeInTheDocument();
    expect(step.getByLabelText(/^Database/)).toBeInTheDocument();
    // What a source can exist without is set afterwards, in its edit form.
    expect(step.queryByLabelText("Description")).toBeNull();
    expect(step.queryByTestId("source-region-select")).toBeNull();
    expect(step.queryByTestId("source-load-management-panel")).toBeNull();
  });

  it("selects the source it created and opens its detail in edit, the rest at defaults", async () => {
    render(<SourcesPage />);
    await openCreate();
    await userEvent.type(screen.getByTestId("sources-id-input"), "crm");
    await userEvent.type(within(panel()!).getByLabelText(/^Host/), "db2");
    await userEvent.type(within(panel()!).getByLabelText(/^Database/), "crm");
    // The list as the server has it once the source exists.
    const made = source({ id: "crm", host: "db2", database: "crm" });
    refetch.mockImplementation(async () => {
      SOURCES = [...SOURCES, made];
      return { data: { sources: SOURCES } };
    });
    await userEvent.click(screen.getByTestId("sources-submit"));
    await waitFor(() => expect(createSource).toHaveBeenCalled());
    expect(createSource.mock.calls[0][0]).toMatchObject({ id: "crm", type: "postgresql" });
    // Its detail, in edit: the whole form, with what the short step did not ask.
    await screen.findByTestId("sources-edit-submit");
    expect(screen.getByTestId("sources-split-title")).toHaveTextContent("crm");
    expect(within(panel()!).getByLabelText("Description")).toHaveValue("");
    await userEvent.click(screen.getByTestId("source-section-load"));
    expect(within(panel()!).getByTestId("source-load-management-panel")).toBeInTheDocument();
  });
});

describe("while a form is open in the panel", () => {
  it("hides the page's + Source: the panel has its own close", async () => {
    render(<SourcesPage />);
    await openCreate();
    expect(screen.queryByTestId("sources-add-toggle")).toBeNull();
    await userEvent.click(screen.getByTestId("sources-split-close"));
    expect(await screen.findByTestId("sources-add-toggle")).toBeInTheDocument();
  });

  it("has its Save and Cancel in the panel's head, and no row at its foot", async () => {
    render(<SourcesPage />);
    await openCreate();
    const head = screen.getByTestId("sources-split-panel").querySelector(".form-footer--head");
    expect(head).not.toBeNull();
    expect(within(head as HTMLElement).getByTestId("sources-submit")).toBeInTheDocument();
    expect(within(head as HTMLElement).getByTestId("sources-cancel")).toBeInTheDocument();
    expect(document.querySelectorAll('[data-testid="sources-submit"]')).toHaveLength(1);
  });
});

describe("a remote schema's refresh in the panel's head", () => {
  it("is an icon named by its hint, which says so while the refresh runs", async () => {
    SOURCES = [source({ id: "shop-api", type: "openapi", host: "", port: 0, database: "" })];
    let finish: (r: Response) => void = () => {};
    const fetchSpy = vi.mocked(globalThis.fetch);
    const answered = fetchSpy.getMockImplementation();
    fetchSpy.mockImplementation(async (url, init) =>
      String(url).includes("/admin/openapi/refresh/")
        ? new Promise<Response>((resolve) => (finish = resolve))
        : answered!(url, init),
    );
    render(<SourcesPage />);
    await userEvent.click(await screen.findByTestId("sources-row-shop-api"));
    const refresh = screen.getByTestId("sources-refresh-schema-shop-api");
    // No text of its own: an icon, named, in the head's one style.
    expect(refresh).toHaveTextContent("");
    expect(refresh).toHaveAccessibleName("Refresh Schema");
    expect(refresh).toHaveAttribute("data-variant", "subtle");
    expect(refresh.querySelector("svg.lucide-refresh-cw")).not.toBeNull();
    await userEvent.hover(refresh);
    expect(await screen.findByRole("tooltip")).toHaveTextContent("Refresh Schema");

    await userEvent.click(refresh);
    // Running: the icon is busy and its hint says so, held open -- pressing the icon closed the
    // hint, and a busy icon is disabled, so pointing at it would show nothing. Its name stays.
    await waitFor(() => expect(refresh).toHaveAttribute("data-loading", "true"));
    expect(refresh).toHaveAccessibleName("Refresh Schema");
    await waitFor(() => expect(screen.getByRole("tooltip")).toHaveTextContent("Refreshing..."));

    finish(new Response(JSON.stringify({})));
    await waitFor(() => expect(refresh).not.toHaveAttribute("data-loading"));
    // Done: the hint is no longer held; pointing at the icon again shows its name.
    await userEvent.unhover(refresh);
    await userEvent.hover(refresh);
    await waitFor(() => expect(screen.getByRole("tooltip")).toHaveTextContent("Refresh Schema"));
  });
});
