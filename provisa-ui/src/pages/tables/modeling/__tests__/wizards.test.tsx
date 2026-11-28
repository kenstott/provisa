// Copyright (c) 2026 Kenneth Stott
// Canary: 4e9a0c2f-81d6-4b3e-a7f5-c2d9e8b16a03
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-2199: each modeling wizard asks in its method's own words, step by step; its review shows
// what the answers become in Provisa's primitives under the method's conventional names, which
// can be changed; and Create makes them through the entity/fact API, with draft descriptions.

import { useState, type ReactElement, type ReactNode } from "react";
import { beforeEach, describe, expect, it, vi } from "vitest";
import userEvent from "@testing-library/user-event";
import { useLocation } from "react-router-dom";
import { renderInForm, screen, waitFor, within } from "../../../../test-utils/render";
import i18n from "../../../../i18n";
import type { Domain, ListedTable } from "../../../../types/admin";
import { StarSchemaWizard } from "../StarSchemaWizard";
import { DataVaultWizard } from "../DataVaultWizard";
import { PanelActionsContext, PanelNoticeContext } from "../../../../components/list/panelActions";
import { PanelLeaveContext, usePanelLeave } from "../../../../components/list/panelLeave";

const t = i18n.getFixedT("en");

const registerEntity = vi.fn();
const registerFact = vi.fn();
const upsertRelationship = vi.fn();

const cols = (...names: string[]) =>
  names.map((columnName) => ({ columnName, dataType: "varchar" }));
const listed = (domainId: string, tableName: string, columns: string[]) =>
  ({
    id: 0,
    sourceId: "pg",
    domainId,
    tableName,
    modelingRole: null,
    columns: cols(...columns),
  }) as unknown as ListedTable;

// One value per hook across renders: a fresh array each render reruns the forms' effects.
const TABLES = {
  tables: [
    listed("sales", "orders", ["order_id", "customer_id", "product_id", "amount"]),
    listed("sales", "customers", ["id", "name", "region"]),
    listed("sales", "products", ["sku", "title"]),
  ],
  loading: false,
  // What the wizard reads back before relating the tables: their names, none aliased.
  refetch: vi.fn(async () => ({ data: { tables: [] } })),
};
vi.mock("../../../../hooks/useAdminQueries", async (importOriginal) => ({
  ...(await importOriginal<typeof import("../../../../hooks/useAdminQueries")>()),
  useTables: () => TABLES,
  useRegisterEntity: () => ({ registerEntity }),
  useRegisterFact: () => ({ registerFact }),
  useUpsertRelationship: () => ({ upsertRelationship }),
}));

const DOMAINS = [{ id: "sales", description: "" }] as unknown as Domain[];

/** Open a picker and choose one of its options. */
async function pick(testId: string, option: string) {
  const input = screen.getByTestId(testId);
  await userEvent.click(input);
  const listbox = document.getElementById(input.getAttribute("aria-controls") ?? "");
  if (!listbox) throw new Error(`${testId} opened no list`);
  await userEvent.click(within(listbox).getByRole("option", { name: option, hidden: true }));
}

function Where() {
  const loc = useLocation();
  return <pre data-testid="where">{loc.search}</pre>;
}

const next = () => userEvent.click(screen.getByTestId("wizard-next"));

beforeEach(() => {
  registerEntity.mockReset().mockResolvedValue({ success: true, message: "" });
  registerFact.mockReset().mockResolvedValue({ success: true, message: "" });
  upsertRelationship.mockReset().mockResolvedValue({ success: true, message: "" });
});

describe("the star schema wizard", () => {
  it("asks for the business process, its table and its fact type first", () => {
    renderInForm(<StarSchemaWizard domains={DOMAINS} onSuccess={vi.fn()} onClose={vi.fn()} />);
    const head = screen.getByTestId("wizard-step-process");
    const strip = within(head).getByTestId("wizard-steps");
    expect(strip).toHaveAttribute("data-active", "0");
    for (const [key, label] of [
      ["process", "star.process.title"],
      ["grain", "star.grain.title"],
      ["dimensions", "star.dimensions.title"],
      ["measures", "star.measures.title"],
      ["review", "review.title"],
    ])
      expect(within(strip).getByTestId(`wizard-steps-${key}`)).toHaveTextContent(
        t(`modelingWizard.${label}`),
      );
    expect(head).toHaveTextContent(t("modelingWizard.star.process.title"));
    expect(screen.getByTestId("star-process")).toBeInTheDocument();
    expect(screen.getByTestId("star-source")).toBeInTheDocument();
    expect(screen.getByTestId("star-fact-type")).toBeInTheDocument();
    expect(screen.queryByTestId("wizard-back")).toBeNull();
  });

  it("does not move on until the step is answered", async () => {
    renderInForm(<StarSchemaWizard domains={DOMAINS} onSuccess={vi.fn()} onClose={vi.fn()} />);
    await next();
    expect(await screen.findByTestId("form-problem")).toHaveTextContent(
      t("modelingWizard.star.missing.process"),
    );
    expect(screen.getByTestId("wizard-step-process")).toBeInTheDocument();
  });

  it("reviews a dim_ entity and a fact_ fact, renamed, and creates them with draft descriptions", async () => {
    const onSuccess = vi.fn();
    renderInForm(<StarSchemaWizard domains={DOMAINS} onSuccess={onSuccess} onClose={vi.fn()} />);
    await userEvent.type(screen.getByTestId("star-process"), "Sales");
    await pick("star-domain", "sales");
    await pick("star-source", "sales.orders");
    await next();

    expect(screen.getByTestId("wizard-step-grain")).toBeInTheDocument();
    await pick("star-grain", "order_id (varchar)");
    await next();

    expect(screen.getByTestId("wizard-step-dimensions")).toBeInTheDocument();
    await userEvent.click(screen.getByTestId("star-dim-add"));
    await userEvent.type(screen.getByTestId("star-dim-name-0"), "Customer");
    await pick("star-dim-via-0", "customer_id (varchar)");
    await pick("star-dim-source-0", "sales.customers");
    await pick("star-dim-key-0", "id (varchar)");
    await next();

    expect(screen.getByTestId("wizard-step-measures")).toBeInTheDocument();
    expect(screen.getByTestId("wizard-next")).toHaveTextContent(t("modelingWizard.toReview"));
    await next();

    // The review: each table under its conventional name, what it becomes, and its draft.
    expect(screen.getByTestId("wizard-step-review")).toBeInTheDocument();
    const [dimName] = screen.getAllByTestId(/^wizard-review-name-dim:/) as HTMLInputElement[];
    expect(dimName.value).toBe("dim_customer");
    expect((screen.getByTestId("wizard-review-name-fact") as HTMLInputElement).value).toBe(
      "fact_sales",
    );
    expect(screen.getByTestId("wizard-review-relationships-fact")).toHaveTextContent(
      "customer_id → dim_customer",
    );
    expect(screen.getByTestId("wizard-review-description-fact")).toHaveTextContent(
      "Draft: star schema fact of the Sales process",
    );

    // A team that does not want the prefix clears it; the fact's relationship follows.
    await userEvent.clear(dimName);
    await userEvent.type(dimName, "customer");
    expect(screen.getByTestId("wizard-review-relationships-fact")).toHaveTextContent(
      "customer_id → customer",
    );

    // REQ-2115: Create changes something, so Polly asks the user first; Back changes nothing.
    expect(screen.getByTestId("wizard-create")).toHaveAttribute("data-changes", "true");
    expect(screen.getByTestId("wizard-back")).toHaveAttribute("data-changes", "false");
    await userEvent.click(screen.getByTestId("wizard-create"));
    await screen.findByTestId("wizard-created");
    expect(registerEntity).toHaveBeenCalledWith(
      expect.objectContaining({
        name: "customer",
        source: "sales.customers",
        domainId: "sales",
        key: ["id"],
        history: "none",
        description: expect.stringMatching(/^Draft: star schema dimension "Customer"/),
      }),
    );
    expect(registerFact).toHaveBeenCalledWith(
      expect.objectContaining({
        name: "fact_sales",
        source: "sales.orders",
        grain: ["order_id"],
        measures: [],
        dimensions: [{ entity: "customer", via: "customer_id" }],
        description: expect.stringMatching(/^Draft: star schema fact/),
      }),
    );
    expect(registerEntity.mock.invocationCallOrder[0]).toBeLessThan(
      registerFact.mock.invocationCallOrder[0],
    );
    const result = screen.getByTestId("wizard-created");
    expect(result).toHaveTextContent(t("modelingWizard.result.created", { count: 2 }));
    expect(within(result).getByTestId("wizard-created-fact")).toHaveTextContent(
      t("modelingWizard.result.relatesTo", { tables: "customer" }),
    );
    await userEvent.click(screen.getByTestId("wizard-done"));
    expect(onSuccess).toHaveBeenCalled();
  });

  it("shows a refusal and creates nothing after it", async () => {
    registerEntity.mockResolvedValue({ success: false, message: "Table exists" });
    renderInForm(<StarSchemaWizard domains={DOMAINS} onSuccess={vi.fn()} onClose={vi.fn()} />);
    await userEvent.type(screen.getByTestId("star-process"), "Sales");
    await pick("star-domain", "sales");
    await pick("star-source", "sales.orders");
    await next();
    await pick("star-grain", "order_id (varchar)");
    await next();
    await userEvent.click(screen.getByTestId("star-dim-add"));
    await userEvent.type(screen.getByTestId("star-dim-name-0"), "Customer");
    await pick("star-dim-via-0", "customer_id (varchar)");
    await pick("star-dim-source-0", "sales.customers");
    await pick("star-dim-key-0", "id (varchar)");
    await next();
    await next();
    await userEvent.click(screen.getByTestId("wizard-create"));
    const result = await screen.findByTestId("wizard-created");
    expect(result).toHaveTextContent(t("modelingWizard.result.stopped", { name: "dim_customer" }));
    expect(result).toHaveTextContent("Table exists");
    expect(registerFact).not.toHaveBeenCalled();
  });
});

describe("the Data Vault wizard", () => {
  it("asks for hubs and their business keys first, in those words", async () => {
    renderInForm(<DataVaultWizard domains={DOMAINS} onSuccess={vi.fn()} onClose={vi.fn()} />);
    const head = screen.getByTestId("wizard-step-hubs");
    expect(head).toHaveTextContent(t("modelingWizard.vault.hubs.title"));
    await userEvent.click(screen.getByTestId("vault-hub-add"));
    const hub = screen.getByTestId("vault-hub-0");
    expect(hub).toHaveTextContent(t("modelingWizard.vault.concept"));
    expect(hub).toHaveTextContent(t("modelingWizard.vault.recordSource"));
    expect(hub).toHaveTextContent(t("tableEditForm.businessKeyLabel"));
    expect(hub).toHaveTextContent(t("modelingWizard.vault.sameAsSource"));
  });

  it("reviews hubs, a link and a satellite as entities and a fact, and creates them", async () => {
    renderInForm(<DataVaultWizard domains={DOMAINS} onSuccess={vi.fn()} onClose={vi.fn()} />);
    await pick("vault-domain", "sales");
    await userEvent.click(screen.getByTestId("vault-hub-add"));
    await userEvent.type(screen.getByTestId("vault-hub-concept-0"), "Customer");
    await pick("vault-hub-source-0", "sales.customers");
    await pick("vault-hub-key-0", "id (varchar)");
    await userEvent.click(screen.getByTestId("vault-hub-add"));
    await userEvent.type(screen.getByTestId("vault-hub-concept-1"), "Product");
    await pick("vault-hub-source-1", "sales.products");
    await pick("vault-hub-key-1", "sku (varchar)");
    await next();

    expect(screen.getByTestId("wizard-step-links")).toBeInTheDocument();
    await userEvent.click(screen.getByTestId("vault-link-add"));
    await pick("vault-link-source-0", "sales.orders");
    await pick("vault-link-hubs-0", "Customer");
    await pick("vault-link-hubs-0", "Product");
    const link = screen.getByTestId("vault-link-0");
    const vias = within(link)
      .getAllByRole("textbox")
      .filter((el) => el.getAttribute("data-testid")?.startsWith("vault-link-via-0-"));
    expect(vias).toHaveLength(2);
    await pick(vias[0].getAttribute("data-testid")!, "customer_id (varchar)");
    await pick(vias[1].getAttribute("data-testid")!, "product_id (varchar)");
    await next();

    expect(screen.getByTestId("wizard-step-satellites")).toBeInTheDocument();
    await pick("vault-history-depth", t("modelingWizard.vault.depth.current"));
    await userEvent.click(screen.getByTestId("vault-sat-add"));
    await pick("vault-sat-parent-0", t("modelingWizard.vault.parentHub", { name: "Customer" }));
    await pick("vault-sat-source-0", "sales.customers");
    await pick("vault-sat-key-0", "id (varchar)");
    await pick("vault-sat-attributes-0", "name (varchar)");
    await next();

    expect(screen.getByTestId("wizard-step-rules")).toBeInTheDocument();
    await next();

    const names = screen
      .getAllByRole("textbox")
      .filter((el) => el.getAttribute("data-testid")?.startsWith("wizard-review-name-"))
      .map((el) => (el as HTMLInputElement).value);
    expect(names).toEqual(["hub_customer", "hub_product", "lnk_customer_product", "sat_customer"]);
    const linkId = screen
      .getAllByTestId(/^wizard-review-relationships-/)[0]
      .getAttribute("data-testid")!;
    expect(screen.getByTestId(linkId)).toHaveTextContent(
      "customer_id → hub_customer; product_id → hub_product",
    );
    // The satellite is related to its hub on the hub's business key.
    expect(screen.getAllByTestId(/^wizard-review-relationships-sat:/)[0]).toHaveTextContent(
      "id → hub_customer",
    );

    await userEvent.click(screen.getByTestId("wizard-create"));
    await screen.findByTestId("wizard-created");
    expect(registerEntity.mock.calls.map(([i]) => i.name)).toEqual([
      "hub_customer",
      "hub_product",
      "sat_customer",
    ]);
    expect(registerEntity.mock.calls[2][0]).toMatchObject({
      key: ["id"],
      attributes: ["name"],
      history: "none",
      description: expect.stringMatching(/^Draft: Data Vault satellite of hub_customer/),
    });
    expect(registerFact).toHaveBeenCalledWith(
      expect.objectContaining({
        name: "lnk_customer_product",
        grain: ["customer_id", "product_id"],
        measures: [],
        dimensions: [
          { entity: "hub_customer", via: "customer_id" },
          { entity: "hub_product", via: "product_id" },
        ],
        description: expect.stringMatching(/^Draft: Data Vault link/),
      }),
    );
    expect(upsertRelationship).toHaveBeenCalledTimes(1);
    expect(upsertRelationship).toHaveBeenCalledWith(
      {
        id: "sat_customer__hub_customer",
        sourceTableId: "sat_customer",
        targetTableId: "hub_customer",
        sourceColumn: "id",
        targetColumn: "id",
        cardinality: "many-to-one",
      },
      true,
    );
    // Created after every table.
    expect(upsertRelationship.mock.invocationCallOrder[0]).toBeGreaterThan(
      registerFact.mock.invocationCallOrder[0],
    );
    await waitFor(() =>
      expect(screen.getByTestId("wizard-created")).toHaveTextContent(
        t("modelingWizard.result.created", { count: 4 }),
      ),
    );
    expect(screen.getByTestId(/^wizard-created-rel:sat:/)).toHaveTextContent(
      "sat_customer → hub_customer",
    );
    expect(screen.getByTestId(/^wizard-created-rel:sat:/)).toHaveTextContent(
      t("modelingWizard.primitive.relationship"),
    );
  });
});

describe("the wizards' step strip (REQ-2277)", () => {
  it("puts the step in the URL, goes back through a completed step and not forward", async () => {
    renderInForm(
      <>
        <StarSchemaWizard domains={DOMAINS} onSuccess={vi.fn()} onClose={vi.fn()} />
        <Where />
      </>,
    );
    await userEvent.type(screen.getByTestId("star-process"), "Sales");
    await pick("star-domain", "sales");
    await pick("star-source", "sales.orders");
    await next();
    expect(screen.getByTestId("where")).toHaveTextContent("?step=2");
    expect(screen.getByTestId("wizard-steps")).toHaveAttribute("data-active", "1");

    // A step not yet reached does nothing; the completed first step goes back.
    await userEvent.click(screen.getByTestId("wizard-steps-dimensions"));
    expect(screen.getByTestId("wizard-step-grain")).toBeInTheDocument();
    await userEvent.click(screen.getByTestId("wizard-steps-process"));
    expect(screen.getByTestId("wizard-step-process")).toBeInTheDocument();
    expect(screen.getByTestId("where")).toHaveTextContent("?step=1");
  });

  it("opens a link to a step past the answered ones at the last answered step", () => {
    renderInForm(<StarSchemaWizard domains={DOMAINS} onSuccess={vi.fn()} onClose={vi.fn()} />, {
      initialEntries: ["/tables?step=4"],
    });
    expect(screen.getByTestId("wizard-step-process")).toBeInTheDocument();
  });

  it("the data vault wizard shows its five steps and its step in the URL", async () => {
    renderInForm(
      <>
        <DataVaultWizard domains={DOMAINS} onSuccess={vi.fn()} onClose={vi.fn()} />
        <Where />
      </>,
    );
    const strip = screen.getByTestId("wizard-steps");
    for (const key of ["hubs", "links", "satellites", "rules", "review"])
      expect(within(strip).getByTestId(`wizard-steps-${key}`)).toBeInTheDocument();
    expect(strip).toHaveAttribute("data-active", "0");
    await next();
    expect(await screen.findByTestId("form-problem")).toBeInTheDocument();
    expect(screen.getByTestId("where")).toHaveTextContent("");
  });
});

// REQ-2199 (amended 2026-11-27): each wizard stays in the + Model detail panel, under its
// unsaved-changes guard, and each step's choices are the panel's command bar (its pinned head, as
// in the source setup flow): the step's primary action, Skip step only where no later step reads
// that step's answers, Back from the second step on, and Close. The step strip shows progress
// only, and a step in it opens that step.
describe("the wizards' command bar and step strip (REQ-2199)", () => {
  /** The detail panel the wizard is in: its pinned head, its notice line and its leave guard. */
  function InPanel({ children }: { children: ReactNode }) {
    const [head, setHead] = useState<HTMLElement | null>(null);
    const [notice, setNotice] = useState<HTMLElement | null>(null);
    const leave = usePanelLeave();
    return (
      <PanelLeaveContext.Provider value={leave}>
        <div data-testid="panel-head" ref={setHead} />
        <div ref={setNotice} />
        {leave.asking !== null && (
          <button data-testid="panel-discard" onClick={() => leave.answer(true)}>
            {leave.asking}
          </button>
        )}
        <PanelActionsContext.Provider value={head}>
          <PanelNoticeContext.Provider value={notice}>
            <div data-testid="panel-content">{children}</div>
          </PanelNoticeContext.Provider>
        </PanelActionsContext.Provider>
      </PanelLeaveContext.Provider>
    );
  }
  const inPanel = (ui: ReactElement) => renderInForm(<InPanel>{ui}</InPanel>);
  const head = () => within(screen.getByTestId("panel-head"));
  /** The step's choices, in the panel's head, in order, and none of them in the panel's content. */
  const choices = () => {
    expect(
      within(screen.getByTestId("panel-content")).queryAllByTestId(
        /^wizard-(next|skip|back|close|create|done)$/,
      ),
    ).toEqual([]);
    return Array.from(screen.getByTestId("panel-head").querySelectorAll("button")).map((b) =>
      b.getAttribute("data-testid"),
    );
  };
  const nextInHead = () => userEvent.click(head().getByTestId("wizard-next"));

  async function vaultToRules() {
    await pick("vault-domain", "sales");
    await userEvent.click(screen.getByTestId("vault-hub-add"));
    await userEvent.type(screen.getByTestId("vault-hub-concept-0"), "Customer");
    await pick("vault-hub-source-0", "sales.customers");
    await pick("vault-hub-key-0", "id (varchar)");
    await nextInHead();
    await nextInHead();
    await nextInHead();
  }

  async function starToDimensions() {
    await userEvent.type(screen.getByTestId("star-process"), "Sales");
    await pick("star-domain", "sales");
    await pick("star-source", "sales.orders");
    await nextInHead();
    await pick("star-grain", "order_id (varchar)");
    await nextInHead();
  }

  it("the Data Vault wizard is in the panel, not a dialog, with Next and Close in its head", async () => {
    inPanel(<DataVaultWizard domains={DOMAINS} onSuccess={vi.fn()} onClose={vi.fn()} />);
    expect(screen.queryByRole("dialog")).toBeNull();
    expect(
      within(screen.getByTestId("panel-content")).getByTestId("wizard-step-hubs"),
    ).toBeInTheDocument();
    await waitFor(() => expect(choices()).toEqual(["wizard-next", "wizard-close"]));
    expect(head().getByTestId("wizard-close")).toHaveTextContent(t("modelingWizard.close"));
  });

  it("a step's problem is the panel's notice, and the step does not move on", async () => {
    inPanel(<DataVaultWizard domains={DOMAINS} onSuccess={vi.fn()} onClose={vi.fn()} />);
    await waitFor(() => expect(choices()).toContain("wizard-next"));
    await nextInHead();
    expect(await screen.findByTestId("form-problem")).toHaveTextContent(
      t("modelingWizard.vault.missing.hubs"),
    );
    expect(screen.getByTestId("wizard-step-hubs")).toBeInTheDocument();
  });

  it("Close leaves an untouched wizard at once", async () => {
    const onClose = vi.fn();
    inPanel(<DataVaultWizard domains={DOMAINS} onSuccess={vi.fn()} onClose={onClose} />);
    await userEvent.click(await head().findByTestId("wizard-close"));
    expect(onClose).toHaveBeenCalled();
    expect(screen.queryByTestId("panel-discard")).toBeNull();
  });

  it("Close asks first when the wizard has answers, behind the panel's dirty guard", async () => {
    const onClose = vi.fn();
    inPanel(<DataVaultWizard domains={DOMAINS} onSuccess={vi.fn()} onClose={onClose} />);
    await userEvent.click(screen.getByTestId("vault-hub-add"));
    await userEvent.type(screen.getByTestId("vault-hub-concept-0"), "Customer");
    await userEvent.click(head().getByTestId("wizard-close"));
    expect(onClose).not.toHaveBeenCalled();
    expect(screen.getByTestId("panel-discard")).toHaveTextContent("Customer");
    await userEvent.click(screen.getByTestId("panel-discard"));
    expect(onClose).toHaveBeenCalled();
  });

  it("offers Skip step in the Data Vault wizard only on business rules, which no step reads", async () => {
    inPanel(<DataVaultWizard domains={DOMAINS} onSuccess={vi.fn()} onClose={vi.fn()} />);
    await waitFor(() => expect(choices()).toEqual(["wizard-next", "wizard-close"]));
    await pick("vault-domain", "sales");
    await userEvent.click(screen.getByTestId("vault-hub-add"));
    await userEvent.type(screen.getByTestId("vault-hub-concept-0"), "Customer");
    await pick("vault-hub-source-0", "sales.customers");
    await pick("vault-hub-key-0", "id (varchar)");
    await nextInHead();
    // Links: the satellites and the rules read them.
    expect(screen.getByTestId("wizard-step-links")).toBeInTheDocument();
    expect(choices()).toEqual(["wizard-next", "wizard-back", "wizard-close"]);
    await nextInHead();
    // Satellites: the rules read them.
    expect(screen.getByTestId("wizard-step-satellites")).toBeInTheDocument();
    expect(choices()).toEqual(["wizard-next", "wizard-back", "wizard-close"]);
    await nextInHead();
    expect(screen.getByTestId("wizard-step-rules")).toBeInTheDocument();
    expect(choices()).toEqual(["wizard-next", "wizard-skip", "wizard-back", "wizard-close"]);
    expect(head().getByTestId("wizard-skip")).toHaveTextContent(t("modelingWizard.skipStep"));
  });

  it("Back opens the step before, and Skip step drops the step's answers and moves on", async () => {
    inPanel(<DataVaultWizard domains={DOMAINS} onSuccess={vi.fn()} onClose={vi.fn()} />);
    await vaultToRules();
    await userEvent.click(head().getByTestId("wizard-back"));
    expect(screen.getByTestId("wizard-step-satellites")).toBeInTheDocument();
    await nextInHead();
    await userEvent.click(screen.getByTestId("vault-rule-add"));
    await userEvent.type(screen.getByTestId("vault-rule-text-0"), "half-written");
    await userEvent.click(head().getByTestId("wizard-skip"));
    expect(screen.getByTestId("wizard-step-review")).toBeInTheDocument();
    await userEvent.click(head().getByTestId("wizard-back"));
    expect(screen.getByTestId("wizard-step-rules")).toBeInTheDocument();
    expect(screen.queryByTestId("vault-rule-text-0")).toBeNull();
  });

  it("the review's head holds Create, Back and Close; the result's holds Done", async () => {
    const onSuccess = vi.fn();
    const onClose = vi.fn();
    inPanel(<DataVaultWizard domains={DOMAINS} onSuccess={onSuccess} onClose={onClose} />);
    await vaultToRules();
    await nextInHead();
    expect(screen.getByTestId("wizard-step-review")).toBeInTheDocument();
    expect(choices()).toEqual(["wizard-create", "wizard-back", "wizard-close"]);
    await userEvent.click(head().getByTestId("wizard-create"));
    await screen.findByTestId("wizard-created");
    await waitFor(() => expect(choices()).toEqual(["wizard-done"]));
    await userEvent.click(head().getByTestId("wizard-done"));
    expect(onSuccess).toHaveBeenCalled();
    // What was made is saved: leaving asks nothing.
    expect(screen.queryByTestId("panel-discard")).toBeNull();
  });

  it("the strip shows how far the wizard has come, and a step in it opens that step", async () => {
    inPanel(<DataVaultWizard domains={DOMAINS} onSuccess={vi.fn()} onClose={vi.fn()} />);
    await vaultToRules();
    const strip = () => screen.getByTestId("wizard-steps");
    expect(strip()).toHaveAttribute("data-active", "3");
    // Back to the hubs from the strip: the strip still shows the progress made.
    await userEvent.click(screen.getByTestId("wizard-steps-hubs"));
    expect(screen.getByTestId("wizard-step-hubs")).toBeInTheDocument();
    expect(strip()).toHaveAttribute("data-active", "3");
    // And forward again to a step already reached, from the strip.
    await userEvent.click(screen.getByTestId("wizard-steps-satellites"));
    expect(screen.getByTestId("wizard-step-satellites")).toBeInTheDocument();
    await userEvent.click(screen.getByTestId("wizard-steps-rules"));
    expect(screen.getByTestId("wizard-step-rules")).toBeInTheDocument();
    // A step not yet reached opens nothing.
    await userEvent.click(screen.getByTestId("wizard-steps-review"));
    expect(screen.getByTestId("wizard-step-rules")).toBeInTheDocument();
  });

  it("the star schema wizard is in the panel; Skip step only on dimensions and measures", async () => {
    inPanel(<StarSchemaWizard domains={DOMAINS} onSuccess={vi.fn()} onClose={vi.fn()} />);
    expect(screen.queryByRole("dialog")).toBeNull();
    await waitFor(() => expect(choices()).toEqual(["wizard-next", "wizard-close"]));
    await userEvent.type(screen.getByTestId("star-process"), "Sales");
    await pick("star-domain", "sales");
    await pick("star-source", "sales.orders");
    await nextInHead();
    // The grain: the fact is keyed on it.
    expect(choices()).toEqual(["wizard-next", "wizard-back", "wizard-close"]);
    await pick("star-grain", "order_id (varchar)");
    await nextInHead();
    expect(screen.getByTestId("wizard-step-dimensions")).toBeInTheDocument();
    expect(choices()).toEqual(["wizard-next", "wizard-skip", "wizard-back", "wizard-close"]);
    await nextInHead();
    expect(screen.getByTestId("wizard-step-measures")).toBeInTheDocument();
    expect(choices()).toEqual(["wizard-next", "wizard-skip", "wizard-back", "wizard-close"]);
    await nextInHead();
    expect(choices()).toEqual(["wizard-create", "wizard-back", "wizard-close"]);
  });

  it("skipping the star schema's dimensions leaves none in the review", async () => {
    inPanel(<StarSchemaWizard domains={DOMAINS} onSuccess={vi.fn()} onClose={vi.fn()} />);
    await starToDimensions();
    await userEvent.click(screen.getByTestId("star-dim-add"));
    await userEvent.type(screen.getByTestId("star-dim-name-0"), "Customer");
    await userEvent.click(head().getByTestId("wizard-skip"));
    expect(screen.getByTestId("wizard-step-measures")).toBeInTheDocument();
    await userEvent.click(head().getByTestId("wizard-skip"));
    expect(screen.getByTestId("wizard-step-review")).toBeInTheDocument();
    expect(screen.queryAllByTestId(/^wizard-review-name-dim:/)).toHaveLength(0);
  });

  it("Close on the star schema wizard asks first once it has answers", async () => {
    const onClose = vi.fn();
    inPanel(<StarSchemaWizard domains={DOMAINS} onSuccess={vi.fn()} onClose={onClose} />);
    await starToDimensions();
    await userEvent.click(head().getByTestId("wizard-close"));
    expect(onClose).not.toHaveBeenCalled();
    await userEvent.click(screen.getByTestId("panel-discard"));
    expect(onClose).toHaveBeenCalled();
  });
});
