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

import { beforeEach, describe, expect, it, vi } from "vitest";
import userEvent from "@testing-library/user-event";
import { renderInForm, screen, waitFor, within } from "../../../../test-utils/render";
import i18n from "../../../../i18n";
import type { Domain, ListedTable } from "../../../../types/admin";
import { StarSchemaWizard } from "../StarSchemaWizard";
import { DataVaultWizard } from "../DataVaultWizard";

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

const next = () => userEvent.click(screen.getByTestId("wizard-next"));

beforeEach(() => {
  registerEntity.mockReset().mockResolvedValue({ success: true, message: "" });
  registerFact.mockReset().mockResolvedValue({ success: true, message: "" });
  upsertRelationship.mockReset().mockResolvedValue({ success: true, message: "" });
});

describe("the star schema wizard", () => {
  it("asks for the business process, its table and its fact type first", () => {
    renderInForm(<StarSchemaWizard domains={DOMAINS} onSuccess={vi.fn()} onCancel={vi.fn()} />);
    const head = screen.getByTestId("wizard-step-process");
    expect(head).toHaveTextContent(t("modelingWizard.stepOf", { index: 1, count: 5 }));
    expect(head).toHaveTextContent(t("modelingWizard.star.process.title"));
    expect(screen.getByTestId("star-process")).toBeInTheDocument();
    expect(screen.getByTestId("star-source")).toBeInTheDocument();
    expect(screen.getByTestId("star-fact-type")).toBeInTheDocument();
    expect(screen.queryByTestId("wizard-back")).toBeNull();
  });

  it("does not move on until the step is answered", async () => {
    renderInForm(<StarSchemaWizard domains={DOMAINS} onSuccess={vi.fn()} onCancel={vi.fn()} />);
    await next();
    expect(await screen.findByTestId("form-problem")).toHaveTextContent(
      t("modelingWizard.star.missing.process"),
    );
    expect(screen.getByTestId("wizard-step-process")).toBeInTheDocument();
  });

  it("reviews a dim_ entity and a fact_ fact, renamed, and creates them with draft descriptions", async () => {
    const onSuccess = vi.fn();
    renderInForm(<StarSchemaWizard domains={DOMAINS} onSuccess={onSuccess} onCancel={vi.fn()} />);
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
    renderInForm(<StarSchemaWizard domains={DOMAINS} onSuccess={vi.fn()} onCancel={vi.fn()} />);
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
    renderInForm(<DataVaultWizard domains={DOMAINS} onSuccess={vi.fn()} onCancel={vi.fn()} />);
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
    renderInForm(<DataVaultWizard domains={DOMAINS} onSuccess={vi.fn()} onCancel={vi.fn()} />);
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
