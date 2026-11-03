// Copyright (c) 2026 Kenneth Stott
// Canary: 7d3c5e10-2b8f-4c61-9a0e-3f1b6d2a8c47
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-2199: what a star schema's and a Data Vault's answers become in Provisa's primitives, under
// each method's conventional names and with first-draft descriptions, and how the plan is created
// through the entity/fact API.

import { describe, expect, it, vi } from "vitest";
import i18n from "../../../../i18n";
import {
  conventionalName,
  createPlan,
  planStar,
  planVault,
  registrationInput,
  type PlannedEntity,
  type PlannedFact,
  type StarAnswers,
  type VaultAnswers,
} from "../plan";

const t = i18n.t.bind(i18n);

const STAR: StarAnswers = {
  process: "Order fulfillment",
  domainId: "sales",
  source: "sales.order_lines",
  factType: "transaction",
  grainStatement: "one row per order line",
  grain: ["order_id", "line_no"],
  dimensions: [
    {
      uid: "d1",
      name: "Customer",
      conformed: null,
      source: "sales.customers",
      key: ["customer_id"],
      attributes: ["name", "region"],
      scd: "type2",
      validTime: "valid_from",
      recordedTime: "loaded_at",
      via: "customer_id",
    },
    {
      uid: "d2",
      name: "Product",
      conformed: null,
      source: "sales.products",
      key: ["sku"],
      attributes: ["title"],
      scd: "type1",
      validTime: "ignored",
      recordedTime: "ignored",
      via: "sku",
    },
    {
      uid: "d3",
      name: "Date",
      conformed: "dim_date",
      source: null,
      key: [],
      attributes: [],
      scd: "type1",
      validTime: null,
      recordedTime: null,
      via: "order_date",
    },
  ],
  measures: [
    { column: "amount", agg: "sum", additivity: "additive" },
    { column: "unit_price", agg: "avg", additivity: "nonAdditive" },
  ],
};

const VAULT: VaultAnswers = {
  domainId: "crm",
  hubs: [
    {
      uid: "h1",
      concept: "Customer",
      source: "crm.customers",
      businessKey: ["customer_no"],
      sameAsSource: "crm.customer_merges",
      sameAsMaster: "master_no",
      sameAsDuplicate: "duplicate_no",
    },
    {
      uid: "h2",
      concept: "Order",
      source: "shop.orders",
      businessKey: ["order_no"],
      sameAsSource: null,
      sameAsMaster: null,
      sameAsDuplicate: null,
    },
  ],
  links: [
    {
      uid: "l1",
      name: "",
      source: "shop.orders",
      hubs: ["h1", "h2"],
      via: { h1: "customer_no", h2: "order_no" },
      ends: true,
      validTime: "assigned_at",
      recordedTime: "loaded_at",
    },
  ],
  historyDepth: "full",
  satellites: [
    {
      uid: "s1",
      name: "CRM",
      parent: "h1",
      source: "crm.customers",
      parentKey: ["customer_no"],
      attributes: ["name", "email"],
      groupedBy: "sensitivity",
      multiActive: [],
      validTime: "valid_from",
      recordedTime: "loaded_at",
    },
    {
      uid: "s2",
      name: "phones",
      parent: "h1",
      source: "crm.phones",
      parentKey: ["customer_no"],
      attributes: ["number"],
      groupedBy: "source",
      multiActive: ["phone_type"],
      validTime: "valid_from",
      recordedTime: "loaded_at",
    },
  ],
  rules: [{ uid: "r1", target: "h1", text: "lifetime value = sum of order totals" }],
};

const byId = <T>(tables: unknown[], id: string) =>
  (tables as { id: string }[]).find((x) => x.id === id) as T;

describe("conventional names", () => {
  it("are the method's prefix and the user's words, lower-case and underscored", () => {
    expect(conventionalName("dim_", "Customer")).toBe("dim_customer");
    expect(conventionalName("fact_", "Order fulfillment")).toBe("fact_order_fulfillment");
    expect(conventionalName("lnk_", "Customer", "same as")).toBe("lnk_customer_same_as");
    expect(conventionalName("sat_", " Product ", "")).toBe("sat_product");
  });
});

describe("a star schema's answers", () => {
  const plan = planStar(STAR, t);

  it("build a dim_ entity per new dimension, its change handling as its history", () => {
    const customer = byId<PlannedEntity>(plan.tables, "dim:d1");
    expect(customer).toMatchObject({
      primitive: "entity",
      role: "dimension",
      name: "dim_customer",
      source: "sales.customers",
      key: ["customer_id"],
      attributes: ["name", "region"],
      history: "scd2",
      validTime: "valid_from",
      recordedTime: "loaded_at",
    });
    // Type 1 overwrites: no history, so no times.
    expect(byId<PlannedEntity>(plan.tables, "dim:d2")).toMatchObject({
      name: "dim_product",
      history: "none",
      validTime: null,
      recordedTime: null,
    });
  });

  it("build no table for a conformed dimension: the fact relates to the existing one", () => {
    expect(plan.tables.map((x) => x.id)).toEqual(["dim:d1", "dim:d2", "fact"]);
    const fact = byId<PlannedFact>(plan.tables, "fact");
    expect(fact.dimensions).toEqual([
      { target: { planned: "dim:d1" }, via: "customer_id" },
      { target: { planned: "dim:d2" }, via: "sku" },
      { target: { existing: "dim_date" }, via: "order_date" },
    ]);
  });

  it("build one fact_ fact at the grain with its measures", () => {
    expect(byId<PlannedFact>(plan.tables, "fact")).toMatchObject({
      primitive: "fact",
      name: "fact_order_fulfillment",
      source: "sales.order_lines",
      grain: ["order_id", "line_no"],
      measures: [
        { column: "amount", agg: "sum" },
        { column: "unit_price", agg: "avg" },
      ],
    });
  });

  it("describe each table by its role and the answers that made it, as a draft", () => {
    const names = { "dim:d1": "customer", "dim:d2": "dim_product", fact: "fact_order_fulfillment" };
    const fact = byId<PlannedFact>(plan.tables, "fact").describe((id) => names[id as "fact"]);
    expect(fact).toMatch(/^Draft: star schema fact of the Order fulfillment process/);
    expect(fact).toContain("Transaction");
    expect(fact).toContain("one row per order line");
    // The dimension's final name, as the user left it at review.
    expect(fact).toContain("customer, dim_product, dim_date");
    expect(fact).toContain("sum(amount), Additive");
    expect(fact).toContain("avg(unit_price), Non-additive");
    const dim = byId<PlannedEntity>(plan.tables, "dim:d1").describe((id) => id);
    expect(dim).toMatch(/^Draft: star schema dimension "Customer"/);
    expect(dim).toContain("Type 2: keep every version");
  });
});

describe("a Data Vault's answers", () => {
  const plan = planVault(VAULT, t);

  it("build a hub_ entity per hub, keyed on its business key, without history", () => {
    expect(byId<PlannedEntity>(plan.tables, "hub:h1")).toMatchObject({
      primitive: "entity",
      role: "hub",
      name: "hub_customer",
      source: "crm.customers",
      key: ["customer_no"],
      attributes: [],
      history: "none",
    });
  });

  it("build a link as a fact with no measures, its grain the keys, related to each hub", () => {
    expect(byId<PlannedFact>(plan.tables, "lnk:l1")).toMatchObject({
      primitive: "fact",
      role: "link",
      name: "lnk_customer_order",
      source: "shop.orders",
      grain: ["customer_no", "order_no"],
      measures: [],
      dimensions: [
        { target: { planned: "hub:h1" }, via: "customer_no" },
        { target: { planned: "hub:h2" }, via: "order_no" },
      ],
    });
  });

  it("build a link that ends an effectivity satellite keyed on its keys with valid time", () => {
    expect(byId<PlannedEntity>(plan.tables, "eff:l1")).toMatchObject({
      primitive: "entity",
      role: "effectivity",
      name: "sat_customer_order_effectivity",
      key: ["customer_no", "order_no"],
      history: "scd2",
      validTime: "assigned_at",
      recordedTime: "loaded_at",
    });
  });

  it("build a same-as link relating the hub to itself on both keys", () => {
    expect(byId<PlannedFact>(plan.tables, "sal:h1")).toMatchObject({
      role: "sameAs",
      name: "lnk_customer_same_as",
      source: "crm.customer_merges",
      grain: ["master_no", "duplicate_no"],
      measures: [],
      dimensions: [
        { target: { planned: "hub:h1" }, via: "master_no" },
        { target: { planned: "hub:h1" }, via: "duplicate_no" },
      ],
    });
  });

  it("build each satellite as an entity keyed on its hub's key, with SCD2 history", () => {
    expect(byId<PlannedEntity>(plan.tables, "sat:s1")).toMatchObject({
      role: "satellite",
      name: "sat_customer_crm",
      key: ["customer_no"],
      attributes: ["name", "email"],
      history: "scd2",
      validTime: "valid_from",
      recordedTime: "loaded_at",
    });
    // Multi-active: the key also tells apart the rows active at once.
    expect(byId<PlannedEntity>(plan.tables, "sat:s2")).toMatchObject({
      name: "sat_customer_phones",
      key: ["customer_no", "phone_type"],
    });
  });

  it("keep no satellite history when only current values are kept", () => {
    const current = planVault({ ...VAULT, historyDepth: "current" }, t);
    expect(byId<PlannedEntity>(current.tables, "sat:s1")).toMatchObject({
      history: "none",
      validTime: null,
      recordedTime: null,
    });
  });

  it("record a business vault rule in its table's description, and build no table for it", () => {
    const hub = byId<PlannedEntity>(plan.tables, "hub:h1").describe((id) => id);
    expect(hub).toMatch(/^Draft: Data Vault hub of the Customer concept/);
    expect(hub).toContain("Business vault rule: lifetime value = sum of order totals.");
    expect(plan.notCreated).toHaveLength(1);
    expect(plan.notCreated[0]).toContain("lifetime value = sum of order totals");
  });

  it("describe a satellite by its hub, sources and history", () => {
    const names: Record<string, string> = { "hub:h1": "hub_customer" };
    const sat = byId<PlannedEntity>(plan.tables, "sat:s2").describe((id) => names[id] ?? id);
    expect(sat).toBe(
      "Draft: Data Vault satellite of hub_customer: number from crm.phones, grouped by Source. " +
        "History: Every version. Multi-active on phone_type.",
    );
  });
});

describe("creating a plan", () => {
  it("registers its entities first, then its facts, by the names kept at review", async () => {
    const plan = planStar(STAR, t);
    const calls: [string, Record<string, unknown>][] = [];
    const api = {
      registerEntity: vi.fn(async (i: Record<string, unknown>) => {
        calls.push(["entity", i]);
        return { success: true, message: "" };
      }),
      registerFact: vi.fn(async (i: Record<string, unknown>) => {
        calls.push(["fact", i]);
        return { success: true, message: "" };
      }),
    };
    const names = { "dim:d1": "customer", "dim:d2": "dim_product", fact: "sales" };
    const rows = await createPlan(plan, names, api);
    expect(calls.map(([k, i]) => `${k} ${String(i.name)}`)).toEqual([
      "entity customer",
      "entity dim_product",
      "fact sales",
    ]);
    expect(calls[0][1]).toMatchObject({
      source: "sales.customers",
      domainId: "sales",
      key: ["customer_id"],
      history: "scd2",
      validTime: "valid_from",
      recordedTime: "loaded_at",
    });
    expect(String(calls[0][1].description)).toMatch(/^Draft: /);
    // A type 1 dimension sends no times.
    expect(calls[1][1]).not.toHaveProperty("validTime");
    expect(calls[2][1]).toMatchObject({
      grain: ["order_id", "line_no"],
      dimensions: [
        { entity: "customer", via: "customer_id" },
        { entity: "dim_product", via: "sku" },
        { entity: "dim_date", via: "order_date" },
      ],
    });
    expect(rows.map((r) => [r.name, r.status])).toEqual([
      ["customer", "created"],
      ["dim_product", "created"],
      ["sales", "created"],
    ]);
    expect(rows[2].relatesTo).toEqual(["customer", "dim_product", "dim_date"]);
  });

  it("stops at the first refusal, leaving what was made and reporting the rest", async () => {
    const plan = planVault(VAULT, t);
    const names = Object.fromEntries(plan.tables.map((x) => [x.id, x.name]));
    const api = {
      registerEntity: vi.fn(async (i: Record<string, unknown>) =>
        i.name === "sat_customer_order_effectivity"
          ? { success: false, message: "name taken" }
          : { success: true, message: "" },
      ),
      registerFact: vi.fn(async () => ({ success: true, message: "" })),
    };
    const rows = await createPlan(plan, names, api);
    expect(api.registerFact).not.toHaveBeenCalled();
    const status = Object.fromEntries(rows.map((r) => [r.name, r.status]));
    expect(status).toMatchObject({
      hub_customer: "created",
      hub_order: "created",
      sat_customer_order_effectivity: "failed",
      sat_customer_crm: "notReached",
      lnk_customer_order: "notReached",
    });
    expect(rows.find((r) => r.status === "failed")?.message).toBe("name taken");
  });

  it("sends a link with no measures", () => {
    const plan = planVault(VAULT, t);
    const names = Object.fromEntries(plan.tables.map((x) => [x.id, x.name]));
    const link = plan.tables.find((x) => x.id === "lnk:l1")!;
    expect(registrationInput(link, plan, names)).toMatchObject({
      name: "lnk_customer_order",
      domainId: "crm",
      grain: ["customer_no", "order_no"],
      measures: [],
      dimensions: [
        { entity: "hub_customer", via: "customer_no" },
        { entity: "hub_order", via: "order_no" },
      ],
    });
  });
});
