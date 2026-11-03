// Copyright (c) 2026 Kenneth Stott
// Canary: bbce1e58-2d31-426c-ae64-5842ceb54b1e
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-2199: what a modeling wizard's answers become in Provisa's primitives. A star schema's or a
// Data Vault's answers are planned as entities and facts (with their relationships and history
// setting), each under the method's conventional name and with a first-draft description, and the
// plan is created through the entity/fact lowering of REQ-1164 -- the API the entity/fact form
// uses. The wizard only generates: nothing here edits or regenerates a table once it is made.

import type { TFunction } from "i18next";

export type History = "none" | "scd2" | "snapshot";

/** What a generated table is in its method. */
export type Role =
  | "dimension"
  | "fact"
  | "hub"
  | "satellite"
  | "linkSatellite"
  | "effectivity"
  | "link"
  | "sameAs";

/** A fact's dimension: a table this plan generates (by its id), or one already registered. */
export type Target = { planned: string } | { existing: string };

interface PlannedBase {
  id: string;
  role: Role;
  /** The method's conventional name, offered at review for the user to keep or change. */
  name: string;
  source: string;
  /** The first-draft description, written from the final names of the plan's tables. */
  describe: (nameOf: (id: string) => string) => string;
}

export interface PlannedEntity extends PlannedBase {
  primitive: "entity";
  key: string[];
  attributes: string[];
  history: History;
  validTime: string | null;
  recordedTime: string | null;
}

export interface PlannedFact extends PlannedBase {
  primitive: "fact";
  grain: string[];
  measures: { column: string; agg: string }[];
  dimensions: { target: Target; via: string }[];
}

export type PlannedTable = PlannedEntity | PlannedFact;

export interface ModelPlan {
  domainId: string;
  tables: PlannedTable[];
  /** Answers the wizard records but does not build: each says what to build over the tables. */
  notCreated: string[];
}

/** A conventional name: the method's prefix and the user's words, lower-case and underscored. */
export function conventionalName(prefix: string, ...words: string[]): string {
  const body = words
    .map((w) =>
      w
        .trim()
        .toLowerCase()
        .replace(/[^a-z0-9]+/g, "_")
        .replace(/^_+|_+$/g, ""),
    )
    .filter(Boolean)
    .join("_");
  return `${prefix}${body}`;
}

const list = (items: string[]) => items.join(", ");

// ---------------------------------------------------------------------------------------------
// Star schema (Kimball)
// ---------------------------------------------------------------------------------------------

export type Scd = "type1" | "type2" | "snapshot";
export type FactType = "transaction" | "periodic" | "accumulating";
export type Additivity = "additive" | "semiAdditive" | "nonAdditive";

export interface StarDimension {
  uid: string;
  name: string;
  /** A conformed dimension: an existing dimension table this fact shares; null builds a new one. */
  conformed: string | null;
  source: string | null;
  key: string[];
  attributes: string[];
  scd: Scd;
  validTime: string | null;
  recordedTime: string | null;
  /** The column of the process's table that points at this dimension. */
  via: string | null;
}

export interface StarMeasure {
  column: string;
  agg: string;
  additivity: Additivity;
}

export interface StarAnswers {
  process: string;
  domainId: string;
  source: string | null;
  factType: FactType;
  grainStatement: string;
  grain: string[];
  dimensions: StarDimension[];
  measures: StarMeasure[];
}

const SCD_HISTORY: Record<Scd, History> = { type1: "none", type2: "scd2", snapshot: "snapshot" };

export function planStar(a: StarAnswers, t: TFunction): ModelPlan {
  const tables: PlannedTable[] = [];
  const dims: { target: Target; via: string; label: string }[] = [];
  for (const d of a.dimensions) {
    if (!d.via) continue;
    if (d.conformed) {
      dims.push({ target: { existing: d.conformed }, via: d.via, label: d.conformed });
      continue;
    }
    const id = `dim:${d.uid}`;
    const history = SCD_HISTORY[d.scd];
    tables.push({
      id,
      primitive: "entity",
      role: "dimension",
      name: conventionalName("dim_", d.name),
      source: d.source ?? "",
      key: d.key,
      attributes: d.attributes,
      history,
      validTime: history === "none" ? null : d.validTime,
      recordedTime: history === "none" ? null : d.recordedTime,
      describe: () =>
        t("modelingWizard.desc.dimension", {
          name: d.name,
          process: a.process,
          key: list(d.key),
          source: d.source ?? "",
          scd: t(`modelingWizard.star.scd.${d.scd}`),
        }),
    });
    dims.push({ target: { planned: id }, via: d.via, label: "" });
  }
  tables.push({
    id: "fact",
    primitive: "fact",
    role: "fact",
    name: conventionalName("fact_", a.process),
    source: a.source ?? "",
    grain: a.grain,
    measures: a.measures.map((m) => ({ column: m.column, agg: m.agg })),
    dimensions: dims.map(({ target, via }) => ({ target, via })),
    describe: (nameOf) =>
      t("modelingWizard.desc.fact", {
        factType: t(`modelingWizard.star.factType.${a.factType}`),
        process: a.process,
        grain: a.grainStatement.trim() || list(a.grain),
        dimensions:
          list(dims.map((d) => ("planned" in d.target ? nameOf(d.target.planned) : d.label))) ||
          t("modelingWizard.desc.none"),
        measures:
          list(
            a.measures.map((m) =>
              t("modelingWizard.desc.measure", {
                column: m.column,
                agg: m.agg,
                additivity: t(`modelingWizard.star.additivity.${m.additivity}`),
              }),
            ),
          ) || t("modelingWizard.desc.none"),
      }),
  });
  return { domainId: a.domainId, tables, notCreated: [] };
}

// ---------------------------------------------------------------------------------------------
// Data Vault (Linstedt)
// ---------------------------------------------------------------------------------------------

export type Grouping = "source" | "rateOfChange" | "sensitivity";
export type HistoryDepth = "full" | "current";

export interface VaultHub {
  uid: string;
  concept: string;
  /** The record source: the registered table the business key is loaded from. */
  source: string | null;
  businessKey: string[];
  /** A same-as link: a table pairing two keys that name the same instance of the concept. */
  sameAsSource: string | null;
  sameAsMaster: string | null;
  sameAsDuplicate: string | null;
}

export interface VaultLink {
  uid: string;
  name: string;
  source: string | null;
  hubs: string[];
  /** The column of the link's table holding each participating hub's business key, by hub uid. */
  via: Record<string, string>;
  /** The relationship can end: an effectivity satellite records when each one starts and ends. */
  ends: boolean;
  validTime: string | null;
  recordedTime: string | null;
}

export interface VaultSatellite {
  uid: string;
  name: string;
  /** The hub or link it describes, by uid. */
  parent: string;
  source: string | null;
  /** The column(s) of the satellite's table holding the parent's key. */
  parentKey: string[];
  attributes: string[];
  groupedBy: Grouping;
  /** Multi-active: the column(s) that tell apart several rows active at once for one key. */
  multiActive: string[];
  validTime: string | null;
  recordedTime: string | null;
}

export interface VaultRule {
  uid: string;
  /** The hub, link or satellite the rule derives from, by uid. */
  target: string;
  text: string;
}

export interface VaultAnswers {
  domainId: string;
  hubs: VaultHub[];
  links: VaultLink[];
  historyDepth: HistoryDepth;
  satellites: VaultSatellite[];
  rules: VaultRule[];
}

const hubId = (uid: string) => `hub:${uid}`;
const linkId = (uid: string) => `lnk:${uid}`;
const satId = (uid: string) => `sat:${uid}`;

export function planVault(a: VaultAnswers, t: TFunction): ModelPlan {
  const tables: PlannedTable[] = [];
  const hubs = new Map(a.hubs.map((h) => [h.uid, h]));
  const links = new Map(a.links.map((l) => [l.uid, l]));
  // A business vault rule is recorded with the table it derives from; it is built as a view.
  const rulesOf = (uid: string) =>
    a.rules
      .filter((r) => r.target === uid && r.text.trim())
      .map((r) => " " + t("modelingWizard.desc.rule", { rule: r.text.trim() }))
      .join("");
  const linkName = (l: VaultLink) =>
    l.name.trim() || l.hubs.map((h) => hubs.get(h)?.concept ?? "").join(" ");

  for (const h of a.hubs) {
    tables.push({
      id: hubId(h.uid),
      primitive: "entity",
      role: "hub",
      name: conventionalName("hub_", h.concept),
      source: h.source ?? "",
      key: h.businessKey,
      attributes: [],
      history: "none",
      validTime: null,
      recordedTime: null,
      describe: () =>
        t("modelingWizard.desc.hub", {
          concept: h.concept,
          key: list(h.businessKey),
          source: h.source ?? "",
        }) + rulesOf(h.uid),
    });
  }
  for (const h of a.hubs) {
    if (!h.sameAsSource || !h.sameAsMaster || !h.sameAsDuplicate) continue;
    const master = h.sameAsMaster;
    const duplicate = h.sameAsDuplicate;
    tables.push({
      id: `sal:${h.uid}`,
      primitive: "fact",
      role: "sameAs",
      name: conventionalName("lnk_", h.concept, "same_as"),
      source: h.sameAsSource,
      grain: [master, duplicate],
      measures: [],
      dimensions: [
        { target: { planned: hubId(h.uid) }, via: master },
        { target: { planned: hubId(h.uid) }, via: duplicate },
      ],
      describe: (nameOf) =>
        t("modelingWizard.desc.sameAs", {
          hub: nameOf(hubId(h.uid)),
          concept: h.concept,
          master,
          duplicate,
          source: h.sameAsSource ?? "",
        }),
    });
  }
  for (const l of a.links) {
    const keys = l.hubs.map((h) => l.via[h]).filter(Boolean);
    tables.push({
      id: linkId(l.uid),
      primitive: "fact",
      role: "link",
      name: conventionalName("lnk_", linkName(l)),
      source: l.source ?? "",
      grain: keys,
      measures: [],
      dimensions: l.hubs
        .filter((h) => l.via[h])
        .map((h) => ({ target: { planned: hubId(h) }, via: l.via[h] })),
      describe: (nameOf) =>
        t("modelingWizard.desc.link", {
          name: linkName(l),
          hubs: list(l.hubs.map((h) => nameOf(hubId(h)))),
          keys: list(keys),
          source: l.source ?? "",
        }) +
        (l.ends ? " " + t("modelingWizard.desc.linkEnds") : "") +
        rulesOf(l.uid),
    });
    if (l.ends) {
      tables.push({
        id: `eff:${l.uid}`,
        primitive: "entity",
        role: "effectivity",
        name: conventionalName("sat_", linkName(l), "effectivity"),
        source: l.source ?? "",
        key: keys,
        attributes: [],
        history: "scd2",
        validTime: l.validTime,
        recordedTime: l.recordedTime,
        describe: (nameOf) =>
          t("modelingWizard.desc.effectivity", {
            link: nameOf(linkId(l.uid)),
            validTime: l.validTime ?? "",
            recordedTime: l.recordedTime ?? "",
          }),
      });
    }
  }
  const history: History = a.historyDepth === "full" ? "scd2" : "none";
  for (const s of a.satellites) {
    const hub = hubs.get(s.parent);
    const link = links.get(s.parent);
    const parentId = hub ? hubId(hub.uid) : linkId(s.parent);
    const parentWords = hub ? hub.concept : link ? linkName(link) : "";
    tables.push({
      id: satId(s.uid),
      primitive: "entity",
      role: hub ? "satellite" : "linkSatellite",
      name: conventionalName("sat_", parentWords, s.name),
      source: s.source ?? "",
      key: [...s.parentKey, ...s.multiActive.filter((c) => !s.parentKey.includes(c))],
      attributes: s.attributes,
      history,
      validTime: history === "none" ? null : s.validTime,
      recordedTime: history === "none" ? null : s.recordedTime,
      describe: (nameOf) =>
        t("modelingWizard.desc.satellite", {
          parent: nameOf(parentId),
          attributes: list(s.attributes),
          source: s.source ?? "",
          grouping: t(`modelingWizard.vault.grouping.${s.groupedBy}`),
          history: t(`modelingWizard.vault.depth.${a.historyDepth}`),
        }) +
        (s.multiActive.length
          ? " " + t("modelingWizard.desc.multiActive", { columns: list(s.multiActive) })
          : "") +
        rulesOf(s.uid),
    });
  }
  const notCreated = a.rules
    .filter((r) => r.text.trim())
    .map((r) => t("modelingWizard.review.ruleNotCreated", { rule: r.text.trim() }));
  return { domainId: a.domainId, tables, notCreated };
}

// ---------------------------------------------------------------------------------------------
// Creating the plan
// ---------------------------------------------------------------------------------------------

export interface CreateApi {
  registerEntity: (
    input: Record<string, unknown>,
  ) => Promise<{ success: boolean; message: string }>;
  registerFact: (input: Record<string, unknown>) => Promise<{ success: boolean; message: string }>;
}

export interface CreatedTable {
  id: string;
  name: string;
  primitive: "entity" | "fact";
  role: Role;
  status: "created" | "failed" | "notReached";
  message?: string;
  /** The tables a created fact was related to. */
  relatesTo: string[];
}

/** The final name of each planned table: the one the user kept or typed at review. */
export type Names = Record<string, string>;

export function targetName(target: Target, names: Names): string {
  return "planned" in target ? (names[target.planned] ?? "") : target.existing;
}

/** The entity/fact input a planned table is registered with: the entity/fact form's shape. */
export function registrationInput(
  table: PlannedTable,
  plan: ModelPlan,
  names: Names,
): Record<string, unknown> {
  const name = names[table.id] ?? table.name;
  const description = table.describe((id) => names[id] ?? id);
  if (table.primitive === "entity") {
    return {
      name,
      source: table.source,
      domainId: plan.domainId,
      key: table.key,
      attributes: table.attributes,
      history: table.history,
      ...(table.history === "none"
        ? {}
        : { validTime: table.validTime, recordedTime: table.recordedTime }),
      description,
    };
  }
  return {
    name,
    source: table.source,
    domainId: plan.domainId,
    grain: table.grain,
    measures: table.measures,
    dimensions: table.dimensions.map((d) => ({ entity: targetName(d.target, names), via: d.via })),
    description,
  };
}

/**
 * Create the plan: its entities first, then the facts that relate to them. The first refusal stops
 * it; what was made stays made, and the rest is reported as not reached.
 */
export async function createPlan(
  plan: ModelPlan,
  names: Names,
  api: CreateApi,
): Promise<CreatedTable[]> {
  const ordered = [
    ...plan.tables.filter((tbl) => tbl.primitive === "entity"),
    ...plan.tables.filter((tbl) => tbl.primitive === "fact"),
  ];
  const out: CreatedTable[] = [];
  let stopped = false;
  for (const table of ordered) {
    const row: CreatedTable = {
      id: table.id,
      name: names[table.id] ?? table.name,
      primitive: table.primitive,
      role: table.role,
      status: "notReached",
      relatesTo:
        table.primitive === "fact" ? table.dimensions.map((d) => targetName(d.target, names)) : [],
    };
    out.push(row);
    if (stopped) continue;
    const input = registrationInput(table, plan, names);
    try {
      const res =
        table.primitive === "entity"
          ? await api.registerEntity(input)
          : await api.registerFact(input);
      row.status = res.success ? "created" : "failed";
      if (!res.success) row.message = res.message;
    } catch (e) {
      row.status = "failed";
      row.message = e instanceof Error ? e.message : String(e);
    }
    if (row.status === "failed") stopped = true;
  }
  return out;
}
