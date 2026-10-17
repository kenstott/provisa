// Copyright (c) 2026 Kenneth Stott
// Canary: 3532577d-ead2-4eea-b618-d00827c54331
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

/**
 * REQ-2010: a named demo's own tour, the demo's track. Its steps are written with the demo
 * (demo/named/<name>/tour.yaml) and reach the UI on /setup/status; each names a page and one object
 * of the demo, and is turned here into a step of the tour runner. Their words are
 * tour.steps.<key>, like every other step's.
 */
import type { Capability } from "../types/auth";
import type { ExampleQuery } from "../api/setup";
import type { TourStep } from "./tourSteps";
import type { TourPrerequisites } from "./tourPrerequisites";

/** REQ-2013: a demo's track needs the sources and tables its steps show. */
export function demoTourPrerequisites(defs: readonly DemoTourStepDef[]): TourPrerequisites {
  return {
    sources: defs.flatMap((d) => (d.source !== undefined ? [d.source] : [])),
    tables: defs.flatMap((d) => (d.table !== undefined ? [d.table] : [])),
  };
}

/** REQ-2012: a step asking Polly is shown only with the answer stored for it when the demo's
 * template was built; nothing is asked live, so a demo not built from a template leaves it out. */
export function demoStepShown(def: DemoTourStepDef): boolean {
  return def.polly === undefined || def.answer !== undefined;
}

/**
 * What marks a demo's track walked: the demo and a hash of its steps' keys and routes. A revised
 * track hashes differently, so a control plane that walked the old one offers the new one again.
 */
export function demoTrackStamp(name: string, defs: readonly DemoTourStepDef[]): string {
  const text = JSON.stringify(defs.map((d) => [d.key, d.route ?? null]));
  // FNV-1a, 32 bits: a short, stable fingerprint, not a secret.
  let hash = 0x811c9dc5;
  for (let i = 0; i < text.length; i++) {
    hash ^= text.charCodeAt(i);
    hash = Math.imul(hash, 0x01000193) >>> 0;
  }
  return `${name}:${hash.toString(16).padStart(8, "0")}`;
}

/** The page a demo's tour leaves the visitor on, from "Look around on my own": tour.yaml's
 * ``landing``, its route and the capability its gate needs. */
export interface DemoLanding {
  route: string;
  capability: Capability;
}

/** One step of tour.yaml, as the server sends it: its page, and exactly one target. */
export interface DemoTourStepDef {
  key: string;
  route?: string;
  capability?: Capability;
  /** A source, by id: its row on the Sources page. */
  source?: string;
  /** A registered table, as `<source id>.<table name>`: its row on the Tables page. */
  table?: string;
  /** A relationship, by alias: its row on the Relationships page. */
  relationship?: string;
  /** An example query of the demo, by name: opened in a new GraphiQL tab and run. */
  example?: string;
  /** A statement: opened in a new SQL explorer tab and run. */
  sql?: string;
  /** A REST call, as "<METHOD> <path>[?<params>]": driven in the OpenAPI page and run. */
  api?: string;
  /** A JSON:API request, its path and query: opened on the JSON:API page and run. */
  jsonapi?: string;
  /** A gRPC call, as the gRPC page takes one ("<Method>(by=[...], funcs=[...])"): sent. */
  grpc?: string;
  /** A Cypher query: run in a new frame of the Graph page. */
  cypher?: string;
  /** A property the Graph page's frame on screen is grouped by (`degTotal`): the frame the step
   *  before ran, kept, and grouped. */
  group_by?: string;
  /** A statement: its lineage built on the Lineage page, each column it reads traced back through
   *  the views it reads to the source fields. */
  lineage?: string;
  /** A glossary term, by name: opened on the Glossary page, its definition and links shown. */
  term?: string;
  /** A data product, by id: opened on the Data Products page, its members and lineage shown. */
  product?: string;
  /** A role (or "all"): shown in the SQL explorer's run-as picker, picked on Next. */
  role?: string;
  /** REQ-1163: a time, ISO 8601, set in the SQL explorer's "as of" picker: the statements after
   *  it read bitemporal views as of it. */
  as_of?: string;
  /** REQ-1163: a time, ISO 8601, set in the SQL explorer's "as recorded at" picker: the
   *  statements after it read bitemporal views as Provisa had recorded them then. */
  as_recorded?: string;
  /** An environment of the model (REQ-2053): shown in the header's environment switcher, picked
   *  on Next, so the steps after it read that environment. */
  env?: string;
  /** A question for Polly: the panel opens on it and its stored `answer`, as an exchange. */
  polly?: string;
  /** REQ-2012: Polly's answer to `polly`, stored when the demo's template was built. */
  answer?: string;
  /** A picture beside the demo's tour.yaml (a PNG), shown in the step's popover on the page the
   *  step before it is on; the server sends it as `image_src`, a data URL. */
  image?: string;
  image_src?: string;
}

/** Answers that come from the server are slow on a loaded demo; a run's result waits this long. */
const RESULT_WAIT_MS = 60000;
// The SQL explorer's own run-as picker (REQ-1013), not the app-wide role: it is what the explorer's
// statements run as.
const RUN_AS = '[data-testid="sql-role"]';
// The header's role picker: what every other page's requests run as (the GraphQL page among them).
const APP_ROLE = '[data-testid="role-selector-trigger"]';
// The header's environment switcher: what every request after it is served from.
const ENV_SWITCHER = '[data-testid="env-switcher-trigger"]';
const SQL_RESULT_ROW = '.sql-results-table tbody tr:not([data-testid="results-grid-empty"])';

export function demoTourSteps(
  defs: readonly DemoTourStepDef[],
  examples: readonly ExampleQuery[],
): TourStep[] {
  const steps: TourStep[] = [];
  let onPage: string | undefined;
  for (const def of defs) {
    onPage = def.route ?? onPage;
    steps.push(demoTourStep(def, examples, steps[steps.length - 1], onPage));
  }
  return steps;
}

function demoTourStep(
  def: DemoTourStepDef,
  examples: readonly ExampleQuery[],
  before: TourStep | undefined,
  onPage: string | undefined,
): TourStep {
  const page = {
    key: def.key,
    ...(def.route !== undefined ? { route: def.route, capability: def.capability } : {}),
  };
  if (def.source !== undefined) {
    return { ...page, element: `[data-testid="sources-row-${def.source}"]` };
  }
  if (def.table !== undefined) {
    return { ...page, element: `[data-table-row="${def.table}"]` };
  }
  if (def.relationship !== undefined) {
    return { ...page, element: `[data-tour-relationship="${def.relationship}"]` };
  }
  if (def.example !== undefined) {
    const example = examples.find((e) => e.name === def.example);
    // The server sends the demo's tour and its examples together; a step naming an example the
    // demo does not ship is a defect of the demo, which tests/unit/test_named_demo_tour.py holds.
    if (example === undefined) {
      throw new Error(`Tour: step ${def.key} names example "${def.example}", not shipped`);
    }
    return {
      ...page,
      openQuery: { stateKey: "query", text: example.query },
      element: ".graphiql-response",
      waitMs: RESULT_WAIT_MS,
    };
  }
  if (def.sql !== undefined) {
    return {
      ...page,
      openQuery: { stateKey: "sql", text: def.sql },
      // Its rows, not the table: the grid of an unrun tab is a table that says it has none.
      readySelector: SQL_RESULT_ROW,
      element: ".sql-results-table",
      waitMs: RESULT_WAIT_MS,
    };
  }
  // Each runs on its page and is awaited as the SQL statement is: by a mark the page shows only
  // once this hand-off's answer is in.
  if (def.api !== undefined) {
    return {
      ...page,
      openQuery: { stateKey: "openApiUrl", text: def.api },
      readySelector: ".openapi-page[data-answered]",
      element: ".openapi-frame",
      waitMs: RESULT_WAIT_MS,
    };
  }
  if (def.jsonapi !== undefined) {
    return {
      ...page,
      openQuery: { stateKey: "jsonapiUrl", text: def.jsonapi },
      readySelector: '[data-testid="jsonapi-summary-tab"]',
      element: ".jsonapi-panel",
      waitMs: RESULT_WAIT_MS,
    };
  }
  if (def.grpc !== undefined) {
    return {
      ...page,
      openQuery: { stateKey: "grpcMethod", text: def.grpc },
      readySelector: ".grpc-panel-right[data-answered]",
      element: ".grpc-panel-right",
      waitMs: RESULT_WAIT_MS,
    };
  }
  if (def.cypher !== undefined) {
    return {
      ...page,
      openQuery: { stateKey: "query", text: def.cypher },
      readySelector: '.graph-stream[data-handoff-status="done"]',
      element: ".graph-stream .gf-frame",
      waitMs: RESULT_WAIT_MS,
    };
  }
  if (def.group_by !== undefined) {
    return {
      ...page,
      openView: { groupBy: def.group_by },
      element: ".graph-stream .gf-frame",
    };
  }
  if (def.lineage !== undefined) {
    // The page builds a statement's graph from its deep link, read when the page opens.
    return {
      ...page,
      route: `${def.route ?? ""}?sql=${encodeURIComponent(def.lineage)}`,
      element: '[data-testid="lineage-graph"]',
      waitMs: RESULT_WAIT_MS,
    };
  }
  if (def.term !== undefined) {
    return {
      ...page,
      clickBefore: `[data-glossary-term="${def.term}"]`,
      element: '[data-testid="glossary-detail"]',
    };
  }
  if (def.product !== undefined) {
    // The page opens the product its deep link names (`?product=<id>`), read when it opens.
    return {
      ...page,
      route: `${def.route ?? ""}?product=${encodeURIComponent(def.product)}`,
      element: '[data-testid="data-product-detail"]',
    };
  }
  if (def.role !== undefined && def.role.includes(",")) {
    // A set of roles, a pseudo-role among them (reviewer,region__eu, REQ-2057): handed to the
    // SQL explorer, which offers it as its own run-as entry and picks it.
    return {
      ...page,
      openQuery: { stateKey: "role", text: def.role },
      element: RUN_AS,
    };
  }
  if (def.role !== undefined && onPage !== "/sql") {
    // Anywhere but the SQL explorer, the role every request of the app runs as: the header's.
    return {
      ...page,
      element: APP_ROLE,
      chooseOnNext: { open: APP_ROLE, option: `[data-role-option="${def.role}"]` },
    };
  }
  if (def.role !== undefined) {
    return {
      ...page,
      element: RUN_AS,
      chooseOnNext: { open: RUN_AS, option: `[data-sql-role-option="${def.role}"]` },
    };
  }
  // Each handed to the SQL explorer, which sets its picker; shown on the picker.
  if (def.as_of !== undefined) {
    return {
      ...page,
      openQuery: { stateKey: "asOf", text: def.as_of },
      element: '[data-testid="sql-as-of"]',
    };
  }
  if (def.as_recorded !== undefined) {
    return {
      ...page,
      openQuery: { stateKey: "asRecorded", text: def.as_recorded },
      element: '[data-testid="sql-as-recorded"]',
    };
  }
  if (def.env !== undefined) {
    return {
      ...page,
      element: ENV_SWITCHER,
      chooseOnNext: { open: ENV_SWITCHER, option: `[data-env-option="${def.env}"]` },
    };
  }
  if (def.polly !== undefined) {
    if (def.answer === undefined) {
      throw new Error(`Tour: step ${def.key} asks Polly with no stored answer`);
    }
    return {
      ...page,
      pollyOpen: true,
      pollyExchange: { question: def.polly, answer: def.answer },
      element: '[data-testid="chat-panel"]',
    };
  }
  if (def.image !== undefined) {
    // On the page of the step before, beside what that step showed: a picture of what the demo
    // looks like outside Provisa (an app, a BI tool) that reads it.
    if (before === undefined) {
      throw new Error(`Tour: step ${def.key} shows image "${def.image}" with no step before it`);
    }
    // The server sends the picture with the step (core/demo.py demo_tour).
    if (def.image_src === undefined) {
      throw new Error(`Tour: step ${def.key} shows image "${def.image}" the server did not send`);
    }
    return { ...page, element: before.element, image: def.image_src };
  }
  throw new Error(`Tour: step ${def.key} names no target`);
}
